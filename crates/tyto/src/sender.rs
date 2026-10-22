use serde_json::{from_str, to_string};
use tokio::spawn;
use tokio::sync::oneshot;
use uuid::Uuid;

use aves::utils::uuid::SpectaUuid;

use super::actor::{Actor, SendCompletion};
use super::messages::{ListenerType, MessagingResponse};
use super::types::{
    ConnectionTransport, MessagingConfig, MessagingCredentials, MessagingError, MessagingSenderConfig,
    NormalizedMessage, PersistencePolicy,
};

impl Actor {
    async fn prepare_send(
        &mut self,
        sender: &MessagingSenderConfig,
    ) -> Result<
        (
            SpectaUuid,
            MessagingConfig,
            MessagingCredentials,
            i64,
            PersistencePolicy,
        ),
        MessagingError,
    > {
        let (listener_type, connection_id, persistence) = sender.connection_info();
        let connection = self
            .store()
            .await?
            .get_connection(&connection_id.to_string())
            .await?
            .ok_or_else(|| MessagingError::ConnectionNotFound {
                connection_id: connection_id.to_string(),
            })?;
        let credential_id = match Uuid::parse_str(&connection.credential_id) {
            Ok(id) => SpectaUuid(id),
            Err(error) => {
                tracing::warn!(%error, connection_id = %connection.id, "Invalid messaging credential identifier");
                return Err(Self::missing_send_credential(listener_type));
            }
        };
        let credentials = self
            .credentials
            .get(&credential_id)
            .cloned()
            .ok_or_else(|| Self::missing_send_credential(listener_type))?;
        let transport = match from_str::<ConnectionTransport>(&connection.transport_settings) {
            Ok(transport) => transport,
            Err(error) => {
                tracing::warn!(%error, %connection_id, "Invalid messaging connection transport settings");
                return Err(error.into());
            }
        };
        Ok((
            connection_id,
            transport.messaging_config(),
            credentials,
            connection.retention_seconds,
            persistence.clone(),
        ))
    }

    pub(super) async fn start_send(
        &mut self,
        reply_to: oneshot::Sender<Result<MessagingResponse, MessagingError>>,
        sender: MessagingSenderConfig,
        message: NormalizedMessage,
    ) {
        let setup = match self.prepare_send(&sender).await {
            Ok(setup) => setup,
            Err(error) => {
                let _ = reply_to.send(Err(error));
                return;
            }
        };
        let (connection_id, settings, credentials, retention_seconds, persistence) = setup;
        let send_tx = self.send_tx.clone();
        let body = message.body.clone();
        let metadata = to_string(&message.metadata).ok();
        spawn(async move {
            let result = Actor::send_message(sender, message, settings, credentials).await;
            if let Err(error) = send_tx
                .send(SendCompletion {
                    reply_to,
                    connection_id,
                    body,
                    metadata,
                    retention_seconds,
                    result,
                    persistence,
                })
                .await
            {
                tracing::warn!(%error, "Could not deliver outbound send completion to actor");
            }
        });
    }

    fn missing_send_credential(listener_type: ListenerType) -> MessagingError {
        MessagingError::ChannelConnectionFailed {
            listener_type,
            message: "configured credential is not available".to_owned(),
        }
    }
}

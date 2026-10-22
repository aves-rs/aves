use std::sync::Arc;

use async_trait::async_trait;
use schemars::{Schema, schema_for};
use tokio::sync::mpsc;

use aves::utils::uuid::SpectaUuid;

use crate::channels::{InboundEvent, MessagingPlugin, MessagingSubscription, MessagingTask};
use crate::messages::ListenerType;
use crate::types::{
    MessagingConfig, MessagingCredentials, MessagingError, MessagingInteraction, MessagingSendError,
    MessagingSendErrorCode, MessagingSendReceipt, MessagingSenderConfig, MessagingSubscriptionError, NormalizedMessage,
    SlackSendMetadata,
};

use super::connection::{SlackConnection, configure};
use super::send::{edit_interactive, send, send_interactive};
use super::subscribe::{listen, run_filters};

/// Stateful Slack messaging plugin.
#[derive(Default)]
pub(crate) struct SlackPlugin {
    connection: Option<Arc<SlackConnection>>,
}

impl SlackPlugin {
    /// Return the authenticated connection or the shared outbound failure.
    fn connected(&self) -> Result<&SlackConnection, MessagingError> {
        self.connection.as_deref().ok_or_else(|| {
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidCredentials,
                message: "Slack plugin is not connected".to_owned(),
            }
            .into()
        })
    }
}

#[async_trait]
impl MessagingPlugin for SlackPlugin {
    async fn connect(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError> {
        self.connection = None;
        self.connection = Some(configure(settings, credentials)?);

        Ok(())
    }

    async fn listen(
        &mut self,
        tx: mpsc::Sender<InboundEvent>,
        interaction_tx: mpsc::Sender<MessagingInteraction>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        let connection =
            self.connection
                .as_ref()
                .cloned()
                .ok_or_else(|| MessagingSubscriptionError::Configuration {
                    listener_type: ListenerType::Slack,
                    message: "Slack plugin is not connected".to_owned(),
                })?;
        listen(connection, tx, interaction_tx).await
    }

    async fn run_filters(
        &mut self,
        subscription: MessagingSubscription,
        rx: mpsc::Receiver<NormalizedMessage>,
        tx: mpsc::Sender<NormalizedMessage>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        run_filters(subscription, rx, tx)
    }

    async fn send(
        &mut self,
        sender: &MessagingSenderConfig,
        message: &NormalizedMessage,
    ) -> Result<MessagingSendReceipt, MessagingError> {
        match sender {
            MessagingSenderConfig::Slack(sender) => send(self.connected()?, sender, message).await,
            _ => Err(unsupported_send()),
        }
    }

    async fn send_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        request_id: SpectaUuid,
        request_text: &str,
    ) -> Result<String, MessagingError> {
        let MessagingSenderConfig::Slack(sender) = sender else {
            return Err(unsupported_send());
        };
        send_interactive(self.connected()?, sender, request_id, request_text).await
    }

    async fn edit_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        provider_message_id: &str,
        request_text: &str,
        outcome_label: &str,
    ) -> Result<(), MessagingError> {
        let MessagingSenderConfig::Slack(sender) = sender else {
            return Err(unsupported_send());
        };
        edit_interactive(
            self.connected()?,
            sender,
            provider_message_id,
            request_text,
            outcome_label,
        )
        .await
    }

    fn send_metadata_schema(&self) -> Schema {
        schema_for!(SlackSendMetadata)
    }
}

fn unsupported_send() -> MessagingError {
    MessagingSendError::Terminal {
        code: MessagingSendErrorCode::Validation,
        message: "Slack provider cannot send messages from other channels".to_string(),
    }
    .into()
}

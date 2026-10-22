use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::str::FromStr;

use serde_json::{Value, from_str, to_string, to_value};
use tokio::sync::oneshot;
use tokio::task::spawn;

use aves::flow::{BoundOutputs, LifecycleCorrelation, NodeLifecycleDispatchError, NodeLifecycleResult};
use aves::utils::uuid::SpectaUuid;

use super::actor::{Actor, NodeEvent, NodePending};
use super::flow_nodes::NodeSetupError;
use super::types::{
    ConnectionTransport, MessageNodeConfig, MessagingCredentials, MessagingError, MessagingSendReceipt,
    MessagingSenderConfig, NormalizedMessage,
};

struct PreparedMessageNode {
    connection_id: SpectaUuid,
    retention_seconds: i64,
    sender: MessagingSenderConfig,
    message: NormalizedMessage,
    metadata: Option<String>,
    transport: ConnectionTransport,
    credentials: MessagingCredentials,
}

impl Actor {
    pub(super) async fn start_message_node(
        &mut self,
        correlation: LifecycleCorrelation,
        config: MessageNodeConfig,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        let prepared = match self.prepare_message_node(&config).await {
            Ok(prepared) => prepared,
            Err(error) => return Self::reply_failed(reply, error.to_string()),
        };
        let PreparedMessageNode {
            connection_id,
            retention_seconds,
            sender,
            message,
            metadata,
            transport,
            credentials,
        } = prepared;
        self.node_pending.insert(
            correlation.clone(),
            NodePending {
                connection_id,
                retention_seconds,
                body: config.body,
                metadata,
                result: None,
            },
        );
        let done = self.node_tx.clone();
        spawn(async move {
            let result = Self::send_message(sender, message, transport.messaging_config(), credentials).await;
            let _ = done
                .send(NodeEvent::MessageSendCompleted(correlation, result))
                .await;
        });
        let _ = reply.send(Ok(NodeLifecycleResult::Running));
    }

    async fn prepare_message_node(
        &mut self,
        config: &MessageNodeConfig,
    ) -> Result<PreparedMessageNode, NodeSetupError> {
        let connection = self
            .store()
            .await?
            .get_connection(&config.connection_id.to_string())
            .await
            .map_err(MessagingError::from)?
            .ok_or(NodeSetupError::UnknownConnection)?;
        let transport = from_str::<ConnectionTransport>(&connection.transport_settings)?;
        let sender =
            MessagingSenderConfig::from_destination(config.provider, config.connection_id, &config.destination);
        let credential_id = SpectaUuid::from_str(&connection.credential_id)?;
        let credentials = self
            .credentials
            .get(&credential_id)
            .cloned()
            .ok_or(NodeSetupError::CredentialUnavailable)?;
        let message = NormalizedMessage {
            body: config.body.clone(),
            metadata: config.metadata.clone(),
        };
        let metadata = Some(to_string(&config.metadata)?);
        Ok(PreparedMessageNode {
            connection_id: config.connection_id,
            retention_seconds: connection.retention_seconds,
            sender,
            message,
            metadata,
            transport,
            credentials,
        })
    }

    pub(super) async fn tick_message_node(
        &mut self,
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        let pending = match self.node_pending.entry(correlation) {
            Entry::Occupied(entry) if entry.get().result.is_some() => entry.remove(),
            Entry::Occupied(_) | Entry::Vacant(_) => {
                let _ = reply.send(Ok(NodeLifecycleResult::Running));
                return;
            }
        };
        let NodePending {
            connection_id,
            retention_seconds,
            body,
            metadata,
            result,
        } = pending;
        match result.expect("terminal pending result was checked") {
            Ok(receipt) => {
                self.finish_message_node(connection_id, retention_seconds, body, metadata, receipt, reply)
                    .await
            }
            Err(error) => {
                let _ = reply.send(Ok(NodeLifecycleResult::Failed {
                    message: error.to_string(),
                    state: Value::Null,
                }));
            }
        }
    }

    async fn finish_message_node(
        &mut self,
        connection_id: SpectaUuid,
        retention_seconds: i64,
        body: String,
        metadata: Option<String>,
        receipt: MessagingSendReceipt,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        self.persist_accepted_send(
            &connection_id,
            &body,
            metadata.as_deref(),
            &receipt,
            retention_seconds,
            [
                "Could not serialize accepted Flow message receipt",
                "Could not persist accepted Flow message; continuing delivery",
                "Could not open message store for accepted Flow message",
            ],
        )
        .await;
        let outputs = match to_value(&receipt) {
            Ok(value) => BoundOutputs::new(BTreeMap::from([("receipt".to_owned(), value)])),
            Err(error) => {
                let _ = reply.send(Ok(NodeLifecycleResult::Failed {
                    message: error.to_string(),
                    state: Value::Null,
                }));
                return;
            }
        };
        let _ = reply.send(Ok(NodeLifecycleResult::Succeeded {
            outputs,
            state: Value::Null,
        }));
    }

    pub(super) fn cancel_message_node(
        &mut self,
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        self.node_pending.remove(&correlation);
        let _ = reply.send(Ok(NodeLifecycleResult::Cancelled));
    }
}

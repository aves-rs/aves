use serde_json::Value;
use thiserror::Error;
use tokio::sync::oneshot;

use aves::flow::{NodeLifecycleDispatchError, NodeLifecycleResult};

use super::actor::Actor;
use super::database::StoreError;
use super::registrations::NodeCommand;
use super::types::MessagingError;

impl Actor {
    /// Routes typed lifecycle commands to the operation-specific methods.
    pub(super) async fn handle_node_command(&mut self, command: NodeCommand) {
        match command {
            NodeCommand::MessageStart {
                correlation,
                config,
                reply,
            } => self.start_message_node(correlation, config, reply).await,
            NodeCommand::MessageTick { correlation, reply } => self.tick_message_node(correlation, reply).await,
            NodeCommand::MessageCancel { correlation, reply } => self.cancel_message_node(correlation, reply),
            NodeCommand::PermissionStart {
                correlation,
                config,
                reply,
            } => self.start_permission_node(correlation, config, reply).await,
            NodeCommand::PermissionTick { correlation, reply } => self.tick_permission_node(correlation, reply).await,
            NodeCommand::PermissionCancel { correlation, reply } => {
                self.cancel_permission_node(correlation, reply).await
            }
        }
    }

    pub(super) fn reply_failed(
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
        message: impl Into<String>,
    ) {
        let _ = reply.send(Ok(NodeLifecycleResult::Failed {
            message: message.into(),
            state: Value::Null,
        }));
    }
}

#[derive(Debug, Error)]
pub(super) enum NodeSetupError {
    #[error(transparent)]
    Messaging(#[from] MessagingError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("unknown connection")]
    UnknownConnection,
    #[error("credential unavailable")]
    CredentialUnavailable,
    #[error("invalid messaging provider type")]
    InvalidProvider,
    #[error("provider does not match connection")]
    ProviderMismatch,
    #[error("PermissionNode does not support Email")]
    EmailNotSupported,
}

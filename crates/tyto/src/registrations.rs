use std::{io::Error as IoError, sync::Arc};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use aves::flow::{
    CancellationDisposition, FlowRegistrationError, LifecycleCorrelation, NodeKind, NodeKindDescriptor,
    NodeKindRegistration, NodeLifecycleContext, NodeLifecycleDispatchError, NodeLifecycleResult, SideEffectDisposition,
    WorkspacePreparation,
};

use super::actor::NodeEvent;
use super::messages::ListenerType;
use super::types::{MessageNodeConfig, PermissionNodeConfig};

/// Typed lifecycle requests routed to the Tyto actor.
pub(super) enum NodeCommand {
    MessageStart {
        correlation: LifecycleCorrelation,
        config: MessageNodeConfig,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
    MessageTick {
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
    MessageCancel {
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
    PermissionStart {
        correlation: LifecycleCorrelation,
        config: PermissionNodeConfig,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
    PermissionTick {
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
    PermissionCancel {
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    },
}

/// Owner-state handle shared by both Tyto Flow registrations.
pub(super) struct TytoNodeHandle {
    pub(super) tx: mpsc::Sender<NodeEvent>,
}

impl TytoNodeHandle {
    /// Sends one typed actor command and returns its lifecycle reply.
    async fn request(
        context: &NodeLifecycleContext,
        command: impl FnOnce(
            LifecycleCorrelation,
            oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
        ) -> NodeCommand,
    ) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
        let owner = Self::owner_state(context)?;
        let (reply, response) = oneshot::channel();
        owner
            .tx
            .send(NodeEvent::Command(command(context.correlation.clone(), reply)))
            .await
            .map_err(|error| NodeLifecycleDispatchError::Send(Box::new(error)))?;
        response
            .await
            .map_err(|_| NodeLifecycleDispatchError::ReplyDropped)?
    }

    fn owner_state(context: &NodeLifecycleContext) -> Result<&Self, NodeLifecycleDispatchError> {
        context
            .owner_state
            .as_ref()
            .and_then(|state| state.downcast_ref::<Self>())
            .ok_or_else(|| {
                NodeLifecycleDispatchError::Owner(Box::new(IoError::other("Tyto node owner state unavailable")))
            })
    }
}

pub(super) fn message_node_registration(handle: Arc<TytoNodeHandle>) -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("MessageNode").expect("static node kind is valid"),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    NodeKindRegistration::typed::<MessageNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.connection_id.0.is_nil() {
                return Err(FlowRegistrationError::Configuration {
                    message: "connection_id must not be nil".to_owned(),
                });
            }
            Ok(())
        },
        move |context, mut config| async move {
            if let Some(body) = context.inputs.get("body").and_then(Value::as_str) {
                config.body = body.to_owned();
            }
            if let Some(metadata) = context.inputs.get("metadata") {
                config.metadata = metadata.clone();
            }
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::MessageStart {
                correlation,
                config,
                reply,
            })
            .await
        },
        move |context, _configuration| async move {
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::MessageTick {
                correlation,
                reply,
            })
            .await
        },
        move |context, _configuration| async move {
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::MessageCancel {
                correlation,
                reply,
            })
            .await
        },
        Some(handle),
        WorkspacePreparation::new(),
    )
}

pub(super) fn permission_node_registration(handle: Arc<TytoNodeHandle>) -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("PermissionNode").expect("static node kind is valid"),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    NodeKindRegistration::typed::<PermissionNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.connection_id.0.is_nil() {
                return Err(FlowRegistrationError::Configuration {
                    message: "connection_id must not be nil".to_owned(),
                });
            }
            if configuration.provider == ListenerType::Email {
                return Err(FlowRegistrationError::Configuration {
                    message: "PermissionNode does not support Email".to_owned(),
                });
            }
            if configuration.duration_seconds == 0 {
                return Err(FlowRegistrationError::Configuration {
                    message: "duration_seconds must be positive".to_owned(),
                });
            }
            Ok(())
        },
        move |context, config| async move {
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::PermissionStart {
                correlation,
                config,
                reply,
            })
            .await
        },
        move |context, _configuration| async move {
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::PermissionTick {
                correlation,
                reply,
            })
            .await
        },
        move |context, _configuration| async move {
            TytoNodeHandle::request(&context, |correlation, reply| NodeCommand::PermissionCancel {
                correlation,
                reply,
            })
            .await
        },
        Some(handle),
        WorkspacePreparation::new(),
    )
}

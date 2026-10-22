use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::auth::{
    AuthCapability, AuthSudoService, Jwt,
    messages::{IssueJwtInput, RevokeJtiInput},
};
use crate::bus::BusRpcContext;
use crate::runtime::ServiceReader;

use super::blackboard::{BoundInputs, BoundOutputs};
use super::mcp::FlowMcpPorts;
use super::messages::{ErasedLifecycleMessage, LifecycleCorrelation, LifecycleStart, LifecycleTick};
use super::registration::{
    FlowNodeRegistry, NodeKind, NodeKindRegistration, NodeLifecycleContext, NodeLifecycleDispatchError,
    NodeLifecycleResult,
};
use super::types::FlowAdapterError;

const NODE_TOKEN_LIFETIME: Duration = Duration::seconds(60 * 60 * 10); // 10 hours

/// Completion returned by an owner lifecycle service.
pub enum OwnerLifecycleResult {
    /// Work was accepted and remains pending.
    Running {
        correlation: LifecycleCorrelation,
        token_jti: Option<Uuid>,
    },
    /// Work completed and supplied its configured output patch.
    Succeeded {
        correlation: LifecycleCorrelation,
        outputs: BoundOutputs,
        state: Value,
    },
    /// Work completed with an owner failure.
    Failed {
        correlation: LifecycleCorrelation,
        message: String,
        state: Value,
    },
}

#[derive(Clone)]
pub(super) struct HostLifecycleAdapter {
    pub(super) auth_service: ServiceReader<dyn AuthSudoService>,
    pub(super) bus_context: BusRpcContext,
    pub(super) mcp_ports: FlowMcpPorts,
}

impl HostLifecycleAdapter {
    fn service<Service: ?Sized + Send + Sync + 'static>(
        reader: &ServiceReader<Service>,
        unavailable: FlowAdapterError,
    ) -> Result<Arc<Service>, FlowAdapterError> {
        reader.get().ok_or(unavailable)
    }

    /// Checks authorization before the selected owner, preserving availability error precedence.
    pub(super) fn ensure_available(
        &self,
        registry: &FlowNodeRegistry,
        kind: &NodeKind,
    ) -> Result<(), FlowAdapterError> {
        Self::service(&self.auth_service, FlowAdapterError::AuthorizationUnavailable)?;
        registry
            .get(kind)
            .map(|_| ())
            .map_err(FlowAdapterError::from)
    }

    fn context(&self, message: &ErasedLifecycleMessage, inputs: BoundInputs) -> NodeLifecycleContext {
        NodeLifecycleContext {
            correlation: message.correlation.clone(),
            execution_root: message.execution_root.clone(),
            node: message.node.clone(),
            inputs,
            node_token: message.node_token.clone(),
            mcp_ports: self.mcp_ports.clone(),
            bus_context: self.bus_context.clone(),
            owner_state: None,
        }
    }

    async fn issue_token(&self, correlation: LifecycleCorrelation) -> Result<(Jwt, Uuid), FlowAdapterError> {
        let service = Self::service(&self.auth_service, FlowAdapterError::AuthorizationUnavailable)?;
        service
            .issue_jwt(IssueJwtInput {
                sub: format!("flow-node:{}", correlation.node_execution_id),
                cap: vec![AuthCapability::AllBus],
                exp: (Utc::now() + NODE_TOKEN_LIFETIME).to_rfc3339(),
            })
            .await
            .map(|output| (output.token, output.jti))
            .map_err(FlowAdapterError::Authorization)
    }

    pub(super) async fn revoke_token(&self, jti: Uuid) -> Result<(), FlowAdapterError> {
        let service = Self::service(&self.auth_service, FlowAdapterError::AuthorizationUnavailable)?;
        service
            .revoke_jti(RevokeJtiInput { jti })
            .await
            .map(|_| ())
            .map_err(FlowAdapterError::Authorization)
    }
}

/// Host-facing bridge for owner-declared typed lifecycle messages.
#[async_trait]
pub(super) trait FlowLifecycleAdapter: Send + Sync {
    /// Dispatches a registered owner's typed start message.
    async fn start(
        &self,
        registration: &NodeKindRegistration,
        message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError>;

    /// Dispatches a registered owner's typed tick message.
    async fn tick(
        &self,
        registration: &NodeKindRegistration,
        message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError>;

    /// Dispatches a registered owner's typed cancel message.
    async fn cancel(
        &self,
        registration: &NodeKindRegistration,
        message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError>;
}

#[async_trait]
impl FlowLifecycleAdapter for HostLifecycleAdapter {
    async fn start(
        &self,
        registration: &NodeKindRegistration,
        mut message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError> {
        let correlation = message.correlation.clone();
        let execution_root = message.execution_root.clone();
        let node = message.node.clone();

        let inputs = message
            .payload
            .downcast::<LifecycleStart<BoundInputs>>()
            .map_err(|_| FlowAdapterError::PayloadTypeMismatch {
                kind: node.node_kind.clone(),
            })?
            .payload;
        let (token, token_jti) = self.issue_token(correlation.clone()).await?;
        message.node_token = Some(token);

        let config = registration
            .decode_configuration(message.node.configuration.clone())
            .map_err(FlowAdapterError::Registration)?;
        let result = registration
            .dispatch_start(
                NodeLifecycleContext {
                    correlation: correlation.clone(),
                    execution_root,
                    node,
                    inputs,
                    node_token: message.node_token.clone(),
                    mcp_ports: self.mcp_ports.clone(),
                    bus_context: self.bus_context.clone(),
                    owner_state: None,
                },
                config,
            )
            .await;

        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.revoke_token(token_jti).await?;
                return Err(map_dispatch_error(registration.descriptor.kind.clone(), error));
            }
        };

        Ok(map_result(correlation, result, Some(token_jti))?)
    }

    async fn tick(
        &self,
        registration: &NodeKindRegistration,
        message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError> {
        let correlation = message.correlation.clone();
        let execution_root = message.execution_root.clone();
        let node = message.node.clone();
        let node_token = message.node_token.clone();

        let inputs = message
            .payload
            .downcast::<LifecycleTick<BoundInputs>>()
            .map_err(|_| FlowAdapterError::PayloadTypeMismatch {
                kind: node.node_kind.clone(),
            })?
            .payload;
        let config = registration
            .decode_configuration(message.node.configuration.clone())
            .map_err(FlowAdapterError::Registration)?;
        let result = registration
            .dispatch_tick(
                NodeLifecycleContext {
                    correlation: correlation.clone(),
                    execution_root,
                    node,
                    inputs,
                    node_token,
                    mcp_ports: self.mcp_ports.clone(),
                    bus_context: self.bus_context.clone(),
                    owner_state: None,
                },
                config,
            )
            .await
            .map_err(|error| map_dispatch_error(registration.descriptor.kind.clone(), error))?;

        map_result(correlation, result, None)
    }

    async fn cancel(
        &self,
        registration: &NodeKindRegistration,
        message: ErasedLifecycleMessage,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError> {
        let config = registration
            .decode_configuration(message.node.configuration.clone())
            .map_err(FlowAdapterError::Registration)?;
        let correlation = message.correlation.clone();

        let result = registration
            .dispatch_cancel(self.context(&message, BoundInputs::default()), config)
            .await
            .map_err(|error| map_dispatch_error(registration.descriptor.kind.clone(), error))?;

        map_result(correlation, result, None)
    }
}

fn map_dispatch_error(kind: NodeKind, error: NodeLifecycleDispatchError) -> FlowAdapterError {
    match error {
        NodeLifecycleDispatchError::ConfigurationTypeMismatch => {
            FlowAdapterError::DispatchConfigurationMismatch { kind }
        }
        NodeLifecycleDispatchError::Send(source) => FlowAdapterError::DispatchSend { kind, source },
        NodeLifecycleDispatchError::ReplyDropped => FlowAdapterError::DispatchReplyDropped { kind },
        NodeLifecycleDispatchError::Owner(source) => FlowAdapterError::Owner { kind, source },
    }
}

fn map_result(
    correlation: LifecycleCorrelation,
    result: NodeLifecycleResult,
    token_jti: Option<Uuid>,
) -> Result<OwnerLifecycleResult, FlowAdapterError> {
    Ok(match result {
        NodeLifecycleResult::Running => OwnerLifecycleResult::Running { correlation, token_jti },
        NodeLifecycleResult::Succeeded { outputs, state } => OwnerLifecycleResult::Succeeded {
            correlation,
            outputs,
            state,
        },
        NodeLifecycleResult::Failed { message, state } => OwnerLifecycleResult::Failed {
            correlation,
            message,
            state,
        },
        NodeLifecycleResult::Cancelled => return Err(FlowAdapterError::Cancelled),
    })
}

/// Validates correlation before delegating lifecycle work to the host adapter.
pub(super) async fn dispatch_start<A: FlowLifecycleAdapter + ?Sized>(
    registry: &FlowNodeRegistry,
    adapter: &A,
    message: ErasedLifecycleMessage,
) -> Result<OwnerLifecycleResult, FlowAdapterError> {
    let registration = registry
        .get(&message.correlation.node_kind)
        .map_err(FlowAdapterError::from)?;
    adapter.start(registration, message).await
}

/// Validates correlation before delegating a tick to the host adapter.
pub(super) async fn dispatch_tick<A: FlowLifecycleAdapter + ?Sized>(
    registry: &FlowNodeRegistry,
    adapter: &A,
    message: ErasedLifecycleMessage,
) -> Result<OwnerLifecycleResult, FlowAdapterError> {
    let registration = registry
        .get(&message.correlation.node_kind)
        .map_err(FlowAdapterError::from)?;
    adapter.tick(registration, message).await
}

/// Routes cancellation of one node invocation to its registered owner.
pub(super) async fn dispatch_cancel<A: FlowLifecycleAdapter + ?Sized>(
    registry: &FlowNodeRegistry,
    adapter: &A,
    message: ErasedLifecycleMessage,
) -> Result<OwnerLifecycleResult, FlowAdapterError> {
    let registration = registry
        .get(&message.correlation.node_kind)
        .map_err(FlowAdapterError::from)?;
    adapter.cancel(registration, message).await
}

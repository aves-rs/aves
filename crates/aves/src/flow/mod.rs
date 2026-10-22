//! Aves-owned Flow runtime domain.

mod actor;
mod adapter;
mod ast;
mod blackboard;
mod catalog;
mod checkpoints;
mod controllers;
mod cron;
mod database;
mod domain;
mod files;
mod graph;
mod handle;
mod mailbox;
mod mcp;
mod messages;
mod mobility;
mod registration;
mod replication;
mod snapshot;
mod storage;
mod sync;
mod tick;
mod triggers;
mod types;
mod workspace;

#[cfg(test)]
mod fixtures;

pub(crate) use ast::resolve_script_limits;
pub(crate) use files::checked_relative;
pub(crate) use mobility::FlowMobilityService;
pub(crate) use sync::{FlowNativeServices, FlowSyncServices};

pub use adapter::OwnerLifecycleResult;
pub use ast::{
    AgentNodeConfig, BlackboardBinding, CapabilityRequest, ContentAddressedPath, ContentAddressedRoot, ContentHash,
    FlowNode, NodeConstraints, NodeDataSpace, OrderNodeConfig, RuntimeConstraints, ScriptNodeConfig,
};
pub use blackboard::{BoundInputs, BoundOutputs};
pub use cron::{CronExpression, CronTriggerConfig, FlowTriggerRecord};
pub use domain::{FlowControlService, FlowDomain, FlowTriggerSourceService};
pub use graph::{FlowGraphAst, GraphNode};
pub use messages::{
    CreateFlowInput, DeleteFlowInput, DeleteFlowTriggerInput, ErasedLifecycleMessage, FireSourceTriggerInput,
    FlowBroadcast, FlowTriggerDeletedOutput, FlowTriggersListedOutput, FlowsListedOutput, GetExecutionDetailInput,
    GetFlowInput, LifecycleCorrelation, LifecycleError, LifecycleStart, LifecycleSuccess, LifecycleTick,
    ListActiveExecutionsInput, ListFlowTriggersInput, ListFlowsInput, ListSourceTriggersInput,
    ListTerminalExecutionsInput, SetEnabledInput, SourceTriggersListedOutput, StartInput, StartedOutput,
    UpdateFlowInput, dispatch_create_flow, dispatch_delete_flow, dispatch_delete_flow_trigger,
    dispatch_get_execution_detail, dispatch_get_flow, dispatch_list_active_executions, dispatch_list_flow_triggers,
    dispatch_list_flows, dispatch_list_terminal_executions, dispatch_set_enabled, dispatch_start, dispatch_update_flow,
    flow_operation_descriptors_with_root_error, rpc_router,
};
pub use mobility::{
    FlowActivationAcknowledgement, FlowAuthorityState, FlowMobilityCommitAcknowledgement, FlowMobilitySnapshot,
    FlowYieldAcknowledgement,
};
pub use registration::{
    CancellationDisposition, NodeKind, NodeKindDescriptor, NodeKindRegistration, NodeLifecycleContext,
    NodeLifecycleDispatchError, NodeLifecycleResult, NodeRegistrationSupply, SideEffectDisposition,
    WorkspacePreparation, WorkspacePreparationError, WorkspaceSeed,
};
pub use snapshot::{NodeExecutionSnapshot, NodeExecutionStatus};
pub use triggers::{FlowStartRequest, TriggerAdapterRegistration, TriggerRegistry};
pub use types::{
    BlackboardError, ControllerError, FlowAdapterError, FlowBootOptions, FlowCronError, FlowDefinition, FlowError,
    FlowExecutionDetail, FlowExecutionId, FlowExecutionSnapshot, FlowExecutionStatus, FlowGraphLoadError, FlowId,
    FlowInvocationKind, FlowMobilityDestination, FlowNodeId, FlowRegistrationError, FlowSettings, FlowTimestamp,
    FlowWorkspaceMapping, FlowWorkspaceMappingMode, FlowWorkspaceSourceError, FlowWorkspaceSourceService,
    NodeExecutionId, ScriptSourceLocation, SnapshotError, TickError, TriggerRegistrationError, WorkspaceError,
};

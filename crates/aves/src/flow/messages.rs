use std::any::Any;
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::auth::{Jwt, MachineIssuerId};
use crate::bus::BusRpcContext;

use super::FlowDomain;
use super::ast::{ContentAddressedPath, ContentAddressedRoot, FlowNode};
use super::cron::FlowTriggerRecord;
use super::mobility::{
    FlowActivationAcknowledgement, FlowMobilityCommitAcknowledgement, FlowMobilitySnapshot, FlowYieldAcknowledgement,
};
use super::registration::NodeKind;
use super::types::{
    FlowDefinition, FlowError, FlowExecutionDetail, FlowExecutionId, FlowExecutionSnapshot, FlowFailure, FlowId,
    FlowInvocationKind, FlowMobilityDestination, FlowMobilityReadiness, FlowNodeId, NodeExecutionId,
    ScriptSourceLocation,
};

/// Correlation carried by every node-owner lifecycle message.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, Type, JsonSchema)]
pub struct LifecycleCorrelation {
    /// Owner kind responsible for this node invocation.
    pub node_kind: NodeKind,
    /// Persisted Flow definition identity.
    pub flow_id: FlowId,
    /// Persisted graph-node identity.
    pub flow_node_id: FlowNodeId,
    /// Identity of this execution of the Flow.
    pub flow_execution_id: FlowExecutionId,
    /// Identity of this execution of the node.
    pub node_execution_id: NodeExecutionId,
}

/// Typed envelope used by a host-owned start message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LifecycleStart<T> {
    /// Durable graph and execution correlation.
    pub correlation: LifecycleCorrelation,
    /// Owner-specific start configuration.
    pub payload: T,
}

/// Typed envelope used by a host-owned tick message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LifecycleTick<T> {
    /// Durable graph and execution correlation.
    pub correlation: LifecycleCorrelation,
    /// Owner-specific tick input.
    pub payload: T,
}

/// Typed envelope returned by a host-owned successful lifecycle message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LifecycleSuccess<T> {
    /// Durable graph and execution correlation.
    pub correlation: LifecycleCorrelation,
    /// Owner-specific terminal result.
    pub payload: T,
}

/// Typed envelope returned by a host-owned failed lifecycle message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LifecycleError<T> {
    /// Durable graph and execution correlation.
    pub correlation: LifecycleCorrelation,
    /// Owner-specific terminal error.
    pub payload: T,
}

/// Erased payload used only at the generic adapter boundary.
pub type ErasedLifecyclePayload = Box<dyn Any + Send + Sync>;

/// Correlated lifecycle message after its owner-specific payload is erased.
pub struct ErasedLifecycleMessage {
    /// Durable graph and execution correlation.
    pub correlation: LifecycleCorrelation,
    /// Complete persisted leaf configuration selected by the graph.
    pub node: FlowNode,
    /// Explicit execution directory below the configured Flow data root.
    pub execution_root: PathBuf,
    /// Fresh node-scoped token, present only for a start dispatch.
    pub node_token: Option<Jwt>,
    /// Owner-specific typed message, erased at the generic boundary.
    pub payload: ErasedLifecyclePayload,
}

/// Internal request wrapper routed through the Flow domain actor.
pub(crate) struct FlowRequest {
    pub payload: FlowRequestPayload,
    pub reply_to: oneshot::Sender<Result<FlowResponse, FlowError>>,
}

/// Flow control requests. Node-owner lifecycle messages are declared by their
/// owning domains and are intentionally not part of this generic domain.
#[aves::bus_requests(
    marker = FlowDomain,
    context = BusRpcContext,
    root_error = FlowError,
    domain_error = FlowError
)]
#[derive(Clone, Debug, Deserialize, Serialize, Type, JsonSchema)]
pub enum FlowRequestPayload {
    #[privileged(reason = "Flow runtime lifecycle is owned by Aves")]
    StartRuntime,

    #[privileged(reason = "Flow runtime lifecycle is owned by Aves")]
    StopRuntime,

    /// Creates a new immutable Flow definition.
    #[operation(name = "createFlow", kind = "mutation", response = FlowCreatedOutput, response_variant = "FlowCreated")]
    CreateFlow { flow: FlowDefinition },

    /// Downloads and imports an explicit native Collection-format blob ticket.
    /// Creates or replaces a persisted adapter-backed trigger.
    #[operation(name = "createFlowTrigger", kind = "mutation", response = FlowTriggerCreatedOutput, response_variant = "FlowTriggerCreated")]
    CreateFlowTrigger { trigger: FlowTriggerRecord },

    /// Adds a typed node to a Flow graph revision.
    #[operation(name = "createFlowNode", kind = "mutation", response = FlowNodeCreatedOutput, response_variant = "FlowNodeCreated")]
    CreateFlowNode { flow_id: FlowId, node: FlowNode },

    /// Lists Flow definitions.
    #[operation(name = "listFlows", kind = "query", response = FlowsListedOutput, response_variant = "FlowsListed")]
    ListFlows,

    /// Lists persisted Cron triggers for one Flow.
    #[operation(name = "listFlowTriggers", kind = "query", response = FlowTriggersListedOutput, response_variant = "FlowTriggersListed")]
    ListFlowTriggers { flow_id: FlowId },

    /// Project locally authoritative enabled triggers to their source domain.
    #[privileged(reason = "Source-domain runtime reconciliation")]
    ListSourceTriggers { adapter_kind: String },

    /// Lists active execution snapshots.
    #[operation(name = "listActiveExecutions", kind = "query", response = ActiveExecutionsListedOutput, response_variant = "ActiveExecutionsListed")]
    ListActiveExecutions,

    /// Lists terminal execution snapshots.
    #[operation(name = "listTerminalExecutions", kind = "query", response = TerminalExecutionsListedOutput, response_variant = "TerminalExecutionsListed")]
    ListTerminalExecutions,

    /// Returns one Flow definition.
    #[operation(name = "getFlow", kind = "query", response = FlowRetrievedOutput, response_variant = "FlowRetrieved")]
    GetFlow { flow_id: FlowId },

    /// Returns the exact saved graph revision and execution detail.
    #[operation(name = "getExecutionDetail", kind = "query", response = ExecutionDetailRetrievedOutput, response_variant = "ExecutionDetailRetrieved")]
    GetExecutionDetail { execution_id: FlowExecutionId },

    /// Reads one UTF-8 Script source file from a mapped or stored location.
    #[operation(name = "readScriptSource", kind = "query", response = ScriptSourceReadOutput, response_variant = "ScriptSourceRead")]
    ReadScriptSource {
        location: ScriptSourceLocation,
        path: String,
    },

    /// Starts a Flow execution through its public runtime operation.
    #[operation(name = "start", kind = "mutation", response = StartedOutput, response_variant = "Started")]
    Start {
        flow_id: FlowId,
        pulse: Value,
        automatic: bool,
        #[serde(default)]
        invocation_kind: FlowInvocationKind,
    },

    /// Fire a persisted event trigger through Flow's authoritative admission.
    #[privileged(reason = "Source-domain event admission")]
    FireSourceTrigger {
        #[specta(type = String)]
        #[schemars(with = "String")]
        trigger_id: uuid::Uuid,
        pulse: Value,
    },

    /// Aborts an active Flow execution through its public runtime operation.
    #[operation(name = "abort", kind = "mutation", response = AbortedOutput, response_variant = "Aborted")]
    Abort { execution_id: FlowExecutionId },

    /// Imports one explicit host-local file or directory into Flow's private content store.
    /// Files are rooted at their basename; directory entries are rooted relative to the directory.
    #[operation(name = "importWorkspace", kind = "mutation", response = WorkspaceImportedOutput, response_variant = "WorkspaceImported")]
    ImportWorkspace { source_path: String },

    #[operation(name = "downloadWorkspaceBlob", kind = "mutation", response = WorkspaceImportedOutput, response_variant = "WorkspaceImported")]
    DownloadWorkspaceBlob { ticket: String },

    /// Enables a validated Flow or pauses future automatic starts without changing its graph.
    #[operation(name = "setEnabled", kind = "mutation", response = FlowUpdatedOutput, response_variant = "FlowUpdated")]
    SetEnabled { flow_id: FlowId, enabled: bool },

    /// Updates a Flow definition by creating its next revision.
    #[operation(name = "updateFlow", kind = "mutation", response = FlowUpdatedOutput, response_variant = "FlowUpdated")]
    UpdateFlow { flow: FlowDefinition },

    /// Updates a persisted trigger.
    #[operation(name = "updateFlowTrigger", kind = "mutation", response = FlowTriggerUpdatedOutput, response_variant = "FlowTriggerUpdated")]
    UpdateFlowTrigger { trigger: FlowTriggerRecord },

    /// Replaces a typed node in a Flow graph revision.
    #[operation(name = "updateFlowNode", kind = "mutation", response = FlowNodeUpdatedOutput, response_variant = "FlowNodeUpdated")]
    UpdateFlowNode { flow_id: FlowId, node: FlowNode },

    /// Saves one UTF-8 Script source file to a mapped directory or content-addressed root.
    #[operation(name = "saveScriptSource", kind = "mutation", response = ScriptSourceSavedOutput, response_variant = "ScriptSourceSaved")]
    SaveScriptSource {
        location: ScriptSourceLocation,
        path: String,
        source: String,
    },

    /// Updates the plain completed-run limit through the existing Flow control
    /// service and immediately prunes eligible terminals, including while idle.
    #[privileged(reason = "Flow retention is runtime-owned")]
    UpdateRetention { retention_completed_runs: u32 },

    /// Prevents new starts for a Flow.
    #[operation(name = "deleteFlow", kind = "mutation", response = FlowDeletedOutput, response_variant = "FlowDeleted")]
    DeleteFlow { flow_id: FlowId },

    /// Removes a trigger.
    #[operation(name = "deleteFlowTrigger", kind = "mutation", response = FlowTriggerDeletedOutput, response_variant = "FlowTriggerDeleted")]
    DeleteFlowTrigger {
        #[specta(type = String)]
        #[schemars(with = "String")]
        trigger_id: Uuid,
    },

    /// Removes a typed node from a Flow graph revision.
    #[operation(name = "deleteFlowNode", kind = "mutation", response = FlowNodeDeletedOutput, response_variant = "FlowNodeDeleted")]
    DeleteFlowNode { flow_id: FlowId, node_id: FlowNodeId },

    /// Selects exactly one already-confirmed peer destination for a Flow synchronization.
    #[operation(name = "selectMobilityDestination", kind = "mutation", response = MobilityDestinationSelectedOutput, response_variant = "MobilityDestinationSelected")]
    SelectMobilityDestination {
        flow_id: FlowId,
        destinations: Vec<FlowMobilityDestination>,
    },

    /// Reads durable selected-Flow replica readiness without changing authority.
    #[operation(name = "getFlowMobilityReadiness", kind = "query", response = FlowMobilityReadinessOutput, response_variant = "FlowMobilityReadiness")]
    GetFlowMobilityReadiness { flow_id: FlowId },

    /// Stages a peer-captured selected-Flow transfer unit without activating it.
    ///
    /// The authenticated remote-operation surface owns peer authorization at
    /// this boundary; the snapshot remains a typed generated operation input.
    #[operation(name = "importMobilitySnapshot", kind = "mutation", response = FlowMobilityCommitAcknowledgementOutput, response_variant = "FlowMobilityCommitAcknowledgement")]
    ImportMobilitySnapshot {
        snapshot: FlowMobilitySnapshot,
        source_peer_key: MachineIssuerId,
    },

    /// Captures the selected Flow's durable transfer unit and begins synchronization.
    #[operation(name = "synchronizeFlow", kind = "mutation", response = FlowMobilityReadinessOutput, response_variant = "FlowMobilityReadiness")]
    SynchronizeFlow { flow_id: FlowId },

    /// Durably fences the current authority before its acknowledgement is handed to a replica.
    #[operation(name = "yieldFlowAuthority", kind = "mutation", response = FlowAuthorityYieldedOutput, response_variant = "FlowAuthorityYielded")]
    YieldFlowAuthority { flow_id: FlowId },

    /// Promotes a fully committed inactive replica only with a source yield acknowledgement.
    #[operation(name = "promoteFlowReplica", kind = "mutation", response = FlowReplicaPromotedOutput, response_variant = "FlowReplicaPromoted")]
    PromoteFlowReplica {
        flow_id: FlowId,
        acknowledgement: FlowYieldAcknowledgement,
    },

    /// Removes a yielded source's transferred Flow state after destination activation.
    #[operation(name = "finalizeFlowTransfer", kind = "mutation", response = FlowTransferFinalizedOutput, response_variant = "FlowTransferFinalized")]
    FinalizeFlowTransfer { flow_id: FlowId },
}

/// Responses produced by the Flow actor, in lifecycle and operation order.
#[aves::bus_responses]
#[derive(Clone, Debug, Deserialize, Serialize, Type, JsonSchema)]
pub enum FlowResponse {
    /// Locally authoritative enabled triggers, empty while Flow is stopped.
    SourceTriggersListed {
        triggers: Vec<FlowTriggerRecord>,
    },
    RuntimeStarted,
    RuntimeStopped,
    /// Created Flow definition.
    FlowCreated {
        flow: FlowDefinition,
    },
    /// Immutable root returned by a local workspace import.
    WorkspaceImported {
        root: ContentAddressedRoot,
    },
    ScriptSourceRead {
        source: String,
    },
    ScriptSourceSaved {
        stored: Option<ContentAddressedPath>,
    },
    /// Flow definition updated.
    FlowUpdated {
        flow: FlowDefinition,
    },
    /// Flow definition retrieved.
    FlowRetrieved {
        flow: FlowDefinition,
    },
    /// Flow definitions listed.
    FlowsListed {
        flows: Vec<FlowDefinition>,
    },
    /// Persisted Cron triggers for one Flow.
    FlowTriggersListed {
        triggers: Vec<FlowTriggerRecord>,
    },
    /// Trigger created.
    FlowTriggerCreated {
        trigger: FlowTriggerRecord,
    },
    /// Trigger updated.
    FlowTriggerUpdated {
        trigger: FlowTriggerRecord,
    },
    /// Trigger deleted.
    FlowTriggerDeleted {
        #[specta(type = String)]
        #[schemars(with = "String")]
        trigger_id: Uuid,
    },
    /// Node created.
    FlowNodeCreated {
        node: FlowNode,
    },
    /// Node updated.
    FlowNodeUpdated {
        node: FlowNode,
    },
    /// Node deleted.
    FlowNodeDeleted {
        flow_id: FlowId,
        node_id: FlowNodeId,
    },
    /// Flow deletion accepted.
    FlowDeleted {
        flow_id: FlowId,
    },
    /// Active executions listed.
    ActiveExecutionsListed {
        executions: Vec<FlowExecutionSnapshot>,
    },
    /// Terminal executions listed.
    TerminalExecutionsListed {
        executions: Vec<FlowExecutionSnapshot>,
    },
    /// A Flow execution detail was retrieved.
    ExecutionDetailRetrieved {
        detail: FlowExecutionDetail,
    },
    /// A Flow execution was accepted.
    Started {
        execution_id: FlowExecutionId,
    },
    /// An active Flow execution was aborted.
    Aborted {
        execution_id: FlowExecutionId,
    },
    /// One destination was accepted after durable pairing validation.
    MobilityDestinationSelected {
        flow_id: FlowId,
        destination: FlowMobilityDestination,
    },
    /// Durable selected-Flow mobility readiness.
    FlowMobilityReadiness {
        readiness: FlowMobilityReadiness,
    },
    /// The destination committed the imported snapshot durably.
    FlowMobilityCommitAcknowledgement {
        acknowledgement: FlowMobilityCommitAcknowledgement,
    },
    /// A source durably stopped scheduling and admitting Flow mutations.
    FlowAuthorityYielded {
        acknowledgement: FlowYieldAcknowledgement,
    },
    /// The destination durably became the sole authority for the transferred Flow.
    FlowReplicaPromoted {
        acknowledgement: FlowActivationAcknowledgement,
    },
    /// A source's exclusive transferred state was removed.
    FlowTransferFinalized {
        flow_id: FlowId,
    },
    /// Completed-run retention was applied.
    RetentionUpdated,
}

/// Observer events emitted by the Flow domain.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FlowBroadcast {
    /// Source domains must reconcile their live listeners against Flow.
    TriggersChanged,
    /// A management-visible Flow definition changed.
    DefinitionChanged { flow: Box<FlowDefinition> },
    /// Observer-visible execution snapshot transition.
    ExecutionStatusChanged {
        flow_name: String,
        snapshot: FlowExecutionSnapshot,
    },
    /// Node-ready events carry correlation only; credentials are requested separately through Auth.
    Ready { correlation: LifecycleCorrelation },
    /// A Flow execution reached a node-failed transition.
    Failed {
        correlation: LifecycleCorrelation,
        failure: FlowFailure,
    },
    /// A Flow execution reached a node-succeeded transition.
    Succeeded { correlation: LifecycleCorrelation },
    /// A Flow execution started.
    Started {
        execution_id: FlowExecutionId,
        flow_name: String,
    },
    /// A Flow execution was aborted.
    Aborted { execution_id: FlowExecutionId },
}

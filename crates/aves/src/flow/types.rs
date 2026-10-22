use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::Error as IoError;
use std::path::PathBuf;

use chrono::{DateTime, ParseError, Utc};
use croner::errors::CronError;
use rspc::{Error as RspcError, ProcedureError, ResolverError};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Map, Value, json};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use turso::{Database, Error as TursoError};
use uuid::{Error as UuidError, Uuid};

use aves::ErrorCode;

use crate::auth::{AvesAuthError, MachineIssuerId};
use crate::bus::DispatchContextError;
use crate::migrations::flows::MIGRATIONS;
use crate::runtime::GeneratedCallError;
use crate::utils::database::{DatabaseError, MigrationRunner, maintain_database, open_database_at_path};

use super::ast::{ContentAddressedRoot, ContentHash, FlowNode};
use super::database::FlowStore;
use super::graph::FlowGraphAst;
use super::messages::{FlowResponse, LifecycleCorrelation};
use super::registration::{
    FlowNodeRegistry, NodeKind, NodeKindRegistration, NodeRegistrationSupply, WorkspacePreparationError,
};
use super::snapshot::{ExecutionSnapshot, ExecutionSnapshotStore, NodeExecutionSnapshot};
use super::tick::{FlowDispatch, FlowTickLoop, ReconstructionState};

/// Default number of completed executions retained per Flow.
pub const DEFAULT_RETENTION_COMPLETED_RUNS: u32 = 10;

/// Flow-owned configuration section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSettings {
    /// Maximum number of completed executions retained per Flow.
    #[serde(default = "default_retention_completed_runs")]
    pub retention_completed_runs: u32,
}

impl Default for FlowSettings {
    fn default() -> Self {
        Self {
            retention_completed_runs: DEFAULT_RETENTION_COMPLETED_RUNS,
        }
    }
}

/// Construction input supplied by an embedding application.
#[derive(Clone)]
pub struct FlowBootOptions {
    /// Root owned by the embedding application for Flow data.
    pub data_root: PathBuf,
    /// Already-open database after the embedding application applied Flow migrations.
    pub database: Database,
    /// Typed lifecycle contracts supplied by the embedding application's owners.
    pub node_registrations: Vec<NodeKindRegistration>,
    /// Shared construction-time supply populated by owner domains.
    pub node_registration_supply: NodeRegistrationSupply,
    /// Maximum number of completed executions retained per Flow.
    pub retention_completed_runs: u32,
}

impl FlowBootOptions {
    /// Opens and migrates `config_dir/flows.db`, with workspace data under `config_dir/flows`.
    pub async fn open(config_dir: PathBuf) -> Result<Self, FlowError> {
        if config_dir.as_os_str().is_empty() {
            return Err(FlowError::EmptyDataRoot);
        }
        let database_path = config_dir.join("flows.db");
        let database = open_database_at_path(&database_path).await?;
        MigrationRunner::new("__flow_migrations", MIGRATIONS)
            .apply(&database)
            .await?;
        maintain_database(&database, &database_path).await;
        let store = FlowStore::new(database.clone());
        store.load_definitions().await?;
        store.load_triggers().await?;
        ExecutionSnapshotStore::new(database.clone())
            .load_all()
            .await?;
        Ok(Self {
            data_root: config_dir.join("flows"),
            database,
            node_registrations: Vec::new(),
            node_registration_supply: NodeRegistrationSupply::default(),
            retention_completed_runs: DEFAULT_RETENTION_COMPLETED_RUNS,
        })
    }

    /// Sets the completed-run retention limit used by the Flow actor at boot.
    pub(super) fn with_retention_completed_runs(mut self, retention_completed_runs: u32) -> Self {
        self.retention_completed_runs = retention_completed_runs;
        self
    }

    /// Adds owner-declared lifecycle contracts before runtime construction.
    pub fn with_node_registrations(
        mut self,
        registrations: impl IntoIterator<Item = NodeKindRegistration>,
    ) -> Result<Self, FlowRegistrationError> {
        let mut registry = FlowNodeRegistry::new();
        for registration in registrations {
            registry.register(registration.clone())?;
            self.node_registrations.push(registration);
        }
        Ok(self)
    }
}

/// Versioned, immutable Flow graph definition exposed by the management API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub struct FlowDefinition {
    /// Stable Flow identity.
    pub flow_id: FlowId,
    /// Human-readable Flow name.
    pub name: String,
    /// Immutable revision number.
    pub revision: i64,
    /// Authoritative graph JSON AST.
    pub graph: FlowGraphAst,
    /// Initial blackboard values.
    pub blackboard_defaults: Value,
    /// Editor-only metadata retained by Flow.
    pub editor_metadata: Value,
    /// Whether source-domain and Cron starts may launch this Flow.
    #[serde(default = "default_automatic_enabled")]
    pub automatic_enabled: bool,
    /// Active-Obsidian directories imported into non-overlapping workspace folders.
    #[serde(default)]
    pub workspace_mappings: Vec<FlowWorkspaceMapping>,
}

/// A known vault-relative directory used as a Flow's execution-root source.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub struct FlowWorkspaceMapping {
    /// The directory on disk to map into the workspace.
    pub directory: String,
    /// Workspace-relative folder; empty denotes the workspace root.
    #[serde(default)]
    pub destination: String,
    /// Terminal source projection policy for a mapped workspace.
    pub mode: FlowWorkspaceMappingMode,
}

/// Terminal source projection policy for a mapped workspace.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub enum FlowWorkspaceMappingMode {
    /// The workspace directory can read, write, and delete files, and remapped from scratch.
    ReadWriteDelete,
    /// The workspace directory is preserved after each run save for deletions (files can be
    /// read/written/deleted during a run), and remapped from scratch.
    ReadWrite,
    /// The workspace directory is never preserved (files can be read/written/deleted during
    /// a run), and remapped from scratch.
    ReadOnly,
}

// TODO: migrate this to a tag system we can use on create/update rather than this hard
// coded enum
/// The source of a Flow invocation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub enum FlowInvocationKind {
    /// The invocation was triggered manually by the user.
    #[default]
    Manual,
    /// The invocation was triggered by a schedule.
    Schedule,
    /// The invocation was triggered by an event.
    Event,
    /// The invocation was triggered by a strategy.
    Strategy,
}

/// Late-bound resolver for the one supported Flow source boundary.
pub trait FlowWorkspaceSourceService: Send + Sync {
    fn resolve_known_directory(&self, relative_identity: &str) -> Result<PathBuf, FlowWorkspaceSourceError>;
}

/// Source identity for the narrow Script text API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub enum ScriptSourceLocation {
    New,
    Mapped { directory: String },
    Stored { root: ContentAddressedRoot },
}

/// Observer-visible execution state, also persisted in recovery snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FlowExecutionStatus {
    /// The execution is currently running.
    Active,
    /// The execution has successfully completed.
    Succeeded,
    /// The execution is currently being aborted.
    Aborting,
    /// The execution has been aborted.
    Aborted,
    /// The execution has failed.
    Failed,
}

impl FlowExecutionStatus {
    pub(super) fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Aborted)
    }
}

/// Serializable execution projection returned by active and terminal queries.
#[derive(Clone, Debug, Deserialize, Serialize, Type, JsonSchema)]
pub struct FlowExecutionSnapshot {
    /// Flow execution identity.
    pub execution_id: FlowExecutionId,
    /// Flow definition identity.
    pub flow_id: FlowId,
    /// Revision pinned by this execution.
    pub revision: i64,
    /// Current observer-visible status.
    pub status: FlowExecutionStatus,
    /// RFC3339 start time.
    pub started_at: FlowTimestamp,
    /// RFC3339 terminal time, when complete.
    pub completed_at: Option<FlowTimestamp>,
    /// Typed terminal diagnostic, when applicable.
    pub error: Option<String>,
}

/// Saved graph and owner-neutral execution detail for one Flow execution.
#[derive(Clone, Debug, Deserialize, Serialize, Type, JsonSchema)]
pub struct FlowExecutionDetail {
    /// The immutable graph revision pinned by the execution.
    pub flow: FlowDefinition,
    /// Existing execution-level observer summary.
    pub execution: FlowExecutionSnapshot,
    /// Persisted or current node execution projections.
    pub nodes: Vec<NodeExecutionSnapshot>,
    /// Blackboard values available to the configured Flow outputs.
    pub blackboard: BTreeMap<String, Value>,
}

/// Durable, selected-destination replication readiness for one Flow replica.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowMobilityReadiness {
    /// Flow whose replica state was inspected.
    pub flow_id: FlowId,
    /// Revision of the durably committed selected-Flow snapshot, when present.
    pub snapshot_generation: Option<i64>,
    /// Whether the retained execution history belongs exclusively to this Flow.
    pub history_complete: bool,
    /// Whether every retained execution has a durable document link.
    pub documents_complete: bool,
    /// Whether the native content commit completed before the snapshot was accepted.
    pub blobs_complete: bool,
    /// Whether this replica is durably inactive and therefore cannot execute work.
    pub inactive: bool,
    /// Typed reason this replica is not ready, if known.
    pub failure: Option<FlowFailure>,
}

/// Public projection of the selected paired destination for one Flow transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub struct FlowMobilityDestination {
    /// Stable public key of a previously confirmed peer pairing.
    pub peer_key: MachineIssuerId,
}

/// Serializable observer projection of a typed Flow failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub struct FlowFailure {
    pub code: String,
    pub message: String,
}

/// A validated UTC timestamp used by persisted Flow lifecycle projections.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FlowTimestamp(DateTime<Utc>);

impl FlowTimestamp {
    /// Captures the current UTC time for a lifecycle projection.
    pub(super) fn now() -> Self {
        Self(Utc::now())
    }

    pub(super) fn from_datetime(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    pub(super) fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub(super) fn as_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

    pub(super) fn parse(value: &str) -> Result<Self, ParseError> {
        DateTime::parse_from_rfc3339(value).map(|value| Self(value.with_timezone(&Utc)))
    }
}

impl Type for FlowTimestamp {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }
    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl JsonSchema for FlowTimestamp {
    fn schema_name() -> Cow<'static, str> {
        "FlowTimestamp".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::FlowTimestamp").into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        <String as JsonSchema>::json_schema(generator)
    }
}

/// Active execution attribution pinned at admission time.
#[derive(Clone, Debug)]
pub(super) struct ActiveExecution {
    pub(super) flow_id: FlowId,
    revision: i64,
    started_at: FlowTimestamp,
}

impl ActiveExecution {
    pub(super) fn new(flow_id: FlowId, revision: i64, started_at: FlowTimestamp) -> Self {
        Self {
            flow_id,
            revision,
            started_at,
        }
    }

    pub(super) fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    pub(super) fn revision(&self) -> i64 {
        self.revision
    }

    pub(super) fn started_at(&self) -> FlowTimestamp {
        self.started_at.clone()
    }
}

/// Captured terminal input retained until its ordered snapshot write succeeds.
#[derive(Clone)]
pub(super) struct PendingTerminalPersistence {
    pub(super) correlation: LifecycleCorrelation,
    pub(super) failure_message: Option<String>,
}

/// Immutable retry data for one prepared checkpoint durability commit.
pub(super) struct PreparedCheckpointCommit {
    pub(super) snapshot: ExecutionSnapshot,
    pub(super) superseded_sequence: Option<u64>,
    pub(super) pending: Vec<PendingTerminalPersistence>,
    pub(super) dispatches: Vec<FlowDispatch>,
    pub(super) snapshot_saved: bool,
}

/// Mutable execution bookkeeping owned by the Flow actor.
pub(super) struct ExecutionState {
    pub(super) active: ActiveExecution,
    pub(super) tick_loop: FlowTickLoop,
    pub(super) nodes: HashMap<FlowNodeId, FlowNode>,
    pub(super) pending_start_reply: Option<oneshot::Sender<Result<FlowResponse, FlowError>>>,
    pub(super) pending_terminal_persistence: HashMap<NodeExecutionId, PendingTerminalPersistence>,
    pub(super) prepared_checkpoint_commit: Option<PreparedCheckpointCommit>,
    pub(super) writer_active: HashMap<NodeExecutionId, LifecycleCorrelation>,
    pub(super) code_roots: Vec<ContentAddressedRoot>,
    pub(super) state_roots: Vec<ContentAddressedRoot>,
    pub(super) workspace_entries: BTreeMap<String, ContentHash>,
    pub(super) workspace_checkpoint: Option<ContentAddressedRoot>,
    pub(super) imported_workspace_root: Option<ContentAddressedRoot>,
    pub(super) workspace_mappings: Vec<FlowWorkspaceMapping>,
    pub(super) checkpoint_sequence: u64,
    pub(super) abort_pending: bool,
    pub(super) dispatch_tasks: Vec<AbortHandle>,
    pub(super) dispatch_in_flight: HashSet<NodeExecutionId>,
}

impl ExecutionState {
    /// Projects current runtime state into its durable snapshot shape.
    pub(super) fn snapshot(
        &self,
        execution_id: FlowExecutionId,
        status: FlowExecutionStatus,
        reconstruction: ReconstructionState,
        error: Option<String>,
    ) -> ExecutionSnapshot {
        let status = if self.abort_pending && !status.is_terminal() {
            FlowExecutionStatus::Aborting
        } else {
            status
        };
        ExecutionSnapshot {
            execution: FlowExecutionSnapshot {
                execution_id,
                flow_id: self.active.flow_id(),
                revision: self.active.revision(),
                completed_at: status.is_terminal().then(FlowTimestamp::now),
                status,
                started_at: self.active.started_at(),
                error,
            },
            blackboard: self.tick_loop.blackboard().values().clone(),
            code_roots: self.code_roots.clone(),
            state_roots: self.state_roots.clone(),
            workspace_checkpoint: self.workspace_checkpoint.clone(),
            workspace_entries: self.workspace_entries.clone(),
            imported_workspace_root: self.imported_workspace_root.clone(),
            reconstruction,
            scratchpad: self.tick_loop.scratchpad_snapshot(),
            nodes: self.tick_loop.node_snapshots(),
        }
    }
}

macro_rules! flow_id {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl Type for $name {
            fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
                String::inline(type_map, generics)
            }

            fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
                String::reference(type_map, generics)
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                self.0.fmt(formatter)
            }
        }

        impl JsonSchema for $name {
            fn schema_name() -> Cow<'static, str> {
                stringify!($name).into()
            }

            fn schema_id() -> Cow<'static, str> {
                concat!(module_path!(), "::", stringify!($name)).into()
            }

            fn inline_schema() -> bool {
                true
            }

            fn json_schema(generator: &mut SchemaGenerator) -> Schema {
                <String as JsonSchema>::json_schema(generator)
            }
        }
    };
}

flow_id!(FlowId, "Stable identity of a Flow definition.");

flow_id!(FlowNodeId, "Stable identity of a node in a Flow graph.");

flow_id!(FlowExecutionId, "Identity of one Flow execution.");

flow_id!(
    NodeExecutionId,
    "Identity of one node execution within a Flow execution."
);

fn default_retention_completed_runs() -> u32 {
    DEFAULT_RETENTION_COMPLETED_RUNS
}

fn default_automatic_enabled() -> bool {
    false
}

/// Typed failures produced by the Aves Flow domain.
#[derive(Debug, Error, ErrorCode)]
pub enum FlowError {
    /// The embedding application did not provide a usable data root.
    #[error("Flow data root must not be empty")]
    EmptyDataRoot,
    /// A requested execution is not owned by this Flow actor.
    #[error("Flow execution was not found")]
    ExecutionNotFound,
    /// Start was interrupted by an abort request.
    #[error("Flow execution was aborted before start completed")]
    ExecutionAborted,
    /// Abort timed out.
    #[error("Flow node cancellation timed out")]
    NodeCancellationTimedOut,
    /// A requested Flow definition is not present.
    #[error("Flow definition was not found")]
    DefinitionNotFound,
    /// A Flow definition references a node kind not registered by this runtime.
    #[error("Flow node kind is not registered: {0:?}")]
    #[error_code("UnregisteredNodeKind")]
    UnregisteredNodeKind(NodeKind),
    /// A requested Flow trigger is not present.
    #[error("Flow trigger was not found")]
    TriggerNotFound,
    /// An automatic trigger attempted to start a Flow whose automatic gate is disabled.
    #[error("automatic Flow starts are disabled for {flow_id}")]
    #[error_code("AutomaticStartDisabled")]
    AutomaticStartDisabled { flow_id: FlowId },
    /// This local Flow replica is durably inactive and cannot perform work.
    #[error("Flow {flow_id} is inactive on this replica")]
    #[error_code("InactiveReplica")]
    InactiveReplica { flow_id: FlowId },
    /// A persisted Flow authority value is not recognized by this runtime.
    #[error("unknown persisted Flow authority state {state}")]
    UnknownAuthorityState { state: String },
    /// A stored mobility snapshot does not match its requested Flow identity.
    #[error("Flow mobility snapshot does not belong to Flow {flow_id}")]
    MobilitySnapshotMismatch { flow_id: FlowId },
    /// A replication operation supplied zero or multiple destination peers.
    #[error("Flow mobility requires exactly one destination")]
    #[error_code("MobilityDestinationCount")]
    MobilityDestinationCount,
    /// A requested destination is not a confirmed, non-revoked pairing.
    #[error("Flow mobility destination is not a confirmed pairing")]
    #[error_code("MobilityDestinationUnavailable")]
    MobilityDestinationUnavailable,
    /// Synchronization was requested before a destination was selected in this actor lifetime.
    #[error("Flow mobility destination has not been selected")]
    #[error_code("MobilityDestinationNotSelected")]
    MobilityDestinationNotSelected,
    /// The selected peer rejected or failed a mobility operation.
    #[error("Flow mobility peer operation failed: {message}")]
    MobilityPeer { message: String },
    /// Promotion was requested from an authority state that cannot become active.
    #[error("Flow replica cannot be promoted from authority state {state}")]
    #[error_code("MobilityPromotionRejected")]
    MobilityPromotionRejected { state: String },
    /// A destination has not durably committed every artifact needed for activation.
    #[error("Flow replica is not ready for promotion: {reason}")]
    #[error_code("MobilityIncomplete")]
    MobilityIncomplete { reason: &'static str },
    /// A requested node does not exist in the active definition revision.
    #[error("Flow node was not found")]
    NodeNotFound,
    /// A create attempted to reuse a stable Flow identity.
    #[error("Flow definition already exists")]
    DefinitionAlreadyExists,
    /// A mutation supplied a revision other than the next immutable revision.
    #[error("Flow revision conflict: expected {expected}, got {actual}")]
    RevisionConflict { expected: i64, actual: i64 },
    /// The runtime has not admitted Flow execution yet.
    #[error("Flow runtime is not started")]
    RuntimeNotStarted,
    /// The Flow actor request channel could not accept a request.
    #[error("Flow request channel is closed")]
    RequestChannelClosed,
    /// A Flow request produced an unexpected response.
    #[error("Flow response was not available")]
    ResponseUnavailable,
    /// A generated Flow service request could not be sent.
    #[error("Flow request channel send failed for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// A generated Flow service response could not be received or projected.
    #[error("Flow response channel receive failed for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// Shared database open or migration failed.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// A transactional Flow persistence operation failed.
    #[error("Flow persistence operation failed: {0}")]
    Persistence(#[from] TursoError),
    /// Graph decoding or validation failed.
    #[error(transparent)]
    Graph(FlowGraphLoadError),
    /// Cron trigger validation or cursor advancement failed.
    #[error(transparent)]
    Cron(#[from] FlowCronError),
    /// A source adapter rejected its persisted trigger configuration.
    #[error(transparent)]
    TriggerRegistration(#[from] TriggerRegistrationError),
    /// Controller composition failed.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// Tree compilation or ticking failed.
    #[error(transparent)]
    Tick(TickError),
    /// JSON encoding or decoding failed.
    #[error("Flow JSON operation failed: {0}")]
    Serialization(#[from] JsonError),
    /// Host lifecycle dispatch failed.
    #[error(transparent)]
    #[error_code("OwnerDispatch")]
    Adapter(#[from] FlowAdapterError),
    /// A node owner rejected work before acknowledging the lifecycle start.
    #[error("Flow node owner rejected start for {kind:?}: {message}")]
    OwnerStartRejected { kind: NodeKind, message: String },
    /// Snapshot persistence failed.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// Start persistence failed and its best-effort compensation also failed.
    #[error("Flow start persistence failed for {execution_id}: {cause}; compensation failed: {compensation}")]
    StartCompensation {
        execution_id: FlowExecutionId,
        #[source]
        cause: Box<FlowError>,
        compensation: Box<FlowError>,
    },
    /// A persisted timestamp could not be decoded.
    #[error("Flow timestamp could not be decoded: {0}")]
    Timestamp(#[from] ParseError),
    /// A persisted Flow UUID could not be decoded.
    #[error("Flow identity could not be decoded: {source}")]
    Identity {
        #[from]
        source: UuidError,
    },
    /// An isolated execution directory could not be created.
    #[error("Flow execution directory {path} could not be created: {source}")]
    ExecutionDirectory {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    /// Dispatch context authentication failed.
    #[error("Flow dispatch context is invalid: {0}")]
    DispatchContext(#[from] DispatchContextError),
    /// Auth rejected token issuance or revocation.
    #[error("Flow authorization failed: {0}")]
    Auth(#[from] AvesAuthError),
    /// A generated response did not match its declared operation.
    #[error("Flow response expected {expected}, got {actual}")]
    UnexpectedResponse { expected: String, actual: String },
    /// Local content-addressed workspace operation failed.
    #[error(transparent)]
    Workspace(WorkspaceError),
    /// The configured Knowledge source could not resolve a known directory.
    #[error(transparent)]
    WorkspaceSource(#[from] FlowWorkspaceSourceError),
    /// AgentNode workspace sources could not be resolved during preparation.
    #[error("Flow AgentNode workspace source resolution failed: {0}")]
    #[error_code("WorkspaceCopySources")]
    WorkspaceCopySources(#[from] WorkspacePreparationError),
    /// Configured workspace inputs provide different contents for one path.
    #[error("Flow workspace path {path} has conflicting content from roots {first_root:?} and {second_root:?}")]
    WorkspaceInputCollision {
        path: PathBuf,
        first_root: ContentAddressedRoot,
        second_root: ContentAddressedRoot,
    },
    /// A terminal execution could not be completely removed during retention.
    #[error("Flow terminal execution retention cleanup failed for {execution_id}: {source}")]
    RetentionCleanup {
        execution_id: FlowExecutionId,
        #[source]
        source: Box<FlowError>,
    },
}

impl FlowError {
    /// Safe structured fields for the public Flow error envelope.
    pub fn rpc_details(&self) -> Map<String, Value> {
        let details = match self {
            Self::RevisionConflict { expected, actual } => json!({"expected": expected, "actual": actual}),
            Self::ChannelSend {
                target_domain,
                operation,
                ..
            }
            | Self::ChannelRecv {
                target_domain,
                operation,
                ..
            } => json!({"target_domain": target_domain, "operation": operation}),
            Self::OwnerStartRejected { kind, .. } => json!({"nodeKind": format!("{kind:?}")}),
            Self::ExecutionDirectory { path, .. } => json!({"path": path.display().to_string()}),
            Self::UnexpectedResponse { expected, actual } => json!({"expected": expected, "actual": actual}),
            Self::WorkspaceInputCollision {
                path,
                first_root,
                second_root,
            } => json!({"path": path.display().to_string(), "first_root": first_root, "second_root": second_root}),
            Self::RetentionCleanup { execution_id, source } => {
                json!({"execution_id": execution_id.to_string(), "source": {"code": source.rpc_code()}})
            }
            Self::StartCompensation {
                execution_id,
                cause,
                compensation,
            } => {
                json!({"execution_id": execution_id.to_string(), "cause": {"code": cause.rpc_code()}, "compensation": {"code": compensation.rpc_code()}})
            }
            _ => return Map::new(),
        };
        match details {
            Value::Object(fields) => fields,
            _ => unreachable!("detail projections are objects"),
        }
    }
}

impl Type for FlowError {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        FlowFailure::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        FlowFailure::reference(type_map, generics)
    }
}

impl From<WorkspaceError> for FlowError {
    fn from(error: WorkspaceError) -> Self {
        match error {
            WorkspaceError::InputCollision {
                path,
                first_root,
                second_root,
            } => Self::WorkspaceInputCollision {
                path,
                first_root,
                second_root,
            },
            error => Self::Workspace(error),
        }
    }
}

impl From<FlowRegistrationError> for FlowError {
    fn from(error: FlowRegistrationError) -> Self {
        Self::Controller(ControllerError::Registration(error))
    }
}

impl From<FlowGraphLoadError> for FlowError {
    fn from(error: FlowGraphLoadError) -> Self {
        match error {
            FlowGraphLoadError::UnregisteredNodeKind(kind) => Self::UnregisteredNodeKind(kind),
            error => Self::Graph(error),
        }
    }
}

impl From<TickError> for FlowError {
    fn from(error: TickError) -> Self {
        match error {
            TickError::Unregistered(kind) => Self::UnregisteredNodeKind(kind),
            error => Self::Tick(error),
        }
    }
}

impl RspcError for FlowError {
    fn into_procedure_error(self) -> ProcedureError {
        let failure = FlowFailure {
            code: self.rpc_code().to_owned(),
            message: self.to_string(),
        };
        ProcedureError::Resolver(ResolverError::new(failure, Some(self)))
    }
}

impl From<GeneratedCallError<FlowError>> for FlowError {
    fn from(error: GeneratedCallError<FlowError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => Self::Auth(error),
            GeneratedCallError::InputClosed => Self::RequestChannelClosed,
            GeneratedCallError::ReplyDropped => Self::ResponseUnavailable,
            GeneratedCallError::UnexpectedResponse { expected, actual } => {
                Self::UnexpectedResponse { expected, actual }
            }
        }
    }
}

/// Typed failures raised while importing or materializing Flow workspace content.
#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("native Iroh workspace substrate is unavailable")]
    NativeSubstrateUnavailable,
    #[error("workspace source has not been stored yet")]
    SourceNotStored,
    #[error("workspace blob ticket is invalid")]
    InvalidTicket,
    #[error("workspace blob ticket format is unsupported: {format}")]
    UnsupportedTicketFormat { format: String },
    #[error("workspace blob ticket capability is denied")]
    CapabilityDenied,
    #[error("workspace blob ticket provider transport is unavailable")]
    ProviderUnavailable,
    #[error("workspace blob ticket provider is unreachable: {source}")]
    ProviderUnreachable {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("workspace blob ticket integrity check failed for {expected}: {source}")]
    IntegrityMismatch {
        expected: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("workspace path {path} is not a regular file or directory")]
    UnsupportedSource { path: PathBuf },
    #[error("workspace path {path} is a symbolic link")]
    SymbolicLink { path: PathBuf },
    #[error("workspace path {path} escapes its content root")]
    PathEscapesRoot { path: PathBuf },
    #[error("workspace collection contains duplicate path {path}")]
    DuplicatePath { path: PathBuf },
    #[error("workspace destinations overlap: {first} and {second}")]
    OverlappingMappings { first: String, second: String },
    #[error("workspace path {path} has conflicting content from roots {first_root:?} and {second_root:?}")]
    InputCollision {
        path: PathBuf,
        first_root: ContentAddressedRoot,
        second_root: ContentAddressedRoot,
    },
    #[error("workspace source {path} has no file name")]
    MissingFileName { path: PathBuf },
    #[error("workspace root {root:?} does not contain configured path {path}")]
    ContentPathNotFound { root: ContentAddressedRoot, path: String },
    #[error("workspace filesystem operation failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("workspace source is not valid UTF-8 at {path}")]
    InvalidUtf8 { path: PathBuf },
    #[error("workspace blob operation failed: {source}")]
    Blob {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("workspace document operation failed: {source}")]
    Document {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("workspace document identity is invalid: {value}")]
    InvalidDocumentIdentity { value: String },
    #[error("workspace content identity is invalid: {value}")]
    InvalidContentIdentity { value: String },
}

impl WorkspaceError {
    /// Constructs a workspace filesystem error while deferring path ownership conversion.
    pub(super) fn io(path: impl Into<PathBuf>, source: IoError) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Constructs a workspace blob error while preserving its boxed source.
    pub(super) fn blob(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self::Blob { source: source.into() }
    }
}

/// Errors at the host adapter boundary.
#[derive(Debug, Error)]
pub enum FlowAdapterError {
    /// Runtime authorization service was not published.
    #[error("runtime authorization service is unavailable")]
    AuthorizationUnavailable,
    #[error("Flow node kind is not registered: {0:?}")]
    UnregisteredNodeKind(NodeKind),
    #[error("Flow node registration failed: {0}")]
    Registration(#[from] FlowRegistrationError),
    #[error("Flow node lifecycle was cancelled")]
    Cancelled,
    #[error("Flow node owner request could not be sent for {kind:?}: {source}")]
    DispatchSend {
        kind: NodeKind,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Flow node owner reply was dropped for {kind:?}")]
    DispatchReplyDropped { kind: NodeKind },
    #[error("Flow node configuration did not match registration for {kind:?}")]
    DispatchConfigurationMismatch { kind: NodeKind },
    #[error("Flow node cancellation target is missing: {flow_node_id:?}")]
    CancelTargetMissing { flow_node_id: FlowNodeId },
    #[error("Flow lifecycle correlation does not match the active node execution")]
    CorrelationMismatch,
    #[error("Flow lifecycle payload did not match the registered {kind:?} owner contract")]
    PayloadTypeMismatch { kind: NodeKind },
    #[error("Flow node owner is unavailable: {kind:?}")]
    OwnerUnavailable { kind: NodeKind },
    #[error("Flow node owner rejected invalid input for {kind:?}: {message}")]
    InvalidOwnerInput { kind: NodeKind, message: String },
    #[error("Flow node owner failed for {kind:?}: {source}")]
    Owner {
        kind: NodeKind,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Flow node authorization failed: {0}")]
    Authorization(#[source] AvesAuthError),
}

/// Errors raised while applying explicit blackboard bindings.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BlackboardError {
    #[error("blackboard input is missing: {key}")]
    MissingInput { key: String },
    #[error("node output is not configured: {name}")]
    UnknownOutput { name: String },
}

/// Typed failures raised while composing or advancing a controller.
#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("selector child order is invalid: expected {expected}, got {actual}")]
    SelectorOrder { expected: usize, actual: usize },
    #[error("parallel controller must contain at least two children")]
    ParallelChildCount,
    #[error("parallel child index is invalid: {child_index}")]
    ChildIndex { child_index: usize },
    #[error("parallel policy cannot discard non-cancellable side-effecting child: {kind:?}")]
    UnsafeDiscard { kind: NodeKind },
    #[error(transparent)]
    Registration(#[from] FlowRegistrationError),
}

/// Failures raised while validating or advancing Flow Cron.
#[derive(Debug, Error)]
pub enum FlowCronError {
    #[error("Flow Cron expression must contain exactly five fields: {expression}")]
    InvalidExpression { expression: String },
    #[error("Flow Cron expression could not be parsed: {expression}: {source}")]
    Parse {
        expression: String,
        #[source]
        source: CronError,
    },
    #[error("Flow Cron expression has no next occurrence: {expression}: {source}")]
    NextOccurrence {
        expression: String,
        #[source]
        source: CronError,
    },
    #[error("Flow trigger is missing adapter_config.cron")]
    MissingCronConfiguration,
    #[error("Flow trigger database operation failed: {0}")]
    Database(#[from] TursoError),
    #[error("Flow trigger JSON serialization failed: {0}")]
    Serialization(#[from] JsonError),
}

/// Errors raised when Flow registers or resolves a node kind.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum FlowRegistrationError {
    #[error("Flow node kind is already registered: {0:?}")]
    Duplicate(NodeKind),
    #[error("Flow node kind is not registered: {0:?}")]
    Unregistered(NodeKind),
    #[error("Flow node kind is malformed")]
    MalformedNodeKind,
    #[error("Flow node configuration could not be decoded: {message}")]
    Configuration { message: String },
}

/// Typed failures while decoding or validating a persisted Flow AST.
#[derive(Debug, Error)]
pub enum FlowGraphLoadError {
    #[error("Flow graph JSON could not be decoded: {0}")]
    Decode(#[from] JsonError),
    #[error("Flow graph is invalid at {path}: {reason}")]
    Invalid { path: String, reason: String },
    #[error("Flow node kind is not registered: {0:?}")]
    UnregisteredNodeKind(NodeKind),
}

/// Errors raised while atomically encoding or storing an execution snapshot.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("failed to serialize Flow execution snapshot: {0}")]
    Serialize(#[from] JsonError),
    #[error("Flow snapshot database operation failed: {source}")]
    Database {
        #[from]
        source: TursoError,
    },
    #[error("Flow snapshot attribution changed for execution {execution_id}")]
    AttributionMismatch { execution_id: FlowExecutionId },
    #[error("Flow snapshot batch contains an execution outside selected Flow {flow_id}")]
    SelectedFlowMismatch { flow_id: FlowId },
    #[error("persisted Flow node status `{status}` is unknown")]
    UnknownNodeStatus { status: String },
    #[error("invalid persisted Flow identity: {0}")]
    Identity(#[from] UuidError),
    #[error("invalid persisted Flow timestamp: {0}")]
    Timestamp(#[from] ParseError),
}

/// Typed failures raised while compiling or ticking a Flow graph.
#[derive(Debug, Error)]
pub enum TickError {
    #[error("Flow node kind is not registered: {0:?}")]
    Unregistered(NodeKind),
    #[error(transparent)]
    Blackboard(#[from] BlackboardError),
}

/// Typed failures at the source-domain trigger registration boundary.
#[derive(Debug, Error)]
pub enum TriggerRegistrationError {
    #[error("Invalid Flow trigger configuration: {reason}")]
    InvalidConfiguration { reason: &'static str },
    #[error("Flow trigger adapter is already registered")]
    DuplicateAdapter,
    #[error("Flow trigger adapter is not registered")]
    UnknownAdapter,
    #[error("Flow trigger payload has the wrong message type")]
    MessageTypeMismatch,
    #[error("Flow trigger payload could not be serialized: {source}")]
    Serialization {
        #[from]
        source: JsonError,
    },
}

/// Typed errors from the embedding application's known workspace source.
#[derive(Debug, Error)]
pub enum FlowWorkspaceSourceError {
    #[error("the active Knowledge backend is not Obsidian")]
    BackendUnavailable,
    #[error("workspace directory identity is invalid: {identity}")]
    InvalidIdentity { identity: String },
    #[error("workspace directory is not known: {identity}")]
    UnknownDirectory { identity: String },
    #[error("workspace directory is not a directory: {path}")]
    NotDirectory { path: PathBuf },
    #[error("workspace source contains a symbolic link: {path}")]
    SymbolicLink { path: PathBuf },
    #[error("workspace path escapes its source root: {path}")]
    PathEscapesRoot { path: PathBuf },
    #[error("workspace source operation failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: IoError,
    },
}

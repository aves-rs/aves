use agent_client_protocol::{Error as AcpError, ErrorCode as AcpErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, json};
use specta::Type;
use thiserror::Error;

use aves::auth::AvesAuthError;
use aves::bus::{BusControlError, DispatchContextError};
use aves::flow::NodeExecutionId;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;

use super::library::Error as LibraryError;
use super::types::{DiscussSessionId, ElicitationRequestId};
/// Errors that can occur in the Discuss domain.
///
/// All variants store serializable data so Discuss errors can cross RPC and IPC
/// boundaries without carrying raw external error types.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub enum DiscussError {
    /// The verified identity has no binding.
    #[error("Session identity is unknown")]
    SessionIdentityUnknown,
    /// The session has no configured agent identity.
    #[error("Session identity is unassigned")]
    SessionIdentityUnassigned,
    /// Multiple sessions were bound to the same verified identity.
    #[error("Session identity is ambiguous")]
    SessionIdentityAmbiguous,
    /// A trusted issued token did not contain a valid issuer claim.
    #[error("Issued worker token has an invalid issuer claim")]
    SessionIdentityInvalidIssuer,
    /// OpenCode cannot provision a CLI login for a single session securely.
    #[error("OpenCode does not support secure per-session CLI tool login provisioning")]
    SessionCliLoginUnsupported,
    /// OpenCode cannot enforce the requested CLI tool selection for a session.
    #[error("OpenCode does not support per-session CLI tool provisioning")]
    SessionCliToolUnsupported,
    /// Channel send error (actor communication).
    #[error("Channel send error")]
    ChannelSend,

    /// Channel receive error (actor communication).
    #[error("Channel receive error")]
    ChannelRecv,

    /// Oneshot reply channel closed.
    #[error("Oneshot reply channel closed")]
    OneshotReply,

    /// ACP session cancelled by the user mid-turn.
    #[error("Discuss session {discuss_session_id} cancelled")]
    SessionCancelled {
        /// The session id that was cancelled.
        discuss_session_id: DiscussSessionId,
    },

    /// Protocol error.
    #[error("Protocol error: {message}")]
    ProtocolError {
        /// The error message.
        message: String,
    },
    /// The provider definitively rejected a saved session ID as missing.
    #[error("The saved provider session no longer exists")]
    ProviderSessionNotFound,

    /// The Provider requires an authenticated local session before it can start.
    #[error("Provider authentication is required")]
    AuthenticationRequired,

    /// The Auth sudo service required to issue a delegated worker token is unavailable.
    #[error("Auth worker-token service is unavailable")]
    WorkerTokenServiceUnavailable,

    /// Flow omitted the required scoped token for an AgentNode runtime.
    #[error("AgentNode start requires a Flow-scoped token")]
    AgentNodeTokenRequired,

    /// Auth failed while issuing a delegated worker token.
    #[error("Auth worker-token issuance failed: {message}")]
    WorkerTokenIssuance {
        /// Safe human-readable Auth failure detail.
        message: String,
    },

    /// Config persistence error.
    #[error("Config error: {message}")]
    Config {
        /// The error message.
        message: String,
    },

    /// Bus request error.
    #[error("Bus error: {message}")]
    Bus {
        /// The error message.
        message: String,
    },

    /// Serialization error.
    #[error("Serialization error: {message}")]
    Serialization {
        /// The error message.
        message: String,
    },

    /// The generated MCP domain catalog could not be prepared for a session.
    #[error("MCP setup error: {message}")]
    McpSetup {
        /// Safe structural collation failure message.
        message: String,
    },

    /// Shared runtime plumbing failure converted at the Discuss domain boundary.
    #[error("{kind} error for {target_domain}.{operation}: {message}")]
    Infrastructure {
        /// Stable infrastructure failure kind.
        kind: String,
        /// Domain expected to send, receive, reply, or broadcast.
        target_domain: String,
        /// Operation that was active when the plumbing failure occurred.
        operation: String,
        /// Safe human-readable failure message.
        message: String,
    },

    /// Turso DB persistence failure. `code` is a stable low-cardinality tag
    /// (`db_query`, `db_constraint`, `db_timeout`, `db_unavailable`) and
    /// `message` is the safe human-readable DB error text.
    #[error("Discuss persistence error [{code}]: {message}")]
    Persistence {
        /// Stable low-cardinality DB failure tag.
        code: String,
        /// Safe human-readable DB error text.
        message: String,
    },

    /// A workspace filesystem operation failed for a plugin-owned destination.
    #[error("workspace filesystem operation for {plugin} at {path} failed: {message}")]
    WorkspaceIo {
        /// Plugin that owns the failed destination.
        plugin: String,
        /// Failed plugin-owned destination.
        path: String,
        /// Safe filesystem error message.
        message: String,
    },

    /// Serializing a plugin-owned workspace output format failed.
    #[error("workspace serialization of {format} for {plugin} at {path} failed: {message}")]
    WorkspaceSerialize {
        /// Plugin that owns the failed destination.
        plugin: String,
        /// Failed plugin-owned destination.
        path: String,
        /// Plugin output format.
        format: String,
        /// Safe serializer error message.
        message: String,
    },

    /// A workspace asset identity cannot safely name a destination segment.
    #[error("workspace asset id {asset_id:?} is invalid: {violation}")]
    WorkspaceInvalidAssetId {
        /// Rejected canonical asset identity.
        asset_id: String,
        /// Safe explanation of the safety violation.
        violation: String,
    },

    /// An editable Provider source is not a safe relative workspace selection.
    #[error("invalid Provider workspace copy source {path:?}: {violation}")]
    InvalidWorkspaceCopySource {
        /// Rejected path.
        path: String,
        /// Safe explanation of the rejection.
        violation: String,
    },

    /// Request targets a chat session that has no live worker.
    #[error("Discuss session not found: discuss_session_id={discuss_session_id}")]
    SessionNotFound {
        /// Session id that had no live worker.
        discuss_session_id: DiscussSessionId,
    },

    /// The session has an active turn that cannot be disturbed.
    #[error("Discuss session busy: discuss_session_id={discuss_session_id}")]
    SessionBusy {
        /// Session id with an active turn.
        discuss_session_id: DiscussSessionId,
    },

    /// Provider did not advertise the capability required for the operation.
    #[error("Discuss unsupported capability: {capability}")]
    UnsupportedCapability {
        /// Capability name the provider did not advertise.
        capability: String,
    },

    /// The selected Provider cannot load an ACP child session for inspection.
    #[error("Provider {provider_name} does not support child-session inspection")]
    InspectorUnavailable {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// An ephemeral inspector failed while loading its child ACP session.
    #[error("Inspector failed to load child session {child_session_id}: {message}")]
    InspectorLoadFailed {
        /// Ephemeral viewer identity that receives the terminal stream error.
        viewer_session_id: DiscussSessionId,
        /// Provider ACP child-session identity.
        child_session_id: String,
        /// Safe typed failure detail.
        message: String,
    },

    /// Request payload failed Discuss domain validation.
    #[error("Discuss validation error: {message}")]
    Validation {
        /// Safe human-readable validation failure.
        message: String,
    },

    /// An AgentNode exceeded one of its saved execution limits.
    #[error("AgentNode limit reached for {limit}: {actual} (maximum {maximum})")]
    AgentNodeLimitReached {
        /// Stable limit name (`maxTurns`, `maxToolCalls`, or `maxDuration`).
        limit: String,
        /// Observed value at the terminal boundary.
        actual: u64,
        /// Resolved node-owned maximum.
        maximum: u64,
    },

    /// No live terminal receiver exists for an AgentNode tick.
    #[error("AgentNode completion not found for {node_execution_id:?}")]
    AgentNodeCompletionNotFound { node_execution_id: NodeExecutionId },

    /// A Flow supplied an AgentNode execution limit outside its finite bounds.
    #[error("invalid AgentNode limit {limit}: {value} (maximum {maximum})")]
    AgentNodeLimitInvalid { limit: String, value: i64, maximum: u64 },

    /// A live AgentNode execution already owns this node execution identity.
    #[error("node execution {node_execution_id} was already started with different input")]
    AgentNodeIdempotencyConflict { node_execution_id: NodeExecutionId },

    /// The ACP worker dropped its terminal responder before completion.
    #[error("AgentNode terminal responder was dropped")]
    AgentNodeCompletionDropped,

    /// A user turn exceeded the maximum payload size accepted by Discuss.
    #[error("Discuss user turn is too large: {actual_bytes} bytes (maximum {max_bytes} bytes)")]
    UserTurnTooLarge {
        /// Size of the rejected UTF-8 payload in bytes.
        actual_bytes: u64,
        /// Maximum accepted UTF-8 payload size in bytes.
        max_bytes: u64,
    },

    /// Pending permission not found, already resolved, stale, or duplicate.
    ///
    /// Covers unknown, duplicate, stale, and already-resolved pending entries.
    #[error(
        "permission not found or already resolved: discuss_session_id={discuss_session_id}, permission_request_id={permission_request_id}"
    )]
    PermissionNotFound {
        /// Session id of the answer target.
        discuss_session_id: DiscussSessionId,
        /// Permission request id of the answer.
        permission_request_id: String,
    },

    /// Cross-session permission answer: the pending permission belongs to a
    /// different `discuss_session_id` than the answer targets.
    #[error("permission session mismatch: answer for {answer_session_id} but pending belongs to {pending_session_id}")]
    SessionMismatch {
        /// Session id the answer was sent to.
        answer_session_id: DiscussSessionId,
        /// Session id the pending permission belongs to.
        pending_session_id: DiscussSessionId,
    },

    /// The selected permission option was not offered by the provider.
    ///
    /// The pending entry is **not** consumed on this error — it stays live so
    /// the user can retry with a valid option.
    #[error("invalid permission option: {option_id} was not offered")]
    InvalidOption {
        /// The rejected option id.
        option_id: String,
    },

    /// The ACP elicitation request was malformed or otherwise invalid.
    #[error("invalid elicitation request: {message}")]
    ElicitationInvalidRequest {
        /// Safe validation detail.
        message: String,
    },

    /// The accepted elicitation content does not satisfy its restricted form.
    /// The pending request remains live so the user can correct and resubmit.
    #[error("invalid elicitation content: {message}")]
    ElicitationInvalidContent {
        /// Safe validation detail.
        message: String,
    },

    /// The elicitation target session no longer exists.
    #[error("elicitation session not found: discuss_session_id={discuss_session_id}")]
    ElicitationSessionNotFound {
        /// Session targeted by the request.
        discuss_session_id: DiscussSessionId,
    },

    /// The elicitation request is no longer pending.
    #[error(
        "elicitation request not pending: discuss_session_id={discuss_session_id}, elicitation_request_id={elicitation_request_id}"
    )]
    ElicitationRequestNotPending {
        /// Session targeted by the answer.
        discuss_session_id: DiscussSessionId,
        /// Elicitation request targeted by the answer.
        elicitation_request_id: ElicitationRequestId,
    },

    /// The elicitation request has already been answered.
    #[error(
        "elicitation request already answered: discuss_session_id={discuss_session_id}, elicitation_request_id={elicitation_request_id}"
    )]
    ElicitationAlreadyAnswered {
        /// Session targeted by the answer.
        discuss_session_id: DiscussSessionId,
        /// Elicitation request targeted by the answer.
        elicitation_request_id: ElicitationRequestId,
    },

    /// Persistence drain failure during shutdown: the ordered persistence
    /// queue could not drain accepted writes before the shutdown deadline.
    #[error("persistence drain failed: {message}")]
    PersistenceDrainFailed {
        /// Safe human-readable drain failure message.
        message: String,
    },

    /// Worker stop timeout during shutdown: a live session worker did not
    /// terminate within the shutdown deadline.
    #[error("worker stop timeout for discuss_session_id={discuss_session_id}")]
    WorkerStopTimeout {
        /// Session id of the worker that timed out.
        discuss_session_id: DiscussSessionId,
    },

    /// Discuss server is shutting down; new session work is rejected.
    #[error("Discuss server is shutting down")]
    ShuttingDown,

    /// A Provider runtime could not be initialized or maintained.
    #[error("Provider runtime failure for {provider_name}: {message}")]
    ProviderRuntime {
        /// Exact installed Provider identifier.
        provider_name: String,
        /// Safe runtime failure detail.
        message: String,
    },

    /// A Provider runtime rejected admission for new work.
    #[error("Provider admission failure for {provider_name}: {message}")]
    ProviderAdmission {
        /// Exact installed Provider identifier.
        provider_name: String,
        /// Safe admission failure detail.
        message: String,
    },

    /// A Provider process or connection exited unexpectedly.
    #[error("Provider process for {provider_name} exited: {message}")]
    ProviderProcessExit {
        /// Exact installed Provider identifier.
        provider_name: String,
        /// Safe process-exit detail.
        message: String,
    },

    /// A Provider exhausted its automatic restart budget.
    #[error("Provider {provider_name} exhausted its automatic restart budget")]
    ProviderRestartExhausted {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// Work was requested from a Provider that is not running.
    #[error("Provider {provider_name} is not running")]
    ProviderNotRunning {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// The requested agent key is not registered in the Discuss server.
    #[error("unknown Discuss agent key `{agent_key}`; register the agent before starting a session")]
    UnknownAgent {
        /// The agent key that was not found.
        agent_key: String,
    },

    /// Accounting is not currently available.
    #[error("Accounting unavailable")]
    AccountingUnavailable,
    /// The requested report range is invalid.
    #[error("Invalid report range")]
    ReportRangeInvalid,
    /// A report aggregation exceeded its numeric bounds.
    #[error("Report aggregation overflow")]
    ReportAggregationOverflow,
    /// The custom asset does not exist.
    #[error("Unknown custom asset")]
    UnknownCustomAsset,
    /// The custom asset is still referenced by the listed providers or teams.
    #[error("Custom asset is referenced: {references:?}")]
    CustomAssetReferenced { references: Vec<String> },
    /// The custom asset identifier collides with another asset.
    #[error("Custom asset id collision")]
    CustomAssetIdCollision,
    /// The requested custom agent is not installed.
    #[error("Custom agent is not installed")]
    CustomAgentNotInstalled,
    /// The team leader already identifies another team.
    #[error("Team leader is in use")]
    TeamLeaderInUse,
    /// An asset import failed at a named stage.
    #[error("Asset import failed at {stage}: {cause}")]
    AssetImport { stage: String, cause: String },
    /// An MCP server name appears more than once.
    #[error("Duplicate MCP server name")]
    DuplicateMcpServerName,
    /// An MCP server name is reserved.
    #[error("Reserved MCP server name")]
    ReservedMcpName,
    /// Provider-managed configuration failed validation.
    #[error("Managed config for {plugin} is invalid: {reason}")]
    ManagedConfigValidation { plugin: String, reason: String },
    /// A usage value exceeded its supported range.
    #[error("Usage value out of range")]
    UsageValueOutOfRange,
    /// Import destination address was blocked.
    #[error("Import address blocked")]
    ImportAddressBlocked,

    /// Legacy IPC boundary fallback for errors that do not yet have a structured variant.
    #[error("{0}")]
    Other(
        /// Safe human-readable fallback error message.
        String,
    ),
}

impl DiscussError {
    /// Stable client-facing error code for the Discuss domain.
    pub fn rpc_code(&self) -> &'static str {
        match self {
            DiscussError::SessionIdentityUnknown => "SessionIdentityUnknown",
            DiscussError::SessionIdentityUnassigned => "SessionIdentityUnassigned",
            DiscussError::SessionIdentityAmbiguous => "SessionIdentityAmbiguous",
            DiscussError::SessionIdentityInvalidIssuer => "SessionIdentityInvalidIssuer",
            DiscussError::SessionCliLoginUnsupported => "SessionCliLoginUnsupported",
            DiscussError::SessionCliToolUnsupported => "SessionCliToolUnsupported",
            DiscussError::ChannelSend => "ChannelSend",
            DiscussError::ChannelRecv => "ChannelRecv",
            DiscussError::OneshotReply => "OneshotReply",
            DiscussError::SessionCancelled { .. } => "SessionCancelled",
            DiscussError::ProtocolError { .. } => "ProtocolError",
            DiscussError::ProviderSessionNotFound => "ProviderSessionNotFound",
            DiscussError::AuthenticationRequired => "AuthenticationRequired",
            DiscussError::WorkerTokenServiceUnavailable => "WorkerTokenServiceUnavailable",
            DiscussError::AgentNodeTokenRequired => "AgentNodeTokenRequired",
            DiscussError::WorkerTokenIssuance { .. } => "WorkerTokenIssuance",
            DiscussError::Config { .. } => "Config",
            DiscussError::Bus { .. } => "Bus",
            DiscussError::Serialization { .. } => "Serialization",
            DiscussError::McpSetup { .. } => "McpSetup",
            DiscussError::Infrastructure { .. } => "Infrastructure",
            DiscussError::Persistence { .. } => "Persistence",
            DiscussError::WorkspaceIo { .. } => "WorkspaceIo",
            DiscussError::WorkspaceSerialize { .. } => "WorkspaceSerialize",
            DiscussError::WorkspaceInvalidAssetId { .. } => "WorkspaceInvalidAssetId",
            DiscussError::InvalidWorkspaceCopySource { .. } => "InvalidWorkspaceCopySource",
            DiscussError::SessionNotFound { .. } => "SessionNotFound",
            DiscussError::SessionBusy { .. } => "SessionBusy",
            DiscussError::UnsupportedCapability { .. } => "UnsupportedCapability",
            DiscussError::InspectorUnavailable { .. } => "InspectorUnavailable",
            DiscussError::InspectorLoadFailed { .. } => "InspectorLoadFailed",
            DiscussError::Validation { .. } => "Validation",
            DiscussError::AgentNodeLimitReached { .. } => "AgentNodeLimitReached",
            DiscussError::AgentNodeCompletionNotFound { .. } => "AgentNodeCompletionNotFound",
            DiscussError::AgentNodeLimitInvalid { .. } => "AgentNodeLimitInvalid",
            DiscussError::AgentNodeIdempotencyConflict { .. } => "AgentNodeIdempotencyConflict",
            DiscussError::AgentNodeCompletionDropped => "AgentNodeCompletionDropped",
            DiscussError::UserTurnTooLarge { .. } => "UserTurnTooLarge",
            DiscussError::PermissionNotFound { .. } => "PermissionNotFound",
            DiscussError::SessionMismatch { .. } => "SessionMismatch",
            DiscussError::InvalidOption { .. } => "InvalidOption",
            DiscussError::ElicitationInvalidRequest { .. } => "InvalidRequest",
            DiscussError::ElicitationInvalidContent { .. } => "InvalidContent",
            DiscussError::ElicitationSessionNotFound { .. } => "SessionNotFound",
            DiscussError::ElicitationRequestNotPending { .. } => "RequestNotPending",
            DiscussError::ElicitationAlreadyAnswered { .. } => "AlreadyAnswered",
            DiscussError::PersistenceDrainFailed { .. } => "PersistenceDrainFailed",
            DiscussError::WorkerStopTimeout { .. } => "WorkerStopTimeout",
            DiscussError::ShuttingDown => "ShuttingDown",
            DiscussError::ProviderRuntime { .. } => "ProviderRuntime",
            DiscussError::ProviderAdmission { .. } => "ProviderAdmission",
            DiscussError::ProviderProcessExit { .. } => "ProviderProcessExit",
            DiscussError::ProviderRestartExhausted { .. } => "ProviderRestartExhausted",
            DiscussError::ProviderNotRunning { .. } => "ProviderNotRunning",
            DiscussError::UnknownAgent { .. } => "UnknownAgent",
            DiscussError::AccountingUnavailable => "AccountingUnavailable",
            DiscussError::ReportRangeInvalid => "ReportRangeInvalid",
            DiscussError::ReportAggregationOverflow => "ReportAggregationOverflow",
            DiscussError::UnknownCustomAsset => "UnknownCustomAsset",
            DiscussError::CustomAssetReferenced { .. } => "CustomAssetReferenced",
            DiscussError::CustomAssetIdCollision => "CustomAssetIdCollision",
            DiscussError::CustomAgentNotInstalled => "CustomAgentNotInstalled",
            DiscussError::TeamLeaderInUse => "TeamLeaderInUse",
            DiscussError::AssetImport { .. } => "AssetImport",
            DiscussError::DuplicateMcpServerName => "DuplicateMcpServerName",
            DiscussError::ReservedMcpName => "ReservedMcpName",
            DiscussError::ManagedConfigValidation { .. } => "ManagedConfigValidation",
            DiscussError::UsageValueOutOfRange => "UsageValueOutOfRange",
            DiscussError::ImportAddressBlocked => "ImportAddressBlocked",
            DiscussError::Other(_) => "Other",
        }
    }
}

impl From<SharedInfraError> for DiscussError {
    fn from(error: SharedInfraError) -> Self {
        let (kind, target_domain, operation, message) = match error {
            SharedInfraError::ChannelSend {
                target_domain,
                operation,
                cause,
            } => ("channel_send", target_domain, operation, cause),
            SharedInfraError::ChannelRecv {
                target_domain,
                operation,
                cause,
            } => ("channel_recv", target_domain, operation, cause),
            SharedInfraError::OneshotReplyTimeout {
                target_domain,
                operation,
                timeout,
            } => ("oneshot_reply_timeout", target_domain, operation, timeout),
            SharedInfraError::OneshotReplyMissing {
                target_domain,
                operation,
                cause,
            } => ("oneshot_reply_missing", target_domain, operation, cause),
        };
        Self::Infrastructure {
            kind: kind.to_owned(),
            target_domain,
            operation,
            message,
        }
    }
}

impl From<GeneratedCallError<DiscussError>> for DiscussError {
    fn from(error: GeneratedCallError<DiscussError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => Self::Bus {
                message: error.to_string(),
            },
            GeneratedCallError::InputClosed => Self::ChannelSend,
            GeneratedCallError::ReplyDropped => Self::OneshotReply,
            GeneratedCallError::UnexpectedResponse { expected, actual } => Self::Bus {
                message: format!("expected {expected}, got {actual}"),
            },
        }
    }
}

impl From<DispatchContextError> for DiscussError {
    fn from(error: DispatchContextError) -> Self {
        Self::Bus {
            message: error.to_string(),
        }
    }
}

impl rspc::Error for DiscussError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        let safe_error = json!({
            "DomainError": {
                "domain": "discuss",
                "code": self.rpc_code(),
                "message": self.to_string(),
                "details": {},
            }
        });
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(safe_error, None::<Self>))
    }
}

impl From<AvesAuthError> for DiscussError {
    fn from(error: AvesAuthError) -> Self {
        DiscussError::WorkerTokenIssuance {
            message: error.to_string(),
        }
    }
}

impl From<AcpError> for DiscussError {
    fn from(err: AcpError) -> Self {
        if err.code == AcpErrorCode::AUTH_REQUIRED {
            DiscussError::AuthenticationRequired
        } else if err.code == AcpErrorCode::RESOURCE_NOT_FOUND && err.message == PROVIDER_SESSION_MISSING {
            DiscussError::ProviderSessionNotFound
        } else {
            DiscussError::ProtocolError { message: err.message }
        }
    }
}

pub(super) const PROVIDER_SESSION_MISSING: &str = "The saved provider session no longer exists";

impl From<BusControlError> for DiscussError {
    fn from(error: BusControlError) -> Self {
        DiscussError::McpSetup {
            message: error.to_string(),
        }
    }
}

/// Map typed Turso failure variants to stable, low-cardinality DB codes.
fn turso_error_code(err: &turso::Error) -> &'static str {
    match err {
        turso::Error::Constraint(_) => "db_constraint",
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_) | turso::Error::Interrupt(_) => "db_timeout",
        turso::Error::Readonly(_)
        | turso::Error::DatabaseFull(_)
        | turso::Error::NotAdb(_)
        | turso::Error::Corrupt(_) => "db_unavailable",
        _ => "db_query",
    }
}

impl From<turso::Error> for DiscussError {
    fn from(err: turso::Error) -> Self {
        let code = turso_error_code(&err);
        DiscussError::Persistence {
            code: code.to_string(),
            message: err.to_string(),
        }
    }
}

impl From<JsonError> for DiscussError {
    fn from(err: JsonError) -> Self {
        DiscussError::Serialization {
            message: err.to_string(),
        }
    }
}

impl From<LibraryError> for DiscussError {
    fn from(error: LibraryError) -> Self {
        match error {
            LibraryError::InvalidAssetId { asset_id, violation } => DiscussError::WorkspaceInvalidAssetId {
                asset_id,
                violation: violation.to_string(),
            },
        }
    }
}

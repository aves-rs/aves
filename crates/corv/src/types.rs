use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    CreateElicitationRequest, McpServer, PermissionOptionId, RequestPermissionRequest, SessionId, StopReason,
};
use serde::{Deserialize, Serialize};
use specta::{DataType, Generics, Type, TypeCollection, datatype::reference::Reference};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use aves::auth::{AvesAuthError, Jwt};
use aves::bus::{BusControlError, BusMcpEndpoint, DispatchContextError};
use aves::flow::NodeExecutionId;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;
use aves::utils::uuid::SpectaUuid;

use super::library::{AgentId, SkillId};

pub(super) use super::elicitation::{
    DiscussElicitationAnswer, DiscussElicitationClassification, DiscussElicitationRequestPayload,
};

/// Persisted launch specification and session defaults for an installed ACP Provider.
#[derive(Debug, Clone, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct AcpProvider {
    #[serde(flatten)]
    server: McpServer,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    default_session_config: serde_json::Value,
    #[serde(default)]
    sluice_mcp_enabled: bool,
    /// Editable paths relative to the agents workspace; None uses provider defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_copy_sources: Option<Vec<String>>,
}

impl AcpProvider {
    pub fn server(&self) -> &McpServer {
        &self.server
    }

    pub fn sluice_mcp_enabled(&self) -> bool {
        self.sluice_mcp_enabled
    }

    pub fn default_session_config(&self) -> &serde_json::Value {
        &self.default_session_config
    }

    pub fn replace_default_session_config(&mut self, value: serde_json::Value) {
        self.default_session_config = value;
    }

    pub(crate) fn workspace_copy_sources(&self) -> Option<&[String]> {
        self.workspace_copy_sources.as_deref()
    }

    pub(crate) fn validate_workspace_copy_sources(&self) -> Result<(), DiscussError> {
        for source in self.workspace_copy_sources.as_deref().unwrap_or_default() {
            let path = Path::new(source);
            let windows_drive = source.as_bytes().get(1) == Some(&b':')
                && source
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic);
            let invalid = if source.is_empty() || source.contains('\\') || source.contains('\0') || windows_drive {
                Some("must be a nonempty portable relative path")
            } else if path.is_absolute() {
                Some("must be relative to the agents workspace")
            } else if path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            }) {
                Some("must not contain parent or root components")
            } else if !path
                .components()
                .any(|component| matches!(component, Component::Normal(_)))
            {
                Some("must not select the whole agents workspace")
            } else if path
                .components()
                .filter(|component| matches!(component, Component::Normal(_)))
                .count()
                == 1
                && path
                    .components()
                    .any(|component| component.as_os_str() == ".agents")
            {
                Some(".agents is always copied and cannot be edited")
            } else {
                None
            };
            if let Some(violation) = invalid {
                return Err(DiscussError::InvalidWorkspaceCopySource {
                    path: source.clone(),
                    violation: violation.to_string(),
                });
            }
        }
        Ok(())
    }
}

impl Type for AcpProvider {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        serde_json::Value::inline(type_map, generics)
    }
}

impl From<AcpProvider> for McpServer {
    fn from(val: AcpProvider) -> Self {
        val.server
    }
}

impl From<McpServer> for AcpProvider {
    fn from(server: McpServer) -> Self {
        Self {
            server,
            default_session_config: serde_json::Value::Null,
            sluice_mcp_enabled: false,
            workspace_copy_sources: None,
        }
    }
}

/// Bounds for a non-interactive AgentNode session, resolved by Discuss.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AgentNodeSessionLimits {
    pub max_turns: u64,
    pub max_tool_calls: u64,
    pub max_duration_secs: u64,
}

/// ACP work for a Flow AgentNode after its execution limits have been validated.
pub(super) struct TaskSessionStart {
    pub(super) provider_name: String,
    pub(super) agent_name: String,
    pub(super) config_overrides: DiscussConfigValues,
    pub(super) sluice_mcp_enabled: bool,
    pub(super) prompt: String,
    pub(super) session_cwd: PathBuf,
    pub(super) limits: AgentNodeSessionLimits,
    pub(super) node_token: Jwt,
    pub(super) flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
}

/// Real ACP terminal result for a non-interactive task session.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "status")]
pub(super) enum AgentNodeTerminalResult {
    Succeeded { output: serde_json::Value },
    Failed { error: DiscussError },
}

/// Immediate tick result while a task session runs or completes.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum AgentNodeTickResult {
    Running,
    Succeeded { output: serde_json::Value },
    Failed { error: DiscussError },
}

impl From<AgentNodeTerminalResult> for AgentNodeTickResult {
    fn from(terminal: AgentNodeTerminalResult) -> Self {
        match terminal {
            AgentNodeTerminalResult::Succeeded { output } => Self::Succeeded { output },
            AgentNodeTerminalResult::Failed { error } => Self::Failed { error },
        }
    }
}

pub(super) fn resolve_agent_node_limits(
    max_turns: Option<i64>,
    max_tool_calls: Option<i64>,
    max_duration_secs: Option<i64>,
) -> Result<AgentNodeSessionLimits, DiscussError> {
    const DEFAULT_MAX_TURNS: u64 = 1;
    const DEFAULT_MAX_TOOL_CALLS: u64 = 100;
    const DEFAULT_MAX_DURATION_SECS: u64 = 1_800;
    const MAX_TURNS: u64 = 100;
    const MAX_TOOL_CALLS: u64 = 1_000;
    const MAX_DURATION_SECS: u64 = 1_800;

    fn resolve(name: &'static str, value: Option<i64>, default: u64, maximum: u64) -> Result<u64, DiscussError> {
        let Some(value) = value else {
            return Ok(default);
        };
        let resolved = u64::try_from(value).map_err(|_| DiscussError::AgentNodeLimitInvalid {
            limit: name.to_owned(),
            value,
            maximum,
        })?;
        if resolved == 0 || resolved > maximum {
            return Err(DiscussError::AgentNodeLimitInvalid {
                limit: name.to_owned(),
                value,
                maximum,
            });
        }
        Ok(resolved)
    }

    Ok(AgentNodeSessionLimits {
        max_turns: resolve("maxTurns", max_turns, DEFAULT_MAX_TURNS, MAX_TURNS)?,
        max_tool_calls: resolve("maxToolCalls", max_tool_calls, DEFAULT_MAX_TOOL_CALLS, MAX_TOOL_CALLS)?,
        max_duration_secs: resolve(
            "maxDuration",
            max_duration_secs,
            DEFAULT_MAX_DURATION_SECS,
            MAX_DURATION_SECS,
        )?,
    })
}

/// Actor-resolved copy selections for all Providers referenced by a Flow graph.
#[derive(Clone)]
pub(super) struct WorkspaceCopySources {
    pub(super) agents_workspace: PathBuf,
    pub(super) sources_by_provider: BTreeMap<String, Vec<String>>,
}

pub(super) struct WorkspaceCopySourceRequest {
    pub(super) provider_names: BTreeSet<String>,
    pub(super) reply_to: oneshot::Sender<Result<WorkspaceCopySources, DiscussError>>,
}

pub(super) struct ActorInboundPorts {
    pub(super) workspace_copy_source_rx: mpsc::Receiver<WorkspaceCopySourceRequest>,
}

/// Discuss-owned, actor-serialized source selection for fresh Flow workspaces.
pub(super) struct WorkspaceCopySourcePort {
    sender: mpsc::Sender<WorkspaceCopySourceRequest>,
}

impl WorkspaceCopySourcePort {
    pub(super) fn channel(capacity: usize) -> (Arc<Self>, mpsc::Receiver<WorkspaceCopySourceRequest>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (Arc::new(Self { sender }), receiver)
    }

    /// Resolve effective relative paths without reading or mutating the workspace.
    pub(super) async fn resolve(&self, provider_names: BTreeSet<String>) -> Result<WorkspaceCopySources, DiscussError> {
        let (reply_to, reply_rx) = oneshot::channel();
        self.sender
            .send(WorkspaceCopySourceRequest {
                provider_names,
                reply_to,
            })
            .await
            .map_err(|_| DiscussError::ShuttingDown)?;
        reply_rx.await.map_err(|_| DiscussError::ShuttingDown)?
    }
}

// ── Desktop-facing DTOs ────────────────────────────────────────────────────
//
// The DTOs below are the wire-facing projections of the Discuss session
// metadata columns and the bounded transcript rows. They are keyed by
// `sluice_session_id` (UUID v4) — the single Sluice-owned session identity.
// `session_name` and `agent_name` are human-readable metadata, never routing
// keys.

// ── Sluice-owned session identity ──────────────────────────────────────────

/// Sluice-owned canonical session identity (UUID v4).
///
/// Generated by the caller (Hades) before opening the Tauri channel and used
/// as the single routing key for all Discuss operations. Only the Discuss
/// domain translates between this and the provider `acp_session_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ::schemars::JsonSchema)]
#[serde(transparent)]
pub struct SluiceSessionId(
    /// The UUID v4 value.
    #[schemars(with = "String")]
    pub SpectaUuid,
);

impl Type for SluiceSessionId {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        SpectaUuid::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        SpectaUuid::reference(type_map, generics)
    }
}

/// The in-memory binding returned for an ephemeral child-session inspector.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InspectorBinding {
    /// Ephemeral Sluice identity used to route inspector stream events.
    pub viewer_session_id: SluiceSessionId,
    /// Parent Sluice chat identity.
    pub parent_session_id: SluiceSessionId,
    /// Provider ACP child-session identity.
    pub child_session_id: String,
}

/// The selected Sluice MCP transport for one live session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DiscussMcpStatus {
    /// Sluice MCP is disabled in the saved agent configuration.
    Off,
    /// Sluice MCP is provisioned through native ACP.
    Acp,
    /// Sluice MCP is provisioned through the authenticated HTTP relay.
    Fallback,
    /// Sluice MCP was enabled but could not be provisioned or was lost.
    Broken,
}

impl SluiceSessionId {
    /// Generate a fresh random UUID v4 session id.
    pub fn new() -> Self {
        Self(SpectaUuid(Uuid::new_v4()))
    }
}

impl Default for SluiceSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SluiceSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for SluiceSessionId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse::<SpectaUuid>()?))
    }
}

/// Opaque typed ID for one pending permission request.
///
/// Generated by the Discuss actor when a provider permission request arrives
/// and used as the key (alongside `sluice_session_id`) for the pending
/// permission registry. It is opaque to Zeus/Hades — they carry it unchanged
/// in the `AnswerPermission` bus request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ::schemars::JsonSchema)]
#[serde(transparent)]
pub struct PermissionRequestId(
    /// The UUID v4 value.
    #[schemars(with = "String")]
    pub Uuid,
);

impl Type for PermissionRequestId {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl PermissionRequestId {
    /// Generate a fresh random permission request id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for PermissionRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PermissionRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque typed ID for one pending elicitation request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ::schemars::JsonSchema)]
#[serde(transparent)]
pub struct ElicitationRequestId(
    /// The UUID v4 value.
    #[schemars(with = "String")]
    pub Uuid,
);

impl Type for ElicitationRequestId {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl ElicitationRequestId {
    /// Generate a fresh elicitation request id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ElicitationRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ElicitationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The caller's answer to a pending permission request.
///
/// `Select` carries the provider option id the user chose; `Cancel` resolves
/// the provider responder as cancelled. The actor validates that a `Select`
/// option was actually offered before resolving the pending entry.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DiscussPermissionOption {
    /// The user selected one of the offered provider options.
    Select {
        /// Provider option id chosen by the user.
        option_id: String,
    },
    /// The user cancelled the permission request.
    Cancel,
}

/// Provider-advertised terminal authentication retained for the native bridge.
///
/// This is an internal Poseidon-to-Zeus contract. Hades receives only the safe
/// method identity and copy; the executable, arguments, and environment stay
/// behind the native process boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscussTerminalAuthMethod {
    /// Opaque Provider-owned authentication method id.
    pub id: String,
    /// Provider-owned human-readable method name.
    pub name: String,
    /// Optional Provider-owned explanatory copy.
    pub description: Option<String>,
    /// Provider-advertised label for the native terminal window.
    pub label: String,
    /// Exact Provider executable retained only for Zeus.
    pub command: PathBuf,
    /// Exact Provider arguments retained only for Zeus.
    pub args: Vec<String>,
    /// Provider launch and authentication environment retained only for Zeus.
    pub env: BTreeMap<String, String>,
}

/// Lifecycle state of one provider-reported todo item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussTodoStatus {
    /// The todo has not started.
    Pending,
    /// The todo is currently being worked on.
    InProgress,
    /// The todo was completed.
    Completed,
    /// The todo was cancelled.
    Cancelled,
}

/// One ordered item in a provider-neutral todo snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussTodoItem {
    /// Provider-neutral todo text.
    pub content: String,
    /// Current lifecycle state.
    pub status: DiscussTodoStatus,
}

/// The current ordered todo list reported for a parent session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussTodoSnapshot {
    /// Ordered todo items; priority is intentionally not part of this contract.
    pub items: Vec<DiscussTodoItem>,
}

/// Surface state for a provider-reported child session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussSubagentSurfaceState {
    /// The child session is being created.
    Loading,
    /// The child session is live and running.
    Live,
    /// The child session completed successfully.
    Done,
    /// The child session reported an error.
    Error,
}

/// One provider-neutral child subagent record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussSubagentRecord {
    /// Stable capture identity sourced from the provider tool call id.
    pub capture_id: String,
    /// Child harness session identity, unavailable until the provider commits it.
    pub child_session_id: Option<String>,
    /// Human-readable Agent name.
    pub agent_name: String,
    /// Optional provider-supplied description or local fallback.
    pub description: Option<String>,
    /// Optional capability used when the provider supplies no description.
    pub fallback_capability: Option<String>,
    /// Current UI-facing child-session state.
    pub surface_state: DiscussSubagentSurfaceState,
    /// Unix timestamp in milliseconds of the latest provider update.
    pub last_updated_at: i64,
}

/// Provider-neutral utility update associated with a parent Sluice session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussUtilityUpdate {
    /// Parent Sluice session identity used for desktop routing.
    pub sluice_session_id: SluiceSessionId,
    /// Discriminated utility payload.
    pub payload: DiscussUtilityPayload,
}

/// Discriminated provider-neutral utility payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DiscussUtilityPayload {
    /// Replacement snapshot of the parent's ordered todos.
    TodoSnapshot { snapshot: DiscussTodoSnapshot },
    /// Current record for one child subagent.
    Subagent { subagent: DiscussSubagentRecord },
}

pub type ConfigSettings = BTreeMap<String, String>;
pub type RetainedAgentsByScope = BTreeMap<&'static str, BTreeSet<AgentId>>;
pub type RetainedSkillsByScope = BTreeMap<&'static str, BTreeSet<SkillId>>;
pub type RetainedTeamAssetsByScope = (RetainedAgentsByScope, RetainedSkillsByScope);

// ── DiscussStreamEvent ─────────────────────────────────────────────────────

/// The single bounded stream event from Discuss session workers to Zeus.
///
/// Replaces the former three fixed channel pairs (`msg_from_agent`,
/// `req_from_agent`, `notif_from_agent`). Zeus owns the receiver; Poseidon
/// receives only the sender during construction and clones it into every
/// spawned/recovered session worker. Multiple worker producers multiplex
/// into Zeus's single receiver.
///
/// Live protocol variants carry both `sluice_session_id` (the routing key) and
/// `acp_session_id` (protocol/diagnostic data). A startup failure may happen
/// before the provider allocates an ACP id, so it carries the Sluice identity
/// and the typed Discuss error only. Zeus routes exclusively by
/// `sluice_session_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DiscussStreamEvent {
    /// A session worker failed before or after provider association.
    ///
    /// This reliable terminal event prevents desktop clients from waiting on
    /// a lossy readiness timeout after Poseidon already knows the failure.
    SessionFailed {
        /// Sluice-owned canonical session identity (routing key).
        sluice_session_id: SluiceSessionId,
        /// Typed Discuss failure retained until the Zeus presentation boundary.
        error: DiscussError,
        /// Provider-advertised terminal recovery methods captured during initialize.
        #[serde(default)]
        terminal_auth_methods: Vec<DiscussTerminalAuthMethod>,
    },
    /// An agent message (content delta, tool update, metadata update, or
    /// turn-ending stop reason) forwarded from the provider to the user.
    AgentMessage {
        /// Sluice-owned canonical session identity (routing key).
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data, not a routing key).
        acp_session_id: SessionId,
        /// Typed ACP protocol payload.
        payload: AcpAgentMessagePayload,
    },
    /// A provider-neutral todo or subagent utility update.
    UtilityUpdate {
        /// Sluice-owned canonical session identity (routing key).
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data, not a routing key).
        acp_session_id: SessionId,
        /// Utility payload and its parent Sluice identity.
        utility: DiscussUtilityUpdate,
    },
    /// A live permission request from the provider that requires reliable
    /// delivery to the user and a typed answer back through the bus.
    ///
    /// The worker-response oneshot is retained inside the Discuss actor,
    /// keyed by `(sluice_session_id, permission_request_id)`. The actor
    /// inserts the pending entry into its registry **before** publishing
    /// this event so an early answer does not race the registration.
    PermissionRequested {
        /// Sluice-owned canonical session identity (routing key).
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data, not a routing key).
        acp_session_id: SessionId,
        /// Opaque typed permission request id.
        permission_request_id: PermissionRequestId,
        /// Serializable typed ACP `RequestPermissionRequest` data.
        payload: Box<RequestPermissionRequest>,
    },
    /// A live elicitation request requiring typed user input.
    ElicitationRequested {
        /// Sluice-owned canonical session identity (routing key).
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data, not a routing key).
        acp_session_id: SessionId,
        /// Opaque typed elicitation request id.
        elicitation_request_id: ElicitationRequestId,
        /// Classification and original request for opaque desktop routing.
        payload: Box<DiscussElicitationRequestPayload>,
    },
}

// ── Pending permission registry entry ──────────────────────────────────────

/// One pending permission request awaiting a typed answer from the user.
///
/// The actor owns the oneshot responder sender and keys the entry by
/// `(sluice_session_id, permission_request_id)`. The entry is removed and
/// resolved atomically only after a valid answer; an invalid option leaves
/// the entry live and returns a typed error.
pub(crate) struct PendingPermission {
    /// The offered permission options, captured at request time so the actor
    /// can validate that a `Select` answer was actually offered.
    pub(crate) offered_option_ids: Vec<PermissionOptionId>,
    /// The oneshot sender that resolves the provider responder.
    pub(crate) responder_tx: oneshot::Sender<DiscussPermissionOption>,
}

/// One pending elicitation request awaiting a typed answer from the user.
pub(crate) struct PendingElicitation {
    /// Reserved transcript position, allocated when the request is received.
    pub(crate) position: i64,
    /// The provider responder resolved by the actor after validation.
    pub(crate) responder_tx: oneshot::Sender<DiscussElicitationAnswer>,
    /// Raw request data retained for the elicitation lifecycle and fallback path.
    pub(crate) raw_request: CreateElicitationRequest,
    /// Canonical restricted-form classification consumed by the answer path.
    pub(crate) classification: DiscussElicitationClassification,
}

// ── Worker registry entry ──────────────────────────────────────────────────

/// One live session worker tracked by the actor.
///
/// The actor keys workers by `sluice_session_id`. The `command_tx` sends
/// `WorkerCommand`s to the worker; the `join_handle` lets `StopServer`
/// request stop, await termination, and confirm drain.
pub(crate) struct WorkerEntry {
    /// JTI of the delegated worker token, retained for terminal-path revocation.
    pub(crate) worker_jti: Option<uuid::Uuid>,
    /// Exact installed Provider key that owns this worker.
    pub(crate) agent_name: String,
    /// Sender for commands routed from the actor to the worker.
    pub(crate) command_tx: mpsc::Sender<WorkerCommand>,
    /// Join handle for the spawned worker task, used by `StopServer` to
    /// await termination.
    pub(crate) join_handle: tokio::task::JoinHandle<()>,
    /// Provider ACP session id once the worker has completed setup.
    pub(crate) acp_session_id: Option<SessionId>,
    /// Whether Zeus is waiting for this worker's desktop channel association.
    pub(crate) desktop_association_requested: bool,
}

/// Private command enum for the worker command channel.
///
/// Routed through the `WorkerRegistry` from the actor to a live session worker.
/// Each variant carries a `oneshot::Sender` for the typed reply (except
/// `Stop`). Channel failures route through the existing
/// Channel failures become `DiscussError::Infrastructure` at the call site.
#[derive(Debug)]
pub(crate) enum WorkerCommand {
    /// Set a provider-exposed config option on the active session.
    SetConfigOption {
        config_id: String,
        /// ACP `SessionConfigValueId` (string) forwarded verbatim to the provider.
        value: String,
        reply: oneshot::Sender<Result<(), DiscussError>>,
    },
    /// List provider-side sessions for the active session's agent.
    ListProviderSessions {
        reply: oneshot::Sender<Result<Vec<ProviderSessionInfo>, DiscussError>>,
    },
    /// Load a provider-side session by its ACP session id.
    LoadProviderSession {
        acp_session_id: SessionId,
        reply: oneshot::Sender<Result<ProviderSessionInfo, DiscussError>>,
    },
    /// Attach a viewer to a provider child session on this worker's connection.
    AttachInspector {
        viewer_session_id: SluiceSessionId,
        child_session_id: SessionId,
        reply: oneshot::Sender<Result<(), DiscussError>>,
    },
    /// Dispose a viewer attached to this worker without cancelling its ACP session.
    DisposeInspector {
        viewer_session_id: SluiceSessionId,
        reply: oneshot::Sender<Result<(), DiscussError>>,
    },
    /// Send a user turn (prompt text) to the worker.
    SendUserTurn {
        text: String,
        reply: oneshot::Sender<Result<(), DiscussError>>,
    },
    /// Send the single non-interactive AgentNode turn and report its real ACP boundary.
    RunAgentNodeTurn {
        text: String,
        limits: AgentNodeSessionLimits,
        accepted: oneshot::Sender<Result<(), DiscussError>>,
        terminal: oneshot::Sender<AgentNodeTerminalResult>,
    },
    /// Cancel the active turn without closing the session.
    CancelTurn {
        reply: oneshot::Sender<Result<(), DiscussError>>,
    },
    /// Request the worker to stop and exit.
    Stop,
}

/// A task session accepted by Discuss, with its ACP terminal delivered to Tasks.
pub(super) struct TaskSessionStarted {
    pub(super) session_id: SluiceSessionId,
    pub(super) terminal: oneshot::Receiver<AgentNodeTerminalResult>,
}

/// Provider-side session info returned by `session/list` or `session/load`.
///
/// Carries the fields needed to merge provider sessions into the local
/// session table. `session_id` is the ACP protocol session id;
/// `updated_at` is optional provider-supplied metadata.
///
/// `config_options` carries the provider-advertised config options refreshed by
/// a `session/load`. It is `None` when the response path cannot supply
/// options (e.g. `session/list`), so the load-refresh merge preserves cached
/// options rather than clearing them.
#[derive(Clone, Debug)]
pub(crate) struct ProviderSessionInfo {
    /// Provider protocol identity; never use this as a Sluice routing key.
    pub(crate) session_id: SessionId,
    pub(crate) updated_at: Option<String>,
    pub(crate) config_options: Option<Vec<DiscussConfigOption>>,
}

/// Registry of live worker entries keyed by `sluice_session_id`.
pub(crate) type WorkerRegistry = HashMap<SluiceSessionId, WorkerEntry>;

/// Pending permission registry keyed by `(sluice_session_id, permission_request_id)`.
pub(crate) type PendingPermissionRegistry = HashMap<(SluiceSessionId, PermissionRequestId), PendingPermission>;

// ── Session metadata DTOs ──────────────────────────────────────────────────

/// Session metadata for a persisted Discuss chat row.
///
/// Carries the extended metadata columns so the session dropdown and load
/// path can restore cached state in one round-trip. The canonical identity is
/// `sluice_session_id`; `session_name` and `agent_name` are human-readable
/// metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussSessionInfo {
    /// True for sessions created by a Flow Agent Task, not an interactive chat.
    #[serde(default)]
    pub is_task_session: bool,
    /// Stable Phanes leader identity; absent for provider-only sessions.
    pub team_leader_id: Option<String>,
    /// Sluice-owned canonical session identity (UUID v4).
    pub sluice_session_id: SluiceSessionId,
    /// Human-readable session name (metadata, not a routing key).
    pub session_name: String,
    /// Agent registry name the session was started with (metadata).
    pub agent_name: String,
    /// Provider-side ACP session id, stored in `acp_session_id`.
    pub acp_session_id: Option<String>,
    /// Human-readable label from ACP `SessionInfo.title` or local fallback.
    pub display_title: Option<String>,
    /// Snippet of the most recent user prompt, for dropdown preview.
    pub last_user_prompt: Option<String>,
    /// Latest parent-session todo snapshot JSON; NULL means never captured.
    pub latest_todo_snapshot_json: Option<String>,
    /// Cached provider config options JSON blob (`latest_config_options_json`).
    pub latest_config_options_json: Option<String>,
    /// Cached available slash commands JSON blob (`available_commands_json`).
    pub available_commands_json: Option<String>,
    /// Unix-epoch ms of the first prompt in this chat.
    pub first_prompt_at: Option<i64>,
    /// Unix-epoch ms of the most recent interaction, for dropdown ordering.
    pub last_interaction_at: Option<i64>,
}

/// One bounded transcript turn for a chat, keyed by `(sluice_session_id, position)`.
///
/// The `role` is constrained by the chat turns CHECK
/// (`user`, `assistant`, `permission`, `notification`, `system`, `elicitation`). The
/// `content_json` carries the turn payload as a JSON string; the client
/// interprets it per role.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussChatTurnRecord {
    /// Sluice-owned canonical session identity (UUID v4).
    pub sluice_session_id: SluiceSessionId,
    /// Per-session turn position, owned by the worker.
    pub position: i64,
    /// Turn role (`user`, `assistant`, `permission`, `notification`, `system`, `elicitation`).
    pub role: String,
    /// Provider-side ACP session id, if known.
    pub acp_session_id: Option<String>,
    /// JSON turn payload, interpreted by the client per role.
    pub content_json: String,
    /// Unix-epoch ms when the turn was persisted.
    pub created_at: i64,
}

/// One selectable value in a provider-exposed ACP session config menu.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussConfigOptionValue {
    /// Opaque provider value identifier sent to `session/set_config_option`.
    pub value: String,
    /// Human-readable value label.
    pub label: String,
}

/// One provider-exposed ACP session config option.
///
/// The `id` and `value` are the provider's option identifiers; Hades presents
/// them under Sluice-friendly labels but persists only the provider IDs. The
/// `category` is a low-cardinality tag (`model`, `mode`, `model_config`,
/// `thought_level`, or a provider-specific string) used for UI grouping.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussConfigOption {
    /// Provider option id.
    pub id: String,
    /// Human-readable option name.
    pub name: String,
    /// Low-cardinality category tag for UI grouping.
    pub category: String,
    /// Current provider value identifier.
    pub value: String,
    /// Available selectable values, flattened from any provider groups.
    pub options: Vec<DiscussConfigOptionValue>,
}

/// Database-backed configuration state for one installed provider.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussProviderConfig {
    /// Whether a Provider session has returned a complete config menu.
    ///
    /// `true` includes a Provider-confirmed empty menu. `false` means Sluice
    /// has not discovered the Provider's settings yet.
    pub options_discovered: bool,
    /// Latest complete provider menu captured from any session for this provider.
    pub options: Vec<DiscussConfigOption>,
    /// User-selected defaults applied when a session has no explicit setting.
    pub defaults: BTreeMap<String, String>,
}

/// The lifecycle state of an installed Provider runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuntimeStatus {
    /// The Provider accepts new work.
    Running,
    /// The Provider is starting or being manually started.
    Starting,
    /// The Provider is waiting to retry after an unexpected exit.
    AutoRestarting,
    /// The Provider does not accept new work.
    Stopped,
}

/// Reduced, safe presentation of the most recent Provider runtime failure.
///
/// This DTO deliberately contains no `DiscussError`, process handle, launch
/// data, credentials, or raw Provider error payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRuntimeFailure {
    /// Stable failure code for client-side routing.
    pub code: String,
    /// Safe human-readable failure message.
    pub message: String,
}

/// Public runtime projection for one installed Provider, keyed by its exact ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRuntimeState {
    /// Exact installed Provider identifier.
    pub provider_name: String,
    /// Current process-local lifecycle state.
    pub status: ProviderRuntimeStatus,
    /// Automatic restart attempts consumed during this process lifetime.
    pub restart_count: u8,
    /// Safe presentation of the latest runtime failure, when one exists.
    pub last_failure: Option<ProviderRuntimeFailure>,
}

/// Typed Agent-specific provider config selections.
pub type DiscussConfigValues = BTreeMap<String, String>;

/// Sparse provider configuration overrides keyed by Team member Agent ID.
pub type DiscussMemberOverrides = BTreeMap<String, DiscussConfigValues>;

/// Caller intent used to configure a newly created interactive session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DiscussSessionSetup {
    /// Use one installed Team and its persisted leader configuration.
    Team {
        /// Stable Phanes Team leader identifier.
        leader_id: String,
    },
    /// Use the Provider directly with the reviewed per-session selections.
    Provider {
        /// Session-level values layered over the Provider defaults.
        config_overrides: DiscussConfigValues,
    },
}

/// Best-effort cleanup issue reported alongside a successful state transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CleanupWarning {
    /// Stable warning category.
    pub kind: String,
    /// Provider that owned the affected workspace file.
    pub provider_name: String,
    /// Agent identifier when the warning concerns one Agent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Affected workspace path when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Human-readable cleanup failure details.
    pub message: String,
}

/// Theme-backed colors available for a Phanes Agent mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AgentThemeColor {
    /// Light shade from the primary gradient.
    PrimaryLight,
    /// Primary application color.
    Primary,
    /// Dark shade from the primary gradient.
    PrimaryDark,
    /// Violet identity color.
    Violet,
    /// Plum identity color.
    Grape,
    /// Orange identity color.
    Orange,
    /// Warning semantic color.
    Warning,
    /// Error semantic color.
    Error,
    /// Primary text color.
    Text,
    /// Dimmed text color.
    Dimmed,
}

/// One canonical Phanes Team member in display order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeamCatalogMember {
    /// Stable Phanes Agent identifier used for member overrides.
    pub agent_id: String,
    /// Human-readable Agent name.
    pub name: String,
    /// Human-readable Agent job title.
    pub job_title: String,
}

/// One Phanes Team and its persisted installation state.
///
/// Teams are identified by their stable leader id. The member and skill names
/// are optional because a persisted installation may outlive its registry
/// definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeamCatalogEntry {
    /// Stable Phanes Team leader identifier.
    pub leader_id: String,
    /// Current or persisted Phanes Team version.
    pub version: String,
    /// Canonical Team name when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Canonical leader role when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_title: Option<String>,
    /// Canonical Team description when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether this Team has a persisted installation.
    pub installed: bool,
    /// Installed provider identifier, when installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    /// Team-member-specific provider config selections, when installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_overrides: Option<DiscussMemberOverrides>,
    /// User-selected theme color for the Team mark.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme_color: Option<AgentThemeColor>,
    /// Whether the current Phanes registry contains this Team definition.
    pub definition_available: bool,
    /// Canonical Team members, including stable IDs, when the definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<TeamCatalogMember>>,
    /// Canonical skill display names when the Team definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_names: Option<Vec<String>>,
}

// ── ACP protocol payload types ─────────────────────────────────────────────
//
// These types describe the actual external Agent Client Protocol and retain
// ACP terminology. They are not Sluice-owned routing concepts.

/// JSON-safe DTO for an ACP session message emitted by the Rust SDK.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum AcpAgentMessagePayload {
    Dispatch {
        session_id: String,
        dispatch: AcpDispatchPayload,
    },
    /// Per-turn stop reason forwarded from `SessionMessage::StopReason`.
    ///
    /// This is a turn boundary, not a session shutdown. The session remains
    /// open and waits for the next user turn.
    StopReason {
        session_id: String,
        stop_reason: StopReason,
    },
}

/// JSON-safe projection of an ACP JSON-RPC dispatch without non-serializable responder state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum AcpDispatchPayload {
    Request { method: String, params: serde_json::Value },
    Notification { method: String, params: serde_json::Value },
    Response { result: AcpDispatchResponsePayload },
}

/// JSON-safe projection of an ACP JSON-RPC response result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum AcpDispatchResponsePayload {
    Ok { value: serde_json::Value },
    Error { message: String },
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors that can occur in the Discuss domain.
///
/// All variants store serializable data so Discuss errors can cross RPC and IPC
/// boundaries without carrying raw external error types.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub enum DiscussError {
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
    #[error("Discuss session {sluice_session_id} cancelled")]
    SessionCancelled {
        /// The Sluice session id that was cancelled.
        sluice_session_id: SluiceSessionId,
    },

    /// Protocol error.
    #[error("Protocol error: {message}")]
    ProtocolError {
        /// The error message.
        message: String,
    },

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
    #[error("Sluice MCP setup error: {message}")]
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
    #[error("Discuss session not found: sluice_session_id={sluice_session_id}")]
    SessionNotFound {
        /// Sluice session id that had no live worker.
        sluice_session_id: SluiceSessionId,
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
        viewer_session_id: SluiceSessionId,
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
        "permission not found or already resolved: sluice_session_id={sluice_session_id}, permission_request_id={permission_request_id}"
    )]
    PermissionNotFound {
        /// Sluice session id of the answer target.
        sluice_session_id: SluiceSessionId,
        /// Permission request id of the answer.
        permission_request_id: String,
    },

    /// Cross-session permission answer: the pending permission belongs to a
    /// different `sluice_session_id` than the answer targets.
    #[error("permission session mismatch: answer for {answer_session_id} but pending belongs to {pending_session_id}")]
    SessionMismatch {
        /// Session id the answer was sent to.
        answer_session_id: SluiceSessionId,
        /// Session id the pending permission belongs to.
        pending_session_id: SluiceSessionId,
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
    #[error("elicitation session not found: sluice_session_id={sluice_session_id}")]
    ElicitationSessionNotFound {
        /// Sluice session targeted by the request.
        sluice_session_id: SluiceSessionId,
    },

    /// The elicitation request is no longer pending.
    #[error(
        "elicitation request not pending: sluice_session_id={sluice_session_id}, elicitation_request_id={elicitation_request_id}"
    )]
    ElicitationRequestNotPending {
        /// Sluice session targeted by the answer.
        sluice_session_id: SluiceSessionId,
        /// Elicitation request targeted by the answer.
        elicitation_request_id: ElicitationRequestId,
    },

    /// The elicitation request has already been answered.
    #[error(
        "elicitation request already answered: sluice_session_id={sluice_session_id}, elicitation_request_id={elicitation_request_id}"
    )]
    ElicitationAlreadyAnswered {
        /// Sluice session targeted by the answer.
        sluice_session_id: SluiceSessionId,
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
    #[error("worker stop timeout for sluice_session_id={sluice_session_id}")]
    WorkerStopTimeout {
        /// Sluice session id of the worker that timed out.
        sluice_session_id: SluiceSessionId,
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
            DiscussError::ChannelSend => "ChannelSend",
            DiscussError::ChannelRecv => "ChannelRecv",
            DiscussError::OneshotReply => "OneshotReply",
            DiscussError::SessionCancelled { .. } => "SessionCancelled",
            DiscussError::ProtocolError { .. } => "ProtocolError",
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
        let safe_error = serde_json::json!({
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

impl From<agent_client_protocol::Error> for DiscussError {
    fn from(err: agent_client_protocol::Error) -> Self {
        if err.code == agent_client_protocol::ErrorCode::AuthRequired {
            DiscussError::AuthenticationRequired
        } else {
            DiscussError::ProtocolError { message: err.message }
        }
    }
}

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

impl From<serde_json::Error> for DiscussError {
    fn from(err: serde_json::Error) -> Self {
        DiscussError::Serialization {
            message: err.to_string(),
        }
    }
}

impl From<super::library::Error> for DiscussError {
    fn from(error: super::library::Error) -> Self {
        match error {
            super::library::Error::InvalidAssetId { asset_id, violation } => DiscussError::WorkspaceInvalidAssetId {
                asset_id,
                violation: violation.to_string(),
            },
        }
    }
}

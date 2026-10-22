//! Typed request, response, and broadcast contracts for Discuss.

use agent_client_protocol::schema::v1::SessionId;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::auth::Jwt;
use aves::bus::{BusMcpEndpoint, BusRpcContext};
use aves::flow::{LifecycleCorrelation, NodeExecutionId};

use super::DiscussDomain;
use super::types::{
    AcpProvider, AgentNodeSessionLimits, AgentNodeTickResult, AgentThemeColor, CleanupWarning, DiscussChatTurnRecord,
    DiscussConfigOption, DiscussConfigValues, DiscussElicitationAnswer, DiscussError, DiscussMcpStatus,
    DiscussMemberOverrides, DiscussPermissionOption, DiscussProviderConfig, DiscussSessionInfo, DiscussSessionSetup,
    DiscussSubagentRecord, ElicitationRequestId, InspectorBinding, PermissionRequestId, ProviderRuntimeState,
    SluiceSessionId, TeamCatalogEntry,
};

/// Discuss domain request wrapper.
///
/// Wraps a payload with its reply channel for actor request/response handling.
pub struct DiscussRequest {
    /// The request payload
    pub payload: DiscussRequestPayload,
    /// Reply channel for the response
    pub reply_to: oneshot::Sender<Result<DiscussResponse, DiscussError>>,
}

/// Payload variants for Discuss requests.
#[aves::bus_requests(marker = DiscussDomain, context = BusRpcContext, root_error = DiscussError, domain_error = DiscussError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscussRequestPayload {
    /// Starts an agent node execution.
    #[privileged(reason = "AgentNode lifecycle is admitted only from Aves Flow")]
    StartAgent {
        correlation: LifecycleCorrelation,
        provider_name: String,
        agent_name: String,
        config_overrides: DiscussConfigValues,
        sluice_mcp_enabled: bool,
        prompt: String,
        session_cwd: std::path::PathBuf,
        max_turns: Option<i64>,
        max_tool_calls: Option<i64>,
        max_duration: Option<i64>,
        #[serde(skip)]
        node_token: Option<Jwt>,
        #[serde(skip)]
        flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
    },

    /// Ticks an agent node execution.
    #[privileged(reason = "AgentNode lifecycle is admitted only from Aves Flow")]
    TickAgent { correlation: LifecycleCorrelation },

    /// Cancels an agent node execution.
    #[privileged(reason = "AgentNode cancellation is admitted only from Aves Flow")]
    CancelAgentExecution { node_execution_id: NodeExecutionId },

    /// Start boot-owned Discuss runtime infrastructure.

    #[operation(name = "startServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StartServer,

    /// Return whether Discuss is currently accepting and running ACP work.

    #[operation(name = "getServerStatus", kind = "query", response = ServerStatusOutput, response_variant = "ServerStatus")]
    GetServerStatus,

    /// Stop the Discuss server: gate new work, resolve pending permissions,
    /// stop workers, and drain persistence before dependent runtime domains stop.
    #[operation(name = "stopServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StopServer,

    /// Start one exact installed Provider runtime.
    #[operation(name = "startProvider", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StartProvider {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// Stop one exact installed Provider runtime.
    #[operation(name = "stopProvider", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StopProvider {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// Send a user turn (prompt text) to a live session worker.

    #[privileged(reason = "User turns are internal bus-only control requests, not public RPC")]
    SendUserTurn {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Prompt text to send to the provider.
        text: String,
    },

    /// Answer a pending permission request.

    #[privileged(reason = "Permission answers are internal bus-only control requests, not public RPC")]
    AnswerPermission {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Opaque typed permission request id.
        permission_request_id: PermissionRequestId,
        /// The caller's selected option or cancellation.
        option: DiscussPermissionOption,
    },

    /// Answer a pending ACP elicitation request.

    #[privileged(reason = "Elicitation answers are internal bus-only control requests, not public RPC")]
    AnswerElicitation {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Opaque typed elicitation request id.
        elicitation_request_id: ElicitationRequestId,
        /// The user's accepted, declined, or cancelled answer.
        answer: DiscussElicitationAnswer,
    },

    /// Cancel one pending ACP elicitation request.

    #[privileged(reason = "Elicitation cancellation is an internal bus-only control request, not public RPC")]
    CancelElicitation {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Opaque typed elicitation request id.
        elicitation_request_id: ElicitationRequestId,
    },

    /// Cancel the active turn for a session without closing the session.

    #[privileged(reason = "Turn cancellation is an internal bus-only control request, not public RPC")]
    CancelTurn {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },

    /// Cancel and close a session.

    #[privileged(reason = "Session cancellation is an internal bus-only control request, not public RPC")]
    CancelSession {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },

    /// Reserve an ephemeral viewer for a Provider child ACP session without starting its worker.

    #[privileged(reason = "Inspector open/dispose is an internal bus-only control request, not public RPC")]
    ReserveInspector {
        /// Parent Sluice chat whose Provider owns the child session.
        parent_session_id: SluiceSessionId,
        /// Provider ACP child-session identity.
        child_session_id: String,
    },

    /// Start a reserved inspector after Zeus has registered its viewer channel.

    #[privileged(reason = "Inspector start is an internal bus-only control request, not public RPC")]
    StartInspector { viewer_session_id: SluiceSessionId },

    /// Tear down only an ephemeral inspector viewer.

    #[privileged(reason = "Inspector open/dispose is an internal bus-only control request, not public RPC")]
    DisposeInspector {
        /// Ephemeral viewer identity returned by `ReserveInspector`.
        viewer_session_id: SluiceSessionId,
    },

    /// List all installed providers.

    #[operation(name = "listProviders", kind = "query", response = ProvidersListOutput, response_variant = "ProvidersList")]
    ListProviders,

    /// Get the database-backed provider menu and defaults for an installed provider.

    #[operation(name = "getProviderConfig", kind = "query", response = ProviderConfigOutput, response_variant = "ProviderConfig")]
    GetProviderConfig {
        /// Exact installed provider name.
        provider_name: String,
    },

    /// Launch a transient Provider connectivity check and refresh its supported session settings.

    #[operation(
        name = "refreshProviderConfig",
        kind = "mutation",
        response = SuccessOutput,
        response_variant = "Success"
    )]
    RefreshProviderConfig {
        /// Exact installed provider name.
        provider_name: String,
    },

    /// New Discuss session.
    ///
    /// The caller generates the `sluice_session_id` (UUID v4) before opening
    /// the Tauri channel. Discuss does not accept a provider ACP session id
    /// as input; reconnect and provider recovery are resolved by Discuss
    /// alone through its persisted mapping.

    #[operation(name = "newSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    NewSession {
        /// Caller-generated Sluice session identity (UUID v4).
        sluice_session_id: SluiceSessionId,
        /// The json representation from the Discuss registry that can be used to spawn the agent.
        agent_key: String,
        /// Human-readable session name (metadata, not a routing key).
        session_name: Option<String>,
        /// Configuration intent for a newly created session. Reconnects use
        /// the settings already persisted on the session anchor.
        setup: Option<DiscussSessionSetup>,
    },

    /// Add a new provider to the Discuss server.

    #[operation(name = "addProvider", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    AddProvider {
        /// Exact installed Provider identifier.
        provider_name: String,
        /// The provider specifics.
        provider_spec: AcpProvider,
    },

    /// Remove a provider from the Discuss server.

    #[operation(
        name = "removeProvider",
        kind = "mutation",
        response = ProviderRemoveSuccessOutput,
        response_variant = "ProviderRemoveSuccess"
    )]
    RemoveProvider {
        /// The name of the provider to remove.
        provider_name: String,
    },

    /// Materialize one installed Provider's shared Agent-workspace configuration.

    #[privileged(reason = "provider config lifecycle is actor-owned")]
    InstallProviderConfig {
        /// Exact installed Provider identifier.
        provider_name: String,
    },

    /// Remove one Provider's shared Agent-workspace configuration.

    #[privileged(reason = "provider config lifecycle is actor-owned")]
    RemoveProviderConfig {
        /// Exact Provider identifier.
        provider_name: String,
    },

    /// List all Phanes-defined teams and their installation state.

    #[operation(name = "listTeamCatalog", kind = "query", response = ListTeamCatalogOutput, response_variant = "ListTeamCatalog")]
    ListTeamCatalog,

    /// Install one Phanes-defined team into an installed provider.

    #[operation(name = "installTeam", kind = "mutation", response = InstallTeamOutput, response_variant = "InstallTeam")]
    InstallTeam {
        /// Stable Phanes team leader identifier.
        leader_id: String,
        /// Exact installed provider identifier.
        provider_name: String,
        /// Sparse provider config selections keyed by Team member Agent ID.
        member_overrides: DiscussMemberOverrides,
        /// User-selected theme color for the Team mark.
        theme_color: Option<AgentThemeColor>,
    },

    /// Edit an installed team's provider and/or config selections.
    #[operation(name = "editTeam", kind = "mutation", response = EditTeamOutput, response_variant = "EditTeam")]
    EditTeam {
        /// Stable Phanes team leader identifier.
        leader_id: String,
        /// Exact installed provider identifier.
        provider_name: String,
        /// Sparse provider config selections keyed by Team member Agent ID.
        member_overrides: DiscussMemberOverrides,
        /// User-selected theme color for the Team mark.
        theme_color: Option<AgentThemeColor>,
    },

    /// Uninstall one installed Phanes team.
    #[operation(name = "uninstallTeam", kind = "mutation", response = UninstallTeamOutput, response_variant = "UninstallTeam")]
    UninstallTeam {
        /// Stable Phanes team leader identifier.
        leader_id: String,
    },

    /// List persisted chat sessions ordered by most recent interaction.
    ///
    /// Read-only query; not subject to the bus safety gate.
    #[operation(name = "listSessions", kind = "query", response = SessionListOutput, response_variant = "SessionList")]
    ListSessions,

    /// Load a single chat session and its bounded recent transcript turns.
    ///
    /// Read-only query keyed by `sluice_session_id`.
    #[operation(name = "loadSession", kind = "query", response = SessionLoadOutput, response_variant = "SessionLoad")]
    LoadSession {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },

    /// Get the provider-exposed config options for a live or loaded session.
    ///
    /// Read-only query keyed by `sluice_session_id`.
    #[operation(name = "getConfigOptions", kind = "query", response = ConfigOptionsOutput, response_variant = "ConfigOptions")]
    GetConfigOptions {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },

    /// Set a provider-exposed config option on an active session.
    ///
    /// Mutation; subject to the bus safety gate.
    #[operation(name = "setConfigOption", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    SetConfigOption {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Provider option id to set.
        config_id: String,
        /// New value for the option as an ACP `SessionConfigValueId` (string).
        value: String,
    },

    /// Rename a local Sluice Discuss chat session display label.
    ///
    /// Updates only the display title; the `sluice_session_id` identity and
    /// provider `acp_session_id` are preserved.
    #[operation(name = "renameSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    RenameSession {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// User-controlled local Sluice display label.
        display_title: String,
    },

    /// Remove a local Sluice Discuss chat session anchor.
    ///
    /// Deletes the session row keyed by `sluice_session_id`.
    /// Related chat turns cascade via FK. Live worker registry and
    /// capability cache entries for the same identity are dropped.
    ///
    /// Mutation; subject to the bus safety gate.
    #[operation(name = "removeSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    RemoveSession {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },
}

/// Response types for Discuss operations.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscussResponse {
    /// A Flow AgentNode session was accepted by its ACP worker.
    AgentNodeStarted {
        correlation: LifecycleCorrelation,
        session_id: SluiceSessionId,
        limits: AgentNodeSessionLimits,
        session_cwd: std::path::PathBuf,
    },
    /// Current live result for a Flow AgentNode execution.
    AgentNodeTicked {
        correlation: LifecycleCorrelation,
        result: AgentNodeTickResult,
    },
    /// A new session has been created successfully
    NewSession {
        /// The Sluice session id of the new session
        session_id: SluiceSessionId,
    },

    /// Binding for an ephemeral child-session inspector.
    ReserveInspector { binding: InspectorBinding },

    /// List of providers available on the Discuss server.
    ProvidersList {
        /// Installed Providers with their process-local runtime state.
        providers: Vec<ProviderRuntimeState>,
        /// Effective agents workspace root, including the config-owned fallback.
        agents_workspace: String,
    },

    /// Internal broadcast: one Provider runtime projection changed.
    #[serde(skip)]
    ProviderRuntimeChanged { state: ProviderRuntimeState },

    /// Database-backed provider menu and user defaults for one provider.
    ProviderConfig {
        config: DiscussProviderConfig,
        /// Effective sources, including the mandatory `.agents` directory.
        workspace_copy_sources: Vec<String>,
    },

    /// Phanes team catalog and persisted installation state.
    ListTeamCatalog { entries: Vec<TeamCatalogEntry> },

    /// Team installation completed, possibly with cleanup warnings.
    InstallTeam { warnings: Vec<CleanupWarning> },

    /// Team edit completed, possibly with cleanup warnings.
    EditTeam { warnings: Vec<CleanupWarning> },

    /// Team uninstall completed, possibly with cleanup warnings.
    UninstallTeam { warnings: Vec<CleanupWarning> },

    /// Provider removal completed, possibly with Agent cleanup warnings.
    ProviderRemoveSuccess { warnings: Vec<CleanupWarning> },

    /// Generic success
    Success,

    /// Current Discuss service lifecycle status.
    ServerStatus { running: bool },

    /// Internal broadcast: Discuss began accepting ACP work.
    #[serde(skip)]
    ServerStarted,

    /// Internal broadcast: Discuss stopped all ACP work.
    #[serde(skip)]
    ServerStopped,

    /// List of persisted chat sessions ordered by most recent interaction.
    SessionList { sessions: Vec<DiscussSessionInfo> },

    /// A single loaded chat session with its bounded recent transcript turns.
    SessionLoad {
        session: DiscussSessionInfo,
        turns: Vec<DiscussChatTurnRecord>,
        subagents: Vec<DiscussSubagentRecord>,
    },

    /// Provider-exposed config options for a live or loaded session.
    ConfigOptions { options: Vec<DiscussConfigOption> },

    /// An informational session warning to be broadcast to subscribers.
    /// The handle converts this to `DiscussBroadcast::SessionWarning`.
    #[serde(skip)]
    SessionWarning {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Human-readable warning message.
        warning: String,
    },
    /// The selected Sluice MCP transport for a live session.
    #[serde(skip)]
    McpStatus {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Current Sluice MCP transport status.
        status: DiscussMcpStatus,
    },
}

/// Events broadcast by the Discuss domain.
///
/// Session lifecycle and warning notifications suitable for the lossy
/// broadcast channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscussBroadcast {
    /// Discuss began accepting ACP work.
    ServerStarted,

    /// Discuss stopped all ACP work.
    ServerStopped,

    /// A session has successfully associated with its provider.
    ///
    /// Published on successful provider association. Zeus uses it to mark a
    /// session associated but must tolerate lag/drops.
    SessionReady {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
    },
    /// An informational session warning (e.g. missing MCP-over-ACP capability).
    ///
    /// The warning string is a simple human-readable message — no nested enum
    /// or category layer. This is a reusable informational broadcast, not a
    /// reliable delivery path.
    SessionWarning {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Human-readable warning message.
        warning: String,
    },
    /// The selected Sluice MCP transport for a live session.
    McpStatus {
        /// Sluice-owned canonical session identity.
        sluice_session_id: SluiceSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Current Sluice MCP transport status.
        status: DiscussMcpStatus,
    },
    /// One Provider runtime lifecycle projection changed.
    ProviderRuntimeChanged { state: ProviderRuntimeState },
}

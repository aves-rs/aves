//! Typed request, response, and broadcast contracts for Discuss.

use std::path::PathBuf;

use agent_client_protocol::v1::schema::SessionId;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::auth::{Jwt, MachineIssuerId};
use aves::bus::{BusMcpEndpoint, BusRpcContext};
use aves::flow::{LifecycleCorrelation, NodeExecutionId};

use super::DiscussDomain;
use super::agent_nodes::{AgentNodeSessionLimits, AgentNodeTickResult};
use super::catalog_types::{
    AgentThemeColor, BuiltinSkillEntry, CustomAgentDetail, CustomAssetMetadata, CustomSkillBundleFile,
    CustomSkillDetail, IncompleteTeamClaim, InstalledAssetClaim, InstalledAssetKind, TeamCatalogEntry,
};
use super::error::DiscussError;
use super::launch_types::{AcpProvider, SessionLaunchConfig};
use super::report_types::{
    DiscussAllowanceGroupBy, DiscussAllowanceProjection, DiscussAllowanceReport, DiscussUsageGroupBy,
    DiscussUsageReport,
};
use super::types::{
    CleanupWarning, CustomMcpEntry, CustomMcpScope, DiscussAuthAccount, DiscussAuthState, DiscussChatTurnRecord,
    DiscussConfigOption, DiscussConfigValues, DiscussElicitationAnswer, DiscussImageAttachment, DiscussMcpStatus,
    DiscussMemberOverrides, DiscussPermissionOption, DiscussProviderConfig, DiscussResourceLink, DiscussSessionId,
    DiscussSessionInfo, DiscussSessionModes, DiscussSessionSetup, DiscussSubagentRecord, DiscussTranscriptCursor,
    DiscussTranscriptPage, ElicitationRequestId, InspectorBinding, PermissionRequestId, ProviderRuntimeState,
    SensitiveMcpDescriptor,
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

/// Whether an existing provider attachment could be reused.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, specta::Type, schemars::JsonSchema)]
pub enum EnsureAttachedSessionOutcome {
    /// The existing worker was kept or a reconnect was started.
    Ensured,
    /// No provider attachment exists; the caller must launch the session.
    NeedsLaunch,
}

/// Payload variants for Discuss requests.
#[aves::bus_requests(marker = DiscussDomain, context = BusRpcContext, root_error = DiscussError, domain_error = DiscussError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscussRequestPayload {
    /// Resolve a verified bus identity to its configured session identity.
    #[privileged(reason = "maps verified bus identities to sessions for application attribution")]
    ResolveSessionIdentity { issuer: MachineIssuerId, subject: String },
    /// Starts an agent node execution.
    #[privileged(reason = "AgentNode lifecycle is admitted only from Aves Flow")]
    StartAgent {
        correlation: LifecycleCorrelation,
        #[serde(default)]
        launch_config: Option<SessionLaunchConfig>,
        provider_name: String,
        agent_name: String,
        #[serde(default)]
        session_name: Option<String>,
        config_overrides: DiscussConfigValues,
        application_mcp_enabled: bool,
        prompt: String,
        session_cwd: PathBuf,
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

    /// Read a provider's authentication status when advertised.
    #[operation(name = "getProviderAuthStatus", kind = "query", response = ProviderAuthStatusOutput, response_variant = "ProviderAuthStatus")]
    GetProviderAuthStatus { provider_name: String },

    /// Send a user turn (prompt text) to a live session worker.

    #[privileged(reason = "User turns are internal bus-only control requests, not public RPC")]
    SendUserTurn {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Prompt text to send to the provider.
        text: String,
        /// Images sent to the provider without durable storage.
        #[serde(default)]
        images: Vec<DiscussImageAttachment>,
        /// Resource links sent to the provider.
        #[serde(default)]
        resource_links: Vec<DiscussResourceLink>,
    },

    /// Answer a pending permission request.

    #[privileged(reason = "Permission answers are internal bus-only control requests, not public RPC")]
    AnswerPermission {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Opaque typed permission request id.
        permission_request_id: PermissionRequestId,
        /// The caller's selected option or cancellation.
        option: DiscussPermissionOption,
    },

    /// Answer a pending ACP elicitation request.

    #[privileged(reason = "Elicitation answers are internal bus-only control requests, not public RPC")]
    AnswerElicitation {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Opaque typed elicitation request id.
        elicitation_request_id: ElicitationRequestId,
        /// The user's accepted, declined, or cancelled answer.
        answer: DiscussElicitationAnswer,
    },

    /// Cancel one pending ACP elicitation request.

    #[privileged(reason = "Elicitation cancellation is an internal bus-only control request, not public RPC")]
    CancelElicitation {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Opaque typed elicitation request id.
        elicitation_request_id: ElicitationRequestId,
    },

    /// Cancel the active turn for a session without closing the session.

    #[privileged(reason = "Turn cancellation is an internal bus-only control request, not public RPC")]
    CancelTurn {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
    },

    /// Cancel and close a session.

    #[privileged(reason = "Session cancellation is an internal bus-only control request, not public RPC")]
    CancelSession {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
    },

    /// Reload a session at a different working directory.
    #[privileged(reason = "Session working directory changes are internal bus-only control requests")]
    ChangeSessionWorkingDir {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Requested session working directory.
        session_cwd: PathBuf,
    },

    /// Ensure an already attached session without loading its transcript.
    #[privileged(reason = "Reconnecting a session at a working directory is internal bus-only control")]
    EnsureAttachedSession {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Requested session working directory.
        session_cwd: PathBuf,
    },

    /// Reserve an ephemeral viewer for a Provider child ACP session without starting its worker.

    #[privileged(reason = "Inspector open/dispose is an internal bus-only control request, not public RPC")]
    ReserveInspector {
        /// Parent chat whose Provider owns the child session.
        parent_session_id: DiscussSessionId,
        /// Provider ACP child-session identity.
        child_session_id: String,
    },

    /// Start a reserved inspector after Zeus has registered its viewer channel.

    #[privileged(reason = "Inspector start is an internal bus-only control request, not public RPC")]
    StartInspector { viewer_session_id: DiscussSessionId },

    /// Tear down only an ephemeral inspector viewer.

    #[privileged(reason = "Inspector open/dispose is an internal bus-only control request, not public RPC")]
    DisposeInspector {
        /// Ephemeral viewer identity returned by `ReserveInspector`.
        viewer_session_id: DiscussSessionId,
    },

    /// List all installed providers.

    #[operation(name = "listProviders", kind = "query", response = ProvidersListOutput, response_variant = "ProvidersList")]
    ListProviders,

    /// Aggregate retained token evidence over an inclusive range.
    #[operation(name = "getTokenUsageReport", kind = "query", response = TokenUsageReportOutput, response_variant = "TokenUsageReport")]
    GetTokenUsageReport {
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        provider_name: Option<String>,
        model_id: Option<String>,
        group_by: DiscussUsageGroupBy,
    },

    /// Aggregate calculated and harness spend without mixing their scopes.
    #[operation(name = "getSpendReport", kind = "query", response = SpendReportOutput, response_variant = "SpendReport")]
    GetSpendReport {
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        provider_name: Option<String>,
        model_id: Option<String>,
        group_by: DiscussUsageGroupBy,
    },

    /// Read sampled historical allowance observations.
    #[operation(name = "getAllowanceReport", kind = "query", response = AllowanceReportOutput, response_variant = "AllowanceReport")]
    GetAllowanceReport {
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        provider_name: Option<String>,
        group_by: DiscussAllowanceGroupBy,
    },

    /// Read the latest in-memory allowance snapshot.
    #[operation(name = "getCurrentAllowance", kind = "query", response = CurrentAllowanceOutput, response_variant = "CurrentAllowance")]
    GetCurrentAllowance,

    /// Replace the current allowance snapshot supplied by the embedding application.
    #[operation(name = "recordAllowance", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    RecordAllowance { entries: Vec<DiscussAllowanceProjection> },

    /// Get the database-backed provider menu and defaults for an installed provider.

    #[operation(name = "getProviderConfig", kind = "query", response = ProviderConfigOutput, response_variant = "ProviderConfig")]
    GetProviderConfig {
        /// Exact installed provider name.
        provider_name: String,
    },

    /// Read the effective provider-managed configuration document.
    #[operation(name = "getProviderManagedConfig", kind = "query", response = ProviderManagedConfigOutput, response_variant = "ProviderManagedConfig")]
    GetProviderManagedConfig { provider_name: String },

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
    /// The caller generates the `discuss_session_id` (UUID v4) before opening
    /// the Tauri channel. Reconnect and provider recovery are resolved through
    /// the persisted mapping.

    #[operation(name = "newSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    NewSession {
        /// Require session-local application MCP credentials on providers that otherwise share them.
        #[serde(default)]
        requires_mcp_isolation: bool,
        /// Optional session-specific launch inputs.
        #[serde(default)]
        launch_config: Option<SessionLaunchConfig>,
        /// Caller-generated session identity (UUID v4).
        discuss_session_id: DiscussSessionId,
        /// The json representation from the Discuss registry that can be used to spawn the agent.
        agent_key: String,
        /// Human-readable session name (metadata, not a routing key).
        session_name: Option<String>,
        /// Configuration intent for a newly created session. Reconnects use
        /// the settings already persisted on the session anchor.
        setup: Option<DiscussSessionSetup>,
        /// Optional execution directory; absence retains the provider workspace default.
        #[serde(default)]
        session_cwd: Option<PathBuf>,
        /// Override application MCP for this session; absent uses the Provider default.
        #[serde(default)]
        application_mcp_enabled: Option<bool>,
    },

    /// Connect a Discuss session to an existing provider conversation.
    #[operation(name = "connectProviderSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    ConnectProviderSession {
        /// Optional session-specific launch inputs.
        #[serde(default)]
        launch_config: Option<SessionLaunchConfig>,
        /// Caller-generated session identity (UUID v4).
        discuss_session_id: DiscussSessionId,
        /// The json representation from the Discuss registry that can be used to spawn the agent.
        agent_key: String,
        /// Human-readable session name (metadata, not a routing key).
        session_name: Option<String>,
        /// Configuration intent for a newly created session. Reconnects use
        /// the settings already persisted on the session anchor.
        setup: Option<DiscussSessionSetup>,
        /// Optional execution directory; absence retains the provider workspace default.
        #[serde(default)]
        session_cwd: Option<PathBuf>,
        /// Override application MCP for this session; absent uses the Provider default.
        #[serde(default)]
        application_mcp_enabled: Option<bool>,
        /// Provider conversation identity to load.
        provider_session_id: String,
    },

    /// Install a Team and resolve a new session in one serialized actor request.
    #[operation(name = "installTeamAndStartSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    InstallTeamAndStartSession {
        /// Team leader to install and use for the session.
        leader_id: String,
        /// Exact installed Provider identifier.
        provider_name: String,
        /// Caller-generated session identity.
        discuss_session_id: DiscussSessionId,
        /// Session setup, which must identify the Team being installed.
        setup: DiscussSessionSetup,
        /// Human-readable session name.
        session_name: Option<String>,
        /// Optional session-specific launch inputs.
        launch_config: Option<SessionLaunchConfig>,
        /// Optional execution directory.
        session_cwd: Option<PathBuf>,
        /// Session-specific application MCP selection.
        application_mcp_enabled: Option<bool>,
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

    /// Replace the complete provider-managed document.
    #[privileged(reason = "provider config lifecycle is actor-owned")]
    SetProviderManagedConfig { provider_name: String, document: String },

    /// Remove one Provider's shared Agent-workspace configuration.

    #[privileged(reason = "provider config lifecycle is actor-owned")]
    RemoveProviderConfig {
        /// Exact Provider identifier.
        provider_name: String,
    },

    /// List custom MCP descriptors with redacted environment and header values.
    #[operation(name = "listCustomMcps", kind = "query", response = ListCustomMcpsOutput, response_variant = "ListCustomMcps")]
    ListCustomMcps,
    /// Create a custom MCP descriptor.
    #[privileged(reason = "custom MCP descriptors may launch arbitrary processes")]
    CreateCustomMcp {
        name: String,
        descriptor: SensitiveMcpDescriptor,
    },
    /// Replace a custom MCP descriptor.
    #[privileged(reason = "custom MCP descriptors may launch arbitrary processes")]
    UpdateCustomMcp {
        name: String,
        descriptor: SensitiveMcpDescriptor,
    },
    /// Delete a custom MCP and its assignments.
    #[privileged(reason = "custom MCP configuration is actor-owned")]
    DeleteCustomMcp { name: String },
    /// Assign a custom MCP to a scope.
    #[privileged(reason = "custom MCP descriptors may launch arbitrary processes")]
    AssignMcp {
        scope_kind: CustomMcpScope,
        scope_id: String,
        mcp_name: String,
    },
    /// Remove a scoped custom MCP assignment.
    #[privileged(reason = "custom MCP configuration is actor-owned")]
    UnassignMcp {
        scope_kind: CustomMcpScope,
        scope_id: String,
        mcp_name: String,
    },

    /// List built-in skill metadata.
    #[operation(name = "listBuiltinSkills", kind = "query", response = ListBuiltinSkillsOutput, response_variant = "ListBuiltinSkills")]
    ListBuiltinSkills,
    /// List recorded provider asset installation claims.
    #[operation(name = "listProviderInstalledAssets", kind = "query", response = ProviderInstalledAssetsOutput, response_variant = "ProviderInstalledAssets")]
    ListProviderInstalledAssets,
    /// List MCP names assigned to an installed provider.
    #[operation(name = "listProviderMcpAssignments", kind = "query", response = ProviderMcpAssignmentsOutput, response_variant = "ProviderMcpAssignments")]
    ListProviderMcpAssignments { provider_name: String },
    /// List cached custom agents.
    #[operation(name = "listCustomAgents", kind = "query", response = ListCustomAgentsOutput, response_variant = "ListCustomAgents")]
    ListCustomAgents,
    /// List cached custom skills.
    #[operation(name = "listCustomSkills", kind = "query", response = ListCustomSkillsOutput, response_variant = "ListCustomSkills")]
    ListCustomSkills,
    /// Read one cached custom agent.
    #[operation(name = "getCustomAgent", kind = "query", response = GetCustomAgentOutput, response_variant = "GetCustomAgent")]
    GetCustomAgent { agent_id: String },
    /// Read one cached custom skill.
    #[operation(name = "getCustomSkill", kind = "query", response = GetCustomSkillOutput, response_variant = "GetCustomSkill")]
    GetCustomSkill { skill_id: String },
    /// Fetch and import a custom agent from an untrusted URL.
    #[privileged(reason = "URL imports fetch external content")]
    ImportCustomAgentFromUrl { agent_id: Option<String>, url: String },
    /// Fetch and import a custom skill from an untrusted URL.
    #[privileged(reason = "URL imports fetch external content")]
    ImportCustomSkillFromUrl { skill_id: Option<String>, url: String },
    /// Set instructions for a physical installed asset path; empty clears them.
    #[operation(name = "setInstalledAssetInstructions", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    SetInstalledAssetInstructions {
        provider_name: String,
        asset_kind: InstalledAssetKind,
        asset_id: String,
        instructions: String,
    },
    /// Read instructions for a physical installed asset path.
    #[operation(name = "getInstalledAssetInstructions", kind = "query", response = InstalledAssetInstructionsOutput, response_variant = "InstalledAssetInstructions")]
    GetInstalledAssetInstructions {
        provider_name: String,
        asset_kind: InstalledAssetKind,
        asset_id: String,
    },
    /// Create a local custom agent.
    #[operation(name = "createCustomAgent", kind = "mutation", response = GetCustomAgentOutput, response_variant = "GetCustomAgent")]
    CreateCustomAgent {
        agent_id: Option<String>,
        name: String,
        job_title: String,
        description: String,
        body: String,
    },
    /// Create a local custom skill.
    #[operation(name = "createCustomSkill", kind = "mutation", response = GetCustomSkillOutput, response_variant = "GetCustomSkill")]
    CreateCustomSkill {
        skill_id: Option<String>,
        name: String,
        description: String,
        files: Vec<CustomSkillBundleFile>,
    },

    /// Update a custom agent; a later successful import pull overwrites local edits.
    #[operation(name = "updateCustomAgent", kind = "mutation", response = GetCustomAgentOutput, response_variant = "GetCustomAgent")]
    UpdateCustomAgent {
        agent_id: String,
        name: String,
        job_title: String,
        description: String,
        body: String,
    },
    /// Update a custom skill; a later successful import pull overwrites local edits.
    #[operation(name = "updateCustomSkill", kind = "mutation", response = GetCustomSkillOutput, response_variant = "GetCustomSkill")]
    UpdateCustomSkill {
        skill_id: String,
        name: String,
        description: String,
        files: Vec<CustomSkillBundleFile>,
    },
    /// Delete an unreferenced custom agent.
    #[operation(name = "deleteCustomAgent", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    DeleteCustomAgent { agent_id: String },
    /// Delete an unreferenced custom skill.
    #[operation(name = "deleteCustomSkill", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    DeleteCustomSkill { skill_id: String },

    /// Install a custom agent for an installed Provider.
    #[operation(name = "installCustomAgent", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    InstallCustomAgent { provider_name: String, agent_id: String },
    /// Remove a Provider's custom agent installation.
    #[operation(name = "uninstallCustomAgent", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    UninstallCustomAgent { provider_name: String, agent_id: String },
    /// Install a custom skill for an installed Provider.
    #[operation(name = "installCustomSkill", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    InstallCustomSkill { provider_name: String, skill_id: String },
    /// Remove a Provider's custom skill installation.
    #[operation(name = "uninstallCustomSkill", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    UninstallCustomSkill { provider_name: String, skill_id: String },

    /// Create a user-defined Team.
    #[operation(name = "createCustomTeam", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    CreateCustomTeam {
        leader_id: String,
        display_name: String,
        followers: Vec<String>,
        skills: Vec<String>,
    },
    /// Update a user-defined Team.
    #[operation(name = "updateCustomTeam", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    UpdateCustomTeam {
        leader_id: String,
        display_name: String,
        followers: Vec<String>,
        skills: Vec<String>,
    },
    /// Delete an uninstalled user-defined Team.
    #[operation(name = "deleteCustomTeam", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    DeleteCustomTeam { leader_id: String },

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
        /// Write the team's files under this directory instead, without recording an installation.
        #[serde(default)]
        root: Option<PathBuf>,
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
    /// Read-only query keyed by `discuss_session_id`.
    #[operation(name = "loadSession", kind = "query", response = SessionLoadOutput, response_variant = "SessionLoad")]
    LoadSession {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// One page of the transcript; absent returns every saved turn in full.
        #[serde(default)]
        page: Option<DiscussTranscriptPage>,
    },

    /// Get the provider-exposed config options for a live or loaded session.
    ///
    /// Read-only query keyed by `discuss_session_id`.
    #[operation(name = "getConfigOptions", kind = "query", response = ConfigOptionsOutput, response_variant = "ConfigOptions")]
    GetConfigOptions {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
    },

    /// Read the modes reported for a live session.
    #[operation(name = "getSessionModes", kind = "query", response = SessionModesOutput, response_variant = "SessionModes")]
    GetSessionModes { discuss_session_id: DiscussSessionId },

    /// Set the current mode on a live session.
    #[operation(name = "setSessionMode", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    SetSessionMode {
        discuss_session_id: DiscussSessionId,
        mode_id: String,
    },

    /// Set a provider-exposed config option on an active session.
    ///
    /// Mutation; subject to the bus safety gate.
    #[operation(name = "setConfigOption", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    SetConfigOption {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Provider option id to set.
        config_id: String,
        /// New value for the option as an ACP `SessionConfigValueId` (string).
        value: String,
    },

    /// Rename a local Discuss chat session display label.
    ///
    /// Updates only the display title; the `discuss_session_id` identity and
    /// provider `acp_session_id` are preserved.
    #[operation(name = "renameSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    RenameSession {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// User-controlled local display label.
        display_title: String,
    },

    /// Remove a local chat session anchor.
    ///
    /// Deletes the session row keyed by `discuss_session_id`.
    /// Related chat turns cascade via FK. Live worker registry and
    /// capability cache entries for the same identity are dropped.
    ///
    /// Mutation; subject to the bus safety gate.
    #[operation(name = "removeSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    RemoveSession {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
    },

    /// Archive the provider session when supported.
    #[operation(name = "archiveSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    ArchiveSession { discuss_session_id: DiscussSessionId },

    /// Unarchive the provider session when supported.
    #[operation(name = "unarchiveSession", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    UnarchiveSession { discuss_session_id: DiscussSessionId },
}

impl DiscussRequestPayload {
    /// Identify operations gated by the Discuss server lifecycle.
    pub(super) fn requires_running_server(&self) -> bool {
        matches!(
            self,
            DiscussRequestPayload::StartAgent { .. }
                | DiscussRequestPayload::TickAgent { .. }
                | DiscussRequestPayload::AddProvider { .. }
                | DiscussRequestPayload::RefreshProviderConfig { .. }
                | DiscussRequestPayload::NewSession { .. }
                | DiscussRequestPayload::ConnectProviderSession { .. }
                | DiscussRequestPayload::InstallTeamAndStartSession { .. }
                | DiscussRequestPayload::LoadSession { .. }
                | DiscussRequestPayload::EnsureAttachedSession { .. }
                | DiscussRequestPayload::GetConfigOptions { .. }
                | DiscussRequestPayload::SetConfigOption { .. }
                | DiscussRequestPayload::GetSessionModes { .. }
                | DiscussRequestPayload::SetSessionMode { .. }
                | DiscussRequestPayload::ArchiveSession { .. }
                | DiscussRequestPayload::UnarchiveSession { .. }
                | DiscussRequestPayload::SendUserTurn { .. }
                | DiscussRequestPayload::AnswerPermission { .. }
                | DiscussRequestPayload::AnswerElicitation { .. }
                | DiscussRequestPayload::CancelElicitation { .. }
                | DiscussRequestPayload::CancelTurn { .. }
                | DiscussRequestPayload::CancelSession { .. }
                | DiscussRequestPayload::ReserveInspector { .. }
                | DiscussRequestPayload::StartInspector { .. }
                | DiscussRequestPayload::DisposeInspector { .. }
        )
    }
}

/// Stable operation label for tracing without recording request contents.
pub(super) fn acp_request_kind(payload: &DiscussRequestPayload) -> &'static str {
    match payload {
        DiscussRequestPayload::ResolveSessionIdentity { .. } => "resolveSessionIdentity",
        DiscussRequestPayload::GetTokenUsageReport { .. } => "getTokenUsageReport",
        DiscussRequestPayload::GetSpendReport { .. } => "getSpendReport",
        DiscussRequestPayload::GetAllowanceReport { .. } => "getAllowanceReport",
        DiscussRequestPayload::GetCurrentAllowance => "getCurrentAllowance",
        DiscussRequestPayload::RecordAllowance { .. } => "recordAllowance",
        DiscussRequestPayload::StartAgent { .. } => "startAgent",
        DiscussRequestPayload::TickAgent { .. } => "tickAgent",
        DiscussRequestPayload::CancelAgentExecution { .. } => "cancelAgentExecution",
        DiscussRequestPayload::StartServer => "startServer",
        DiscussRequestPayload::StopServer => "stopServer",
        DiscussRequestPayload::GetServerStatus => "getServerStatus",
        DiscussRequestPayload::StartProvider { .. } => "startProvider",
        DiscussRequestPayload::StopProvider { .. } => "stopProvider",
        DiscussRequestPayload::SendUserTurn { .. } => "sendUserTurn",
        DiscussRequestPayload::AnswerPermission { .. } => "answerPermission",
        DiscussRequestPayload::AnswerElicitation { .. } => "answerElicitation",
        DiscussRequestPayload::CancelElicitation { .. } => "cancelElicitation",
        DiscussRequestPayload::CancelTurn { .. } => "cancelTurn",
        DiscussRequestPayload::CancelSession { .. } => "cancelSession",
        DiscussRequestPayload::ReserveInspector { .. } => "reserveInspector",
        DiscussRequestPayload::StartInspector { .. } => "startInspector",
        DiscussRequestPayload::DisposeInspector { .. } => "disposeInspector",
        DiscussRequestPayload::AddProvider { .. } => "addProvider",
        DiscussRequestPayload::ListProviders => "listProviders",
        DiscussRequestPayload::GetProviderConfig { .. } => "getProviderConfig",
        DiscussRequestPayload::GetProviderManagedConfig { .. } => "getProviderManagedConfig",
        DiscussRequestPayload::SetProviderManagedConfig { .. } => "setProviderManagedConfig",
        DiscussRequestPayload::RefreshProviderConfig { .. } => "refreshProviderConfig",
        DiscussRequestPayload::RemoveProvider { .. } => "removeProvider",
        DiscussRequestPayload::InstallProviderConfig { .. } => "installProviderConfig",
        DiscussRequestPayload::RemoveProviderConfig { .. } => "removeProviderConfig",
        DiscussRequestPayload::ListCustomMcps => "listCustomMcps",
        DiscussRequestPayload::CreateCustomMcp { .. } => "createCustomMcp",
        DiscussRequestPayload::UpdateCustomMcp { .. } => "updateCustomMcp",
        DiscussRequestPayload::DeleteCustomMcp { .. } => "deleteCustomMcp",
        DiscussRequestPayload::AssignMcp { .. } => "assignMcp",
        DiscussRequestPayload::UnassignMcp { .. } => "unassignMcp",
        DiscussRequestPayload::ListBuiltinSkills => "listBuiltinSkills",
        DiscussRequestPayload::ListProviderInstalledAssets => "listProviderInstalledAssets",
        DiscussRequestPayload::ListProviderMcpAssignments { .. } => "listProviderMcpAssignments",
        DiscussRequestPayload::ListCustomAgents => "listCustomAgents",
        DiscussRequestPayload::ListCustomSkills => "listCustomSkills",
        DiscussRequestPayload::GetCustomAgent { .. } => "getCustomAgent",
        DiscussRequestPayload::GetCustomSkill { .. } => "getCustomSkill",
        DiscussRequestPayload::ImportCustomAgentFromUrl { .. } => "importCustomAgentFromUrl",
        DiscussRequestPayload::ImportCustomSkillFromUrl { .. } => "importCustomSkillFromUrl",
        DiscussRequestPayload::SetInstalledAssetInstructions { .. } => "setInstalledAssetInstructions",
        DiscussRequestPayload::GetInstalledAssetInstructions { .. } => "getInstalledAssetInstructions",
        DiscussRequestPayload::CreateCustomAgent { .. } => "createCustomAgent",
        DiscussRequestPayload::CreateCustomSkill { .. } => "createCustomSkill",
        DiscussRequestPayload::UpdateCustomAgent { .. } => "updateCustomAgent",
        DiscussRequestPayload::UpdateCustomSkill { .. } => "updateCustomSkill",
        DiscussRequestPayload::DeleteCustomAgent { .. } => "deleteCustomAgent",
        DiscussRequestPayload::DeleteCustomSkill { .. } => "deleteCustomSkill",
        DiscussRequestPayload::InstallCustomAgent { .. } => "installCustomAgent",
        DiscussRequestPayload::UninstallCustomAgent { .. } => "uninstallCustomAgent",
        DiscussRequestPayload::InstallCustomSkill { .. } => "installCustomSkill",
        DiscussRequestPayload::UninstallCustomSkill { .. } => "uninstallCustomSkill",
        DiscussRequestPayload::CreateCustomTeam { .. } => "createCustomTeam",
        DiscussRequestPayload::UpdateCustomTeam { .. } => "updateCustomTeam",
        DiscussRequestPayload::DeleteCustomTeam { .. } => "deleteCustomTeam",
        DiscussRequestPayload::ListTeamCatalog => "listTeamCatalog",
        DiscussRequestPayload::InstallTeam { .. } => "installTeam",
        DiscussRequestPayload::InstallTeamAndStartSession { .. } => "installTeamAndStartSession",
        DiscussRequestPayload::EditTeam { .. } => "editTeam",
        DiscussRequestPayload::UninstallTeam { .. } => "uninstallTeam",
        DiscussRequestPayload::NewSession { .. } => "newSession",
        DiscussRequestPayload::ConnectProviderSession { .. } => "connectProviderSession",
        DiscussRequestPayload::ListSessions => "listSessions",
        DiscussRequestPayload::LoadSession { .. } => "loadSession",
        DiscussRequestPayload::GetConfigOptions { .. } => "getConfigOptions",
        DiscussRequestPayload::GetSessionModes { .. } => "getSessionModes",
        DiscussRequestPayload::GetProviderAuthStatus { .. } => "getProviderAuthStatus",
        DiscussRequestPayload::SetSessionMode { .. } => "setSessionMode",
        DiscussRequestPayload::ArchiveSession { .. } => "archiveSession",
        DiscussRequestPayload::UnarchiveSession { .. } => "unarchiveSession",
        DiscussRequestPayload::SetConfigOption { .. } => "setConfigOption",
        DiscussRequestPayload::RemoveSession { .. } => "removeSession",
        DiscussRequestPayload::ChangeSessionWorkingDir { .. } => "changeSessionWorkingDir",
        DiscussRequestPayload::EnsureAttachedSession { .. } => "ensureAttachedSession",
        DiscussRequestPayload::RenameSession { .. } => "renameSession",
    }
}

/// Response types for Discuss operations.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscussResponse {
    /// The result of checking the existing provider attachment.
    AttachedSessionEnsured { outcome: EnsureAttachedSessionOutcome },
    /// Session and configured agent associated with a verified identity.
    SessionIdentityResolved {
        discuss_session_id: DiscussSessionId,
        agent_provider: String,
        agent_name: String,
        node_execution_id: Option<NodeExecutionId>,
    },
    /// Token usage report.
    TokenUsageReport { report: DiscussUsageReport },
    /// Spend report.
    SpendReport { report: DiscussUsageReport },
    /// Historical allowance report.
    AllowanceReport { report: DiscussAllowanceReport },
    /// Current normalized provider snapshot.
    CurrentAllowance { entries: Vec<DiscussAllowanceProjection> },
    /// A Flow AgentNode session was accepted by its ACP worker.
    AgentNodeStarted {
        correlation: LifecycleCorrelation,
        session_id: DiscussSessionId,
        limits: AgentNodeSessionLimits,
        session_cwd: PathBuf,
    },
    /// Current live result for a Flow AgentNode execution.
    AgentNodeTicked {
        correlation: LifecycleCorrelation,
        result: AgentNodeTickResult,
    },
    /// A new session has been created successfully
    NewSession {
        /// The session id of the new session
        session_id: DiscussSessionId,
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

    /// Internal broadcast: a Provider was removed and has no runtime projection any more.
    #[serde(skip)]
    ProviderRemoved { provider_name: String },

    /// Internal broadcast: custom agents, skills, teams or their installations changed.
    #[serde(skip)]
    CatalogChanged,

    /// Database-backed provider menu and user defaults for one provider.
    ProviderConfig {
        config: DiscussProviderConfig,
        /// Effective sources, including the mandatory `.agents` directory.
        workspace_copy_sources: Vec<String>,
    },

    /// Effective managed document and any fallback warning.
    ProviderManagedConfig {
        available: bool,
        kind: Option<String>,
        path: Option<String>,
        document: Option<String>,
        warnings: Vec<CleanupWarning>,
    },

    /// Stored custom MCP descriptors.
    ListCustomMcps { entries: Vec<CustomMcpEntry> },
    /// Built-in skill metadata in library order.
    ListBuiltinSkills { entries: Vec<BuiltinSkillEntry> },
    /// Recorded provider installation claims.
    ProviderInstalledAssets {
        agents: Vec<InstalledAssetClaim>,
        skills: Vec<InstalledAssetClaim>,
        incomplete_teams: Vec<IncompleteTeamClaim>,
    },
    /// MCP names assigned to a provider.
    ProviderMcpAssignments { mcp_names: Vec<String> },
    /// Cached custom agent metadata.
    ListCustomAgents { entries: Vec<CustomAssetMetadata> },
    /// Cached custom skill metadata.
    ListCustomSkills { entries: Vec<CustomAssetMetadata> },
    /// Complete cached agent.
    GetCustomAgent { agent: CustomAgentDetail },
    /// Complete cached skill.
    GetCustomSkill { skill: CustomSkillDetail },

    /// Appended instructions; empty means no instructions.
    InstalledAssetInstructions { instructions: String },

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
        /// Where the saved transcript continues before a requested page; absent when the page
        /// reached its start or no page was requested.
        #[serde(default)]
        earlier: Option<DiscussTranscriptCursor>,
    },

    /// Provider-exposed config options for a live or loaded session.
    ConfigOptions {
        options: Vec<DiscussConfigOption>,
        /// Provider option that selects the primary agent, if supported.
        #[serde(default)]
        agent_config_option_id: Option<String>,
    },

    /// Current modes for a session, if reported by its provider.
    SessionModes { modes: Option<DiscussSessionModes> },

    /// Provider-reported authentication status and display accounts.
    ProviderAuthStatus {
        status: DiscussAuthState,
        accounts: Vec<DiscussAuthAccount>,
    },

    /// Internal session activity broadcast.
    #[serde(skip)]
    SessionActivity {
        discuss_session_id: DiscussSessionId,
        active: bool,
    },

    /// An informational session warning to be broadcast to subscribers.
    /// The handle converts this to `DiscussBroadcast::SessionWarning`.
    #[serde(skip)]
    SessionWarning {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Human-readable warning message.
        warning: String,
    },
    /// The selected MCP transport for a live session.
    #[serde(skip)]
    McpStatus {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Current MCP transport status.
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
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
    },
    /// A session's turn activity changed.
    SessionActivity {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Whether a turn is active.
        active: bool,
    },
    /// An informational session warning (e.g. missing MCP-over-ACP capability).
    ///
    /// The warning string is a simple human-readable message — no nested enum
    /// or category layer. This is a reusable informational broadcast, not a
    /// reliable delivery path.
    SessionWarning {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Human-readable warning message.
        warning: String,
    },
    /// The selected MCP transport for a live session.
    McpStatus {
        /// Application-owned canonical session identity.
        discuss_session_id: DiscussSessionId,
        /// Provider ACP session id (protocol/diagnostic data).
        acp_session_id: SessionId,
        /// Current MCP transport status.
        status: DiscussMcpStatus,
    },
    /// One Provider runtime lifecycle projection changed.
    ProviderRuntimeChanged { state: ProviderRuntimeState },
    /// A Provider was removed; it no longer appears in `listProviders`.
    ProviderRemoved { provider_name: String },
    /// Custom agents, skills, teams or their installations changed.
    CatalogChanged,
}

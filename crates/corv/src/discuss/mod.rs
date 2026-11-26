mod actor;
mod agent_nodes;
mod agents;
mod allowance;
mod asset_sources;
mod assets;
mod boot;
mod catalog_types;
mod config;
mod database;
mod discovery;
mod domain;
mod elicitation;
mod error;
mod handle;
mod identity;
mod launch_types;
mod library;
mod mcp;
mod mcp_direct;
mod mcp_http;
mod mcp_relay;
mod messages;
mod migration;
mod plugins;
mod pricing;
mod provider;
mod report_types;
mod reports;
mod session;
mod session_launch;
mod session_reads;
mod session_turns;
mod sessions;
mod storage;
mod supervisor;
mod teams;
mod types;
mod usage;
mod worker;

#[cfg(test)]
mod test_library;

use domain::{DiscussKernelServices, DiscussProviderConfigSudoServiceImpl, DiscussStreamingServiceImpl};

pub use boot::{DiscussBootOptions, DiscussSettings};
pub use catalog_types::{AgentThemeColor, InstalledAssetKind};
pub use discovery::{
    DiscoveredProvider, DiscoveryOutcome, DiscoverySource, ProviderEnvironment, ProviderLaunch, discover_providers,
};
pub use domain::{DiscussDomain, DiscussIdentityService, DiscussProviderConfigSudoService, DiscussStreamingService};
pub use elicitation::{
    DiscussElicitationAnswer, DiscussElicitationClassification, DiscussElicitationRequestPayload,
    DiscussRestrictedFormField, DiscussRestrictedFormFieldType, DiscussRestrictedFormOption,
    DiscussRestrictedFormSchema,
};
pub use error::DiscussError;
pub use launch_types::{
    AcpProvider, Effect, LaunchLimitation, LaunchLimitationReason, LaunchLimitationSubject, LaunchPermissionOutcome,
    Operation, Rule, RuleSet, SessionCliLogin, SessionCliTool, SessionLaunchConfig,
};
pub use library::{
    AgentDefinition, AgentId, AgentRole, AgentSummary, AssetLibrary, CommandDefinition, CommandId, CommandSummary,
    SkillBundleFile, SkillDefinition, SkillId, SkillSummary, TeamDefinition, TeamSummary,
};
pub use mcp::discuss_mcp_domains;
pub use mcp_http::run_mcp;
pub use messages::{
    AddProviderInput, AnswerElicitationInput, AnswerPermissionInput, ArchiveSessionInput, AssignMcpInput,
    AttachedSessionEnsuredOutput, CancelElicitationInput, CancelSessionInput, CancelTurnInput,
    ChangeSessionWorkingDirInput, ConfigOptionsOutput, ConnectProviderSessionInput, CreateCustomMcpInput,
    DeleteCustomMcpInput, DiscussBroadcast, DiscussResponse, DisposeInspectorInput, EnsureAttachedSessionInput,
    EnsureAttachedSessionOutcome, GetConfigOptionsInput, GetCustomAgentOutput, GetCustomSkillOutput,
    GetProviderAuthStatusInput, GetProviderManagedConfigInput, GetServerStatusInput, GetSessionModesInput,
    ImportCustomAgentFromUrlInput, ImportCustomSkillFromUrlInput, InstallTeamAndStartSessionInput, InstallTeamInput,
    InstallTeamOutput, ListCustomMcpsInput, ListCustomMcpsOutput, ListProvidersInput, ListSessionsInput,
    LoadSessionInput, NewSessionInput, ProviderAuthStatusOutput, ProviderManagedConfigOutput,
    ProviderRemoveSuccessOutput, ProvidersListOutput, RecordAllowanceInput, RemoveProviderInput, RemoveSessionInput,
    RenameSessionInput, ReserveInspectorInput, ReserveInspectorOutput, ResolveSessionIdentityInput, SendUserTurnInput,
    SessionIdentityResolvedOutput, SessionListOutput, SessionLoadOutput, SetConfigOptionInput,
    SetProviderManagedConfigInput, SetSessionModeInput, StartInspectorInput, StartProviderInput, StartServerInput,
    StopProviderInput, StopServerInput, SuccessOutput, UnarchiveSessionInput, UnassignMcpInput, UpdateCustomMcpInput,
    discuss_operation_descriptors_with_root_error, dispatch_add_provider, dispatch_archive_session,
    dispatch_connect_provider_session, dispatch_get_config_options, dispatch_get_provider_auth_status,
    dispatch_get_provider_managed_config, dispatch_get_server_status, dispatch_get_session_modes,
    dispatch_install_team, dispatch_install_team_and_start_session, dispatch_list_custom_mcps, dispatch_list_providers,
    dispatch_list_sessions, dispatch_load_session, dispatch_record_allowance, dispatch_remove_provider,
    dispatch_remove_session, dispatch_rename_session, dispatch_set_config_option, dispatch_set_session_mode,
    dispatch_start_provider, dispatch_start_server, dispatch_stop_provider, dispatch_stop_server,
    dispatch_unarchive_session, rpc_router, send_new_session,
};
pub use plugins::install_team_at_root;
pub use report_types::{
    DiscussAllowanceAccountInfo, DiscussAllowanceAmount, DiscussAllowanceExtraWindow, DiscussAllowancePool,
    DiscussAllowanceProjection, DiscussAllowanceResetKind, DiscussAllowanceSavedResets, DiscussAllowanceStale,
    DiscussAllowanceWindow, DiscussAllowanceWindows,
};
pub use types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, CleanupWarning, CustomMcpEntry,
    CustomMcpScope, DiscussAuthState, DiscussBackgroundWork, DiscussBackgroundWorkKind, DiscussBackgroundWorkStatus,
    DiscussChatTurnRecord, DiscussConfigOption, DiscussConfigOptionValue, DiscussImageAttachment, DiscussMcpStatus,
    DiscussMemberOverrides, DiscussPermissionOption, DiscussResourceLink, DiscussSessionId, DiscussSessionInfo,
    DiscussSessionModes, DiscussSessionSetup, DiscussStreamEvent, DiscussSubagentRecord, DiscussSubagentSurfaceState,
    DiscussTerminalAuthMethod, DiscussTodoItem, DiscussTodoSnapshot, DiscussTodoStatus, DiscussTranscriptCursor,
    DiscussTranscriptPage, DiscussTurnUsage, DiscussUtilityPayload, DiscussUtilityUpdate, ElicitationRequestId,
    InspectorBinding, PermissionRequestId, ProviderRuntimeFailure, ProviderRuntimeState, ProviderRuntimeStatus,
    SensitiveMcpDescriptor,
};

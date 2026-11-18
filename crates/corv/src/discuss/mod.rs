mod actor;
mod agent_nodes;
mod agents;
mod allowance;
mod asset_sources;
mod assets;
mod boot;
mod config;
mod database;
mod discovery;
mod domain;
mod elicitation;
mod handle;
mod identity;
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
mod reports;
mod session;
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
pub use discovery::{
    DiscoveredProvider, DiscoveryOutcome, DiscoverySource, ProviderEnvironment, ProviderLaunch, discover_providers,
};
pub use domain::{DiscussDomain, DiscussIdentityService, DiscussProviderConfigSudoService, DiscussStreamingService};
pub use elicitation::{
    DiscussElicitationAnswer, DiscussElicitationClassification, DiscussElicitationRequestPayload,
    DiscussRestrictedFormField, DiscussRestrictedFormFieldType, DiscussRestrictedFormOption,
    DiscussRestrictedFormSchema,
};
pub use library::{
    AgentDefinition, AgentId, AgentRole, AgentSummary, AssetLibrary, CommandDefinition, CommandId, CommandSummary,
    SkillBundleFile, SkillDefinition, SkillId, SkillSummary, TeamDefinition, TeamSummary,
};
pub use mcp::discuss_mcp_domains;
pub use mcp_http::run_mcp;
pub use messages::{
    AddProviderInput, AnswerElicitationInput, AnswerPermissionInput, AssignMcpInput, CancelElicitationInput,
    CancelSessionInput, CancelTurnInput, ConfigOptionsOutput, CreateCustomMcpInput, DeleteCustomMcpInput,
    DiscussBroadcast, DiscussResponse, DisposeInspectorInput, GetConfigOptionsInput, GetCustomAgentOutput,
    GetCustomSkillOutput, GetProviderManagedConfigInput, GetServerStatusInput, ImportCustomAgentFromUrlInput,
    ImportCustomSkillFromUrlInput, InstallTeamAndStartSessionInput, InstallTeamInput, InstallTeamOutput,
    ListCustomMcpsInput, ListCustomMcpsOutput, ListProvidersInput, ListSessionsInput, LoadSessionInput,
    NewSessionInput, ProviderManagedConfigOutput, ProviderRemoveSuccessOutput, ProvidersListOutput,
    RecordAllowanceInput, RemoveProviderInput, RemoveSessionInput, RenameSessionInput, ReserveInspectorInput,
    ReserveInspectorOutput, ResolveSessionIdentityInput, SendUserTurnInput, SessionIdentityResolvedOutput,
    SessionListOutput, SessionLoadOutput, SetConfigOptionInput, SetProviderManagedConfigInput, StartInspectorInput,
    StartServerInput, StopServerInput, SuccessOutput, UnassignMcpInput, UpdateCustomMcpInput,
    discuss_operation_descriptors_with_root_error, dispatch_add_provider, dispatch_get_config_options,
    dispatch_get_provider_managed_config, dispatch_get_server_status, dispatch_install_team,
    dispatch_install_team_and_start_session, dispatch_list_custom_mcps, dispatch_list_providers,
    dispatch_list_sessions, dispatch_load_session, dispatch_record_allowance, dispatch_remove_provider,
    dispatch_remove_session, dispatch_rename_session, dispatch_set_config_option, dispatch_start_server,
    dispatch_stop_server, rpc_router, send_new_session,
};
pub use plugins::install_team_at_root;
pub use types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, AcpProvider, AgentThemeColor,
    CleanupWarning, CustomMcpEntry, CustomMcpScope, DiscussAllowanceAccountInfo, DiscussAllowanceAmount,
    DiscussAllowanceExtraWindow, DiscussAllowancePool, DiscussAllowanceProjection, DiscussAllowanceResetKind,
    DiscussAllowanceSavedResets, DiscussAllowanceStale, DiscussAllowanceWindow, DiscussAllowanceWindows,
    DiscussChatTurnRecord, DiscussConfigOption, DiscussConfigOptionValue, DiscussError, DiscussImageAttachment,
    DiscussMcpStatus, DiscussMemberOverrides, DiscussPermissionOption, DiscussSessionId, DiscussSessionInfo,
    DiscussSessionSetup, DiscussStreamEvent, DiscussSubagentRecord, DiscussSubagentSurfaceState,
    DiscussTerminalAuthMethod, DiscussTodoItem, DiscussTodoSnapshot, DiscussTodoStatus, DiscussUtilityPayload,
    DiscussUtilityUpdate, Effect, ElicitationRequestId, InspectorBinding, InstalledAssetKind, LaunchLimitation,
    LaunchLimitationReason, LaunchLimitationSubject, LaunchPermissionOutcome, Operation, PermissionRequestId,
    ProviderRuntimeFailure, ProviderRuntimeState, ProviderRuntimeStatus, Rule, RuleSet, SensitiveMcpDescriptor,
    SessionCliLogin, SessionCliTool, SessionLaunchConfig,
};

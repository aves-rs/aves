mod actor;
mod agent_nodes;
mod agents;
mod boot;
mod config;
mod database;
mod domain;
mod elicitation;
mod handle;
mod library;
mod mcp;
mod mcp_direct;
mod mcp_http;
mod mcp_relay;
mod messages;
mod migration;
mod plugins;
mod provider;
mod session;
mod sessions;
mod storage;
mod supervisor;
mod teams;
mod types;
mod worker;

#[cfg(test)]
mod test_library;

use domain::{DiscussKernelServices, DiscussProviderConfigSudoServiceImpl, DiscussStreamingServiceImpl};

pub use boot::{DiscussBootOptions, DiscussSettings};
pub use domain::{DiscussDomain, DiscussStreamingService};
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
    AddProviderInput, AnswerElicitationInput, AnswerPermissionInput, CancelElicitationInput, CancelSessionInput,
    CancelTurnInput, ConfigOptionsOutput, DiscussBroadcast, DiscussResponse, DisposeInspectorInput,
    GetConfigOptionsInput, GetServerStatusInput, ListProvidersInput, ListSessionsInput, LoadSessionInput,
    NewSessionInput, ProviderRemoveSuccessOutput, ProvidersListOutput, RemoveProviderInput, RemoveSessionInput,
    RenameSessionInput, ReserveInspectorInput, ReserveInspectorOutput, SendUserTurnInput, SessionListOutput,
    SessionLoadOutput, SetConfigOptionInput, StartInspectorInput, StartServerInput, StopServerInput, SuccessOutput,
    discuss_operation_descriptors_with_root_error, dispatch_add_provider, dispatch_get_config_options,
    dispatch_get_server_status, dispatch_list_providers, dispatch_list_sessions, dispatch_load_session,
    dispatch_remove_provider, dispatch_remove_session, dispatch_rename_session, dispatch_set_config_option,
    dispatch_start_server, dispatch_stop_server, rpc_router, send_new_session,
};
pub use types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, AcpProvider, CleanupWarning,
    DiscussChatTurnRecord, DiscussConfigOption, DiscussConfigOptionValue, DiscussError, DiscussMcpStatus,
    DiscussPermissionOption, DiscussSessionInfo, DiscussSessionSetup, DiscussStreamEvent, DiscussSubagentRecord,
    DiscussSubagentSurfaceState, DiscussTerminalAuthMethod, DiscussTodoItem, DiscussTodoSnapshot, DiscussTodoStatus,
    DiscussUtilityPayload, DiscussUtilityUpdate, ElicitationRequestId, InspectorBinding, PermissionRequestId,
    ProviderRuntimeFailure, ProviderRuntimeState, ProviderRuntimeStatus, SluiceSessionId,
};

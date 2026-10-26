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

pub use boot::DiscussBootOptions;
pub use domain::{DiscussDomain, DiscussStreamingService};
pub use elicitation::{DiscussElicitationAnswer, DiscussElicitationClassification};
pub use library::{
    AgentDefinition, AgentId, AgentRole, AgentSummary, AssetLibrary, CommandDefinition, CommandId, CommandSummary,
    SkillBundleFile, SkillDefinition, SkillId, SkillSummary, TeamDefinition, TeamSummary,
};
pub use mcp::discuss_mcp_domains;
pub use mcp_http::run_mcp;
pub use messages::{
    AnswerElicitationInput, AnswerPermissionInput, CancelElicitationInput, CancelSessionInput, CancelTurnInput,
    DiscussBroadcast, DiscussResponse, DisposeInspectorInput, GetServerStatusInput, NewSessionInput,
    ReserveInspectorInput, SendUserTurnInput, StartInspectorInput, StartServerInput, StopServerInput,
    discuss_operation_descriptors_with_root_error, dispatch_get_server_status, dispatch_start_server,
    dispatch_stop_server, rpc_router, send_new_session,
};
pub use types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, DiscussError, DiscussMcpStatus,
    DiscussPermissionOption, DiscussSessionSetup, DiscussStreamEvent, DiscussTerminalAuthMethod, DiscussUtilityPayload,
    ElicitationRequestId, PermissionRequestId, ProviderRuntimeState, SluiceSessionId,
};

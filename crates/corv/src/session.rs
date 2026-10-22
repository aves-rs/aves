//! ACP session execution for interactive chats.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AuthCapabilities, AuthMethod, ClientCapabilities, CreateElicitationRequest, CreateElicitationResponse,
    ElicitationAction, ElicitationCapabilities, ElicitationFormCapabilities, ElicitationScope, InitializeRequest,
    LoadSessionRequest, McpServer, NewSessionRequest, NewSessionResponse, RequestPermissionRequest,
    SessionConfigOptionValue, SessionId, SessionInfo, SetSessionConfigOptionRequest,
};
use agent_client_protocol::{
    AcpAgent, Agent, ConnectionTo, Dispatch, DynamicHandlerGuard, HandleDispatchFrom, Handled, SessionMessage,
    UntypedMessage,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Deserialize;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use super::types::AgentNodeTerminalResult;
use aves::auth::messages::{IssueJwtInput, RevokeJtiInput};
use aves::auth::{AuthCapability, AuthSudoService, Jwt};
use aves::bus::{BusMcpEndpoint, BusRpcContext, BusSudoService};
use aves::runtime::ServiceReader;

use super::agents::{AssistantTurnBuffer, FlushPayload};
use super::boot::DiscussMcpDomain;
use super::config::{ConfigPreferenceReconciler, assigned_agent_mode_selection, without_mode_preferences};
use super::messages::DiscussResponse;
use super::plugins::{self, ConfigSettings};
use super::storage::DiscussPersistence;
use super::supervisor::{
    CallbackEvent, CallbackRouteRegistry, CallbackRouteTarget, CallbackRouter, ElicitationCorrelationId,
};
use super::types::{
    DiscussConfigOption, DiscussError, DiscussMcpStatus, DiscussStreamEvent, ElicitationRequestId, PendingElicitation,
    PendingPermission, PermissionRequestId, ProviderSessionInfo, SluiceSessionId, WorkerCommand,
};
use super::worker::{run_connected_session, run_isolated};

pub(super) const SESSION_WORKER_JWT_EXPIRATION: ChronoDuration = ChronoDuration::days(30);

pub(super) struct IssuedWorkerToken {
    pub(super) token: Jwt,
    pub(super) jti: Uuid,
}

#[derive(Default)]
pub(super) struct AgentNodeTerminalState {
    pub(super) limit_error: Option<DiscussError>,
}

impl AgentNodeTerminalState {
    pub(super) fn record_limit_breach(
        &mut self,
        session_id: &str,
        limit: &'static str,
        actual: u64,
        maximum: u64,
    ) -> Result<Option<UntypedMessage>, agent_client_protocol::Error> {
        if self.limit_error.is_some() {
            return Ok(None);
        }
        self.limit_error = Some(DiscussError::AgentNodeLimitReached {
            limit: limit.to_string(),
            actual,
            maximum,
        });
        Ok(Some(UntypedMessage::new(
            "session/cancel",
            serde_json::json!({"sessionId": session_id}),
        )?))
    }

    pub(super) fn finish(&mut self, output: serde_json::Value) -> AgentNodeTerminalResult {
        self.limit_error
            .take()
            .map_or(AgentNodeTerminalResult::Succeeded { output }, |error| {
                AgentNodeTerminalResult::Failed { error }
            })
    }
}

pub(super) struct WorkerSessionParams {
    pub(super) stream_tx: mpsc::Sender<DiscussStreamEvent>,
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) auth_launch_server: agent_client_protocol::schema::v1::McpServer,
    pub(super) session_name: String,
    pub(super) agent_name: String,
    pub(super) assigned_agent_name: Option<String>,
    pub(super) reconnect_acp_session_id: Option<SessionId>,
    pub(super) persisted_config_options: Vec<DiscussConfigOption>,
    pub(super) session_settings: ConfigSettings,
    pub(super) restored_persisted_transcript: bool,
    pub(super) next_turn_position: i64,
    pub(super) agents_workspace: PathBuf,
    pub(super) sluice_mcp_enabled: bool,
    pub(super) bus_service: ServiceReader<dyn BusSudoService>,
    pub(super) flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
    pub(super) worker_jwt: Jwt,
    pub(super) root_context: BusRpcContext,
    pub(super) mcp_domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    pub(super) broadcast_tx: mpsc::Sender<DiscussResponse>,
    pub(super) persistence: DiscussPersistence,
    pub(super) worker_command_rx: mpsc::Receiver<WorkerCommand>,
    pub(super) capabilities_update_tx: mpsc::Sender<(SluiceSessionId, SessionCapabilitiesCache)>,
    pub(super) worker_ready_tx: mpsc::Sender<WorkerReady>,
    pub(super) agent_defaults: ConfigSettings,
    pub(super) pending_permission_tx: mpsc::Sender<PendingPermissionRegistration>,
    pub(super) pending_permission_cancel_tx: mpsc::Sender<PendingPermissionCancel>,
    pub(super) pending_elicitation_tx: mpsc::Sender<PendingElicitationRegistration>,
    pub(super) pending_elicitation_cancel_tx: mpsc::Sender<PendingElicitationCancel>,
    pub(super) announce_desktop_association: bool,
    pub(super) transient_connectivity: bool,
    pub(super) callback_router: Arc<Mutex<CallbackRouter>>,
    pub(super) callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    pub(super) shared_connection: Option<ConnectionTo<Agent>>,
    pub(super) shared_mcp_capabilities: Option<(bool, bool)>,
}

pub(super) struct WorkerReady {
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) acp_session_id: SessionId,
    pub(super) announce_desktop_association: bool,
    pub(super) transient_connectivity: bool,
    pub(super) config_options: Option<Vec<DiscussConfigOption>>,
    pub(super) supports_config_updates: bool,
}

pub(super) struct PendingPermissionRegistration {
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) permission_request_id: PermissionRequestId,
    pub(super) pending: PendingPermission,
    pub(super) registration_ack: oneshot::Sender<Result<(), DiscussError>>,
}

pub(super) struct PendingPermissionCancel {
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) permission_request_id: PermissionRequestId,
    pub(super) cancellation_ack: oneshot::Sender<()>,
}

pub(super) struct PendingElicitationRegistration {
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) elicitation_request_id: ElicitationRequestId,
    pub(super) pending: PendingElicitation,
    pub(super) registration_ack: oneshot::Sender<Result<(), DiscussError>>,
}

pub(super) struct PendingElicitationCancel {
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) elicitation_request_id: Option<ElicitationRequestId>,
    pub(super) cancellation_ack: oneshot::Sender<()>,
}

#[derive(Default)]
pub struct SessionAnchorMetadata<'a> {
    pub is_task_session: bool,
    pub team_leader_id: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub session_name: Option<&'a str>,
    pub acp_session_id: Option<&'a str>,
    pub display_title: Option<&'a str>,
    pub last_user_prompt: Option<&'a str>,
    pub latest_todo_snapshot_json: Option<&'a str>,
    pub latest_config_options_json: Option<&'a str>,
    pub config_settings_json: Option<&'a str>,
    pub available_commands_json: Option<&'a str>,
    pub clear: SessionAnchorClear,
}

#[derive(Default, Clone, Copy)]
pub struct SessionAnchorClear {
    pub display_title: bool,
    pub latest_config_options_json: bool,
    pub available_commands_json: bool,
}

pub(super) struct AttachedInspectorReaders(pub(super) HashMap<SluiceSessionId, JoinHandle<()>>);

pub(super) fn load_session_request(
    session_id: SessionId,
    agents_workspace: PathBuf,
    mcp_servers: &[McpServer],
) -> LoadSessionRequest {
    let request = LoadSessionRequest::new(session_id, agents_workspace);
    if mcp_servers.is_empty() {
        request
    } else {
        request.mcp_servers(mcp_servers.to_vec())
    }
}

pub(super) async fn issue_worker_jwt(
    auth_service: Option<&ServiceReader<dyn AuthSudoService>>,
    exp: ChronoDuration,
) -> Result<IssuedWorkerToken, DiscussError> {
    let service = auth_service
        .and_then(ServiceReader::get)
        .ok_or(DiscussError::WorkerTokenServiceUnavailable)?;
    let output = service
        .issue_jwt(IssueJwtInput {
            sub: "agent".to_owned(),
            cap: vec![AuthCapability::AllBus],
            exp: (Utc::now() + exp).to_rfc3339(),
        })
        .await
        .map_err(DiscussError::from)?;
    Ok(IssuedWorkerToken {
        token: output.token,
        jti: output.jti,
    })
}

pub(super) fn provider_sessions_from_schema(sessions: Vec<SessionInfo>) -> Vec<ProviderSessionInfo> {
    sessions
        .into_iter()
        .map(|session| ProviderSessionInfo {
            session_id: session.session_id,
            updated_at: session.updated_at,
            config_options: None,
        })
        .collect()
}

fn explicit_elicitation_session_id(request: &CreateElicitationRequest) -> Option<SessionId> {
    match request.scope() {
        ElicitationScope::Session(scope) => Some(scope.session_id.clone()),
        ElicitationScope::Request(_) => None,
        _ => None,
    }
}

fn elicitation_correlation_id(request: &CreateElicitationRequest) -> Option<ElicitationCorrelationId> {
    serde_json::to_value(request)
        .ok()?
        .get("elicitationId")
        .and_then(serde_json::Value::as_str)
        .map(ElicitationCorrelationId::new)
}

/// Revoke a delegated worker token without changing the worker's business outcome.
pub(super) async fn revoke_worker_jti(
    auth_service: Option<&ServiceReader<dyn AuthSudoService>>,
    jti: Option<Uuid>,
    worker_kind: &'static str,
) {
    let Some(jti) = jti else {
        return;
    };
    let Some(service) = auth_service.and_then(ServiceReader::get) else {
        tracing::warn!(target: "application_acp", %jti, worker_kind, "could not revoke delegated worker token because Auth is unavailable");
        return;
    };
    if let Err(error) = service.revoke_jti(RevokeJtiInput { jti }).await {
        tracing::warn!(target: "application_acp", ?error, %jti, worker_kind, "delegated worker token revocation failed; token expiry remains the fallback");
    }
}

/// Routes Provider callbacks to the session worker that owns their ACP session.
pub(super) struct SessionCallbackRouter {
    routes: Arc<Mutex<CallbackRouteRegistry>>,
}

impl SessionCallbackRouter {
    pub(super) fn new(routes: Arc<Mutex<CallbackRouteRegistry>>) -> Self {
        Self { routes }
    }

    async fn target_for_session(&self, session_id: &SessionId) -> Option<CallbackRouteTarget> {
        self.routes
            .lock()
            .await
            .callback_router
            .lock()
            .await
            .session_target(session_id)
            .cloned()
    }

    async fn forward(&self, target: CallbackRouteTarget, event: CallbackEvent) -> Result<(), CallbackEvent> {
        let queue = self.routes.lock().await.queue_for(&target);
        let Some(queue) = queue else {
            return Err(event);
        };
        queue.send(event).await.map_err(|send_error| send_error.0)
    }

    async fn fail_closed(event: CallbackEvent) -> Result<(), agent_client_protocol::Error> {
        match event {
            CallbackEvent::Permission { responder, .. } => {
                responder.respond_with_internal_error("callback target is unavailable")?;
            }
            CallbackEvent::Elicitation { responder, .. } => {
                responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
            }
            CallbackEvent::ElicitationComplete => {}
        }
        Ok(())
    }

    async fn route_permission(
        &self,
        request: RequestPermissionRequest,
        target: CallbackRouteTarget,
        responder: agent_client_protocol::Responder<agent_client_protocol::schema::v1::RequestPermissionResponse>,
    ) -> Result<(), agent_client_protocol::Error> {
        let event = CallbackEvent::Permission {
            request,
            responder,
            target: target.clone(),
        };
        if let Err(event) = self.forward(target, event).await {
            Self::fail_closed(event).await?;
        }
        Ok(())
    }

    async fn route_elicitation(
        &self,
        request: CreateElicitationRequest,
        raw_request: UntypedMessage,
        session_id: SessionId,
        target: CallbackRouteTarget,
        responder: agent_client_protocol::Responder<serde_json::Value>,
    ) -> Result<Handled<Dispatch>, agent_client_protocol::Error> {
        let correlation_id = elicitation_correlation_id(&request);
        if let Some(correlation_id) = &correlation_id
            && !self
                .routes
                .lock()
                .await
                .callback_router
                .lock()
                .await
                .register_elicitation(correlation_id.clone(), target.clone())
        {
            return Ok(Handled::No {
                message: Dispatch::Request(raw_request, responder),
                retry: false,
            });
        }
        let event = CallbackEvent::Elicitation {
            request,
            acp_session_id: session_id,
            responder: responder.cast::<CreateElicitationResponse>(),
            target: target.clone(),
        };
        if let Err(event) = self.forward(target, event).await {
            if let Some(correlation_id) = &correlation_id {
                self.routes
                    .lock()
                    .await
                    .callback_router
                    .lock()
                    .await
                    .take_elicitation(correlation_id);
            }
            Self::fail_closed(event).await?;
        }
        Ok(Handled::Yes)
    }

    async fn route_elicitation_complete(
        &self,
        notification: UntypedMessage,
    ) -> Result<Handled<Dispatch>, agent_client_protocol::Error> {
        let Some(correlation_id) = notification
            .params()
            .get("elicitationId")
            .and_then(serde_json::Value::as_str)
            .map(ElicitationCorrelationId::new)
        else {
            return Ok(Handled::No {
                message: Dispatch::Notification(notification),
                retry: false,
            });
        };
        let Some(target) = self
            .routes
            .lock()
            .await
            .callback_router
            .lock()
            .await
            .elicitation_target(&correlation_id)
        else {
            return Ok(Handled::No {
                message: Dispatch::Notification(notification),
                retry: false,
            });
        };
        let _ = self
            .forward(target, CallbackEvent::ElicitationComplete)
            .await;
        self.routes
            .lock()
            .await
            .callback_router
            .lock()
            .await
            .take_elicitation(&correlation_id);
        Ok(Handled::Yes)
    }
}

impl HandleDispatchFrom<Agent> for SessionCallbackRouter {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _cx: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, agent_client_protocol::Error> {
        match message {
            Dispatch::Request(request, responder) => {
                let raw_request = request.clone();
                if let Ok(permission) = serde_json::from_value::<RequestPermissionRequest>(request.params().clone())
                    && let Some(target) = self.target_for_session(&permission.session_id).await
                {
                    self.route_permission(
                        permission,
                        target,
                        responder.cast::<agent_client_protocol::schema::v1::RequestPermissionResponse>(),
                    )
                    .await?;
                    return Ok(Handled::Yes);
                }
                if let Ok(elicitation) = serde_json::from_value::<CreateElicitationRequest>(request.params().clone())
                    && let Some(session_id) = explicit_elicitation_session_id(&elicitation)
                    && let Some(target) = self.target_for_session(&session_id).await
                {
                    return self
                        .route_elicitation(elicitation, raw_request.clone(), session_id, target, responder)
                        .await;
                }
                Ok(Handled::No {
                    message: Dispatch::Request(raw_request, responder),
                    retry: false,
                })
            }
            Dispatch::Notification(notification) if notification.method() == "elicitation/complete" => {
                self.route_elicitation_complete(notification).await
            }
            message => Ok(Handled::No { message, retry: false }),
        }
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "SessionCallbackRouter"
    }
}

pub(super) async fn spawn_inspector_reader_reap(reader: JoinHandle<()>) {
    reader.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), reader).await;
}

impl Drop for AttachedInspectorReaders {
    fn drop(&mut self) {
        for (_, reader) in self.0.drain() {
            tokio::spawn(spawn_inspector_reader_reap(reader));
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TerminalAuthExtension {
    command: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    label: Option<String>,
}

pub fn initialize_request() -> InitializeRequest {
    let mut meta = serde_json::Map::new();
    meta.insert("terminal-auth".to_string(), serde_json::Value::Bool(true));
    let capabilities = ClientCapabilities::new()
        .auth(AuthCapabilities::new().terminal(true))
        .elicitation(ElicitationCapabilities::new().form(ElicitationFormCapabilities::new()))
        .meta(meta);
    InitializeRequest::new(ProtocolVersion::V1).client_capabilities(capabilities)
}

pub fn terminal_auth_methods(
    methods: &[AuthMethod],
    launch_server: &McpServer,
) -> Vec<super::types::DiscussTerminalAuthMethod> {
    methods
        .iter()
        .filter_map(|method| {
            let AuthMethod::Terminal(terminal) = method else {
                return None;
            };
            let extension = terminal
                .meta
                .as_ref()
                .and_then(|meta| meta.get("terminal-auth"))
                .and_then(|value| serde_json::from_value::<TerminalAuthExtension>(value.clone()).ok())
                .filter(|extension| !extension.command.as_os_str().is_empty());
            let (command, args, mut env, label) = if let Some(extension) = extension {
                (
                    extension.command,
                    extension.args,
                    extension.env,
                    extension.label.unwrap_or_else(|| terminal.name.clone()),
                )
            } else {
                let McpServer::Stdio(server) = launch_server else {
                    return None;
                };
                let mut args = server.args.clone();
                args.extend(terminal.args.clone());
                let env = server
                    .env
                    .iter()
                    .map(|variable| (variable.name.clone(), variable.value.clone()))
                    .collect();
                (server.command.clone(), args, env, terminal.name.clone())
            };
            env.extend(terminal.env.clone());
            Some(super::types::DiscussTerminalAuthMethod {
                id: terminal.id.to_string(),
                name: terminal.name.clone(),
                description: terminal.description.clone(),
                label,
                command,
                args,
                env,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpTransportSelection {
    None,
    NativeAcp,
    Http,
}

pub fn select_mcp_transport(enabled: bool, native_acp: bool, http: bool) -> McpTransportSelection {
    if !enabled {
        McpTransportSelection::None
    } else if native_acp {
        McpTransportSelection::NativeAcp
    } else if http {
        McpTransportSelection::Http
    } else {
        McpTransportSelection::None
    }
}

pub fn mcp_status_for_transport(
    enabled: bool,
    transport: McpTransportSelection,
    provisioned: bool,
) -> DiscussMcpStatus {
    if !enabled {
        DiscussMcpStatus::Off
    } else if !provisioned {
        DiscussMcpStatus::Broken
    } else {
        match transport {
            McpTransportSelection::NativeAcp => DiscussMcpStatus::Acp,
            McpTransportSelection::Http => DiscussMcpStatus::Fallback,
            McpTransportSelection::None => DiscussMcpStatus::Broken,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct SessionCapabilitiesCache {
    pub(super) list: bool,
    pub(super) load: bool,
    pub(super) set_config_option: bool,
}

impl SessionCapabilitiesCache {
    pub(super) fn from_agent_capabilities(
        plugin: &dyn plugins::DiscussAgentPlugin,
        caps: &agent_client_protocol::schema::v1::AgentCapabilities,
    ) -> Self {
        Self {
            list: plugin.supports_session_list(caps.session_capabilities.list.is_some()),
            load: caps.load_session,
            set_config_option: false,
        }
    }
}

pub(super) struct SessionWorkerFailure {
    pub(super) error: DiscussError,
    pub(super) terminal_auth_methods: Vec<super::types::DiscussTerminalAuthMethod>,
}

pub async fn run(agent: AcpAgent, params: WorkerSessionParams) -> Result<(), SessionWorkerFailure> {
    match params.shared_connection.clone() {
        Some(connection) => run_connected_session(params, connection)
            .await
            .map_err(|error| SessionWorkerFailure {
                error: DiscussError::from(error),
                terminal_auth_methods: Vec::new(),
            }),
        None => run_isolated(agent, params).await,
    }
}

pub(super) async fn ensure_agents_workspace_dir(path: &std::path::Path) {
    if let Err(error) = tokio::fs::create_dir_all(path).await {
        panic!(
            "ACP agents workspace directory does not exist and could not be created at `{}`: {error}",
            path.display()
        );
    }
}

pub(super) fn flush_cancelled_assistant_turn(buffer: &mut AssistantTurnBuffer) -> Option<FlushPayload> {
    let flush = buffer.flush("cancelled".to_string());
    super::database::should_persist_assistant_turn(&flush).then_some(flush)
}

#[derive(Default)]
pub(super) struct McpAcpCapabilityWarningState {
    delivered: bool,
}

impl McpAcpCapabilityWarningState {
    pub(super) async fn publish_once(
        &mut self,
        broadcast_tx: &mpsc::Sender<DiscussResponse>,
        sluice_session_id: super::types::SluiceSessionId,
        provider_session_id: &SessionId,
        agent_name: &str,
    ) -> Result<(), DiscussError> {
        if self.delivered {
            return Ok(());
        }
        tracing::debug!(
            target: "application_acp",
            acp_session_id = %provider_session_id.to_string(),
            agent_name = %agent_name,
            "publishing missing MCP-over-ACP capability warning as SessionWarning broadcast"
        );
        broadcast_tx
            .send(DiscussResponse::SessionWarning {
                sluice_session_id,
                acp_session_id: provider_session_id.clone(),
                warning: format!(
                    "agent `{agent_name}` does not advertise MCP-over-ACP capability; \
                     Sluice MCP tools will not be available for this session"
                ),
            })
            .await
            .map_err(|error| channel_send_error("mcp_capability_warning.broadcast", error.to_string()))?;
        self.delivered = true;
        Ok(())
    }
}

pub(super) async fn publish_mcp_status(
    broadcast_tx: &mpsc::Sender<DiscussResponse>,
    sluice_session_id: super::types::SluiceSessionId,
    provider_session_id: &SessionId,
    status: DiscussMcpStatus,
) -> Result<(), DiscussError> {
    broadcast_tx
        .send(DiscussResponse::McpStatus {
            sluice_session_id,
            acp_session_id: provider_session_id.clone(),
            status,
        })
        .await
        .map_err(|error| channel_send_error("mcp_status.broadcast", error.to_string()))?;
    Ok(())
}

struct SessionMessageRouter {
    session_id: SessionId,
    route_target: CallbackRouteTarget,
    callback_router: Option<Arc<Mutex<CallbackRouter>>>,
    update_tx: mpsc::UnboundedSender<SessionMessage>,
}

impl SessionMessageRouter {
    fn new(
        session_id: SessionId,
        route_target: CallbackRouteTarget,
        callback_router: Option<Arc<Mutex<CallbackRouter>>>,
        update_tx: mpsc::UnboundedSender<SessionMessage>,
    ) -> Self {
        Self {
            session_id,
            route_target,
            callback_router,
            update_tx,
        }
    }
}

impl HandleDispatchFrom<Agent> for SessionMessageRouter {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _cx: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, agent_client_protocol::Error> {
        let session_id: Option<SessionId> = match &message {
            Dispatch::Request(request, _) if request.method() == "elicitation/create" => None,
            Dispatch::Request(request, _) | Dispatch::Notification(request) => request
                .params()
                .get("sessionId")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?,
            Dispatch::Response(_, _) => None,
        };
        let route_matches = session_id.as_ref().is_some_and(|session_id| {
            *session_id == self.session_id
                && self.callback_router.as_ref().is_none_or(|router| {
                    router
                        .try_lock()
                        .ok()
                        .and_then(|router| router.session_target(session_id).cloned())
                        .is_some_and(|target| target == self.route_target)
                })
        });
        if route_matches {
            self.update_tx
                .send(SessionMessage::SessionMessage(message))
                .map_err(|_| agent_client_protocol::Error::internal_error())?;
            return Ok(Handled::Yes);
        }
        Ok(Handled::No { message, retry: false })
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        format!("SessionMessageRouter({})", self.session_id)
    }
}

pub struct WorkerSession {
    session_id: SessionId,
    route_target: CallbackRouteTarget,
    connection: ConnectionTo<Agent>,
    update_tx: mpsc::UnboundedSender<SessionMessage>,
    update_rx: mpsc::UnboundedReceiver<SessionMessage>,
    _handler_registration: DynamicHandlerGuard<Agent>,
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
}

impl WorkerSession {
    pub fn attach(
        connection: ConnectionTo<Agent>,
        response: NewSessionResponse,
        route_target: CallbackRouteTarget,
        callback_router: Arc<Mutex<CallbackRouter>>,
        callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
        callback_tx: Option<mpsc::Sender<CallbackEvent>>,
    ) -> Result<Self, agent_client_protocol::Error> {
        let NewSessionResponse { session_id, .. } = response;
        let (update_tx, update_rx) = mpsc::unbounded_channel();
        let handler_registration = connection.add_dynamic_handler(SessionMessageRouter::new(
            session_id.clone(),
            route_target.clone(),
            Some(callback_router.clone()),
            update_tx.clone(),
        ))?;
        if let (Some(routes), Some(callback_tx)) = (&callback_routes, callback_tx) {
            let registered = routes
                .try_lock()
                .map_err(|_| agent_client_protocol::Error::internal_error())?
                .register_queue(route_target.clone(), callback_tx);
            if !registered {
                return Err(agent_client_protocol::Error::internal_error());
            }
        }
        if !callback_router
            .try_lock()
            .map_err(|_| agent_client_protocol::Error::internal_error())?
            .register_session(session_id.clone(), route_target.clone())
        {
            return Err(agent_client_protocol::Error::internal_error());
        }
        Ok(Self {
            session_id,
            route_target,
            connection,
            update_tx,
            update_rx,
            _handler_registration: handler_registration,
            callback_router,
            callback_routes,
        })
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn connection(&self) -> &ConnectionTo<Agent> {
        &self.connection
    }

    pub fn send_prompt(&mut self, prompt: impl ToString) -> Result<(), agent_client_protocol::Error> {
        self.connection
            .send_request_to(
                Agent,
                agent_client_protocol::schema::v1::PromptRequest::new(
                    self.session_id.clone(),
                    vec![prompt.to_string().into()],
                ),
            )
            .on_receiving_result({
                let update_tx = self.update_tx.clone();
                async move |result| {
                    let response = result?;
                    update_tx
                        .send(SessionMessage::StopReason(response.stop_reason))
                        .map_err(|_| agent_client_protocol::Error::internal_error())?;
                    Ok(())
                }
            })
    }

    pub async fn read_update(&mut self) -> Result<SessionMessage, agent_client_protocol::Error> {
        self.update_rx
            .recv()
            .await
            .ok_or_else(agent_client_protocol::Error::internal_error)
    }
}

impl Drop for WorkerSession {
    fn drop(&mut self) {
        if let Ok(mut router) = self.callback_router.try_lock() {
            router.remove_target(&self.route_target);
        }
        if let Some(callback_routes) = &self.callback_routes
            && let Ok(mut routes) = callback_routes.try_lock()
        {
            routes.remove_target(&self.route_target);
        }
    }
}

pub(super) struct OpenConfiguredSessionOptions {
    pub(super) reconnect_session_id: Option<SessionId>,
    pub(super) cwd: PathBuf,
    pub(super) mcp_servers: Vec<McpServer>,
    pub(super) assigned_agent_name: Option<String>,
    pub(super) resolved_config: ConfigSettings,
    pub(super) persisted_config_options: Vec<DiscussConfigOption>,
}

pub(super) struct ConfiguredSession {
    pub(super) session: WorkerSession,
    pub(super) config_options: Vec<DiscussConfigOption>,
    pub(super) applied_settings: ConfigSettings,
    pub(super) supports_config_updates: bool,
}

pub(super) async fn open_configured_session(
    connection: ConnectionTo<Agent>,
    provider_name: &str,
    callback_target: CallbackRouteTarget,
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: Option<mpsc::Sender<CallbackEvent>>,
    options: OpenConfiguredSessionOptions,
) -> Result<ConfiguredSession, DiscussError> {
    tracing::debug!(provider = provider_name, cwd = %options.cwd.display(), reconnect = options.reconnect_session_id.is_some(), "Requesting ACP session open");
    let session_response = if let Some(session_id) = options.reconnect_session_id {
        let response = connection
            .send_request(load_session_request(
                session_id.clone(),
                options.cwd.clone(),
                &options.mcp_servers,
            ))
            .block_task()
            .await?;
        NewSessionResponse::new(session_id).config_options(response.config_options)
    } else {
        connection
            .send_request_to(Agent, {
                let request = NewSessionRequest::new(options.cwd);
                if options.mcp_servers.is_empty() {
                    request
                } else {
                    request.mcp_servers(options.mcp_servers)
                }
            })
            .block_task()
            .await?
    };

    tracing::debug!(provider = provider_name, acp_session_id = %session_response.session_id, "ACP session open response received");
    let initial_config_options = session_response.config_options.clone();
    let initial_config_metadata = session_response.meta.clone();
    let session = WorkerSession::attach(
        connection,
        session_response,
        callback_target,
        callback_router,
        callback_routes,
        callback_tx,
    )?;

    let agent_plugin = plugins::for_agent(provider_name);
    let normalized_options =
        agent_plugin.initial_config_options(initial_config_options.clone(), initial_config_metadata.as_ref());
    let supports_config_updates = initial_config_options.is_some() || normalized_options.is_some();
    let mut provider_options = normalized_options.unwrap_or(options.persisted_config_options);
    tracing::debug!(provider = provider_name, acp_session_id = %session.session_id(), assigned_agent = ?options.assigned_agent_name, option_count = provider_options.len(), "Resolving ACP session configuration");

    let mut applied_settings = options.resolved_config.clone();
    let selected_mode_config_id = if let Some(assigned_agent_name) = options.assigned_agent_name {
        let mode_selection =
            assigned_agent_mode_selection(&provider_options, &assigned_agent_name).ok_or_else(|| {
                DiscussError::Validation {
                    message: format!("Provider does not advertise assigned Agent `{assigned_agent_name}`"),
                }
            })?;
        let selected_mode_config_id = mode_selection.config_id.clone();
        let selected_mode_value = mode_selection.value.clone();
        tracing::debug!(provider = provider_name, acp_session_id = %session.session_id(), team = %assigned_agent_name, "Applying assigned Agent mode");
        let response = session
            .connection()
            .send_request(SetSessionConfigOptionRequest::new(
                session.session_id().clone(),
                mode_selection.config_id,
                SessionConfigOptionValue::value_id(mode_selection.value),
            ))
            .block_task()
            .await?;
        provider_options = agent_plugin
            .initial_config_options(Some(response.config_options), None)
            .unwrap_or_default();
        applied_settings.insert(selected_mode_config_id.clone(), selected_mode_value);
        Some(selected_mode_config_id)
    } else {
        None
    };

    let preferences = selected_mode_config_id
        .as_deref()
        .map(|config_id| without_mode_preferences(&options.resolved_config, &provider_options, config_id))
        .unwrap_or(options.resolved_config);
    let mut reconciler = ConfigPreferenceReconciler::new(&preferences, provider_options);

    while let Some(selection) = reconciler.next() {
        let response = session
            .connection()
            .send_request(SetSessionConfigOptionRequest::new(
                session.session_id().clone(),
                selection.config_id,
                SessionConfigOptionValue::value_id(selection.value),
            ))
            .block_task()
            .await?;
        reconciler.adopt_provider_options(
            agent_plugin
                .initial_config_options(Some(response.config_options), None)
                .unwrap_or_default(),
        );
    }

    Ok(ConfiguredSession {
        session,
        config_options: reconciler.finish(),
        applied_settings,
        supports_config_updates,
    })
}

pub(super) fn channel_send_error(operation: &'static str, cause: impl Into<String>) -> DiscussError {
    DiscussError::Infrastructure {
        kind: "channel_send".to_string(),
        target_domain: "discuss".to_string(),
        operation: operation.to_string(),
        message: cause.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_elicitation_routes_by_session_without_url_correlation() {
        let request: CreateElicitationRequest = serde_json::from_value(serde_json::json!({
            "sessionId": "ses_opencode",
            "toolCallId": "call_question",
            "mode": "form",
            "message": "Which package manager?",
            "requestedSchema": {
                "type": "object",
                "properties": {
                    "pm": { "type": "string", "oneOf": [{ "const": "bun", "title": "Bun" }] }
                },
                "required": ["pm"]
            }
        }))
        .expect("OpenCode form request should decode");

        assert_eq!(
            explicit_elicitation_session_id(&request),
            Some(SessionId::new("ses_opencode"))
        );
        assert!(elicitation_correlation_id(&request).is_none());
    }

    #[test]
    fn request_scoped_elicitation_has_no_session_route() {
        let request: CreateElicitationRequest = serde_json::from_value(serde_json::json!({
            "requestId": 12,
            "mode": "form",
            "message": "Which package manager?",
            "requestedSchema": { "type": "object", "properties": {} }
        }))
        .expect("request-scoped form should decode");

        assert!(explicit_elicitation_session_id(&request).is_none());
    }
}

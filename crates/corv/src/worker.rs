use std::{collections::HashMap, sync::Arc, time::Duration};

use agent_client_protocol::schema::v1::{
    CreateElicitationResponse, ElicitationAction, ElicitationMode, ListSessionsRequest, NewSessionResponse,
    RequestPermissionRequest, RequestPermissionResponse, SessionConfigOptionValue, SessionId,
    SetSessionConfigOptionRequest,
};
use agent_client_protocol::{
    AcpAgent, Agent, Client, ConnectionTo, DynamicHandlerGuard, Responder, SessionMessage, UntypedMessage,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use super::types::{AgentNodeSessionLimits, AgentNodeTerminalResult};
use aves::auth::Jwt;
use aves::bus::{BusMcpEndpoint, BusRpcContext, BusSudoService};
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::agents::{
    AgentMessageForwardContext, AgentMessageForwardOutcome, AssistantTurnBuffer, TranscriptPositionAllocator,
    prepare_elicitation_request, prepare_permission_request, resolve_permission_responder, send_message_to_user,
};
use super::boot::DiscussMcpDomain;
use super::config::merged_config_preferences;
use super::database::{PersistenceJob, enqueue_persistence_job, enqueue_utility_persistence, spawn_persistence_queue};
use super::elicitation::content_to_sdk_answer;
use super::mcp_direct::McpAcpShim;
use super::mcp_relay::{McpHttpLease, endpoint_to_acp_descriptor};
use super::plugins::{self, DiscussAgentPlugin, PluginCaptureState};
use super::session::{
    AgentNodeTerminalState, AttachedInspectorReaders, McpAcpCapabilityWarningState, McpTransportSelection,
    OpenConfiguredSessionOptions, PendingElicitationCancel, PendingElicitationRegistration, PendingPermissionCancel,
    PendingPermissionRegistration, SessionCallbackRouter, SessionCapabilitiesCache, SessionWorkerFailure, WorkerReady,
    WorkerSession, WorkerSessionParams, channel_send_error, ensure_agents_workspace_dir,
    flush_cancelled_assistant_turn, initialize_request, load_session_request, mcp_status_for_transport,
    open_configured_session, provider_sessions_from_schema, publish_mcp_status, select_mcp_transport,
    spawn_inspector_reader_reap, terminal_auth_methods,
};
use super::supervisor::{CallbackEvent, CallbackRouteRegistry, CallbackRouteTarget, CallbackRouter};
use super::types::{
    DiscussElicitationAnswer, DiscussElicitationClassification, DiscussElicitationRequestPayload, DiscussError,
    DiscussPermissionOption, DiscussStreamEvent, DiscussUtilityPayload, DiscussUtilityUpdate, PendingPermission,
    ProviderSessionInfo, SluiceSessionId, WorkerCommand,
};

pub(super) async fn run_isolated(agent: AcpAgent, params: WorkerSessionParams) -> Result<(), SessionWorkerFailure> {
    let session_name = params.session_name.clone();
    let auth_launch_server = params.auth_launch_server.clone();
    let agent_plugin = plugins::for_agent(&params.agent_name);
    let callback_router = params.callback_router.clone();
    let callback_routes = Arc::new(Mutex::new(CallbackRouteRegistry::new(callback_router)));
    let callback_routes_for_connection = callback_routes.clone();
    let captured_terminal_auth_methods = Arc::new(Mutex::new(Vec::new()));
    let captured_terminal_auth_methods_for_connection = captured_terminal_auth_methods.clone();

    let result = Client
        .builder()
        .name(session_name)
        .connect_with(agent, move |connection: ConnectionTo<Agent>| async move {
            let _callback_handler =
                connection.add_dynamic_handler(SessionCallbackRouter::new(callback_routes_for_connection))?;
            let init_response = connection
                .send_request(initialize_request())
                .block_task()
                .await?;
            *captured_terminal_auth_methods_for_connection.lock().await =
                terminal_auth_methods(&init_response.auth_methods, &auth_launch_server);

            let mut params = params;
            params
                .capabilities_update_tx
                .send((
                    params.sluice_session_id,
                    SessionCapabilitiesCache::from_agent_capabilities(agent_plugin, &init_response.agent_capabilities),
                ))
                .await
                .map_err(|error| {
                    agent_client_protocol::Error::into_internal_error(channel_send_error(
                        "isolated.capabilities_update",
                        error.to_string(),
                    ))
                })?;
            params.callback_routes = Some(callback_routes);
            params.shared_mcp_capabilities = Some((
                init_response.agent_capabilities.mcp_capabilities.acp,
                init_response.agent_capabilities.mcp_capabilities.http,
            ));
            run_connected_session(params, connection).await
        })
        .await;

    let terminal_auth_methods = captured_terminal_auth_methods.lock().await.clone();
    result.map_err(|error| SessionWorkerFailure {
        error: DiscussError::from(error),
        terminal_auth_methods,
    })
}

pub(super) async fn run_connected_session(
    params: WorkerSessionParams,
    connection: ConnectionTo<Agent>,
) -> Result<(), agent_client_protocol::Error> {
    let setup = prepare_worker_session(&params, connection).await?;
    let WorkerSessionParams {
        stream_tx,
        sluice_session_id,
        auth_launch_server,
        session_name,
        agent_name,
        next_turn_position,
        agents_workspace,
        persistence,
        restored_persisted_transcript,
        worker_command_rx,
        worker_ready_tx,
        callback_router,
        callback_routes,
        root_context,
        pending_permission_tx,
        pending_permission_cancel_tx,
        pending_elicitation_tx,
        pending_elicitation_cancel_tx,
        announce_desktop_association,
        transient_connectivity,
        ..
    } = params;
    let WorkerSessionSetup {
        callback_tx,
        mut callback_rx,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
        opened,
    } = setup;
    let mut session = opened.session;
    let mut latest_config_options = opened.latest_config_options;
    let mut session_settings = opened.session_settings;

    let (persistence_queue, persistence_drain) = spawn_persistence_queue(
        persistence,
        sluice_session_id.to_string(),
        agent_name.clone(),
        session_name,
        root_context.clone(),
    );
    let mut position_allocator = TranscriptPositionAllocator::new(next_turn_position);
    let agent_plugin = plugins::for_agent(&agent_name);
    acknowledge_worker_ready(
        &persistence_queue,
        sluice_session_id,
        session.session_id(),
        &latest_config_options,
        &session_settings,
        &worker_ready_tx,
        announce_desktop_association,
        transient_connectivity,
        opened.supports_config_updates,
    )
    .await?;

    let mut capture_state = PluginCaptureState::for_plugin(agent_plugin);
    tracing::info!(%sluice_session_id, acp_session_id = %session.session_id(), "ACP session started and ready for a prompt");
    let (resolved_subagent_tx, mut resolved_subagent_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    if let PluginCaptureState::OpenCode(state) = &mut capture_state {
        state.configure_db_lookup(&auth_launch_server, session.session_id(), resolved_subagent_tx);
    }
    let mut buffer = AssistantTurnBuffer::default();
    let mut worker_command_rx = worker_command_rx;
    let mut attached_readers = AttachedInspectorReaders(HashMap::new());
    let mut awaiting_turn = true;
    let mut agent_node_terminal: Option<oneshot::Sender<AgentNodeTerminalResult>> = None;
    let mut agent_node_limits: Option<AgentNodeSessionLimits> = None;
    let mut agent_node_turns = 0_u64;
    let mut agent_node_terminal_state = AgentNodeTerminalState::default();
    let mut agent_node_deadline: Option<Instant> = None;
    let mut suppress_transcript = restored_persisted_transcript;
    loop {
        tokio::select! {
            update = session.read_update() => {
                let agent_node_was_active = agent_node_terminal.is_some();
                handle_session_update(
                    &session,
                    &stream_tx,
                    sluice_session_id,
                    agent_plugin,
                    &mut capture_state,
                    update?,
                    &mut buffer,
                    &persistence_queue,
                    &mut position_allocator,
                    &mut awaiting_turn,
                    &mut suppress_transcript,
                    &mut agent_node_terminal,
                    &mut agent_node_limits,
                    &mut agent_node_terminal_state,
                    &mut agent_node_deadline,
                ).await?;
                if agent_node_was_active && agent_node_terminal.is_none() {
                    break;
                }
            }
            _ = async {
                match agent_node_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                handle_agent_node_deadline(
                    &session,
                    &mut agent_node_deadline,
                    &agent_node_terminal,
                    agent_node_limits,
                    &mut agent_node_terminal_state,
                )?;
            }
            Some(subagent) = resolved_subagent_rx.recv() => {
                forward_resolved_subagent(
                    subagent,
                    &stream_tx,
                    sluice_session_id,
                    session.session_id(),
                    &persistence_queue,
                )
                .await?;
            }
            Some(callback) = callback_rx.recv() => {
                dispatch_callback(
                    callback,
                    &pending_permission_tx,
                    &pending_permission_cancel_tx,
                    &position_allocator,
                    &pending_elicitation_tx,
                    &pending_elicitation_cancel_tx,
                    &stream_tx,
                    transient_connectivity,
                )
                .await?;
            }
            command = worker_command_rx.recv() => {
                match command {
                    None | Some(WorkerCommand::Stop) => break,
                    Some(WorkerCommand::SendUserTurn { text, reply }) => {
                        handle_send_user_turn(
                            &mut session,
                            text,
                            &persistence_queue,
                            &mut position_allocator,
                            &mut awaiting_turn,
                            &mut suppress_transcript,
                            reply,
                        )
                        .await?;
                    }
                    Some(WorkerCommand::RunAgentNodeTurn {
                        text,
                        limits,
                        accepted,
                        terminal,
                    }) => {
                        handle_agent_node_turn(
                            &mut session,
                            text,
                            limits,
                            accepted,
                            terminal,
                            &persistence_queue,
                            &mut position_allocator,
                            &mut awaiting_turn,
                            &mut suppress_transcript,
                            &mut agent_node_limits,
                            &mut agent_node_turns,
                            &mut agent_node_terminal_state,
                            &mut agent_node_deadline,
                            &mut agent_node_terminal,
                            sluice_session_id,
                        )
                        .await?;
                    }
                    Some(WorkerCommand::CancelTurn { reply }) => {
                        cancel_turn(
                            &session,
                            sluice_session_id,
                            &mut agent_node_terminal,
                            &mut buffer,
                            &persistence_queue,
                            &mut position_allocator,
                            reply,
                        )
                        .await?;
                    }
                    Some(WorkerCommand::SetConfigOption { config_id, value, reply }) => {
                        let result = persist_set_config_option(
                            &session,
                            config_id,
                            value,
                            agent_plugin,
                            &mut latest_config_options,
                            &mut session_settings,
                            &persistence_queue,
                        )
                        .await;
                        let _ = reply.send(result);
                    }
                    Some(WorkerCommand::ListProviderSessions { reply }) => {
                        let _ = reply.send(list_provider_sessions(&session).await);
                    }
                    Some(WorkerCommand::LoadProviderSession { acp_session_id, reply }) => {
                        let _ = reply.send(load_provider_session(&session, &acp_session_id, &agents_workspace).await);
                    }
                    Some(WorkerCommand::AttachInspector {
                        viewer_session_id,
                        child_session_id,
                        reply,
                    }) => {
                        let result = attach_inspector(
                            &session,
                            &agents_workspace,
                            &mcp_descriptors,
                            &callback_router,
                            &callback_routes,
                            &callback_tx,
                            &stream_tx,
                            agent_plugin,
                            &mut attached_readers,
                            viewer_session_id,
                            &child_session_id,
                        )
                        .await;
                        let _ = reply.send(result);
                    }
                    Some(WorkerCommand::DisposeInspector { viewer_session_id, reply }) => {
                        if let Some(reader) = attached_readers.0.remove(&viewer_session_id) {
                            tokio::spawn(spawn_inspector_reader_reap(reader));
                        }
                        let _ = reply.send(Ok(()));
                    }
                }
            }
        }
    }
    teardown_connected_worker(
        mcp_handlers,
        mcp_shims,
        mcp_http_lease,
        persistence_queue,
        persistence_drain,
    )
    .await;
    Ok(())
}

async fn teardown_connected_worker(
    mcp_handlers: Vec<DynamicHandlerGuard<Agent>>,
    mcp_shims: Vec<McpAcpShim>,
    mcp_http_lease: Option<McpHttpLease>,
    persistence_queue: mpsc::Sender<PersistenceJob>,
    persistence_drain: JoinHandle<()>,
) {
    drop(mcp_handlers);
    drop(mcp_shims);
    if let Some(lease) = mcp_http_lease {
        let _ = lease.close().await;
    }
    drop(persistence_queue);
    let _ = persistence_drain.await;
}

async fn list_provider_sessions(session: &WorkerSession) -> Result<Vec<ProviderSessionInfo>, DiscussError> {
    session
        .connection()
        .send_request(ListSessionsRequest::new())
        .block_task()
        .await
        .map(|response| provider_sessions_from_schema(response.sessions))
        .map_err(DiscussError::from)
}

async fn load_provider_session(
    session: &WorkerSession,
    acp_session_id: &SessionId,
    agents_workspace: &std::path::Path,
) -> Result<ProviderSessionInfo, DiscussError> {
    session
        .connection()
        .send_request(load_session_request(
            acp_session_id.clone(),
            agents_workspace.to_path_buf(),
            &[],
        ))
        .block_task()
        .await
        .map(|_| ProviderSessionInfo {
            session_id: acp_session_id.clone(),
            updated_at: None,
            config_options: None,
        })
        .map_err(DiscussError::from)
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn acknowledge_worker_ready(
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    sluice_session_id: SluiceSessionId,
    acp_session_id: &SessionId,
    latest_config_options: &[super::types::DiscussConfigOption],
    session_settings: &super::plugins::ConfigSettings,
    worker_ready_tx: &mpsc::Sender<WorkerReady>,
    announce_desktop_association: bool,
    transient_connectivity: bool,
    supports_config_updates: bool,
) -> Result<(), agent_client_protocol::Error> {
    let (session_open_tx, session_open_rx) = oneshot::channel();
    persistence_queue
        .send(PersistenceJob::SessionOpen {
            acp_session_id: acp_session_id.to_string(),
            completion_tx: Some(session_open_tx),
        })
        .await
        .map_err(|error| {
            agent_client_protocol::Error::into_internal_error(channel_send_error(
                "shared.persist_session_open",
                error.to_string(),
            ))
        })?;
    session_open_rx
        .await
        .map_err(|error| {
            agent_client_protocol::Error::into_internal_error(DiscussError::Infrastructure {
                kind: "oneshot_reply_missing".to_string(),
                target_domain: "discuss".to_string(),
                operation: "persist_session_open".to_string(),
                message: error.to_string(),
            })
        })?
        .map_err(agent_client_protocol::Error::into_internal_error)?;
    if !latest_config_options.is_empty() {
        let (config_tx, config_rx) = oneshot::channel();
        persistence_queue
            .send(PersistenceJob::ConfigOptions {
                options: latest_config_options.to_vec(),
                session_settings: Some(session_settings.clone()),
                completion_tx: Some(config_tx),
            })
            .await
            .map_err(|error| {
                agent_client_protocol::Error::into_internal_error(channel_send_error(
                    "shared.persist_config_options",
                    error.to_string(),
                ))
            })?;
        config_rx
            .await
            .map_err(|error| {
                agent_client_protocol::Error::into_internal_error(DiscussError::Infrastructure {
                    kind: "oneshot_reply_missing".to_string(),
                    target_domain: "discuss".to_string(),
                    operation: "persist_config_options".to_string(),
                    message: error.to_string(),
                })
            })?
            .map_err(agent_client_protocol::Error::into_internal_error)?;
    }
    worker_ready_tx
        .send(WorkerReady {
            sluice_session_id,
            acp_session_id: acp_session_id.clone(),
            announce_desktop_association,
            transient_connectivity,
            config_options: Some(latest_config_options.to_vec()),
            supports_config_updates,
        })
        .await
        .map_err(|error| {
            agent_client_protocol::Error::into_internal_error(channel_send_error(
                "shared.worker_ready",
                error.to_string(),
            ))
        })?;
    Ok(())
}

struct WorkerSessionSetup {
    callback_tx: mpsc::Sender<CallbackEvent>,
    callback_rx: mpsc::Receiver<CallbackEvent>,
    mcp_shims: Vec<McpAcpShim>,
    mcp_handlers: Vec<DynamicHandlerGuard<Agent>>,
    mcp_http_lease: Option<McpHttpLease>,
    mcp_descriptors: Vec<agent_client_protocol::schema::v1::McpServer>,
    opened: OpenedWorkerSession,
}

async fn prepare_worker_session(
    params: &WorkerSessionParams,
    connection: ConnectionTo<Agent>,
) -> Result<WorkerSessionSetup, agent_client_protocol::Error> {
    let (callback_tx, callback_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    tracing::debug!(%params.sluice_session_id, provider = %params.agent_name, cwd = %params.agents_workspace.display(), "Preparing ACP session transports");
    let PreparedMcpTransports {
        mcp_transport,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
    } = prepare_mcp_transports(
        &connection,
        params.shared_mcp_capabilities,
        params.sluice_mcp_enabled,
        params.flow_mcp_endpoints.as_deref(),
        params.mcp_domains,
        params.sluice_session_id,
        &params.root_context,
        &params.worker_jwt,
        &params.bus_service,
    )
    .await?;
    let provisioned_mcp = !mcp_descriptors.is_empty();
    let opened = open_worker_session(
        connection,
        &params.agents_workspace,
        &params.agent_name,
        params.sluice_session_id,
        params.callback_router.clone(),
        params.callback_routes.clone(),
        &callback_tx,
        &params.broadcast_tx,
        OpenConfiguredSessionOptions {
            reconnect_session_id: params.reconnect_acp_session_id.clone(),
            cwd: params.agents_workspace.clone(),
            mcp_servers: mcp_descriptors.clone(),
            assigned_agent_name: params.assigned_agent_name.clone(),
            resolved_config: merged_config_preferences(&params.agent_defaults, &params.session_settings),
            persisted_config_options: params.persisted_config_options.clone(),
        },
        mcp_transport,
        params.sluice_mcp_enabled,
        provisioned_mcp,
    )
    .await?;
    Ok(WorkerSessionSetup {
        callback_tx,
        callback_rx,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
        opened,
    })
}

struct OpenedWorkerSession {
    session: WorkerSession,
    latest_config_options: Vec<super::types::DiscussConfigOption>,
    session_settings: super::plugins::ConfigSettings,
    supports_config_updates: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn open_worker_session(
    connection: ConnectionTo<Agent>,
    agents_workspace: &std::path::Path,
    agent_name: &str,
    sluice_session_id: SluiceSessionId,
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: &mpsc::Sender<CallbackEvent>,
    broadcast_tx: &mpsc::Sender<super::messages::DiscussResponse>,
    options: OpenConfiguredSessionOptions,
    mcp_transport: McpTransportSelection,
    sluice_mcp_enabled: bool,
    provisioned_mcp: bool,
) -> Result<OpenedWorkerSession, agent_client_protocol::Error> {
    ensure_agents_workspace_dir(agents_workspace).await;
    tracing::debug!(%sluice_session_id, provider = %agent_name, cwd = %agents_workspace.display(), mcp_servers = options.mcp_servers.len(), "Opening configured ACP session");
    let configured = open_configured_session(
        connection,
        agent_name,
        CallbackRouteTarget::Chat { sluice_session_id },
        callback_router,
        callback_routes,
        Some(callback_tx.clone()),
        options,
    )
    .await
    .map_err(agent_client_protocol::Error::into_internal_error)?;
    let session = configured.session;
    tracing::debug!(%sluice_session_id, acp_session_id = %session.session_id(), "ACP session configuration completed");
    let latest_config_options = configured.config_options;
    let session_settings = configured.applied_settings;
    let supports_config_updates = configured.supports_config_updates;
    let mut capability_warning = McpAcpCapabilityWarningState::default();
    if sluice_mcp_enabled && mcp_transport == McpTransportSelection::None {
        capability_warning
            .publish_once(broadcast_tx, sluice_session_id, session.session_id(), agent_name)
            .await
            .map_err(agent_client_protocol::Error::into_internal_error)?;
    }
    publish_mcp_status(
        broadcast_tx,
        sluice_session_id,
        session.session_id(),
        mcp_status_for_transport(sluice_mcp_enabled, mcp_transport, provisioned_mcp),
    )
    .await
    .map_err(agent_client_protocol::Error::into_internal_error)?;
    Ok(OpenedWorkerSession {
        session,
        latest_config_options,
        session_settings,
        supports_config_updates,
    })
}

struct PreparedMcpTransports {
    mcp_transport: McpTransportSelection,
    mcp_shims: Vec<McpAcpShim>,
    mcp_handlers: Vec<DynamicHandlerGuard<Agent>>,
    mcp_http_lease: Option<McpHttpLease>,
    mcp_descriptors: Vec<agent_client_protocol::schema::v1::McpServer>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn prepare_mcp_transports(
    connection: &ConnectionTo<Agent>,
    shared_mcp_capabilities: Option<(bool, bool)>,
    sluice_mcp_enabled: bool,
    flow_mcp_endpoints: Option<&[BusMcpEndpoint]>,
    mcp_domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    sluice_session_id: SluiceSessionId,
    root_context: &BusRpcContext,
    worker_jwt: &Jwt,
    bus_service: &ServiceReader<dyn BusSudoService>,
) -> Result<PreparedMcpTransports, agent_client_protocol::Error> {
    let mcp_transport = shared_mcp_capabilities
        .map(|(native, http)| select_mcp_transport(sluice_mcp_enabled, native, http))
        .unwrap_or(McpTransportSelection::None);
    let mut mcp_shims = Vec::new();
    let mut mcp_handlers: Vec<DynamicHandlerGuard<Agent>> = Vec::new();
    let mut mcp_http_lease = None;
    let mut mcp_descriptors = Vec::new();
    if mcp_transport == McpTransportSelection::NativeAcp {
        for domain in mcp_domains().map_err(agent_client_protocol::Error::into_internal_error)? {
            let shim = McpAcpShim::new(
                &domain,
                sluice_session_id.to_string(),
                root_context.with_jwt(worker_jwt.clone()),
                worker_jwt.clone(),
            );
            mcp_descriptors.push(shim.descriptor());
            mcp_handlers.push(connection.add_dynamic_handler(shim.dynamic_handler())?);
            mcp_shims.push(shim);
        }
    } else if mcp_transport == McpTransportSelection::Http
        && let Some(endpoints) = flow_mcp_endpoints
    {
        mcp_descriptors.extend(
            endpoints
                .iter()
                .map(|endpoint| endpoint_to_acp_descriptor(endpoint, worker_jwt)),
        );
    } else if mcp_transport == McpTransportSelection::Http
        && let Some(service) = bus_service.get()
        && let Ok(lease) = McpHttpLease::open(sluice_session_id.to_string(), service, worker_jwt.clone()).await
    {
        mcp_descriptors.extend(lease.descriptors().to_vec());
        mcp_http_lease = Some(lease);
    }

    Ok(PreparedMcpTransports {
        mcp_transport,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
    })
}

async fn handle_send_user_turn(
    session: &mut WorkerSession,
    text: String,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    position_allocator: &mut TranscriptPositionAllocator,
    awaiting_turn: &mut bool,
    suppress_transcript: &mut bool,
    reply: oneshot::Sender<Result<(), DiscussError>>,
) -> Result<(), agent_client_protocol::Error> {
    if !*awaiting_turn {
        let _ = reply.send(Err(DiscussError::Validation {
            message: "user turn received while the prior turn is active".to_string(),
        }));
        return Ok(());
    }
    if let Err(error) = persistence_queue
        .send(PersistenceJob::InitialTurn {
            acp_session_id: session.session_id().to_string(),
            position: position_allocator.next(),
            initial_prompt: text.clone(),
        })
        .await
    {
        let _ = reply.send(Err(channel_send_error("shared.persist_user_turn", error.to_string())));
        return Ok(());
    }
    *suppress_transcript = false;
    session.send_prompt(text)?;
    *awaiting_turn = false;
    let _ = reply.send(Ok(()));
    Ok(())
}

async fn cancel_turn(
    session: &WorkerSession,
    sluice_session_id: SluiceSessionId,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    buffer: &mut AssistantTurnBuffer,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    position_allocator: &mut TranscriptPositionAllocator,
    reply: oneshot::Sender<Result<(), DiscussError>>,
) -> Result<(), agent_client_protocol::Error> {
    if let Some(terminal) = agent_node_terminal.take() {
        let _ = terminal.send(AgentNodeTerminalResult::Failed {
            error: DiscussError::SessionCancelled { sluice_session_id },
        });
    }
    if let Some(flush) = flush_cancelled_assistant_turn(buffer)
        && let Err(error) = persistence_queue
            .send(PersistenceJob::AssistantTurn {
                assistant_position: position_allocator.next(),
                flush,
            })
            .await
    {
        let _ = reply.send(Err(channel_send_error(
            "shared.persist_cancelled_turn",
            error.to_string(),
        )));
        return Ok(());
    }
    let notification = UntypedMessage::new(
        "session/cancel",
        serde_json::json!({"sessionId": session.session_id().to_string()}),
    )?;
    session.connection().send_notification(notification)?;
    let _ = reply.send(Ok(()));
    Ok(())
}

fn handle_agent_node_deadline(
    session: &WorkerSession,
    deadline: &mut Option<Instant>,
    terminal: &Option<oneshot::Sender<AgentNodeTerminalResult>>,
    limits: Option<AgentNodeSessionLimits>,
    terminal_state: &mut AgentNodeTerminalState,
) -> Result<(), agent_client_protocol::Error> {
    *deadline = None;
    if terminal.is_some() && terminal_state.limit_error.is_none() {
        let limits = limits.expect("AgentNode deadline requires limits");
        if let Some(notification) = terminal_state.record_limit_breach(
            &session.session_id().to_string(),
            "maxDuration",
            limits.max_duration_secs,
            limits.max_duration_secs,
        )? {
            session.connection().send_notification(notification)?;
        }
    }
    Ok(())
}

async fn forward_resolved_subagent(
    subagent: super::types::DiscussSubagentRecord,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    sluice_session_id: SluiceSessionId,
    acp_session_id: &SessionId,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
) -> Result<(), agent_client_protocol::Error> {
    let payload = DiscussUtilityPayload::Subagent { subagent };
    stream_tx
        .send(DiscussStreamEvent::UtilityUpdate {
            sluice_session_id,
            acp_session_id: acp_session_id.clone(),
            utility: DiscussUtilityUpdate {
                sluice_session_id,
                payload: payload.clone(),
            },
        })
        .await
        .map_err(|error| {
            agent_client_protocol::Error::into_internal_error(channel_send_error(
                "shared.resolved_subagent",
                error.to_string(),
            ))
        })?;
    enqueue_utility_persistence(persistence_queue, Some(payload)).await;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn handle_agent_node_turn(
    session: &mut WorkerSession,
    text: String,
    limits: AgentNodeSessionLimits,
    accepted: oneshot::Sender<Result<(), DiscussError>>,
    terminal: oneshot::Sender<AgentNodeTerminalResult>,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    position_allocator: &mut TranscriptPositionAllocator,
    awaiting_turn: &mut bool,
    suppress_transcript: &mut bool,
    agent_node_limits: &mut Option<AgentNodeSessionLimits>,
    agent_node_turns: &mut u64,
    agent_node_terminal_state: &mut AgentNodeTerminalState,
    agent_node_deadline: &mut Option<Instant>,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    sluice_session_id: SluiceSessionId,
) -> Result<(), agent_client_protocol::Error> {
    tracing::debug!(%sluice_session_id, acp_session_id = %session.session_id(), awaiting_turn = *awaiting_turn, "Agent task prompt command received");
    if !*awaiting_turn || agent_node_terminal.is_some() {
        let _ = accepted.send(Err(DiscussError::Validation {
            message: "AgentNode turn received while another turn is active".to_string(),
        }));
        return Ok(());
    }
    if let Err(error) = persistence_queue
        .send(PersistenceJob::InitialTurn {
            acp_session_id: session.session_id().to_string(),
            position: position_allocator.next(),
            initial_prompt: text.clone(),
        })
        .await
    {
        let _ = accepted.send(Err(channel_send_error(
            "shared.persist_agent_node_turn",
            error.to_string(),
        )));
        return Ok(());
    }
    *suppress_transcript = false;
    tracing::debug!(%sluice_session_id, acp_session_id = %session.session_id(), "Sending Agent task prompt to ACP");
    session.send_prompt(text)?;
    tracing::debug!(%sluice_session_id, acp_session_id = %session.session_id(), "Agent task prompt submitted to ACP");
    *awaiting_turn = false;
    *agent_node_limits = Some(limits);
    *agent_node_turns = agent_node_turns.saturating_add(1);
    if *agent_node_turns > limits.max_turns
        && let Some(notification) = agent_node_terminal_state.record_limit_breach(
            &session.session_id().to_string(),
            "maxTurns",
            *agent_node_turns,
            limits.max_turns,
        )?
    {
        session.connection().send_notification(notification)?;
    }
    *agent_node_deadline = Some(Instant::now() + Duration::from_secs(limits.max_duration_secs));
    *agent_node_terminal = Some(terminal);
    let _ = accepted.send(Ok(()));
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn handle_elicitation_callback(
    request: agent_client_protocol::schema::v1::CreateElicitationRequest,
    acp_session_id: agent_client_protocol::schema::v1::SessionId,
    responder: Responder<CreateElicitationResponse>,
    target: CallbackRouteTarget,
    position_allocator: &TranscriptPositionAllocator,
    pending_elicitation_tx: &mpsc::Sender<PendingElicitationRegistration>,
    pending_elicitation_cancel_tx: &mpsc::Sender<PendingElicitationCancel>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    transient_connectivity: bool,
) -> Result<(), agent_client_protocol::Error> {
    let CallbackRouteTarget::Chat {
        sluice_session_id: viewer_session_id,
    } = target;
    if matches!(&request.mode, ElicitationMode::Url(_)) {
        responder.respond(CreateElicitationResponse::new(ElicitationAction::Decline))?;
        return Ok(());
    }
    let (elicitation_request_id, pending, responder_rx) = prepare_elicitation_request(&request, position_allocator);
    let payload = DiscussElicitationRequestPayload {
        classification: pending.classification.clone(),
        raw_fallback: matches!(
            &pending.classification,
            DiscussElicitationClassification::FormFallback { .. }
                | DiscussElicitationClassification::OtherRawFallback { .. }
        ),
        message: request.message.clone(),
        raw_request: request.clone(),
    };
    let (registration_ack, registration_ack_rx) = oneshot::channel();
    if let Err(send_error) = pending_elicitation_tx
        .send(PendingElicitationRegistration {
            sluice_session_id: viewer_session_id,
            elicitation_request_id,
            pending,
            registration_ack,
        })
        .await
    {
        let _ = send_error
            .0
            .pending
            .responder_tx
            .send(DiscussElicitationAnswer::Cancel);
        responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
        return Ok(());
    }
    match registration_ack_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
            let _ = pending_elicitation_cancel_tx
                .send(PendingElicitationCancel {
                    sluice_session_id: viewer_session_id,
                    elicitation_request_id: Some(elicitation_request_id),
                    cancellation_ack,
                })
                .await;
            let _ = cancellation_ack_rx.await;
            let answer = responder_rx
                .await
                .unwrap_or(DiscussElicitationAnswer::Cancel);
            if let Ok(response) = content_to_sdk_answer(answer) {
                responder.respond(response)?;
            } else {
                responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
            }
            return Err(agent_client_protocol::Error::into_internal_error(error));
        }
        Err(_) => {
            responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
            return Ok(());
        }
    }
    let stream_event = DiscussStreamEvent::ElicitationRequested {
        sluice_session_id: viewer_session_id,
        acp_session_id,
        elicitation_request_id,
        payload: Box::new(payload),
    };
    if !transient_connectivity && stream_tx.send(stream_event).await.is_err() {
        let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
        if pending_elicitation_cancel_tx
            .send(PendingElicitationCancel {
                sluice_session_id: viewer_session_id,
                elicitation_request_id: Some(elicitation_request_id),
                cancellation_ack,
            })
            .await
            .is_ok()
        {
            let _ = cancellation_ack_rx.await;
        }
        responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
    } else if let Ok(answer) = responder_rx.await {
        if let Ok(response) = content_to_sdk_answer(answer) {
            responder.respond(response)?;
        } else {
            responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
        }
    } else {
        responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel))?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn dispatch_callback(
    callback: CallbackEvent,
    pending_permission_tx: &mpsc::Sender<PendingPermissionRegistration>,
    pending_permission_cancel_tx: &mpsc::Sender<PendingPermissionCancel>,
    position_allocator: &TranscriptPositionAllocator,
    pending_elicitation_tx: &mpsc::Sender<PendingElicitationRegistration>,
    pending_elicitation_cancel_tx: &mpsc::Sender<PendingElicitationCancel>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    transient_connectivity: bool,
) -> Result<(), agent_client_protocol::Error> {
    match callback {
        CallbackEvent::Permission {
            request,
            responder,
            target,
        } => {
            handle_permission_callback(
                request,
                responder,
                target,
                pending_permission_tx,
                pending_permission_cancel_tx,
                stream_tx,
                transient_connectivity,
            )
            .await?;
        }
        CallbackEvent::Elicitation {
            request,
            acp_session_id,
            responder,
            target,
        } => {
            handle_elicitation_callback(
                request,
                acp_session_id,
                responder,
                target,
                position_allocator,
                pending_elicitation_tx,
                pending_elicitation_cancel_tx,
                stream_tx,
                transient_connectivity,
            )
            .await?;
        }
        CallbackEvent::ElicitationComplete => {}
    }
    Ok(())
}

async fn handle_permission_callback(
    request: RequestPermissionRequest,
    responder: Responder<RequestPermissionResponse>,
    target: CallbackRouteTarget,
    pending_permission_tx: &mpsc::Sender<PendingPermissionRegistration>,
    pending_permission_cancel_tx: &mpsc::Sender<PendingPermissionCancel>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    transient_connectivity: bool,
) -> Result<(), agent_client_protocol::Error> {
    let CallbackRouteTarget::Chat {
        sluice_session_id: viewer_session_id,
    } = target;
    let (permission_request_id, offered_option_ids, responder_rx, responder_tx) =
        prepare_permission_request(&request.session_id, &request);
    let (registration_ack, registration_ack_rx) = oneshot::channel();
    if let Err(send_error) = pending_permission_tx
        .send(PendingPermissionRegistration {
            sluice_session_id: viewer_session_id,
            permission_request_id,
            pending: PendingPermission {
                offered_option_ids,
                responder_tx,
            },
            registration_ack,
        })
        .await
    {
        let _ = send_error
            .0
            .pending
            .responder_tx
            .send(DiscussPermissionOption::Cancel);
        responder.respond_with_internal_error("permission registration failed")?;
        return Ok(());
    }
    match registration_ack_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let option = responder_rx
                .await
                .unwrap_or(DiscussPermissionOption::Cancel);
            resolve_permission_responder(responder, option).await?;
            return Err(agent_client_protocol::Error::into_internal_error(error));
        }
        Err(error) => {
            let _ = responder_rx.await;
            responder.respond_with_internal_error(error.to_string())?;
            return Ok(());
        }
    }
    let stream_event = DiscussStreamEvent::PermissionRequested {
        sluice_session_id: viewer_session_id,
        acp_session_id: request.session_id.clone(),
        permission_request_id,
        payload: Box::new(request),
    };
    if !transient_connectivity && stream_tx.send(stream_event).await.is_err() {
        let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
        if pending_permission_cancel_tx
            .send(PendingPermissionCancel {
                sluice_session_id: viewer_session_id,
                permission_request_id,
                cancellation_ack,
            })
            .await
            .is_ok()
        {
            let _ = cancellation_ack_rx.await;
            if let Ok(option) = responder_rx.await {
                resolve_permission_responder(responder, option).await?;
            }
        } else {
            responder.respond_with_internal_error("permission stream is unavailable")?;
        }
    } else if let Ok(option) = responder_rx.await {
        resolve_permission_responder(responder, option).await?;
    } else {
        responder.respond_with_internal_error("permission response was cancelled")?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn attach_inspector(
    session: &WorkerSession,
    agents_workspace: &std::path::Path,
    mcp_descriptors: &[agent_client_protocol::schema::v1::McpServer],
    callback_router: &Arc<Mutex<CallbackRouter>>,
    callback_routes: &Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: &mpsc::Sender<CallbackEvent>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    agent_plugin: &'static dyn DiscussAgentPlugin,
    attached_readers: &mut AttachedInspectorReaders,
    viewer_session_id: SluiceSessionId,
    child_session_id: &SessionId,
) -> Result<(), DiscussError> {
    if attached_readers.0.contains_key(&viewer_session_id) {
        return Ok(());
    }
    ensure_agents_workspace_dir(agents_workspace).await;
    let load_response = session
        .connection()
        .send_request(load_session_request(
            child_session_id.clone(),
            agents_workspace.to_path_buf(),
            mcp_descriptors,
        ))
        .block_task()
        .await?;
    let child_response = NewSessionResponse::new(child_session_id.clone()).config_options(load_response.config_options);
    let mut child = WorkerSession::attach(
        session.connection().clone(),
        child_response,
        CallbackRouteTarget::Chat {
            sluice_session_id: viewer_session_id,
        },
        callback_router.clone(),
        callback_routes.clone(),
        Some(callback_tx.clone()),
    )?;
    let stream_tx = stream_tx.clone();
    let join_handle = tokio::spawn(async move {
        let mut buffer = AssistantTurnBuffer::default();
        let mut capture_state = PluginCaptureState::for_plugin(agent_plugin);
        loop {
            let update = match child.read_update().await {
                Ok(update) => update,
                Err(error) => {
                    let _ = stream_tx
                        .send(DiscussStreamEvent::SessionFailed {
                            sluice_session_id: viewer_session_id,
                            error: DiscussError::from(error),
                            terminal_auth_methods: Vec::new(),
                        })
                        .await;
                    break;
                }
            };
            match send_message_to_user(
                AgentMessageForwardContext {
                    stream_tx: &stream_tx,
                    sluice_session_id: viewer_session_id,
                    session: child.session_id(),
                    agent_plugin,
                    capture_state: &mut capture_state,
                },
                update,
                &mut buffer,
                false,
            )
            .await
            {
                Ok(AgentMessageForwardOutcome::TurnBoundary { .. }) => {
                    buffer = AssistantTurnBuffer::default();
                }
                Ok(AgentMessageForwardOutcome::Continue { .. }) => {}
                Err(error) => {
                    let _ = stream_tx
                        .send(DiscussStreamEvent::SessionFailed {
                            sluice_session_id: viewer_session_id,
                            error: DiscussError::from(error),
                            terminal_auth_methods: Vec::new(),
                        })
                        .await;
                    break;
                }
            }
        }
    });
    attached_readers.0.insert(viewer_session_id, join_handle);
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn handle_session_update(
    session: &WorkerSession,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    sluice_session_id: SluiceSessionId,
    agent_plugin: &'static dyn DiscussAgentPlugin,
    capture_state: &mut PluginCaptureState,
    update: SessionMessage,
    buffer: &mut AssistantTurnBuffer,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    position_allocator: &mut TranscriptPositionAllocator,
    awaiting_turn: &mut bool,
    suppress_transcript: &mut bool,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    agent_node_limits: &mut Option<AgentNodeSessionLimits>,
    agent_node_terminal_state: &mut AgentNodeTerminalState,
    agent_node_deadline: &mut Option<Instant>,
) -> Result<(), agent_client_protocol::Error> {
    let outcome = send_message_to_user(
        AgentMessageForwardContext {
            stream_tx,
            sluice_session_id,
            session: session.session_id(),
            agent_plugin,
            capture_state,
        },
        update,
        buffer,
        *suppress_transcript,
    )
    .await?;
    if let Some(limits) = *agent_node_limits {
        let tool_calls = buffer.tool_call_count();
        if tool_calls > limits.max_tool_calls
            && let Some(notification) = agent_node_terminal_state.record_limit_breach(
                &session.session_id().to_string(),
                "maxToolCalls",
                tool_calls,
                limits.max_tool_calls,
            )?
        {
            session.connection().send_notification(notification)?;
        }
    }
    let metadata_updates = buffer.take_metadata_updates();
    if !metadata_updates.is_empty() {
        enqueue_persistence_job(
            persistence_queue,
            PersistenceJob::MetadataUpdates {
                updates: metadata_updates,
            },
        )
        .await;
    }
    match outcome {
        AgentMessageForwardOutcome::Continue { utility_payload } => {
            enqueue_utility_persistence(persistence_queue, utility_payload).await;
        }
        AgentMessageForwardOutcome::TurnBoundary { flush, utility_payload } => {
            let terminal_output = serde_json::json!({
                "text": flush.aggregated_text.clone(),
                "thought": flush.aggregated_thought.clone(),
                "parts": flush.parts.clone(),
                "stopReason": flush.stop_reason.clone(),
            });
            enqueue_utility_persistence(persistence_queue, utility_payload).await;
            if super::database::should_persist_assistant_turn(&flush) {
                persistence_queue
                    .send(PersistenceJob::AssistantTurn {
                        assistant_position: position_allocator.next(),
                        flush,
                    })
                    .await
                    .map_err(|error| {
                        agent_client_protocol::Error::into_internal_error(channel_send_error(
                            "shared.persist_assistant_turn",
                            error.to_string(),
                        ))
                    })?;
            }
            *suppress_transcript = false;
            *awaiting_turn = true;
            if let Some(terminal) = agent_node_terminal.take() {
                let result = agent_node_terminal_state.finish(terminal_output);
                tracing::info!(%sluice_session_id, acp_session_id = %session.session_id(), "Agent task turn stopped");
                let _ = terminal.send(result);
            }
            *agent_node_limits = None;
            *agent_node_deadline = None;
        }
    }
    Ok(())
}

async fn persist_set_config_option(
    session: &WorkerSession,
    config_id: String,
    value: String,
    agent_plugin: &dyn plugins::DiscussAgentPlugin,
    latest_config_options: &mut Vec<super::types::DiscussConfigOption>,
    session_settings: &mut super::types::ConfigSettings,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
) -> Result<(), DiscussError> {
    let response = session
        .connection()
        .send_request(SetSessionConfigOptionRequest::new(
            session.session_id().clone(),
            config_id.clone(),
            SessionConfigOptionValue::value_id(value.clone()),
        ))
        .block_task()
        .await
        .map_err(DiscussError::from)?;
    let response_options = response.config_options;
    if let Some(options) = agent_plugin.initial_config_options(Some(response_options), None) {
        *latest_config_options = options;
    }
    session_settings.insert(config_id, value);
    let (completion_tx, completion_rx) = oneshot::channel();
    persistence_queue
        .send(PersistenceJob::ConfigOptions {
            options: latest_config_options.clone(),
            session_settings: Some(session_settings.clone()),
            completion_tx: Some(completion_tx),
        })
        .await
        .map_err(|error| channel_send_error("shared.persist_config_options", error.to_string()))?;
    completion_rx
        .await
        .map_err(|error| DiscussError::Infrastructure {
            kind: "oneshot_reply_missing".to_string(),
            target_domain: "discuss".to_string(),
            operation: "persist_config_options".to_string(),
            message: error.to_string(),
        })??;
    Ok(())
}

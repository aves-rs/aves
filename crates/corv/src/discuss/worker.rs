use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use agent_client_protocol::v1::schema::{
    CreateElicitationRequest, CreateElicitationResponse, ElicitationAction, ElicitationMode, ListSessionsRequest,
    McpServer, NewSessionResponse, RequestPermissionRequest, RequestPermissionResponse, ResumeSessionRequest,
    SessionConfigOptionValue, SessionId, SetSessionConfigOptionRequest, SetSessionTitleRequest,
};
use agent_client_protocol::v1::{ConnectionTo, SessionMessage};
use agent_client_protocol::{AcpAgent, Agent, Client, DynamicHandlerGuard, Responder, UntypedMessage};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use super::types::DiscussCompletionKind;
use super::types::{AgentNodeSessionLimits, AgentNodeTerminalResult};
use super::usage::UsageTurnAccumulator;
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
use super::database::{
    LaunchLimitationsPersistence, PersistenceJob, enqueue_persistence_job, enqueue_utility_persistence,
    spawn_persistence_queue,
};
use super::elicitation::content_to_sdk_answer;
use super::mcp_direct::McpAcpShim;
use super::mcp_relay::{McpHttpLease, descriptors_for_registration, endpoint_to_acp_descriptor};
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
use super::supervisor::{
    CallbackEvent, CallbackRouteRegistry, CallbackRouteTarget, CallbackRouter, McpRegistrationClaim,
};
use super::types::{
    DiscussElicitationAnswer, DiscussElicitationClassification, DiscussElicitationRequestPayload, DiscussError,
    DiscussImageAttachment, DiscussPermissionOption, DiscussSessionId, DiscussStreamEvent, DiscussUtilityPayload,
    DiscussUtilityUpdate, PendingPermission, ProviderSessionInfo, WorkerCommand,
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
        .v1()
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
                    params.discuss_session_id,
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
            params.system_prompt_supported = init_response
                .agent_capabilities
                .session_capabilities
                .system_prompt
                .is_some();
            params.title_supported = init_response
                .agent_capabilities
                .session_capabilities
                .title
                .is_some();
            run_connected_session(params, connection).await
        })
        .await;

    let terminal_auth_methods = captured_terminal_auth_methods.lock().await.clone();
    result.map_err(|error| SessionWorkerFailure {
        error: Box::new(DiscussError::from(error)),
        terminal_auth_methods,
    })
}

pub(super) async fn run_connected_session(
    params: WorkerSessionParams,
    connection: ConnectionTo<Agent>,
) -> Result<(), agent_client_protocol::Error> {
    let setup = prepare_worker_session(&params, connection).await?;
    let WorkerSessionParams {
        launch_config: _launch_config,
        mut launch_limitations,
        stream_tx,
        discuss_session_id,
        auth_launch_server: _,
        session_name,
        agent_name,
        next_turn_position,
        next_usage_ordinal,
        price_index,
        agents_workspace,
        persistence,
        restored_persisted_transcript,
        worker_command_rx,
        worker_ready_tx,
        activity_update_tx,
        callback_router,
        callback_routes,
        root_context,
        pending_permission_tx,
        pending_permission_cancel_tx,
        pending_elicitation_tx,
        pending_elicitation_cancel_tx,
        announce_desktop_association,
        transient_connectivity,
        provider_title,
        title_supported,
        ..
    } = params;
    let WorkerSessionSetup {
        callback_tx,
        mut callback_rx,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
        mut mcp_claim,
        opened,
    } = setup;
    let mut session = opened.session;
    if let Some(title) = provider_title {
        push_provider_title(session.connection(), session.session_id(), title_supported, title).await;
    }
    let mut latest_config_options = opened.latest_config_options;
    let mut session_settings = opened.session_settings;

    let (persistence_queue, persistence_drain) = spawn_persistence_queue(
        persistence,
        discuss_session_id.to_string(),
        agent_name.clone(),
        session_name,
        root_context.clone(),
    );
    let mut position_allocator = TranscriptPositionAllocator::new(next_turn_position);
    let agent_plugin = plugins::for_agent(&agent_name);
    launch_limitations.extend(opened.launch_limitations);
    acknowledge_worker_ready(
        &persistence_queue,
        discuss_session_id,
        session.session_id(),
        &latest_config_options,
        &session_settings,
        &worker_ready_tx,
        announce_desktop_association,
        transient_connectivity,
        opened.supports_config_updates,
        launch_limitations,
    )
    .await?;

    let mut capture_state = PluginCaptureState::for_plugin(agent_plugin);
    tracing::info!(%discuss_session_id, acp_session_id = %session.session_id(), "ACP session started and ready for a prompt");
    let mut buffer = AssistantTurnBuffer::default();
    let mut usage = UsageTurnAccumulator::new(discuss_session_id.to_string(), agent_name.clone(), next_usage_ordinal);
    usage.set_pricing(price_index, agent_plugin);
    let mut worker_command_rx = worker_command_rx;
    let mut attached_readers = AttachedInspectorReaders(HashMap::new());
    let mut awaiting_turn = true;
    let mut agent_node_terminal: Option<oneshot::Sender<AgentNodeTerminalResult>> = None;
    let mut agent_node_limits: Option<AgentNodeSessionLimits> = None;
    let mut agent_node_turns = 0_u64;
    let mut agent_node_terminal_state = AgentNodeTerminalState::default();
    let mut agent_node_deadline: Option<Instant> = None;
    let mut suppress_transcript = restored_persisted_transcript;
    let mut state = ConnectedLoopState {
        session: &mut session,
        callback_rx: &mut callback_rx,
        worker_command_rx: &mut worker_command_rx,
        mcp_claim: &mut mcp_claim,
        capture_state: &mut capture_state,
        buffer: &mut buffer,
        usage: &mut usage,
        latest_config_options: &mut latest_config_options,
        session_settings: &mut session_settings,
        position_allocator: &mut position_allocator,
        attached_readers: &mut attached_readers,
        awaiting_turn: &mut awaiting_turn,
        activity_update_tx: &activity_update_tx,
        agent_node_terminal: &mut agent_node_terminal,
        agent_node_limits: &mut agent_node_limits,
        agent_node_turns: &mut agent_node_turns,
        agent_node_terminal_state: &mut agent_node_terminal_state,
        agent_node_deadline: &mut agent_node_deadline,
        suppress_transcript: &mut suppress_transcript,
        title_supported,
    };
    let result = pump_connected_session(
        &mut state,
        &stream_tx,
        discuss_session_id,
        agent_plugin,
        &persistence_queue,
        &pending_permission_tx,
        &pending_permission_cancel_tx,
        &pending_elicitation_tx,
        &pending_elicitation_cancel_tx,
        transient_connectivity,
        &agents_workspace,
        &mcp_descriptors,
        &callback_router,
        &callback_routes,
        &callback_tx,
    )
    .await;
    if result.is_err() {
        usage
            .finish(DiscussCompletionKind::Failed, None, &persistence_queue)
            .await;
    }
    teardown_connected_worker(
        mcp_handlers,
        mcp_shims,
        mcp_http_lease,
        persistence_queue,
        persistence_drain,
    )
    .await;
    result
}

struct ConnectedLoopState<'a> {
    session: &'a mut WorkerSession,
    callback_rx: &'a mut mpsc::Receiver<CallbackEvent>,
    worker_command_rx: &'a mut mpsc::Receiver<WorkerCommand>,
    mcp_claim: &'a mut Option<McpRegistrationClaim>,
    capture_state: &'a mut PluginCaptureState,
    buffer: &'a mut AssistantTurnBuffer,
    usage: &'a mut UsageTurnAccumulator,
    latest_config_options: &'a mut Vec<super::types::DiscussConfigOption>,
    session_settings: &'a mut super::plugins::ConfigSettings,
    position_allocator: &'a mut TranscriptPositionAllocator,
    attached_readers: &'a mut AttachedInspectorReaders,
    awaiting_turn: &'a mut bool,
    activity_update_tx: &'a mpsc::Sender<(DiscussSessionId, bool)>,
    agent_node_terminal: &'a mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    agent_node_limits: &'a mut Option<AgentNodeSessionLimits>,
    agent_node_turns: &'a mut u64,
    agent_node_terminal_state: &'a mut AgentNodeTerminalState,
    agent_node_deadline: &'a mut Option<Instant>,
    suppress_transcript: &'a mut bool,
    title_supported: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the connected worker's existing context"
)]
async fn pump_connected_session(
    state: &mut ConnectedLoopState<'_>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    discuss_session_id: DiscussSessionId,
    agent_plugin: &'static dyn DiscussAgentPlugin,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    pending_permission_tx: &mpsc::Sender<PendingPermissionRegistration>,
    pending_permission_cancel_tx: &mpsc::Sender<PendingPermissionCancel>,
    pending_elicitation_tx: &mpsc::Sender<PendingElicitationRegistration>,
    pending_elicitation_cancel_tx: &mpsc::Sender<PendingElicitationCancel>,
    transient_connectivity: bool,
    agents_workspace: &Path,
    mcp_descriptors: &[McpServer],
    callback_router: &Arc<Mutex<CallbackRouter>>,
    callback_routes: &Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: &mpsc::Sender<CallbackEvent>,
) -> Result<(), agent_client_protocol::Error> {
    let ConnectedLoopState {
        session,
        callback_rx,
        worker_command_rx,
        mcp_claim,
        capture_state,
        buffer,
        usage,
        latest_config_options,
        session_settings,
        position_allocator,
        attached_readers,
        awaiting_turn,
        activity_update_tx,
        agent_node_terminal,
        agent_node_limits,
        agent_node_turns,
        agent_node_terminal_state,
        agent_node_deadline,
        suppress_transcript,
        title_supported,
    } = state;
    loop {
        tokio::select! {
            update = session.read_update() => {
                let agent_node_was_active = agent_node_terminal.is_some();
                handle_session_update(
                    session,
                    stream_tx,
                    discuss_session_id,
                    agent_plugin,
                    capture_state,
                    update?,
                    buffer,
                    usage,
                    latest_config_options,
                    persistence_queue,
                    position_allocator,
                    awaiting_turn,
                    activity_update_tx,
                    suppress_transcript,
                    agent_node_terminal,
                    agent_node_limits,
                    agent_node_terminal_state,
                    agent_node_deadline,
                ).await?;
                // One update can change several subagents; the capture returned the first.
                while let Some(subagent) = capture_state.next_subagent() {
                    forward_subagent(subagent, stream_tx, discuss_session_id, session.session_id(), persistence_queue)
                        .await?;
                }
                if agent_node_was_active && agent_node_terminal.is_none() {
                    break;
                }
            }
            _ = async {
                match **agent_node_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                handle_agent_node_deadline(
                    session,
                    agent_node_deadline,
                    agent_node_terminal,
                    **agent_node_limits,
                    agent_node_terminal_state,
                )?;
            }
            Some(callback) = callback_rx.recv() => {
                dispatch_callback(
                    callback,
                    pending_permission_tx,
                    pending_permission_cancel_tx,
                    position_allocator,
                    pending_elicitation_tx,
                    pending_elicitation_cancel_tx,
                    stream_tx,
                    transient_connectivity,
                )
                .await?;
            }
            command = worker_command_rx.recv() => {
                match command {
                    None | Some(WorkerCommand::Stop) => break,
                    Some(WorkerCommand::SendUserTurn { text, images, reply }) => {
                        if **awaiting_turn && let Some(claim) = &mut **mcp_claim {
                            refresh_stale_mcp_registration(session, agents_workspace, mcp_descriptors, claim).await;
                        }
                        handle_send_user_turn(
                            TurnState {
                                session,
                                position_allocator,
                                awaiting_turn,
                                activity_update_tx,
                                usage,
                                suppress_transcript,
                                options: latest_config_options,
                            },
                            text,
                            images,
                            persistence_queue,
                            discuss_session_id,
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
                            session,
                            text,
                            limits,
                            accepted,
                            terminal,
                            persistence_queue,
                            position_allocator,
                            awaiting_turn,
                            activity_update_tx,
                            suppress_transcript,
                            usage,
                            latest_config_options,
                            agent_node_limits,
                            agent_node_turns,
                            agent_node_terminal_state,
                            agent_node_deadline,
                            agent_node_terminal,
                            discuss_session_id,
                        )
                        .await?;
                    }
                    Some(WorkerCommand::CancelTurn { reply }) => {
                        cancel_turn(
                            TurnState {
                                session,
                                position_allocator,
                                awaiting_turn,
                                activity_update_tx,
                                usage,
                                suppress_transcript,
                                options: latest_config_options,
                            },
                            discuss_session_id,
                            agent_node_terminal,
                            buffer,
                            persistence_queue,
                            reply,
                        )
                        .await?;
                    }
                    Some(WorkerCommand::SetConfigOption { config_id, value, reply }) => {
                        let result = persist_set_config_option(
                            session,
                            config_id,
                            value,
                            agent_plugin,
                            latest_config_options,
                            session_settings,
                            persistence_queue,
                        )
                        .await;
                        let _ = reply.send(result);
                    }
                    Some(WorkerCommand::SetTitle { title }) => {
                        push_provider_title(session.connection(), session.session_id(), *title_supported, title).await;
                    }
                    Some(WorkerCommand::ListProviderSessions { reply }) => {
                        let _ = reply.send(list_provider_sessions(session).await);
                    }
                    Some(WorkerCommand::LoadProviderSession { acp_session_id, reply }) => {
                        let _ = reply.send(load_provider_session(session, &acp_session_id, agents_workspace).await);
                    }
                    Some(WorkerCommand::AttachInspector {
                        viewer_session_id,
                        child_session_id,
                        reply,
                    }) => {
                        let result = attach_inspector(
                            session,
                            agents_workspace,
                            mcp_descriptors,
                            callback_router,
                            callback_routes,
                            callback_tx,
                            stream_tx,
                            agent_plugin,
                            attached_readers,
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
        if let Some(claim) = &mut **mcp_claim {
            claim.set_turn_active(!**awaiting_turn);
        }
    }
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

/// Re-registers this session's MCP endpoints when the shared Provider's
/// workspace registration points at an ended session's endpoints, or the
/// workspace idled long enough for the Provider to have discarded it.
async fn refresh_stale_mcp_registration(
    session: &WorkerSession,
    agents_workspace: &Path,
    mcp_descriptors: &[McpServer],
    claim: &mut McpRegistrationClaim,
) {
    if !claim.needs_refresh() {
        return;
    }
    let request = ResumeSessionRequest::new(session.session_id().clone(), agents_workspace.to_path_buf())
        .mcp_servers(descriptors_for_registration(mcp_descriptors, claim.next_generation()));
    match session
        .connection()
        .send_request(request)
        .block_task()
        .await
    {
        Ok(_) => {
            claim.reclaim();
            tracing::info!(target: "application_acp", acp_session_id = %session.session_id(), "re-registered stale or idle shared MCP endpoints");
        }
        Err(error) => {
            tracing::warn!(target: "application_acp", ?error, acp_session_id = %session.session_id(), "could not re-register stale or idle shared MCP endpoints");
        }
    }
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

/// Best-effort `session/set_title`: sent only to agents that advertise it, and a failure never ends the session.
async fn push_provider_title(connection: &ConnectionTo<Agent>, session_id: &SessionId, supported: bool, title: String) {
    if !supported {
        return;
    }
    if let Err(error) = connection
        .send_request(SetSessionTitleRequest::new(session_id.clone(), title))
        .block_task()
        .await
    {
        tracing::debug!(target: "application_acp", %error, "could not set provider session title");
    }
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
    discuss_session_id: DiscussSessionId,
    acp_session_id: &SessionId,
    latest_config_options: &[super::types::DiscussConfigOption],
    session_settings: &super::plugins::ConfigSettings,
    worker_ready_tx: &mpsc::Sender<WorkerReady>,
    announce_desktop_association: bool,
    transient_connectivity: bool,
    supports_config_updates: bool,
    launch_limitations: Vec<super::types::LaunchLimitation>,
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
    if !transient_connectivity {
        persistence_queue
            .persist_launch_limitations(launch_limitations)
            .await
            .map_err(agent_client_protocol::Error::into_internal_error)?;
    }
    worker_ready_tx
        .send(WorkerReady {
            discuss_session_id,
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
    mcp_descriptors: Vec<McpServer>,
    mcp_claim: Option<McpRegistrationClaim>,
    opened: OpenedWorkerSession,
}

async fn prepare_worker_session(
    params: &WorkerSessionParams,
    connection: ConnectionTo<Agent>,
) -> Result<WorkerSessionSetup, agent_client_protocol::Error> {
    let (callback_tx, callback_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    tracing::debug!(%params.discuss_session_id, provider = %params.agent_name, cwd = %params.agents_workspace.display(), "Preparing ACP session transports");
    let PreparedMcpTransports {
        mcp_transport,
        mcp_shims,
        mcp_handlers,
        mcp_http_lease,
        mcp_descriptors,
    } = prepare_mcp_transports(
        &connection,
        params.shared_mcp_capabilities,
        params.application_mcp_enabled,
        params.flow_mcp_endpoints.as_deref(),
        params.mcp_domains,
        params.discuss_session_id,
        &params.root_context,
        &params.worker_jwt,
        &params.bus_service,
    )
    .await?;
    let provisioned_mcp = !mcp_descriptors.is_empty();
    let mcp_claim = params
        .shared_mcp_registrations
        .as_ref()
        .filter(|_| provisioned_mcp)
        .map(|registrations| registrations.claim(&params.agents_workspace, params.discuss_session_id));
    let opened = open_worker_session(
        connection,
        &params.agents_workspace,
        &params.agent_name,
        params.discuss_session_id,
        params.callback_router.clone(),
        params.callback_routes.clone(),
        &callback_tx,
        &params.broadcast_tx,
        OpenConfiguredSessionOptions {
            launch_meta: params.launch_meta.clone(),
            launch_config: params.launch_config.clone(),
            reconnect_session_id: params.reconnect_acp_session_id.clone(),
            cwd: params.agents_workspace.clone(),
            mcp_servers: if super::plugins::for_agent(&params.agent_name).uses_mcp_launch_env() {
                mcp_descriptors.clone()
            } else {
                mcp_descriptors
                    .iter()
                    .cloned()
                    .chain(
                        params
                            .launch_config
                            .iter()
                            .flat_map(|config| config.mcp_servers.iter().cloned()),
                    )
                    .collect()
            },
            primary_agent: params.primary_agent.clone(),
            system_prompt_supported: params.system_prompt_supported,
            resolved_config: merged_config_preferences(&params.agent_defaults, &params.session_settings),
            persisted_config_options: params.persisted_config_options.clone(),
        },
        mcp_transport,
        params.application_mcp_enabled,
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
        mcp_claim,
        opened,
    })
}

struct OpenedWorkerSession {
    session: WorkerSession,
    latest_config_options: Vec<super::types::DiscussConfigOption>,
    session_settings: super::plugins::ConfigSettings,
    supports_config_updates: bool,
    launch_limitations: Vec<super::types::LaunchLimitation>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn open_worker_session(
    connection: ConnectionTo<Agent>,
    agents_workspace: &std::path::Path,
    agent_name: &str,
    discuss_session_id: DiscussSessionId,
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: &mpsc::Sender<CallbackEvent>,
    broadcast_tx: &mpsc::Sender<super::messages::DiscussResponse>,
    options: OpenConfiguredSessionOptions,
    mcp_transport: McpTransportSelection,
    application_mcp_enabled: bool,
    provisioned_mcp: bool,
) -> Result<OpenedWorkerSession, agent_client_protocol::Error> {
    ensure_agents_workspace_dir(agents_workspace).await;
    tracing::debug!(%discuss_session_id, provider = %agent_name, cwd = %agents_workspace.display(), mcp_servers = options.mcp_servers.len(), "Opening configured ACP session");
    let configured = open_configured_session(
        connection,
        agent_name,
        CallbackRouteTarget::Chat { discuss_session_id },
        callback_router,
        callback_routes,
        Some(callback_tx.clone()),
        options,
    )
    .await
    .map_err(|error| {
        // The ACP boundary reduces this to a bare internal error; record the typed cause here.
        tracing::warn!(%discuss_session_id, provider = %agent_name, %error, "Configured ACP session open failed");
        agent_client_protocol::Error::into_internal_error(error)
    })?;
    let session = configured.session;
    tracing::debug!(%discuss_session_id, acp_session_id = %session.session_id(), "ACP session configuration completed");
    let latest_config_options = configured.config_options;
    let session_settings = configured.applied_settings;
    let supports_config_updates = configured.supports_config_updates;
    let launch_limitations = configured.launch_limitations;
    let mut capability_warning = McpAcpCapabilityWarningState::default();
    if application_mcp_enabled && mcp_transport == McpTransportSelection::None {
        capability_warning
            .publish_once(broadcast_tx, discuss_session_id, session.session_id(), agent_name)
            .await
            .map_err(agent_client_protocol::Error::into_internal_error)?;
    }
    publish_mcp_status(
        broadcast_tx,
        discuss_session_id,
        session.session_id(),
        mcp_status_for_transport(application_mcp_enabled, mcp_transport, provisioned_mcp),
    )
    .await
    .map_err(agent_client_protocol::Error::into_internal_error)?;
    Ok(OpenedWorkerSession {
        session,
        latest_config_options,
        session_settings,
        supports_config_updates,
        launch_limitations,
    })
}

struct PreparedMcpTransports {
    mcp_transport: McpTransportSelection,
    mcp_shims: Vec<McpAcpShim>,
    mcp_handlers: Vec<DynamicHandlerGuard<Agent>>,
    mcp_http_lease: Option<McpHttpLease>,
    mcp_descriptors: Vec<McpServer>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn prepare_mcp_transports(
    connection: &ConnectionTo<Agent>,
    shared_mcp_capabilities: Option<(bool, bool)>,
    application_mcp_enabled: bool,
    flow_mcp_endpoints: Option<&[BusMcpEndpoint]>,
    mcp_domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    discuss_session_id: DiscussSessionId,
    root_context: &BusRpcContext,
    worker_jwt: &Jwt,
    bus_service: &ServiceReader<dyn BusSudoService>,
) -> Result<PreparedMcpTransports, agent_client_protocol::Error> {
    let mcp_transport = shared_mcp_capabilities
        .map(|(native, http)| select_mcp_transport(application_mcp_enabled, native, http))
        .unwrap_or(McpTransportSelection::None);
    let mut mcp_shims = Vec::new();
    let mut mcp_handlers: Vec<DynamicHandlerGuard<Agent>> = Vec::new();
    let mut mcp_http_lease = None;
    let mut mcp_descriptors = Vec::new();
    if mcp_transport == McpTransportSelection::NativeAcp {
        for domain in mcp_domains().map_err(agent_client_protocol::Error::into_internal_error)? {
            let shim = McpAcpShim::new(
                &domain,
                discuss_session_id.to_string(),
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
        && let Ok(lease) = McpHttpLease::open(discuss_session_id.to_string(), service, worker_jwt.clone()).await
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

struct TurnState<'a> {
    session: &'a mut WorkerSession,
    position_allocator: &'a mut TranscriptPositionAllocator,
    awaiting_turn: &'a mut bool,
    activity_update_tx: &'a mpsc::Sender<(DiscussSessionId, bool)>,
    usage: &'a mut UsageTurnAccumulator,
    suppress_transcript: &'a mut bool,
    options: &'a [super::types::DiscussConfigOption],
}

async fn handle_send_user_turn(
    state: TurnState<'_>,
    text: String,
    images: Vec<DiscussImageAttachment>,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    discuss_session_id: DiscussSessionId,
    reply: oneshot::Sender<Result<(), DiscussError>>,
) -> Result<(), agent_client_protocol::Error> {
    let TurnState {
        session,
        position_allocator,
        awaiting_turn,
        activity_update_tx,
        usage,
        suppress_transcript,
        options,
    } = state;
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
    usage.dispatch(
        options
            .iter()
            .find(|option| option.category == "model")
            .map(|option| option.value.clone()),
    );
    session.send_user_turn(text, images)?;
    set_turn_activity(awaiting_turn, false, activity_update_tx, discuss_session_id).await?;
    let _ = reply.send(Ok(()));
    Ok(())
}

async fn cancel_turn(
    state: TurnState<'_>,
    discuss_session_id: DiscussSessionId,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    buffer: &mut AssistantTurnBuffer,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    reply: oneshot::Sender<Result<(), DiscussError>>,
) -> Result<(), agent_client_protocol::Error> {
    let TurnState {
        session,
        position_allocator,
        awaiting_turn,
        activity_update_tx,
        usage,
        ..
    } = state;
    if let Some(terminal) = agent_node_terminal.take() {
        let _ = terminal.send(AgentNodeTerminalResult::Failed {
            error: DiscussError::SessionCancelled { discuss_session_id },
        });
    }
    let flush = flush_cancelled_assistant_turn(buffer);
    usage
        .finish(
            DiscussCompletionKind::Cancelled,
            Some("cancelled".to_string()),
            persistence_queue,
        )
        .await;
    if let Some(mut flush) = flush {
        flush.usage = usage.finalized_snapshot();
        let assistant_position = position_allocator.next();
        if let Err(error) = persistence_queue
            .send(PersistenceJob::AssistantTurn {
                assistant_position,
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
        usage.bind_assistant_position(assistant_position);
    }
    let notification = UntypedMessage::new(
        "session/cancel",
        serde_json::json!({"sessionId": session.session_id().to_string()}),
    )?;
    session.connection().send_notification(notification)?;
    set_turn_activity(awaiting_turn, true, activity_update_tx, discuss_session_id).await?;
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

async fn forward_subagent(
    payload: DiscussUtilityPayload,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    discuss_session_id: DiscussSessionId,
    acp_session_id: &SessionId,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
) -> Result<(), agent_client_protocol::Error> {
    stream_tx
        .send(DiscussStreamEvent::UtilityUpdate {
            discuss_session_id,
            acp_session_id: acp_session_id.clone(),
            utility: DiscussUtilityUpdate {
                discuss_session_id,
                payload: payload.clone(),
            },
        })
        .await
        .map_err(|error| {
            agent_client_protocol::Error::into_internal_error(channel_send_error("shared.subagent", error.to_string()))
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
    activity_update_tx: &mpsc::Sender<(DiscussSessionId, bool)>,
    suppress_transcript: &mut bool,
    usage: &mut UsageTurnAccumulator,
    options: &[super::types::DiscussConfigOption],
    agent_node_limits: &mut Option<AgentNodeSessionLimits>,
    agent_node_turns: &mut u64,
    agent_node_terminal_state: &mut AgentNodeTerminalState,
    agent_node_deadline: &mut Option<Instant>,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    discuss_session_id: DiscussSessionId,
) -> Result<(), agent_client_protocol::Error> {
    tracing::debug!(%discuss_session_id, acp_session_id = %session.session_id(), awaiting_turn = *awaiting_turn, "Agent task prompt command received");
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
    tracing::debug!(%discuss_session_id, acp_session_id = %session.session_id(), "Sending Agent task prompt to ACP");
    usage.dispatch(
        options
            .iter()
            .find(|option| option.category == "model")
            .map(|option| option.value.clone()),
    );
    session.send_prompt(text)?;
    tracing::debug!(%discuss_session_id, acp_session_id = %session.session_id(), "Agent task prompt submitted to ACP");
    set_turn_activity(awaiting_turn, false, activity_update_tx, discuss_session_id).await?;
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
    request: CreateElicitationRequest,
    acp_session_id: SessionId,
    responder: Responder<CreateElicitationResponse>,
    target: CallbackRouteTarget,
    position_allocator: &TranscriptPositionAllocator,
    pending_elicitation_tx: &mpsc::Sender<PendingElicitationRegistration>,
    pending_elicitation_cancel_tx: &mpsc::Sender<PendingElicitationCancel>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    transient_connectivity: bool,
) -> Result<(), agent_client_protocol::Error> {
    let CallbackRouteTarget::Chat {
        discuss_session_id: viewer_session_id,
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
            discuss_session_id: viewer_session_id,
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
                    discuss_session_id: viewer_session_id,
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
        discuss_session_id: viewer_session_id,
        acp_session_id,
        elicitation_request_id,
        payload: Box::new(payload),
    };
    if !transient_connectivity && stream_tx.send(stream_event).await.is_err() {
        let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
        if pending_elicitation_cancel_tx
            .send(PendingElicitationCancel {
                discuss_session_id: viewer_session_id,
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
        discuss_session_id: viewer_session_id,
    } = target;
    let (permission_request_id, offered_option_ids, responder_rx, responder_tx) =
        prepare_permission_request(&request.session_id, &request);
    let (registration_ack, registration_ack_rx) = oneshot::channel();
    if let Err(send_error) = pending_permission_tx
        .send(PendingPermissionRegistration {
            discuss_session_id: viewer_session_id,
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
        discuss_session_id: viewer_session_id,
        acp_session_id: request.session_id.clone(),
        permission_request_id,
        payload: Box::new(request),
    };
    if !transient_connectivity && stream_tx.send(stream_event).await.is_err() {
        let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
        if pending_permission_cancel_tx
            .send(PendingPermissionCancel {
                discuss_session_id: viewer_session_id,
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
    mcp_descriptors: &[McpServer],
    callback_router: &Arc<Mutex<CallbackRouter>>,
    callback_routes: &Option<Arc<Mutex<CallbackRouteRegistry>>>,
    callback_tx: &mpsc::Sender<CallbackEvent>,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    agent_plugin: &'static dyn DiscussAgentPlugin,
    attached_readers: &mut AttachedInspectorReaders,
    viewer_session_id: DiscussSessionId,
    child_session_id: &SessionId,
) -> Result<Vec<super::types::DiscussConfigOption>, DiscussError> {
    if attached_readers.0.contains_key(&viewer_session_id) {
        return Ok(Vec::new());
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
    let child_options = agent_plugin
        .initial_config_options(load_response.config_options.clone(), None)
        .unwrap_or_default();
    let child_response = NewSessionResponse::new(child_session_id.clone()).config_options(load_response.config_options);
    let mut child = WorkerSession::attach(
        session.connection().clone(),
        child_response,
        CallbackRouteTarget::Chat {
            discuss_session_id: viewer_session_id,
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
                            discuss_session_id: viewer_session_id,
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
                    discuss_session_id: viewer_session_id,
                    session: child.session_id(),
                    agent_plugin,
                    capture_state: &mut capture_state,
                    usage: None,
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
                            discuss_session_id: viewer_session_id,
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
    Ok(child_options)
}

#[expect(
    clippy::too_many_arguments,
    reason = "preserve the single worker and existing ACP callback/stream context without introducing new shared state"
)]
async fn handle_session_update(
    session: &WorkerSession,
    stream_tx: &mpsc::Sender<DiscussStreamEvent>,
    discuss_session_id: DiscussSessionId,
    agent_plugin: &'static dyn DiscussAgentPlugin,
    capture_state: &mut PluginCaptureState,
    update: SessionMessage,
    buffer: &mut AssistantTurnBuffer,
    usage: &mut UsageTurnAccumulator,
    latest_config_options: &mut Vec<super::types::DiscussConfigOption>,
    persistence_queue: &mpsc::Sender<PersistenceJob>,
    position_allocator: &mut TranscriptPositionAllocator,
    awaiting_turn: &mut bool,
    activity_update_tx: &mpsc::Sender<(DiscussSessionId, bool)>,
    suppress_transcript: &mut bool,
    agent_node_terminal: &mut Option<oneshot::Sender<AgentNodeTerminalResult>>,
    agent_node_limits: &mut Option<AgentNodeSessionLimits>,
    agent_node_terminal_state: &mut AgentNodeTerminalState,
    agent_node_deadline: &mut Option<Instant>,
) -> Result<(), agent_client_protocol::Error> {
    let outcome = send_message_to_user(
        AgentMessageForwardContext {
            stream_tx,
            discuss_session_id,
            session: session.session_id(),
            agent_plugin,
            capture_state,
            usage: Some((usage, persistence_queue)),
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
    for metadata in &metadata_updates {
        if let Some(options) = agent_plugin.metadata_config_options(metadata) {
            *latest_config_options = options;
        }
    }
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
            let mut flush = *flush;
            usage
                .finish(
                    if flush.stop_reason == "cancelled" {
                        DiscussCompletionKind::Cancelled
                    } else {
                        DiscussCompletionKind::Completed
                    },
                    Some(flush.stop_reason.clone()),
                    persistence_queue,
                )
                .await;
            flush.usage = usage.finalized_snapshot();
            let terminal_output = serde_json::json!({
                "text": flush.aggregated_text.clone(),
                "thought": flush.aggregated_thought.clone(),
                "parts": flush.parts.iter().filter(|part| {
                    part.get("kind").and_then(serde_json::Value::as_str) != Some("image")
                }).collect::<Vec<_>>(),
                "stopReason": flush.stop_reason.clone(),
            });
            enqueue_utility_persistence(persistence_queue, utility_payload).await;
            if super::database::should_persist_assistant_turn(&flush) {
                let assistant_position = position_allocator.next();
                persistence_queue
                    .send(PersistenceJob::AssistantTurn {
                        assistant_position,
                        flush,
                    })
                    .await
                    .map_err(|error| {
                        agent_client_protocol::Error::into_internal_error(channel_send_error(
                            "shared.persist_assistant_turn",
                            error.to_string(),
                        ))
                    })?;
                usage.bind_assistant_position(assistant_position);
            }
            *suppress_transcript = false;
            set_turn_activity(awaiting_turn, true, activity_update_tx, discuss_session_id).await?;
            if let Some(terminal) = agent_node_terminal.take() {
                let result = agent_node_terminal_state.finish(terminal_output);
                tracing::info!(%discuss_session_id, acp_session_id = %session.session_id(), "Agent task turn stopped");
                let _ = terminal.send(result);
            }
            *agent_node_limits = None;
            *agent_node_deadline = None;
        }
    }
    Ok(())
}

async fn set_turn_activity(
    awaiting_turn: &mut bool,
    idle: bool,
    activity_update_tx: &mpsc::Sender<(DiscussSessionId, bool)>,
    discuss_session_id: DiscussSessionId,
) -> Result<(), agent_client_protocol::Error> {
    if *awaiting_turn != idle {
        *awaiting_turn = idle;
        activity_update_tx
            .send((discuss_session_id, !idle))
            .await
            .map_err(|error| {
                agent_client_protocol::Error::into_internal_error(channel_send_error(
                    "shared.activity_update",
                    error.to_string(),
                ))
            })?;
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::v1::schema::{SessionId, SetSessionTitleRequest, SetSessionTitleResponse};
    use agent_client_protocol::{Agent, Channel, Client, on_receive_request};

    use super::push_provider_title;

    /// Push one title through a real client connection to an in-process agent and report what the agent received.
    async fn titles_received_by_agent(supported: bool, agent_accepts: bool) -> Vec<(String, String)> {
        let (client_side, agent_side) = Channel::duplex();
        let received = Arc::new(Mutex::new(Vec::new()));
        let recorder = received.clone();
        let agent = tokio::spawn(
            Agent
                .v1()
                .on_receive_request(
                    async move |request: SetSessionTitleRequest, responder, _connection| {
                        recorder
                            .lock()
                            .unwrap()
                            .push((request.session_id.to_string(), request.title.clone()));
                        if agent_accepts {
                            responder.respond(SetSessionTitleResponse::new(request.title))
                        } else {
                            responder.respond_with_internal_error("title rejected")
                        }
                    },
                    on_receive_request!(),
                )
                .connect_to(agent_side),
        );
        Client
            .v1()
            .connect_with(client_side, async move |connection| {
                push_provider_title(
                    &connection,
                    &SessionId::from("provider-session".to_owned()),
                    supported,
                    "yach | Tidy the README | perform".to_owned(),
                )
                .await;
                Ok(())
            })
            .await
            .expect("a failed provider title never fails the client connection");
        agent.abort();
        let received = received.lock().unwrap().clone();
        received
    }

    #[tokio::test]
    async fn supported_provider_receives_the_session_title() {
        let received = titles_received_by_agent(true, true).await;

        assert_eq!(
            received,
            [(
                "provider-session".to_owned(),
                "yach | Tidy the README | perform".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn unsupported_provider_receives_no_title_request() {
        let received = titles_received_by_agent(false, true).await;

        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn provider_rejecting_the_title_does_not_fail_the_session() {
        let received = titles_received_by_agent(true, false).await;

        assert_eq!(received.len(), 1);
    }
}

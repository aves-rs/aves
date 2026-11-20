use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{McpServer, Meta, SessionId};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, Client};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::types::TaskSessionStart;
use aves::auth::{AuthSudoService, Jwt};
use aves::bus::BusMcpEndpoint;
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{
    Actor, PROVIDER_CHILD_REAP_DEADLINE, SpawnSessionOptions, TransientWorkerEvent, channel_send_error,
    validate_user_turn,
};
use super::config::{
    config_options_from_json, config_settings_from_json, config_settings_from_value, provider_config_options_json,
};
use super::database::persist_session_anchor_inner;
use super::identity::{Binding, flow_node_subject, persist_binding};
use super::library::{AgentId, ResolvedAgent};
use super::mcp::composed_launch_config;
use super::messages::DiscussResponse;
use super::plugins::{self, LaunchContext, PrimaryAgentSelection};
use super::session::{
    PendingElicitationCancel, SESSION_WORKER_JWT_EXPIRATION, SessionWorkerFailure, WorkerSessionParams,
    ensure_agents_workspace_dir, initialize_request, issue_worker_jwt, load_session_request, revoke_worker_jti,
    run as run_session_worker, worker_subject,
};
use super::storage::{
    DiscussSessionRow, LoadedSessionRefresh, delete_discuss_session, list_recent_discuss_sessions,
    load_discuss_agent_config, load_discuss_chat_turns_window, load_discuss_session, load_discuss_subagents,
    load_discuss_team_installation, merge_refresh_discuss_session, next_usage_ordinal, present_acp_session_id,
    provider_updated_at_ms, rename_discuss_session, update_session_launch_config, update_session_launch_context,
    update_session_setup_kind, upsert_discuss_agent_config,
};
use super::supervisor::{
    CallbackRouteRegistry, CallbackRouter, ProviderSupervisor, ProviderSupervisorResources, SharedMcpRegistrations,
};
use super::types::{
    AcpProvider, ConfigSettings, DiscussChatTurnRecord, DiscussConfigOption, DiscussError, DiscussMemberOverrides,
    DiscussSessionId, DiscussSessionInfo, DiscussSessionSetup, DiscussStreamEvent, LaunchLimitation,
    SessionLaunchConfig, TaskSessionStarted, WorkerCommand, WorkerEntry,
};

struct SessionLaunchPreparation {
    launch_server: McpServer,
    launch_meta: Option<Meta>,
    launch_limitations: Vec<LaunchLimitation>,
    effective_config: Option<SessionLaunchConfig>,
    assigned_agent_name: Option<String>,
    setup_settings: ConfigSettings,
    persisted_setup_kind: Option<&'static str>,
    primary_agent: Option<ResolvedAgent>,
    isolated_configured: bool,
    agent: AcpAgent,
}

struct SessionWorkerInputs<'a> {
    discuss_session_id: DiscussSessionId,
    key: &'a str,
    assigned_agent_name: Option<String>,
    agent: AcpAgent,
    launch_config: Option<SessionLaunchConfig>,
    launch_meta: Option<Meta>,
    launch_limitations: Vec<LaunchLimitation>,
    launch_server: McpServer,
    session_name: String,
    provider_title: Option<String>,
    primary_agent: Option<ResolvedAgent>,
    reconnect_acp_session_id: Option<SessionId>,
    persisted_config_options: Vec<DiscussConfigOption>,
    session_settings: ConfigSettings,
    restored_persisted_transcript: bool,
    next_turn_position: i64,
    next_usage_ordinal: i64,
    session_cwd: Option<PathBuf>,
    application_mcp_enabled: bool,
    flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
    node_token: Option<Jwt>,
    agent_defaults: ConfigSettings,
    announce_desktop_association: bool,
    transient_connectivity: bool,
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    shared_connection: Option<ConnectionTo<Agent>>,
    shared_mcp_capabilities: Option<(bool, bool)>,
    system_prompt_supported: bool,
    title_supported: bool,
    shared_mcp_registrations: Option<SharedMcpRegistrations>,
}

impl Actor {
    fn ensure_provider_accepts_session(&self, key: &str, transient_connectivity: bool) -> Result<(), DiscussError> {
        if !transient_connectivity
            && !self
                .provider_supervisors
                .get(key)
                .is_some_and(ProviderSupervisor::accepts_work)
        {
            return Err(DiscussError::ProviderNotRunning {
                provider_name: key.to_string(),
            });
        }
        Ok(())
    }

    async fn handle_existing_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
        announce_desktop_association: bool,
    ) -> Result<bool, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        if self
            .worker_registry
            .get(&discuss_session_id)
            .is_some_and(|entry| entry.command_tx.is_closed())
        {
            if let Some(entry) = self.worker_registry.remove(&discuss_session_id) {
                revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
                if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                    supervisor.disassociate_chat(&discuss_session_id);
                }
            }
            self.worker_capabilities.remove(&discuss_session_id);
        }
        if let Some(entry) = self.worker_registry.get_mut(&discuss_session_id) {
            let ready_for_desktop_association = announce_desktop_association && entry.acp_session_id.is_some();
            if announce_desktop_association {
                entry.desktop_association_requested = !ready_for_desktop_association;
            }
            if ready_for_desktop_association {
                self.announce_desktop_association(discuss_session_id)
                    .await?;
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) async fn start_task_session(
        &mut self,
        options: TaskSessionStart,
    ) -> Result<TaskSessionStarted, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let TaskSessionStart {
            node_execution_id,
            launch_config,
            provider_name,
            agent_name,
            session_name,
            config_overrides,
            application_mcp_enabled,
            prompt,
            session_cwd,
            limits,
            node_token,
            flow_mcp_endpoints,
        } = options;
        validate_user_turn(&prompt, &[])?;
        let session_id = DiscussSessionId::new();
        let subject = flow_node_subject(node_execution_id);
        let assigned_agent = (!agent_name.is_empty()).then_some(agent_name.as_str());
        tracing::debug!(
            %session_id,
            provider = %provider_name,
            team = %agent_name,
            requested_cwd = %session_cwd.display(),
            "Starting task session in execution workspace"
        );

        let setup = if agent_name.is_empty() {
            Some(DiscussSessionSetup::Provider {
                config_overrides: BTreeMap::new(),
            })
        } else {
            Some(DiscussSessionSetup::Team {
                leader_id: agent_name.clone(),
            })
        };

        let (terminal, terminal_rx) = oneshot::channel();
        self.spawn_session(
            session_id,
            &provider_name,
            SpawnSessionOptions {
                launch_config,
                name: Some(session_name.unwrap_or_else(|| agent_name.clone())),
                setup,
                session_config_overrides: config_overrides,
                acp_session_id: None,
                announce_desktop_association: false,
                transient_connectivity: false,
                session_cwd: Some(session_cwd),
                application_mcp_enabled: Some(application_mcp_enabled),
                node_token: Some(node_token.clone()),
                flow_mcp_endpoints,
            },
        )
        .await?;
        if let Err(error) = persist_binding(
            &self.persistence,
            Binding {
                token: &node_token,
                subject: &subject,
                session_id,
                provider: assigned_agent.map(|_| provider_name.as_str()),
                agent: assigned_agent,
                node_id: Some(node_execution_id),
            },
        )
        .await
        {
            tracing::warn!(%session_id, %error, "Failed to persist task session identity binding");
        }
        let entry = self
            .worker_registry
            .get(&session_id)
            .ok_or(DiscussError::SessionNotFound {
                discuss_session_id: session_id,
            })?;
        let (accepted, accepted_rx) = oneshot::channel();
        entry
            .command_tx
            .clone()
            .send(WorkerCommand::RunAgentNodeTurn {
                text: prompt,
                limits,
                accepted,
                terminal,
            })
            .await
            .map_err(|error| channel_send_error("agent_node_start.worker_command", error.to_string()))?;
        tracing::debug!(%session_id, "Agent task prompt queued; awaiting worker acceptance");
        accepted_rx
            .await
            .map_err(|_| DiscussError::OneshotReply)??;
        tracing::info!(%session_id, "Agent task started; worker accepted the prompt");
        Ok(super::types::TaskSessionStarted {
            session_id,
            terminal: terminal_rx,
        })
    }

    pub(super) async fn spawn_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        options: SpawnSessionOptions,
    ) -> Result<DiscussResponse, DiscussError> {
        let SpawnSessionOptions {
            launch_config,
            name,
            setup,
            session_config_overrides,
            acp_session_id,
            announce_desktop_association,
            transient_connectivity,
            session_cwd,
            application_mcp_enabled: requested_application_mcp_enabled,
            node_token,
            flow_mcp_endpoints,
        } = options;
        if self
            .handle_existing_session(discuss_session_id, announce_desktop_association)
            .await?
        {
            return Ok(DiscussResponse::Success);
        }
        let Some(agent_spec) = self.agents.get(key) else {
            return Err(DiscussError::UnknownAgent {
                agent_key: key.to_string(),
            });
        };
        self.ensure_provider_accepts_session(key, transient_connectivity)?;
        let session_name = name.clone().unwrap_or_else(|| "A new session".to_string());
        let caller_title = name.clone();
        // NewSession is keyed only by the caller-owned id. Resolve the
        // provider recovery id from the persisted anchor rather than accepting
        // one from Zeus or Hades.
        let acp_session_id = if transient_connectivity {
            None
        } else {
            self.resolve_persisted_acp_session_id(discuss_session_id, acp_session_id)
                .await?
        };
        let reconnect_acp_session_id = acp_session_id.map(SessionId::from);
        let stored_launch = if transient_connectivity {
            None
        } else {
            let conn = self.persistence.acquire_connection().await?;
            load_discuss_session(&conn, &discuss_session_id.to_string()).await?
        };
        let provider_title = if transient_connectivity {
            None
        } else {
            initial_provider_title(
                stored_launch
                    .as_ref()
                    .and_then(|row| row.display_title.as_deref()),
                caller_title,
            )
        };
        let stored_cwd = stored_launch
            .as_ref()
            .and_then(|row| row.session_cwd_json.as_deref())
            .map(serde_json::from_str::<PathBuf>)
            .transpose()?;
        let launch_config = stored_launch
            .as_ref()
            .and_then(|row| row.session_launch_config_json.as_deref())
            .map(serde_json::from_str::<SessionLaunchConfig>)
            .transpose()?
            .or(launch_config);
        let session_cwd = stored_cwd.or(session_cwd);
        let requested_application_mcp_enabled = stored_launch
            .as_ref()
            .and_then(|row| row.session_mcp_enabled)
            .map(|enabled| enabled != 0)
            .or(requested_application_mcp_enabled);
        let (
            restored_persisted_transcript,
            next_turn_position,
            persisted_config_options,
            mut session_settings,
            restored_setup,
        ) = self
            .load_session_restore_state(
                discuss_session_id,
                transient_connectivity,
                reconnect_acp_session_id.is_some(),
            )
            .await?;
        // Resolve the restored setup before choosing the launch: a root-materialized Team
        // needs an isolated launch configured against its restored session root, even when
        // the caller supplied no launch config.
        let setup = if reconnect_acp_session_id.is_some() {
            restored_setup
        } else {
            setup
        };
        let composed_config = composed_launch_config(
            &self.persistence,
            self.mcp_domains,
            key,
            setup.as_ref(),
            discuss_session_id,
            launch_config.as_ref(),
        )
        .await?;
        let SessionLaunchPreparation {
            launch_server,
            assigned_agent_name,
            setup_settings,
            persisted_setup_kind,
            primary_agent,
            isolated_configured,
            agent,
            launch_meta,
            launch_limitations,
            effective_config,
        } = self
            .prepare_session_launch(
                discuss_session_id,
                key,
                agent_spec.server(),
                setup,
                session_cwd.as_ref(),
                (composed_config.as_ref(), launch_config.is_some()),
            )
            .await?;
        let (
            callback_router,
            callback_routes,
            shared_connection,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        ) = self
            .provider_supervisors
            .get(key)
            .filter(|_| !isolated_configured)
            .and_then(|supervisor| match &supervisor.resources {
                ProviderSupervisorResources::Shared {
                    connection: Some(connection),
                    callback_router,
                    callback_routes,
                    mcp_acp,
                    mcp_http,
                    system_prompt,
                    title_supported,
                    mcp_registrations,
                    ..
                } => Some((
                    callback_router.clone(),
                    callback_routes.clone(),
                    Some(connection.clone()),
                    Some((*mcp_acp, *mcp_http)),
                    *system_prompt,
                    *title_supported,
                    Some(mcp_registrations.clone()),
                )),
                ProviderSupervisorResources::Shared { .. } | ProviderSupervisorResources::Isolated { .. } => None,
            })
            .unwrap_or_else(|| {
                (
                    Arc::new(Mutex::new(CallbackRouter::default())),
                    None,
                    None,
                    None,
                    false,
                    false,
                    None,
                )
            });
        let configured_defaults = config_settings_from_value(agent_spec.default_session_config());
        let conn = self.persistence.acquire_connection().await?;
        let next_usage_ordinal = next_usage_ordinal(&conn, &discuss_session_id.to_string()).await?;
        let agent_defaults = if transient_connectivity {
            configured_defaults
        } else {
            load_discuss_agent_config(&conn, key)
                .await?
                .map(|row| config_settings_from_json(&row.default_config_json))
                .transpose()?
                .unwrap_or(configured_defaults)
        };
        drop(conn);
        if reconnect_acp_session_id.is_none() {
            session_settings.extend(setup_settings);
        }
        session_settings.extend(session_config_overrides);
        if !transient_connectivity {
            persist_session_anchor_inner(
                &self.persistence,
                &discuss_session_id.to_string(),
                super::session::SessionAnchorMetadata {
                    agent_name: Some(key),
                    session_name: Some(&session_name),
                    team_leader_id: assigned_agent_name.as_deref(),
                    is_task_session: node_token.is_some(),
                    ..Default::default()
                },
            )
            .await?;
            let conn = self.persistence.acquire_connection().await?;
            update_session_launch_context(
                &conn,
                &discuss_session_id.to_string(),
                session_cwd
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                requested_application_mcp_enabled,
            )
            .await?;
            update_session_launch_config(&conn, &discuss_session_id.to_string(), launch_config.as_ref()).await?;
            update_session_setup_kind(&conn, &discuss_session_id.to_string(), persisted_setup_kind).await?;
        }
        let application_mcp_enabled =
            requested_application_mcp_enabled.unwrap_or_else(|| agent_spec.application_mcp_enabled());
        self.start_session_worker(SessionWorkerInputs {
            discuss_session_id,
            key,
            assigned_agent_name,
            agent,
            launch_config: effective_config,
            launch_meta,
            launch_limitations,
            launch_server,
            session_name,
            provider_title,
            primary_agent,
            reconnect_acp_session_id,
            persisted_config_options,
            session_settings,
            restored_persisted_transcript,
            next_turn_position,
            next_usage_ordinal,
            session_cwd,
            application_mcp_enabled,
            flow_mcp_endpoints,
            node_token,
            agent_defaults,
            announce_desktop_association,
            transient_connectivity,
            callback_router,
            callback_routes,
            shared_connection,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        })
        .await
    }

    async fn start_session_worker(&mut self, inputs: SessionWorkerInputs<'_>) -> Result<DiscussResponse, DiscussError> {
        let SessionWorkerInputs {
            discuss_session_id,
            key,
            assigned_agent_name,
            agent,
            launch_config,
            launch_meta,
            launch_limitations,
            launch_server,
            session_name,
            provider_title,
            primary_agent,
            reconnect_acp_session_id,
            persisted_config_options,
            session_settings,
            restored_persisted_transcript,
            next_turn_position,
            next_usage_ordinal,
            session_cwd,
            application_mcp_enabled,
            flow_mcp_endpoints,
            node_token,
            agent_defaults,
            announce_desktop_association,
            transient_connectivity,
            callback_router,
            callback_routes,
            shared_connection,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        } = inputs;
        let stream_tx = self.stream_tx.clone();
        let broadcast_tx = self.broadcast_tx.clone();
        let persistence = self.persistence.clone();
        let capabilities_update_tx = self.capabilities_update_tx.clone();
        let activity_update_tx = self.activity_update_tx.clone();
        let worker_ready_tx = self.worker_ready_tx.clone();
        let pending_permission_tx = self.pending_permission_tx.clone();
        let pending_permission_cancel_tx = self.pending_permission_cancel_tx.clone();
        let pending_elicitation_tx = self.pending_elicitation_tx.clone();
        let pending_elicitation_cancel_tx = self.pending_elicitation_cancel_tx.clone();
        let agents_workspace = session_cwd.unwrap_or_else(|| self.agents_workspace.clone());
        let bus_service = self.bus_service.clone();
        let auth_service = self.auth_service.clone();
        let shared_mcp_registrations = shared_mcp_registrations.filter(|_| node_token.is_none());
        let (worker_jwt, worker_jti) = if let Some(node_token) = node_token {
            (node_token, None)
        } else {
            let worker_auth = issue_worker_jwt(
                self.auth_service.as_ref(),
                discuss_session_id,
                SESSION_WORKER_JWT_EXPIRATION,
            )
            .await?;
            let subject = worker_subject(discuss_session_id);
            if !transient_connectivity
                && let Err(error) = persist_binding(
                    &self.persistence,
                    Binding {
                        token: &worker_auth.token,
                        subject: &subject,
                        session_id: discuss_session_id,
                        provider: assigned_agent_name.as_ref().map(|_| key),
                        agent: assigned_agent_name.as_deref(),
                        node_id: None,
                    },
                )
                .await
            {
                revoke_worker_jti(self.auth_service.as_ref(), Some(worker_auth.jti), "identity binding").await;
                return Err(error);
            }
            (worker_auth.token, Some(worker_auth.jti))
        };
        let root_context = self.root_context.clone();
        let mcp_domains = self.mcp_domains;
        let agent_name = key.to_string();
        let worker_agent_name = agent_name.clone();
        let worker_name_for_reporting = agent_name.clone();
        let workspace_for_reporting = agents_workspace.clone();

        // Create the worker command channel keyed by `discuss_session_id`.
        let (worker_command_tx, worker_command_rx) = mpsc::channel::<WorkerCommand>(DEFAULT_BUFFER_SIZE);

        tracing::debug!(
            target: "application_acp",
            discuss_session_id = %discuss_session_id,
            session_name = %session_name,
            agent_name = %agent_name,
            reconnect = reconnect_acp_session_id.is_some(),
            restored_persisted_transcript,
            next_turn_position,
            next_usage_ordinal,
            "spawning session worker"
        );

        let transient_worker_tx_for_failure = transient_connectivity.then(|| self.transient_worker_tx.clone());
        let stream_tx_for_failure = self.stream_tx.clone();
        let pending_elicitation_cancel_tx_for_failure = self.pending_elicitation_cancel_tx.clone();
        let worker_params = WorkerSessionParams {
            launch_config,
            launch_meta,
            launch_limitations,
            stream_tx,
            discuss_session_id,
            auth_launch_server: launch_server,
            session_name,
            provider_title,
            agent_name,
            primary_agent,
            reconnect_acp_session_id,
            persisted_config_options,
            session_settings,
            restored_persisted_transcript,
            next_turn_position,
            next_usage_ordinal,
            price_index: self.price_index.clone(),
            agents_workspace,
            application_mcp_enabled,
            bus_service,
            flow_mcp_endpoints,
            worker_jwt,
            root_context,
            mcp_domains,
            broadcast_tx,
            persistence,
            worker_command_rx,
            capabilities_update_tx,
            activity_update_tx,
            worker_ready_tx,
            agent_defaults,
            pending_permission_tx,
            pending_permission_cancel_tx,
            pending_elicitation_tx,
            pending_elicitation_cancel_tx,
            announce_desktop_association,
            transient_connectivity,
            callback_router,
            callback_routes,
            shared_connection,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        };
        let join_handle = tokio::spawn(run_session_worker_with_reporting(
            agent,
            worker_params,
            discuss_session_id,
            worker_name_for_reporting,
            workspace_for_reporting,
            auth_service,
            worker_jti,
            transient_worker_tx_for_failure,
            pending_elicitation_cancel_tx_for_failure,
            stream_tx_for_failure,
        ));

        self.worker_registry.insert(
            discuss_session_id,
            WorkerEntry {
                agent_name: worker_agent_name,
                worker_jti,
                command_tx: worker_command_tx,
                join_handle,
                active_turn: false,
                acp_session_id: None,
                desktop_association_requested: announce_desktop_association,
            },
        );
        if !transient_connectivity && let Some(supervisor) = self.provider_supervisors.get_mut(key) {
            supervisor.associate_chat(discuss_session_id);
            if let ProviderSupervisorResources::Isolated { child_sessions, .. } = &mut supervisor.resources {
                child_sessions.insert(discuss_session_id);
            }
        }

        Ok(DiscussResponse::Success)
    }

    async fn prepare_session_launch(
        &self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        server: &McpServer,
        setup: Option<DiscussSessionSetup>,
        session_cwd: Option<&PathBuf>,
        (launch_config, explicit_config): (Option<&SessionLaunchConfig>, bool),
    ) -> Result<SessionLaunchPreparation, DiscussError> {
        let plugin = plugins::for_agent(key);
        let mut launch_server = plugin.launch_server(server);
        if let Some(provider) = self.agents.get(key) {
            plugins::apply_managed_launch_env(plugin, &mut launch_server, provider.managed_document())?;
        }
        let root_default_config = SessionLaunchConfig::default();
        let launch_workspace = Self::launch_workspace(setup.as_ref(), session_cwd, &self.agents_workspace)?;
        if let Some(DiscussSessionSetup::TeamAtRoot { leader_id }) = setup.as_ref() {
            self.materialize_custom_team_at_root(key, launch_workspace, leader_id)
                .await?;
        }
        let restores_team_at_root = matches!(setup, Some(DiscussSessionSetup::TeamAtRoot { .. }));
        let persisted_setup_kind = Self::setup_kind_for(setup.as_ref());
        let (assigned_agent_name, setup_settings) = self.resolve_session_setup(key, setup).await?;
        let primary_agent = if let Some(leader_id) = assigned_agent_name.as_deref() {
            let agent_id =
                AgentId::try_from(leader_id.to_string()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                    asset_id: leader_id.to_string(),
                    violation: error.to_string(),
                })?;
            Some(self.resolve_agent(&agent_id).await?)
        } else {
            None
        };
        let launch_inputs = match launch_config {
            Some(config) => Some((config, launch_workspace)),
            None if restores_team_at_root => Some((&root_default_config, launch_workspace)),
            None => None,
        };
        let prepared = if let Some((config, _)) = launch_inputs {
            plugin.launch_preparation(&LaunchContext::new(&self.agents_workspace, discuss_session_id), config)?
        } else {
            Default::default()
        };
        let effective_config = launch_inputs.map(|(config, _)| config.clone());
        let mut isolated_configured = if let Some((config, workspace)) = launch_inputs {
            let configured = plugin.configured_launch_server(&launch_server, workspace, config)?;
            let isolated =
                explicit_config || plugin.uses_mcp_launch_env() || configured != launch_server || restores_team_at_root;
            launch_server = configured;
            isolated
        } else {
            false
        };
        // A launch-argument selection names the Agent per process, so the session needs its own.
        if let Some(agent) = &primary_agent
            && let PrimaryAgentSelection::LaunchArguments(arguments) =
                plugin.primary_agent_selection(&agent.selection_view())
        {
            let McpServer::Stdio(server) = &mut launch_server else {
                return Err(DiscussError::Validation {
                    message: "Launch-argument Agent selection requires a stdio ACP server".to_string(),
                });
            };
            server.args.extend(arguments);
            isolated_configured = true;
        }
        let agent = Self::acp_agent_from_server(&launch_server)?;
        Ok(SessionLaunchPreparation {
            launch_server,
            assigned_agent_name,
            setup_settings,
            persisted_setup_kind,
            primary_agent,
            isolated_configured,
            agent,
            launch_meta: prepared.meta,
            launch_limitations: prepared.limitations,
            effective_config,
        })
    }

    async fn load_session_restore_state(
        &self,
        discuss_session_id: DiscussSessionId,
        transient_connectivity: bool,
        reconnect: bool,
    ) -> Result<
        (
            bool,
            i64,
            Vec<DiscussConfigOption>,
            ConfigSettings,
            Option<DiscussSessionSetup>,
        ),
        DiscussError,
    > {
        Ok(if transient_connectivity {
            (false, 0, Vec::new(), ConfigSettings::new(), None)
        } else if reconnect {
            let conn = self.persistence.acquire_connection().await?;
            let turns = load_discuss_chat_turns_window(&conn, &discuss_session_id.to_string()).await?;
            let row = load_discuss_session(&conn, &discuss_session_id.to_string()).await?;
            let next_position = turns.last().map(|turn| turn.position + 1).unwrap_or(0);
            let persisted_config_options = row
                .as_ref()
                .and_then(|row| row.latest_config_options_json.as_deref())
                .map(config_options_from_json)
                .transpose()?
                .unwrap_or_default();
            let session_settings = row
                .as_ref()
                .and_then(|row| row.config_settings_json.as_deref())
                .map(config_settings_from_json)
                .transpose()?
                .unwrap_or_default();
            (
                !turns.is_empty(),
                next_position,
                persisted_config_options,
                session_settings,
                row.as_ref().and_then(Self::restored_team_setup),
            )
        } else {
            (false, 0, Vec::new(), ConfigSettings::new(), None)
        })
    }

    /// Workspace a launch is configured against. A root-materialized Team must launch from its
    /// restored session root and never falls back to the central homebase; other launches keep the homebase.
    fn launch_workspace<'a>(
        setup: Option<&DiscussSessionSetup>,
        session_cwd: Option<&'a PathBuf>,
        homebase: &'a PathBuf,
    ) -> Result<&'a PathBuf, DiscussError> {
        match (setup, session_cwd) {
            (Some(DiscussSessionSetup::TeamAtRoot { .. }), Some(root)) => Ok(root),
            (Some(DiscussSessionSetup::TeamAtRoot { leader_id }), None) => Err(DiscussError::Validation {
                message: format!("Team `{leader_id}` has no execution root to launch from"),
            }),
            _ => Ok(homebase),
        }
    }

    pub(super) async fn resolve_session_setup(
        &self,
        provider_name: &str,
        setup: Option<DiscussSessionSetup>,
    ) -> Result<(Option<String>, ConfigSettings), DiscussError> {
        match setup {
            None => Ok((None, ConfigSettings::new())),
            Some(DiscussSessionSetup::Provider { config_overrides }) => Ok((None, config_overrides)),
            Some(DiscussSessionSetup::Agent { agent_id }) => {
                self.resolve_standalone_agent_setup(provider_name, agent_id)
                    .await
            }
            Some(DiscussSessionSetup::TeamAtRoot { leader_id }) => Ok((Some(leader_id), ConfigSettings::new())),
            Some(DiscussSessionSetup::Team { leader_id }) => {
                let conn = self.persistence.acquire_connection().await?;
                let Some(installation) = load_discuss_team_installation(&conn, &leader_id).await? else {
                    return Err(DiscussError::Validation {
                        message: format!("Team `{leader_id}` is not installed"),
                    });
                };
                if installation.provider_name != provider_name {
                    return Err(DiscussError::Validation {
                        message: format!(
                            "Team `{leader_id}` is installed for Provider `{}`, not `{provider_name}`",
                            installation.provider_name
                        ),
                    });
                }
                let mut member_overrides =
                    serde_json::from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)?;
                Ok((
                    Some(leader_id.clone()),
                    member_overrides.remove(&leader_id).unwrap_or_default(),
                ))
            }
        }
    }

    fn acp_agent_from_server(server: &McpServer) -> Result<AcpAgent, DiscussError> {
        let McpServer::Stdio(server) = server else {
            return Err(DiscussError::Validation {
                message: "Provider connectivity checks require a stdio ACP server".to_string(),
            });
        };
        let config = AcpAgentConfig::new(server.command.clone())
            .args(server.args.clone())
            .envs(
                server
                    .env
                    .iter()
                    .map(|variable| (variable.name.clone(), variable.value.clone())),
            );
        Ok(AcpAgent::new(config))
    }

    pub(super) async fn resolve_persisted_acp_session_id(
        &self,
        discuss_session_id: DiscussSessionId,
        requested_acp_session_id: Option<String>,
    ) -> Result<Option<String>, DiscussError> {
        if let Some(acp_session_id) = present_acp_session_id(requested_acp_session_id.as_deref()) {
            return Ok(Some(acp_session_id));
        }

        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_session(&conn, &discuss_session_id.to_string()).await?;
        Ok(row.and_then(|session| present_acp_session_id(session.acp_session_id.as_deref())))
    }

    pub(super) async fn announce_desktop_association(
        &self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<(), DiscussError> {
        self.broadcast_tx
            .send(DiscussResponse::NewSession {
                session_id: discuss_session_id,
            })
            .await
            .map_err(|error| channel_send_error("announce_desktop_association.broadcast", error.to_string()))?;
        Ok(())
    }

    // ── Additive request handlers ──────────────────────────────────────────

    /// List local ACP chat sessions ordered by most recent interaction.
    ///
    /// Reads local `acp_agent_sessions` rows only as the session identity
    /// source. For every local row that has a non-empty `acp_session_id`,
    /// checks the matching worker's server-side capability state and, when the
    /// `list` bit is set, forwards a `WorkerCommand::ListProviderSessions` to
    /// the live worker so provider `SessionInfo` can refresh freshness metadata
    /// (`last_interaction_at`) on **already-local** rows matched by
    /// `acp_session_id`. Provider titles remain metadata-only; local
    /// display labels are user-controlled.
    ///
    /// Provider-listed sessions that do not already have a local
    /// `discuss_sessions` row are **not** materialized. A provider list row
    /// is a candidate only; local identity is created solely by explicit
    /// create / load / import / persist paths.
    pub(super) async fn handle_list_sessions(&mut self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let local_rows = list_recent_discuss_sessions(&conn).await?;

        let mut sessions: Vec<DiscussSessionInfo> = local_rows
            .iter()
            .filter_map(|row| {
                Some(DiscussSessionInfo {
                    launch_limitations: row.launch_limitations.clone(),
                    active_turn: DiscussSessionId::from_str(&row.discuss_session_id)
                        .ok()
                        .and_then(|id| self.worker_registry.get(&id))
                        .is_some_and(|entry| !entry.join_handle.is_finished() && entry.active_turn),
                    is_task_session: row.is_task_session,
                    team_leader_id: row.team_leader_id.clone(),
                    discuss_session_id: DiscussSessionId::from_str(&row.discuss_session_id).ok()?,
                    session_name: row.session_name.clone(),
                    agent_name: row.agent_name.clone(),
                    acp_session_id: present_acp_session_id(row.acp_session_id.as_deref()),
                    display_title: row.display_title.clone().filter(|t| !t.is_empty()),
                    last_user_prompt: row.last_user_prompt.clone().filter(|p| !p.is_empty()),
                    latest_todo_snapshot_json: row.latest_todo_snapshot_json.clone(),
                    latest_config_options_json: row.latest_config_options_json.clone(),
                    available_commands_json: row.available_commands_json.clone(),
                    first_prompt_at: row.first_prompt_at,
                    last_interaction_at: row.last_interaction_at,
                })
            })
            .collect();

        // Defensive dedupe by local identity first, then by provider session id
        // so a corrupted DB cannot surface the same chat twice in the dropdown.
        sessions = dedupe_session_list(sessions);

        // For every persisted row with a non-empty acp_session_id, check the
        // server-side capability state and forward ListProviderSessions to
        // the live worker when the list bit is set. An empty-string id is the
        // canonical "no provider session id yet" sentinel, so it is treated
        // the same as an absent id here.
        //
        // Provider-only rows are intentionally ignored: list/refresh must never
        // insert into `discuss_sessions`.
        for row in &local_rows {
            if present_acp_session_id(row.acp_session_id.as_deref()).is_none() {
                continue;
            }
            let Ok(discuss_session_id) = DiscussSessionId::from_str(&row.discuss_session_id) else {
                continue;
            };
            if let Some(entry) = self.worker_registry.get(&discuss_session_id)
                && let Some(caps) = self.worker_capabilities.get(&discuss_session_id)
                && caps.list
            {
                let command_tx = entry.command_tx.clone();
                let (reply, reply_rx) = oneshot::channel();
                if command_tx
                    .send(WorkerCommand::ListProviderSessions { reply })
                    .await
                    .is_err()
                {
                    self.worker_registry.remove(&discuss_session_id);
                    self.worker_capabilities.remove(&discuss_session_id);
                } else if let Ok(Ok(provider_sessions)) = reply_rx.await {
                    // Merge provider-listed freshness metadata onto already-local
                    // rows matched by acp_session_id. Provider-only rows are not
                    // materialized.
                    for ps in provider_sessions {
                        // Provider session id match: update last_interaction_at.
                        if let Some(ref acp_id) = row.acp_session_id
                            && ps.session_id.to_string() == *acp_id
                        {
                            let now_ms = provider_updated_at_ms(ps.updated_at.as_deref());
                            let _ = merge_refresh_discuss_session(
                                &conn,
                                &row.discuss_session_id,
                                &LoadedSessionRefresh {
                                    acp_session_id: None,
                                    // ProviderSessionInfo does not expose a title;
                                    // retain the locally persisted session metadata.
                                    display_title: None,
                                    latest_config_options_json: None,
                                    last_interaction_at: now_ms,
                                },
                            )
                            .await;
                        }
                    }
                }
            }
        }

        Ok(DiscussResponse::SessionList { sessions })
    }

    /// Reload an existing session at the requested working directory.
    /// An anchor without a provider session id uses the normal first-attachment path.
    pub(super) async fn handle_change_working_dir(
        &mut self,
        discuss_session_id: DiscussSessionId,
        session_cwd: PathBuf,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let session_id = discuss_session_id.to_string();
        let row = load_discuss_session(&conn, &session_id)
            .await?
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
        let stored_cwd = row
            .session_cwd_json
            .as_deref()
            .map(serde_json::from_str::<PathBuf>)
            .transpose()?;
        let live_worker = self
            .worker_registry
            .get(&discuss_session_id)
            .filter(|entry| !entry.join_handle.is_finished() && !entry.command_tx.is_closed());
        let cwd_changed = stored_cwd.as_ref() != Some(&session_cwd);
        if !cwd_changed && live_worker.is_some() {
            return Ok(DiscussResponse::Success);
        }
        if cwd_changed && live_worker.is_some_and(|entry| entry.active_turn) {
            return Err(DiscussError::SessionBusy { discuss_session_id });
        }
        if let Some(entry) = self.worker_registry.remove(&discuss_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            drop(entry.command_tx);
            let mut join_handle = entry.join_handle;
            if tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, &mut join_handle)
                .await
                .is_err()
            {
                join_handle.abort();
                return Err(DiscussError::WorkerStopTimeout { discuss_session_id });
            }
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
            if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                supervisor.disassociate_chat(&discuss_session_id);
            }
            self.worker_capabilities.remove(&discuss_session_id);
            self.cancel_pending_permissions_for_session(&discuss_session_id);
            self.cancel_pending_elicitations_for_session(&discuss_session_id);
        }
        if cwd_changed {
            update_session_launch_context(&conn, &session_id, Some(serde_json::to_string(&session_cwd)?), None).await?;
        }
        let setup = match Self::restored_team_setup(&row) {
            Some(DiscussSessionSetup::Team { leader_id }) => {
                update_session_setup_kind(&conn, &session_id, Some("team_at_root")).await?;
                Some(DiscussSessionSetup::TeamAtRoot { leader_id })
            }
            setup => setup,
        };
        drop(conn);
        self.spawn_session(
            discuss_session_id,
            &row.agent_name,
            SpawnSessionOptions {
                launch_config: None,
                name: Some(row.session_name.clone()),
                setup,
                session_config_overrides: BTreeMap::new(),
                acp_session_id: None,
                announce_desktop_association: true,
                transient_connectivity: false,
                session_cwd: None,
                application_mcp_enabled: None,
                node_token: None,
                flow_mcp_endpoints: None,
            },
        )
        .await
    }

    /// Remove a local Discuss chat session anchor and clean live worker state.
    ///
    /// Deletes the `discuss_sessions` row for `discuss_session_id`.
    /// Related `discuss_chat_turns` cascade via FK. Dropping the worker registry
    /// sender closes the command channel so the session worker exits; the
    /// capability cache entry is removed so subsequent config/list calls fail
    /// closed for the removed identity.
    ///
    /// Idempotent: a missing DB row still cleans live worker state and returns
    /// `Success`.
    pub(super) async fn handle_remove_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let mut conn = self.persistence.acquire_connection().await?;
        let tx = conn.transaction().await?;
        super::mcp::clear_mcp_scope(
            &tx,
            super::types::CustomMcpScope::Session,
            &discuss_session_id.to_string(),
        )
        .await?;
        delete_discuss_session(&tx, &discuss_session_id.to_string()).await?;
        tx.commit().await?;

        if let Some(entry) = self.worker_registry.remove(&discuss_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
            if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                supervisor.disassociate_chat(&discuss_session_id);
            }
        }
        if let Some(pending) = self.pending_provider_tests.remove(&discuss_session_id) {
            let reply = self.restore_provider_test_snapshot(pending, false).await;
            let _ = reply.send(Err(DiscussError::SessionCancelled { discuss_session_id }));
        }
        self.worker_capabilities.remove(&discuss_session_id);
        self.cancel_pending_permissions_for_session(&discuss_session_id);
        self.cancel_pending_elicitations_for_session(&discuss_session_id);

        Ok(DiscussResponse::Success)
    }

    /// Load a single chat session and its bounded recent transcript turns.
    ///
    /// Missing-load is always a graceful fallback: when the provider does not
    /// advertise `load` (live-worker or short-lived branch), the local row +
    /// bounded turns are returned and `UnsupportedCapability` is never raised.
    /// When a provider refresh succeeds, the refreshed metadata is persisted
    /// and the response is rebuilt from the re-read row so the client always
    /// sees the freshest persisted state rather than the pre-refresh snapshot.
    pub(super) async fn handle_load_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let key = discuss_session_id;
        tracing::debug!(
            target: "application_acp",
            discuss_session_id = %discuss_session_id,
            has_live_worker = self.worker_registry.contains_key(&key),
            "loading session (metadata + bounded transcript window)"
        );
        let conn = self.persistence.acquire_connection().await?;
        let mut row = load_discuss_session(&conn, &discuss_session_id.to_string())
            .await?
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;

        let turns = load_discuss_chat_turns_window(&conn, &discuss_session_id.to_string()).await?;
        let subagents = load_discuss_subagents(&conn, &discuss_session_id.to_string()).await?;
        let turn_records: Vec<DiscussChatTurnRecord> = turns
            .iter()
            .filter_map(|t| {
                Some(DiscussChatTurnRecord {
                    discuss_session_id: DiscussSessionId::from_str(&t.discuss_session_id).ok()?,
                    position: t.position,
                    role: t.role.clone().unwrap_or_default(),
                    acp_session_id: t.acp_session_id.clone(),
                    content_json: t.content_json.clone(),
                    created_at: t.created_at,
                })
            })
            .collect();

        let acp_session_id = present_acp_session_id(row.acp_session_id.as_deref());

        // LIVE-WORKER BRANCH
        if let Some(entry) = self.worker_registry.get(&key) {
            let load_supported = self
                .worker_capabilities
                .get(&key)
                .map(|c| c.load)
                .unwrap_or(false);
            if load_supported && let Some(ref acp_sid) = acp_session_id {
                let command_tx = entry.command_tx.clone();
                let (reply, reply_rx) = oneshot::channel();
                if command_tx
                    .send(WorkerCommand::LoadProviderSession {
                        acp_session_id: SessionId::from(acp_sid.clone()),
                        reply,
                    })
                    .await
                    .is_err()
                {
                    self.worker_registry.remove(&key);
                    self.worker_capabilities.remove(&key);
                } else if let Ok(Ok(ps)) = reply_rx.await {
                    let now_ms = provider_updated_at_ms(ps.updated_at.as_deref());
                    let refreshed_options_json = provider_config_options_json(&ps.config_options)?;
                    row = self
                        .refresh_loaded_session_row(
                            discuss_session_id,
                            LoadedSessionRefresh {
                                acp_session_id: Some(ps.session_id.to_string()),
                                display_title: None,
                                latest_config_options_json: refreshed_options_json,
                                last_interaction_at: now_ms,
                            },
                        )
                        .await?;
                }
            }
        } else if let Some(acp_sid) = acp_session_id {
            // NO-LIVE-WORKER BRANCH
            if !self
                .provider_supervisors
                .get(&row.agent_name)
                .is_some_and(ProviderSupervisor::accepts_work)
            {
                return Err(DiscussError::ProviderNotRunning {
                    provider_name: row.agent_name.clone(),
                });
            }
            let Some(agent_spec) = self.agents.get(&row.agent_name) else {
                return Err(DiscussError::SessionNotFound { discuss_session_id });
            };
            if let Some(options) = load_provider_session_short_lived(
                &row.agent_name,
                agent_spec,
                SessionId::from(acp_sid),
                self.agents_workspace.clone(),
            )
            .await?
            {
                let options_json = serde_json::to_string(&options)?;
                let defaults = load_discuss_agent_config(&conn, &row.agent_name)
                    .await?
                    .map(|config| config_settings_from_json(&config.default_config_json))
                    .transpose()?
                    .unwrap_or_default();
                upsert_discuss_agent_config(&conn, &row.agent_name, Some(&options), &defaults).await?;
                row = self
                    .refresh_loaded_session_row(
                        discuss_session_id,
                        LoadedSessionRefresh {
                            acp_session_id: None,
                            display_title: None,
                            latest_config_options_json: Some(options_json),
                            last_interaction_at: chrono::Utc::now().timestamp_millis(),
                        },
                    )
                    .await?;
            }
        }

        let session = DiscussSessionInfo {
            launch_limitations: row.launch_limitations,
            active_turn: self
                .worker_registry
                .get(&discuss_session_id)
                .is_some_and(|entry| !entry.join_handle.is_finished() && entry.active_turn),
            is_task_session: row.is_task_session,
            team_leader_id: row.team_leader_id,
            discuss_session_id: DiscussSessionId::from_str(&row.discuss_session_id).map_err(|_| {
                DiscussError::Serialization {
                    message: "invalid discuss_session_id in Discuss session row".to_string(),
                }
            })?,
            session_name: row.session_name,
            agent_name: row.agent_name,
            acp_session_id: present_acp_session_id(row.acp_session_id.as_deref()),
            display_title: row.display_title.filter(|t| !t.is_empty()),
            last_user_prompt: row.last_user_prompt.filter(|p| !p.is_empty()),
            latest_todo_snapshot_json: row.latest_todo_snapshot_json,
            latest_config_options_json: row.latest_config_options_json,
            available_commands_json: row.available_commands_json,
            first_prompt_at: row.first_prompt_at,
            last_interaction_at: row.last_interaction_at,
        };

        Ok(DiscussResponse::SessionLoad {
            session,
            turns: turn_records,
            subagents,
        })
    }

    /// Persist a provider-refreshed metadata delta onto the chat anchor row and
    /// re-read the row so the load response reflects the freshest persisted
    /// state.
    ///
    /// R4: this uses a merge-safe **partial** `UPDATE`
    /// (`discuss_sessions_load_refresh.sql`) rather than the full-column
    /// upsert. Only the refreshed columns are touched, each via
    /// `COALESCE(new, existing)`, and `last_interaction_at` moves monotonically
    /// via `MAX(...)`. A `None` field in `refresh` therefore carries the
    /// existing value forward — a provider that omits a title or config options
    /// never erases known-good state — and a stale refresh interleaved with a
    /// newer queued write from the worker's per-session persistence queue can
    /// never clobber the fresher metadata or move the timestamp backward.
    ///
    /// The `row` snapshot is intentionally not written back: because the update
    /// is partial and merge-safe, carry-forward columns (`last_user_prompt`,
    /// `available_commands_json`, `first_prompt_at`) are left untouched in the
    /// database instead of being re-written from a possibly-stale snapshot.
    async fn refresh_loaded_session_row(
        &self,
        discuss_session_id: DiscussSessionId,
        refresh: LoadedSessionRefresh,
    ) -> Result<DiscussSessionRow, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let discuss_session_id_string = discuss_session_id.to_string();
        merge_refresh_discuss_session(&conn, &discuss_session_id_string, &refresh).await?;
        let refreshed = load_discuss_session(&conn, &discuss_session_id_string)
            .await?
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
        Ok(refreshed)
    }

    /// Get the database-backed provider menu for a session.
    ///
    /// Workers eagerly replace the persisted snapshot whenever the provider
    /// publishes a complete menu. Reads never synchronously query a provider.
    pub(super) async fn handle_get_config_options(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_session(&conn, &discuss_session_id.to_string()).await?;
        let row = row.ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
        let options = match row.latest_config_options_json.as_deref() {
            Some(json) => config_options_from_json(json)?,
            None => load_discuss_agent_config(&conn, &row.agent_name)
                .await?
                .and_then(|config| config.config_options_json)
                .as_deref()
                .map(config_options_from_json)
                .transpose()?
                .unwrap_or_default(),
        };

        Ok(DiscussResponse::ConfigOptions { options })
    }

    /// Set a provider-exposed config option on an active session.
    ///
    /// Fails closed on the stored server-side `set_config_option` capability
    /// (R1.2): the mutation is only authorized when the actor holds a live
    /// worker for the session AND the worker-reported
    /// `SessionCapabilitiesCache.set_config_option` bit is set. That bit is
    /// raised by the worker only from genuine provider server-side signals
    /// (`session/new` advertising config options, `session/load` advertising
    /// config options, `config_option_update`, or a successful
    /// `session/set_config_option`) — never from the Hades `providerCapabilities`
    /// cache, never from the `session/list` bit alone, and never from an inline
    /// "does the worker currently return non-empty options" probe at mutation
    /// time. Absent capability fails closed with
    /// `UnsupportedCapability { capability: "set_config_option" }`.
    pub(super) async fn handle_set_config_option(
        &mut self,
        discuss_session_id: DiscussSessionId,
        config_id: String,
        value: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let key = discuss_session_id;

        let command_tx = self
            .worker_registry
            .get(&key)
            .map(|e| e.command_tx.clone())
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;

        let set_config_supported = self
            .worker_capabilities
            .get(&key)
            .map(|caps| caps.set_config_option)
            .unwrap_or(false);
        if !set_config_supported {
            return Err(DiscussError::UnsupportedCapability {
                capability: "set_config_option".to_string(),
            });
        }

        let (reply, reply_rx) = oneshot::channel();
        command_tx
            .send(WorkerCommand::SetConfigOption {
                config_id,
                value,
                reply,
            })
            .await
            .map_err(|error| channel_send_error("handle_set_config_option.worker_command", error.to_string()))?;

        let result = reply_rx.await.map_err(|_| DiscussError::OneshotReply)?;

        result?;
        Ok(DiscussResponse::Success)
    }

    /// Rename the local display label for a chat anchor.
    ///
    /// The stable `discuss_session_id` key and any provider `acp_session_id`
    /// are preserved; this updates only the user-controlled local label stored
    /// on the anchor row.
    pub(super) async fn handle_rename_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
        display_title: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        let title = display_title.trim();
        if title.is_empty() {
            return Err(DiscussError::Validation {
                message: "session display title cannot be empty".to_string(),
            });
        }

        let conn = self.persistence.acquire_connection().await?;
        rename_discuss_session(&conn, discuss_session_id, title).await?;
        if let Some(entry) = self.worker_registry.get(&discuss_session_id) {
            let _ = entry.command_tx.try_send(WorkerCommand::SetTitle {
                title: title.to_owned(),
            });
        }
        Ok(DiscussResponse::Success)
    }
}

/// Title pushed to the provider when a worker starts: a user rename wins over the caller's name, and
/// a session started without a name keeps whatever title the provider gives it.
fn initial_provider_title(stored_display_title: Option<&str>, caller_name: Option<String>) -> Option<String> {
    stored_display_title
        .filter(|title| !title.trim().is_empty())
        .map(str::to_owned)
        .or(caller_name)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the moved session-worker reporting helper's inputs unchanged during the module refactor."
)]
async fn run_session_worker_with_reporting(
    agent: AcpAgent,
    params: WorkerSessionParams,
    discuss_session_id: DiscussSessionId,
    agent_name: String,
    agents_workspace: PathBuf,
    auth_service: Option<ServiceReader<dyn AuthSudoService>>,
    worker_jti: Option<uuid::Uuid>,
    transient_worker_tx: Option<mpsc::Sender<TransientWorkerEvent>>,
    pending_elicitation_cancel_tx: mpsc::Sender<PendingElicitationCancel>,
    stream_tx: mpsc::Sender<DiscussStreamEvent>,
) {
    tracing::info!(%discuss_session_id, provider = %agent_name, cwd = %agents_workspace.display(), "Discuss session worker started");
    let result = run_session_worker(agent, params).await;

    tracing::info!(%discuss_session_id, failed = result.is_err(), "Discuss session worker stopped");

    if let Err(failure) = &result {
        tracing::debug!(%discuss_session_id, error = %failure.error, "Discuss session worker failure");
    }
    revoke_worker_jti(auth_service.as_ref(), worker_jti, "transient").await;

    if let Err(error) = result {
        let SessionWorkerFailure {
            error,
            terminal_auth_methods,
        } = error;
        let error = *error;
        tracing::warn!(?error, "Discuss session worker stopped");
        if let Some(transient_worker_tx) = transient_worker_tx {
            let _ = transient_worker_tx
                .send(TransientWorkerEvent {
                    discuss_session_id,
                    result: Err(error),
                })
                .await;
        } else {
            let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
            if pending_elicitation_cancel_tx
                .send(PendingElicitationCancel {
                    discuss_session_id,
                    elicitation_request_id: None,
                    cancellation_ack,
                })
                .await
                .is_ok()
            {
                let _ = cancellation_ack_rx.await;
            }
            if let Err(send_error) = stream_tx
                .send(DiscussStreamEvent::SessionFailed {
                    discuss_session_id,
                    error,
                    terminal_auth_methods,
                })
                .await
            {
                tracing::warn!(
                    ?send_error,
                    "Discuss session failure could not reach the desktop bridge"
                );
            }
        }
    }
}

async fn load_provider_session_short_lived(
    agent_name: &str,
    agent_spec: &AcpProvider,
    acp_session_id: SessionId,
    agents_workspace: PathBuf,
) -> Result<Option<Vec<DiscussConfigOption>>, DiscussError> {
    let agent_plugin = plugins::for_agent(agent_name);
    let application_mcp_enabled = agent_spec.application_mcp_enabled();
    let agent_key = serde_json::to_string(agent_spec.server())?;
    let agent = AcpAgent::from_str(&agent_key)?;

    Client
        .v1()
        .name("acp-session-load")
        .connect_with(agent, move |cx: ConnectionTo<Agent>| async move {
            let init_response = cx.send_request(initialize_request()).block_task().await?;

            // Missing-load fallback: a provider that does not advertise `load`
            // is a graceful case for `acp.loadSession` — the caller returns the
            // local row + bounded turns. Signal that with `Ok(None)` here rather
            // than an `UnsupportedCapability` error so the load never fails on a
            // missing capability. `UnsupportedCapability` is reserved for
            // explicit provider-only mutations with no local fallback.
            if !init_response.agent_capabilities.load_session {
                return Ok(None);
            }

            if application_mcp_enabled && !init_response.agent_capabilities.mcp_capabilities.acp {
                tracing::debug!(
                    target: "application_acp",
                    "short-lived ACP session load skipped MCP because the agent lacks MCP-over-ACP support"
                );
            }

            // The cwd is the config-owned ACP agents workspace, matching the
            // live session/new and session/load paths. Create it first so a
            // missing directory cannot fail the provider load with a protocol
            // error about a non-existent cwd.
            ensure_agents_workspace_dir(&agents_workspace).await;
            let response = cx
                .send_request(load_session_request(acp_session_id, agents_workspace, &[]))
                .block_task()
                .await?;

            Ok(agent_plugin.initial_config_options(response.config_options, None))
        })
        .await
        .map_err(DiscussError::from)
}

/// Deduplicate session list rows by local identity, then by provider session id.
///
/// First-seen wins so the interaction-ordered list from SQL stays stable.
/// Empty-string / absent `acp_session_id` values do not participate in the
/// provider-id merge (they are not real provider identities).
pub(super) fn dedupe_session_list(sessions: Vec<DiscussSessionInfo>) -> Vec<DiscussSessionInfo> {
    let mut seen_identity = HashSet::<DiscussSessionId>::new();
    let mut seen_acp_id = HashSet::<String>::new();
    let mut out = Vec::with_capacity(sessions.len());

    for session in sessions {
        let identity = session.discuss_session_id;
        if !seen_identity.insert(identity)
            || present_acp_session_id(session.acp_session_id.as_deref())
                .is_some_and(|acp_id| !seen_acp_id.insert(acp_id))
        {
            continue;
        }
        out.push(session);
    }

    out
}

#[cfg(test)]
mod team_at_root_setup_tests {
    use super::*;
    use crate::discuss::storage::DiscussSessionRow;

    fn row(team_leader_id: Option<&str>, setup_kind: Option<&str>) -> DiscussSessionRow {
        DiscussSessionRow {
            is_task_session: false,
            session_cwd_json: None,
            session_mcp_enabled: None,
            session_launch_config_json: None,
            launch_limitations: Vec::new(),
            session_setup_kind: setup_kind.map(str::to_string),
            team_leader_id: team_leader_id.map(str::to_string),
            discuss_session_id: "00000000-0000-0000-0000-000000000001".to_string(),
            agent_name: "opencode".to_string(),
            session_name: "ticket".to_string(),
            acp_session_id: None,
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            available_commands_json: None,
            first_prompt_at: None,
            last_interaction_at: None,
        }
    }

    #[test]
    fn only_root_materialized_team_persists_a_setup_kind() {
        let leader = "chief-of-staff".to_string();
        assert_eq!(
            Actor::setup_kind_for(Some(&DiscussSessionSetup::TeamAtRoot {
                leader_id: leader.clone()
            })),
            Some("team_at_root")
        );
        assert_eq!(
            Actor::setup_kind_for(Some(&DiscussSessionSetup::Team {
                leader_id: leader.clone()
            })),
            None
        );
        assert_eq!(
            Actor::setup_kind_for(Some(&DiscussSessionSetup::Provider {
                config_overrides: Default::default()
            })),
            None
        );
        assert_eq!(Actor::setup_kind_for(None), None);
    }

    #[test]
    fn reconnect_restores_root_materialized_team_with_exact_leader() {
        let restored = Actor::restored_team_setup(&row(Some("chief-of-staff"), Some("team_at_root")));
        assert_eq!(
            restored,
            Some(DiscussSessionSetup::TeamAtRoot {
                leader_id: "chief-of-staff".to_string()
            })
        );
    }

    #[test]
    fn legacy_team_rows_still_restore_the_central_team_setup() {
        // Rows written before migration 4 have no setup kind and must keep the central path.
        let restored = Actor::restored_team_setup(&row(Some("sector-analyst"), None));
        assert_eq!(
            restored,
            Some(DiscussSessionSetup::Team {
                leader_id: "sector-analyst".to_string()
            })
        );
    }

    #[test]
    fn provider_sessions_without_a_leader_restore_no_team() {
        assert_eq!(Actor::restored_team_setup(&row(None, None)), None);
        assert_eq!(Actor::restored_team_setup(&row(None, Some("team_at_root"))), None);
    }

    #[test]
    fn root_materialized_team_launches_from_the_restored_session_root() {
        let homebase = std::path::PathBuf::from("/central/homebase");
        let root = std::path::PathBuf::from("/tickets/execution-root");
        let setup = DiscussSessionSetup::TeamAtRoot {
            leader_id: "lead".to_string(),
        };
        assert_eq!(
            Actor::launch_workspace(Some(&setup), Some(&root), &homebase).expect("root launch"),
            &root
        );
    }

    #[test]
    fn root_team_without_a_session_root_fails_validation() {
        let homebase = std::path::PathBuf::from("/central/homebase");
        let setup = DiscussSessionSetup::TeamAtRoot {
            leader_id: "lead".to_string(),
        };
        assert!(matches!(
            Actor::launch_workspace(Some(&setup), None, &homebase),
            Err(DiscussError::Validation { .. })
        ));
    }

    #[test]
    fn central_team_and_provider_launches_keep_the_homebase_workspace() {
        let homebase = std::path::PathBuf::from("/central/homebase");
        let root = std::path::PathBuf::from("/tickets/execution-root");
        let team = DiscussSessionSetup::Team {
            leader_id: "lead".to_string(),
        };
        let provider = DiscussSessionSetup::Provider {
            config_overrides: Default::default(),
        };
        assert_eq!(
            Actor::launch_workspace(Some(&team), Some(&root), &homebase).expect("team"),
            &homebase
        );
        assert_eq!(
            Actor::launch_workspace(Some(&provider), Some(&root), &homebase).expect("provider"),
            &homebase
        );
        assert_eq!(
            Actor::launch_workspace(None, Some(&root), &homebase).expect("no setup"),
            &homebase
        );
    }

    #[test]
    fn reconnect_restores_root_team_launch_workspace_from_the_stored_root() {
        let homebase = std::path::PathBuf::from("/central/homebase");
        let root = std::path::PathBuf::from("/tickets/execution-root");
        let restored = Actor::restored_team_setup(&row(Some("lead"), Some("team_at_root")));
        assert_eq!(
            Actor::launch_workspace(restored.as_ref(), Some(&root), &homebase).expect("reconnect"),
            &root
        );
    }

    #[test]
    fn setup_kind_round_trips_through_reconnect() {
        let setup = DiscussSessionSetup::TeamAtRoot {
            leader_id: "chief-of-staff".to_string(),
        };
        let kind = Actor::setup_kind_for(Some(&setup));
        let restored = Actor::restored_team_setup(&row(Some("chief-of-staff"), kind));
        assert_eq!(restored, Some(setup));
    }
}

#[cfg(test)]
mod provider_title_tests {
    use super::initial_provider_title;

    #[test]
    fn user_rename_wins_over_the_callers_name() {
        let title = initial_provider_title(Some("My own name"), Some("yach | Tidy | perform".to_owned()));

        assert_eq!(title.as_deref(), Some("My own name"));
    }

    #[test]
    fn callers_name_is_pushed_when_the_session_was_never_renamed() {
        assert_eq!(
            initial_provider_title(None, Some("Sonya | yach".to_owned())).as_deref(),
            Some("Sonya | yach")
        );
        assert_eq!(
            initial_provider_title(Some("   "), Some("Sonya | yach".to_owned())).as_deref(),
            Some("Sonya | yach")
        );
    }

    #[test]
    fn unnamed_session_pushes_no_title() {
        assert_eq!(initial_provider_title(None, None), None);
    }
}

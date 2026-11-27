use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{AgentCapabilities, McpServer, Meta, SessionId};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent};
use serde_json::{from_str as json_from_str, to_string as json_to_string};
use tokio::spawn;
use tokio::sync::{Mutex, mpsc, oneshot};
use uuid::Uuid;

use aves::auth::{AuthSudoService, Jwt};
use aves::bus::BusMcpEndpoint;
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, SpawnSessionOptions};
use super::config::{config_options_from_json, config_settings_from_json, config_settings_from_value};
use super::database::persist_session_anchor_inner;
use super::error::DiscussError;
use super::identity::{Binding, persist_binding};
use super::launch_types::{LaunchLimitation, SessionLaunchConfig};
use super::library::{AgentId, ResolvedAgent, ResolvedTeam};
use super::mcp::composed_launch_config;
use super::messages::DiscussResponse;
use super::plugins::{self, ConnectionStrategy, DiscussAgentPlugin, LaunchContext, PrimaryAgentSelection};
use super::session::{
    PendingElicitationCancel, SESSION_WORKER_JWT_EXPIRATION, SessionAnchorMetadata, SessionWorkerFailure,
    WorkerSessionParams, issue_worker_jwt, revoke_worker_jti, run as run_session_worker, worker_subject,
};
use super::storage::{
    load_discuss_agent_config, load_discuss_chat_turns_window, load_discuss_session, load_discuss_team_installation,
    next_usage_ordinal, update_session_launch_config, update_session_launch_context, update_session_setup_kind,
};
use super::supervisor::{
    CallbackRouteRegistry, CallbackRouter, ProviderSupervisor, ProviderSupervisorResources, SharedMcpRegistrations,
};
use super::types::{
    ConfigSettings, DiscussConfigOption, DiscussMemberOverrides, DiscussSessionId, DiscussSessionSetup,
    DiscussStreamEvent, WorkerCommand, WorkerEntry,
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

struct ConfiguredSessionInputs<'a> {
    launch_inputs: Option<(&'a SessionLaunchConfig, &'a Path)>,
    materialization: Option<(&'a Path, &'a [&'a AgentId])>,
    explicit_config: bool,
    restores_team_at_root: bool,
}

fn configure_session_server(
    plugin: &dyn DiscussAgentPlugin,
    server: &McpServer,
    inputs: ConfiguredSessionInputs<'_>,
) -> Result<(McpServer, bool, Vec<LaunchLimitation>), DiscussError> {
    let Some((config, workspace)) = inputs.launch_inputs else {
        return Ok((server.clone(), false, Vec::new()));
    };
    if let Some((root, team)) = inputs.materialization
        && plugin.connection_strategy() == ConnectionStrategy::Shared
        && plugin.materializes_session_config()
    {
        let limitations = plugin.materialize_session_config(root, team, config)?;
        return Ok((server.clone(), false, limitations));
    }
    let configured = plugin.configured_launch_server(server, workspace, config)?;
    let isolated =
        inputs.explicit_config || plugin.uses_mcp_launch_env() || configured != *server || inputs.restores_team_at_root;
    Ok((configured, isolated, Vec::new()))
}

/// Append a launch-argument Agent selection to the stdio server. Such a selection names the Agent
/// per process, so the session needs its own; returns whether arguments were applied.
fn apply_launch_arguments(
    plugin: &dyn DiscussAgentPlugin,
    primary_agent: Option<&ResolvedAgent>,
    launch_server: &mut McpServer,
) -> Result<bool, DiscussError> {
    let Some(agent) = primary_agent else {
        return Ok(false);
    };
    let PrimaryAgentSelection::LaunchArguments(arguments) = plugin.primary_agent_selection(&agent.selection_view())
    else {
        return Ok(false);
    };
    let McpServer::Stdio(server) = launch_server else {
        return Err(DiscussError::Validation {
            message: "Launch-argument Agent selection requires a stdio ACP server".to_string(),
        });
    };
    server.args.extend(arguments);
    Ok(true)
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
    shared: SharedWorkerResources,
}

/// Connection resources a session worker borrows from its Provider's shared supervisor.
struct SharedWorkerResources {
    callback_router: Arc<Mutex<CallbackRouter>>,
    callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    shared_connection: Option<ConnectionTo<Agent>>,
    agent_capabilities: Option<AgentCapabilities>,
    shared_mcp_capabilities: Option<(bool, bool)>,
    system_prompt_supported: bool,
    title_supported: bool,
    shared_mcp_registrations: Option<SharedMcpRegistrations>,
}

/// Launch inputs after the persisted anchor's stored values take precedence over the caller's.
struct ResolvedLaunchInputs {
    requires_mcp_isolation: bool,
    reconnect_acp_session_id: Option<SessionId>,
    provider_title: Option<String>,
    launch_config: Option<SessionLaunchConfig>,
    session_cwd: Option<PathBuf>,
    application_mcp_enabled: Option<bool>,
}

impl Actor {
    fn ensure_provider_accepts_session(&self, key: &str) -> Result<(), DiscussError> {
        if !self
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
            if let Some(entry) = self.remove_worker(&discuss_session_id).await {
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

    pub(super) async fn spawn_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        options: SpawnSessionOptions,
    ) -> Result<DiscussResponse, DiscussError> {
        if self
            .handle_existing_session(discuss_session_id, options.announce_desktop_association)
            .await?
        {
            return Ok(DiscussResponse::Success);
        }
        let Some(agent_spec) = self.agents.get(key) else {
            return Err(DiscussError::UnknownAgent {
                agent_key: key.to_string(),
            });
        };
        self.ensure_provider_accepts_session(key)?;
        let launch = self
            .resolve_launch_inputs(discuss_session_id, &options)
            .await?;
        let SpawnSessionOptions {
            name,
            setup,
            session_config_overrides,
            announce_desktop_association,
            node_token,
            flow_mcp_endpoints,
            ..
        } = options;
        let session_name = name.unwrap_or_else(|| "A new session".to_string());
        let reconnect = launch.reconnect_acp_session_id.is_some();
        let (
            restored_persisted_transcript,
            next_turn_position,
            persisted_config_options,
            mut session_settings,
            restored_setup,
        ) = self
            .load_session_restore_state(discuss_session_id, reconnect)
            .await?;
        // Restore the setup before materializing the session's launch configuration.
        let setup = if reconnect { restored_setup } else { setup };
        let composed_config = composed_launch_config(
            &self.persistence,
            self.mcp_domains,
            key,
            setup.as_ref(),
            discuss_session_id,
            launch.launch_config.as_ref(),
        )
        .await?;
        let prepared = self
            .prepare_session_launch(
                discuss_session_id,
                key,
                agent_spec.server(),
                setup,
                launch.session_cwd.as_ref(),
                (composed_config.as_ref(), launch.launch_config.is_some()),
            )
            .await?;
        let isolate_mcp = launch.requires_mcp_isolation && plugins::for_agent(key).needs_process_for_mcp_isolation();
        let shared = self.shared_worker_resources(key, prepared.isolated_configured || isolate_mcp);
        let configured_defaults = config_settings_from_value(agent_spec.default_session_config());
        let (next_usage_ordinal, agent_defaults) = self
            .session_agent_defaults(discuss_session_id, key, configured_defaults)
            .await?;
        self.persist_session_launch(
            discuss_session_id,
            key,
            &session_name,
            node_token.is_some(),
            &prepared,
            &launch,
        )
        .await?;
        if !reconnect {
            session_settings.extend(prepared.setup_settings);
        }
        session_settings.extend(session_config_overrides);
        let application_mcp_enabled = launch
            .application_mcp_enabled
            .unwrap_or_else(|| agent_spec.application_mcp_enabled());
        self.start_session_worker(SessionWorkerInputs {
            discuss_session_id,
            key,
            assigned_agent_name: prepared.assigned_agent_name,
            agent: prepared.agent,
            launch_config: prepared.effective_config,
            launch_meta: prepared.launch_meta,
            launch_limitations: prepared.launch_limitations,
            launch_server: prepared.launch_server,
            session_name,
            provider_title: launch.provider_title,
            primary_agent: prepared.primary_agent,
            reconnect_acp_session_id: launch.reconnect_acp_session_id,
            persisted_config_options,
            session_settings,
            restored_persisted_transcript,
            next_turn_position,
            next_usage_ordinal,
            session_cwd: launch.session_cwd,
            application_mcp_enabled,
            flow_mcp_endpoints,
            node_token,
            agent_defaults,
            announce_desktop_association,
            shared,
        })
        .await
    }

    /// Resolve the launch inputs, letting the persisted anchor's stored values win over the caller's.
    async fn resolve_launch_inputs(
        &self,
        discuss_session_id: DiscussSessionId,
        options: &SpawnSessionOptions,
    ) -> Result<ResolvedLaunchInputs, DiscussError> {
        // NewSession is keyed only by the caller-owned id. Resolve the provider recovery id from
        // the persisted anchor rather than accepting one from Zeus or Hades.
        let acp_session_id = self
            .resolve_persisted_acp_session_id(discuss_session_id, options.acp_session_id.clone())
            .await?;
        let conn = self.persistence.acquire_connection().await?;
        let stored = load_discuss_session(&conn, &discuss_session_id.to_string()).await?;
        drop(conn);
        let stored_cwd = stored
            .as_ref()
            .and_then(|row| row.session_cwd_json.as_deref())
            .map(json_from_str::<PathBuf>)
            .transpose()?;
        let stored_config = stored
            .as_ref()
            .and_then(|row| row.session_launch_config_json.as_deref())
            .map(json_from_str::<SessionLaunchConfig>)
            .transpose()?;
        Ok(ResolvedLaunchInputs {
            requires_mcp_isolation: options.requires_mcp_isolation || stored.as_ref().is_some_and(|row| row.requires_mcp_isolation),
            reconnect_acp_session_id: acp_session_id.map(SessionId::from),
            provider_title: initial_provider_title(
                stored.as_ref().and_then(|row| row.display_title.as_deref()),
                options.name.clone(),
            ),
            launch_config: stored_config.or_else(|| options.launch_config.clone()),
            session_cwd: stored_cwd.or_else(|| options.session_cwd.clone()),
            application_mcp_enabled: stored
                .as_ref()
                .and_then(|row| row.session_mcp_enabled)
                .map(|enabled| enabled != 0)
                .or(options.application_mcp_enabled),
        })
    }

    /// Borrow the Provider's shared connection unless the launch needs its own process.
    fn shared_worker_resources(&self, key: &str, isolated_configured: bool) -> SharedWorkerResources {
        self.provider_supervisors
            .get(key)
            .filter(|_| !isolated_configured)
            .and_then(|supervisor| match &supervisor.resources {
                ProviderSupervisorResources::Shared {
                    connection: Some(connection),
                    capabilities,
                    callback_router,
                    callback_routes,
                    mcp_registrations,
                    ..
                } => Some(SharedWorkerResources {
                    callback_router: callback_router.clone(),
                    callback_routes: callback_routes.clone(),
                    shared_connection: Some(connection.clone()),
                    agent_capabilities: Some(AgentCapabilities::clone(capabilities)),
                    shared_mcp_capabilities: Some((
                        capabilities.mcp_capabilities.acp,
                        capabilities.mcp_capabilities.http,
                    )),
                    system_prompt_supported: capabilities.session_capabilities.system_prompt.is_some(),
                    title_supported: capabilities.session_capabilities.title.is_some(),
                    shared_mcp_registrations: Some(mcp_registrations.clone()),
                }),
                ProviderSupervisorResources::Shared { .. } | ProviderSupervisorResources::Isolated { .. } => None,
            })
            .unwrap_or_else(|| SharedWorkerResources {
                callback_router: Arc::new(Mutex::new(CallbackRouter::default())),
                callback_routes: None,
                shared_connection: None,
                agent_capabilities: None,
                shared_mcp_capabilities: None,
                system_prompt_supported: false,
                title_supported: false,
                shared_mcp_registrations: None,
            })
    }

    /// The session's next usage ordinal and its Provider's default settings, stored defaults first.
    async fn session_agent_defaults(
        &self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        configured_defaults: ConfigSettings,
    ) -> Result<(i64, ConfigSettings), DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let next_usage_ordinal = next_usage_ordinal(&conn, &discuss_session_id.to_string()).await?;
        let agent_defaults = load_discuss_agent_config(&conn, key)
            .await?
            .map(|row| config_settings_from_json(&row.default_config_json))
            .transpose()?
            .unwrap_or(configured_defaults);
        Ok((next_usage_ordinal, agent_defaults))
    }

    /// Persist the anchor and launch context a later reconnect restores the session from.
    async fn persist_session_launch(
        &self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        session_name: &str,
        is_task_session: bool,
        prepared: &SessionLaunchPreparation,
        launch: &ResolvedLaunchInputs,
    ) -> Result<(), DiscussError> {
        let session_id = discuss_session_id.to_string();
        persist_session_anchor_inner(
            &self.persistence,
            &session_id,
            SessionAnchorMetadata {
                agent_name: Some(key),
                session_name: Some(session_name),
                team_leader_id: prepared.assigned_agent_name.as_deref(),
                is_task_session,
                ..Default::default()
            },
        )
        .await?;
        let conn = self.persistence.acquire_connection().await?;
        update_session_launch_context(
            &conn,
            &session_id,
            launch
                .session_cwd
                .as_ref()
                .map(json_to_string)
                .transpose()?,
            launch.application_mcp_enabled,
            Some(launch.requires_mcp_isolation),
        )
        .await?;
        update_session_launch_config(&conn, &session_id, launch.launch_config.as_ref()).await?;
        update_session_setup_kind(&conn, &session_id, prepared.persisted_setup_kind).await?;
        Ok(())
    }

    async fn start_session_worker(
        &mut self,
        mut inputs: SessionWorkerInputs<'_>,
    ) -> Result<DiscussResponse, DiscussError> {
        let discuss_session_id = inputs.discuss_session_id;
        let key = inputs.key;
        let announce_desktop_association = inputs.announce_desktop_association;
        if inputs.node_token.is_some() {
            inputs.shared.shared_mcp_registrations = None;
        }
        let (worker_jwt, worker_jti) = self
            .session_worker_auth(
                discuss_session_id,
                key,
                inputs.assigned_agent_name.as_deref(),
                inputs.node_token.take(),
            )
            .await?;

        // Create the worker command channel keyed by `discuss_session_id`.
        let (worker_command_tx, worker_command_rx) = mpsc::channel::<WorkerCommand>(DEFAULT_BUFFER_SIZE);

        tracing::debug!(
            target: "application_acp",
            discuss_session_id = %discuss_session_id,
            session_name = %inputs.session_name,
            agent_name = %key,
            reconnect = inputs.reconnect_acp_session_id.is_some(),
            restored_persisted_transcript = inputs.restored_persisted_transcript,
            next_turn_position = inputs.next_turn_position,
            next_usage_ordinal = inputs.next_usage_ordinal,
            "spawning session worker"
        );

        let (agent, worker_params) = self.worker_session_params(inputs, worker_jwt, worker_command_rx);
        let worker_name_for_reporting = worker_params.agent_name.clone();
        let workspace_for_reporting = worker_params.agents_workspace.clone();
        let join_handle = spawn(run_session_worker_with_reporting(
            agent,
            worker_params,
            discuss_session_id,
            worker_name_for_reporting,
            workspace_for_reporting,
            self.auth_service.clone(),
            worker_jti,
            self.pending_elicitation_cancel_tx.clone(),
            self.stream_tx.clone(),
        ));

        self.worker_registry.insert(
            discuss_session_id,
            WorkerEntry {
                agent_name: key.to_string(),
                worker_jti,
                command_tx: worker_command_tx,
                join_handle,
                active_turn: false,
                acp_session_id: None,
                desktop_association_requested: announce_desktop_association,
            },
        );
        if let Some(supervisor) = self.provider_supervisors.get_mut(key) {
            supervisor.associate_chat(discuss_session_id);
            if let ProviderSupervisorResources::Isolated { child_sessions, .. } = &mut supervisor.resources {
                child_sessions.insert(discuss_session_id);
            }
        }

        Ok(DiscussResponse::Success)
    }

    /// A task session runs on its node's token; any other session gets a fresh worker token
    /// bound to its identity, revoked again if the binding cannot be persisted.
    async fn session_worker_auth(
        &self,
        discuss_session_id: DiscussSessionId,
        key: &str,
        assigned_agent_name: Option<&str>,
        node_token: Option<Jwt>,
    ) -> Result<(Jwt, Option<Uuid>), DiscussError> {
        if let Some(node_token) = node_token {
            return Ok((node_token, None));
        }
        let worker_auth = issue_worker_jwt(
            self.auth_service.as_ref(),
            discuss_session_id,
            SESSION_WORKER_JWT_EXPIRATION,
        )
        .await?;
        let subject = worker_subject(discuss_session_id);
        if let Err(error) = persist_binding(
            &self.persistence,
            Binding {
                token: &worker_auth.token,
                subject: &subject,
                session_id: discuss_session_id,
                provider: assigned_agent_name.map(|_| key),
                agent: assigned_agent_name,
                node_id: None,
            },
        )
        .await
        {
            revoke_worker_jti(self.auth_service.as_ref(), Some(worker_auth.jti), "identity binding").await;
            return Err(error);
        }
        Ok((worker_auth.token, Some(worker_auth.jti)))
    }

    /// Combine the launch inputs with the actor's shared channels into the worker's parameters.
    fn worker_session_params(
        &self,
        inputs: SessionWorkerInputs<'_>,
        worker_jwt: Jwt,
        worker_command_rx: mpsc::Receiver<WorkerCommand>,
    ) -> (AcpAgent, WorkerSessionParams) {
        let SharedWorkerResources {
            callback_router,
            callback_routes,
            shared_connection,
            agent_capabilities,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        } = inputs.shared;
        let params = WorkerSessionParams {
            launch_config: inputs.launch_config,
            launch_meta: inputs.launch_meta,
            launch_limitations: inputs.launch_limitations,
            stream_tx: self.stream_tx.clone(),
            discuss_session_id: inputs.discuss_session_id,
            auth_launch_server: inputs.launch_server,
            session_name: inputs.session_name,
            provider_title: inputs.provider_title,
            agent_name: inputs.key.to_string(),
            primary_agent: inputs.primary_agent,
            reconnect_acp_session_id: inputs.reconnect_acp_session_id,
            persisted_config_options: inputs.persisted_config_options,
            session_settings: inputs.session_settings,
            restored_persisted_transcript: inputs.restored_persisted_transcript,
            next_turn_position: inputs.next_turn_position,
            next_usage_ordinal: inputs.next_usage_ordinal,
            price_index: self.price_index.clone(),
            agents_workspace: inputs
                .session_cwd
                .unwrap_or_else(|| self.agents_workspace.clone()),
            application_mcp_enabled: inputs.application_mcp_enabled,
            bus_service: self.bus_service.clone(),
            flow_mcp_endpoints: inputs.flow_mcp_endpoints,
            worker_jwt,
            root_context: self.root_context.clone(),
            mcp_domains: self.mcp_domains,
            broadcast_tx: self.broadcast_tx.clone(),
            persistence: self.persistence.clone(),
            worker_command_rx,
            capabilities_update_tx: self.capabilities_update_tx.clone(),
            activity_update_tx: self.activity_update_tx.clone(),
            worker_ready_tx: self.worker_ready_tx.clone(),
            agent_defaults: inputs.agent_defaults,
            pending_permission_tx: self.pending_permission_tx.clone(),
            pending_permission_cancel_tx: self.pending_permission_cancel_tx.clone(),
            pending_elicitation_tx: self.pending_elicitation_tx.clone(),
            pending_elicitation_cancel_tx: self.pending_elicitation_cancel_tx.clone(),
            announce_desktop_association: inputs.announce_desktop_association,
            callback_router,
            callback_routes,
            shared_connection,
            agent_capabilities,
            shared_mcp_capabilities,
            system_prompt_supported,
            title_supported,
            shared_mcp_registrations,
        };
        (inputs.agent, params)
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
        let session_root = session_cwd.filter(|root| {
            if *root == &self.agents_workspace {
                return false;
            }
            match (root.canonicalize(), self.agents_workspace.canonicalize()) {
                (Ok(root), Ok(homebase)) => root != homebase,
                _ => true,
            }
        });
        let root_config = (plugin.connection_strategy() == ConnectionStrategy::Shared
            && plugin.materializes_session_config()
            && session_root.is_some())
        .then_some(launch_config.unwrap_or(&root_default_config));
        if let Some(DiscussSessionSetup::TeamAtRoot { leader_id }) = setup.as_ref() {
            self.materialize_custom_team_at_root(key, launch_workspace, leader_id, root_config)
                .await?;
        }
        let restores_team_at_root = matches!(setup, Some(DiscussSessionSetup::TeamAtRoot { .. }));
        let persisted_setup_kind = Self::setup_kind_for(setup.as_ref());
        let (assigned_agent_name, setup_settings) = self.resolve_session_setup(key, setup.clone()).await?;
        let primary_agent = self
            .resolve_primary_agent(assigned_agent_name.as_deref())
            .await?;
        let launch_inputs = match launch_config {
            Some(config) => Some((config, launch_workspace)),
            None if restores_team_at_root => Some((&root_default_config, launch_workspace)),
            None if root_config.is_some() => Some((&root_default_config, launch_workspace)),
            None => None,
        };
        let mut prepared = if let Some((config, _)) = launch_inputs {
            plugin.launch_preparation(&LaunchContext::new(&self.agents_workspace, discuss_session_id), config)?
        } else {
            Default::default()
        };
        let effective_config = launch_inputs.map(|(config, _)| config.clone());
        let launch_arguments = primary_agent.as_ref().is_some_and(|agent| {
            matches!(
                plugin.primary_agent_selection(&agent.selection_view()),
                PrimaryAgentSelection::LaunchArguments(_)
            )
        });
        let materialize = plugin.connection_strategy() == ConnectionStrategy::Shared
            && plugin.materializes_session_config()
            && session_root.is_some()
            && !launch_arguments;
        let members = match setup.as_ref() {
            Some(DiscussSessionSetup::TeamAtRoot { leader_id }) if materialize && launch_inputs.is_some() => {
                self.team_at_root_members(leader_id).await?
            }
            _ => Vec::new(),
        };
        let members: Vec<_> = members.iter().collect();
        let (configured, isolated_configured, limitations) = configure_session_server(
            plugin,
            &launch_server,
            ConfiguredSessionInputs {
                launch_inputs: launch_inputs.map(|(config, workspace)| (config, workspace.as_path())),
                materialization: session_root
                    .filter(|_| materialize)
                    .map(|root| (root.as_path(), members.as_slice())),
                explicit_config,
                restores_team_at_root,
            },
        )?;
        launch_server = configured;
        prepared.limitations.extend(limitations);
        let launch_selected = apply_launch_arguments(plugin, primary_agent.as_ref(), &mut launch_server)?;
        let agent = Self::acp_agent_from_server(&launch_server)?;
        Ok(SessionLaunchPreparation {
            launch_server,
            assigned_agent_name,
            setup_settings,
            persisted_setup_kind,
            primary_agent,
            isolated_configured: isolated_configured || launch_selected,
            agent,
            launch_meta: prepared.meta,
            launch_limitations: prepared.limitations,
            effective_config,
        })
    }

    /// Resolve the Agent a session's Team leader names, if it has one.
    async fn resolve_primary_agent(&self, leader_id: Option<&str>) -> Result<Option<ResolvedAgent>, DiscussError> {
        let Some(leader_id) = leader_id else {
            return Ok(None);
        };
        let agent_id =
            AgentId::try_from(leader_id.to_string()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                asset_id: leader_id.to_string(),
                violation: error.to_string(),
            })?;
        Ok(Some(self.resolve_agent(&agent_id).await?))
    }

    /// The leader and followers a root-materialized Team writes into the session root.
    async fn team_at_root_members(&self, leader_id: &str) -> Result<Vec<AgentId>, DiscussError> {
        let leader =
            AgentId::try_from(leader_id.to_string()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                asset_id: leader_id.to_string(),
                violation: error.to_string(),
            })?;
        let mut members = Vec::new();
        match self.resolve_team(&leader).await? {
            ResolvedTeam::Builtin(team) => {
                members.push(team.leader.clone());
                members.extend(team.followers.iter().cloned());
            }
            ResolvedTeam::Custom(team) => {
                members.push(team.leader.clone());
                members.extend(team.followers.iter().cloned());
            }
        }
        Ok(members)
    }

    async fn load_session_restore_state(
        &self,
        discuss_session_id: DiscussSessionId,
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
        Ok(if reconnect {
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
                    json_from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)?;
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
    worker_jti: Option<Uuid>,
    pending_elicitation_cancel_tx: mpsc::Sender<PendingElicitationCancel>,
    stream_tx: mpsc::Sender<DiscussStreamEvent>,
) {
    tracing::info!(%discuss_session_id, provider = %agent_name, cwd = %agents_workspace.display(), "Discuss session worker started");
    let activity_update_tx = params.activity_update_tx.clone();
    let result = run_session_worker(agent, params).await;

    tracing::info!(%discuss_session_id, failed = result.is_err(), "Discuss session worker stopped");
    // A stopped worker finishes no turn; the actor reports the end only if one was in flight.
    let _ = activity_update_tx.send((discuss_session_id, false)).await;

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
        if !matches!(error, DiscussError::ProviderSessionNotFound) {
            tracing::warn!(?error, "Discuss session worker stopped");
        }
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

#[cfg(test)]
mod team_at_root_setup_tests {
    use super::*;
    use crate::discuss::storage::DiscussSessionRow;
    use agent_client_protocol::v1::schema::{EnvVariable, McpServerStdio};

    #[test]
    fn shared_materializing_provider_keeps_unconfigured_server_with_root() {
        let root = tempfile::tempdir().unwrap();
        let plugin = plugins::for_agent("opencode");
        let mut stdio = McpServerStdio::new("OpenCode", "opencode");
        stdio
            .env
            .push(EnvVariable::new("OPENCODE_CONFIG_CONTENT", "{}"));
        let server = McpServer::Stdio(stdio);
        let config = SessionLaunchConfig {
            mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("example", "example"))],
            ..Default::default()
        };
        let (configured, isolated, _) = configure_session_server(
            plugin,
            &server,
            ConfiguredSessionInputs {
                launch_inputs: Some((&config, root.path())),
                materialization: Some((root.path(), &[])),
                explicit_config: true,
                restores_team_at_root: false,
            },
        )
        .unwrap();
        assert!(!isolated);
        assert_eq!(configured, server);
    }

    #[test]
    fn shared_provider_with_config_and_no_root_is_isolated() {
        let root = tempfile::tempdir().unwrap();
        let plugin = plugins::for_agent("opencode");
        let mut stdio = McpServerStdio::new("OpenCode", "opencode");
        stdio
            .env
            .push(EnvVariable::new("OPENCODE_CONFIG_CONTENT", "{}"));
        let server = McpServer::Stdio(stdio);
        let config = SessionLaunchConfig {
            mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("example", "example"))],
            ..Default::default()
        };
        let (configured, isolated, _) = configure_session_server(
            plugin,
            &server,
            ConfiguredSessionInputs {
                launch_inputs: Some((&config, root.path())),
                materialization: None,
                explicit_config: true,
                restores_team_at_root: false,
            },
        )
        .unwrap();
        assert!(isolated);
        assert_ne!(configured, server);
    }

    fn row(team_leader_id: Option<&str>, setup_kind: Option<&str>) -> DiscussSessionRow {
        DiscussSessionRow {
            is_task_session: false,
            session_cwd_json: None,
            session_mcp_enabled: None,
            requires_mcp_isolation: false,
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
        let homebase = PathBuf::from("/central/homebase");
        let root = PathBuf::from("/tickets/execution-root");
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
        let homebase = PathBuf::from("/central/homebase");
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
        let homebase = PathBuf::from("/central/homebase");
        let root = PathBuf::from("/tickets/execution-root");
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
        let homebase = PathBuf::from("/central/homebase");
        let root = PathBuf::from("/tickets/execution-root");
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

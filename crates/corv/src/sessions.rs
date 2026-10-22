use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{McpServer, PermissionOptionId, SessionId};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, Client, ConnectionTo};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::types::TaskSessionStart;
use aves::auth::AuthSudoService;
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{
    Actor, CANCEL_TURN_DEADLINE, SpawnSessionOptions, TransientWorkerEvent, channel_send_error, validate_user_turn,
};
use super::config::{
    config_options_from_json, config_settings_from_json, config_settings_from_value, provider_config_options_json,
};
use super::database::*;
use super::elicitation::validate_elicitation_content;
use super::messages::DiscussResponse;
use super::plugins;
use super::session::{
    PendingElicitationCancel, SESSION_WORKER_JWT_EXPIRATION, SessionWorkerFailure, WorkerSessionParams,
    ensure_agents_workspace_dir, initialize_request, issue_worker_jwt, load_session_request, revoke_worker_jti,
    run as run_session_worker,
};
use super::storage::{
    DiscussSessionRow, ElicitationPersistenceInput, LoadedSessionRefresh, delete_discuss_session,
    list_recent_discuss_sessions, load_discuss_agent_config, load_discuss_chat_turns_window, load_discuss_session,
    load_discuss_subagents, load_discuss_team_installation, merge_refresh_discuss_session, present_acp_session_id,
    provider_updated_at_ms, rename_discuss_session, upsert_discuss_agent_config,
};
use super::supervisor::{CallbackRouter, ProviderSupervisor, ProviderSupervisorResources};
use super::types::{
    AcpProvider, ConfigSettings, DiscussChatTurnRecord, DiscussConfigOption, DiscussElicitationAnswer, DiscussError,
    DiscussMemberOverrides, DiscussPermissionOption, DiscussSessionInfo, DiscussSessionSetup, DiscussStreamEvent,
    ElicitationRequestId, InspectorBinding, PendingElicitation, PermissionRequestId, SluiceSessionId,
    TaskSessionStarted, WorkerCommand, WorkerEntry,
};

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
        sluice_session_id: SluiceSessionId,
        announce_desktop_association: bool,
    ) -> Result<bool, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        if self
            .worker_registry
            .get(&sluice_session_id)
            .is_some_and(|entry| entry.command_tx.is_closed())
        {
            if let Some(entry) = self.worker_registry.remove(&sluice_session_id) {
                revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
                if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                    supervisor.disassociate_chat(&sluice_session_id);
                }
            }
            self.worker_capabilities.remove(&sluice_session_id);
        }
        if let Some(entry) = self.worker_registry.get_mut(&sluice_session_id) {
            let ready_for_desktop_association = announce_desktop_association && entry.acp_session_id.is_some();
            if announce_desktop_association {
                entry.desktop_association_requested = !ready_for_desktop_association;
            }
            if ready_for_desktop_association {
                self.announce_desktop_association(sluice_session_id).await?;
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
            provider_name,
            agent_name,
            config_overrides,
            sluice_mcp_enabled,
            prompt,
            session_cwd,
            limits,
            node_token,
            flow_mcp_endpoints,
        } = options;
        validate_user_turn(&prompt)?;
        let session_id = SluiceSessionId::new();
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
                name: Some(agent_name.clone()),
                setup,
                session_config_overrides: config_overrides,
                acp_session_id: None,
                announce_desktop_association: false,
                transient_connectivity: false,
                session_cwd: Some(session_cwd),
                sluice_mcp_enabled: Some(sluice_mcp_enabled),
                node_token: Some(node_token),
                flow_mcp_endpoints,
            },
        )
        .await?;
        let entry = self
            .worker_registry
            .get(&session_id)
            .ok_or(DiscussError::SessionNotFound {
                sluice_session_id: session_id,
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
        sluice_session_id: SluiceSessionId,
        key: &str,
        options: SpawnSessionOptions,
    ) -> Result<DiscussResponse, DiscussError> {
        let SpawnSessionOptions {
            name,
            setup,
            session_config_overrides,
            acp_session_id,
            announce_desktop_association,
            transient_connectivity,
            session_cwd,
            sluice_mcp_enabled: requested_sluice_mcp_enabled,
            node_token,
            flow_mcp_endpoints,
        } = options;
        if self
            .handle_existing_session(sluice_session_id, announce_desktop_association)
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
        let session_name = name.unwrap_or_else(|| "A new session".to_string());
        // NewSession is keyed only by the caller-owned Sluice id. Resolve the
        // provider recovery id from the persisted anchor rather than accepting
        // one from Zeus or Hades.
        let acp_session_id = if transient_connectivity {
            None
        } else {
            self.resolve_persisted_acp_session_id(sluice_session_id, acp_session_id)
                .await?
        };
        let reconnect_acp_session_id = acp_session_id.map(SessionId::from);
        let (
            restored_persisted_transcript,
            next_turn_position,
            persisted_config_options,
            mut session_settings,
            team_leader_id,
        ) = if transient_connectivity {
            (false, 0, Vec::new(), ConfigSettings::new(), None)
        } else if reconnect_acp_session_id.is_some() {
            let conn = self.persistence.acquire_connection().await?;
            let turns = load_discuss_chat_turns_window(&conn, &sluice_session_id.to_string()).await?;
            let row = load_discuss_session(&conn, &sluice_session_id.to_string()).await?;
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
                row.and_then(|row| row.team_leader_id),
            )
        } else {
            (false, 0, Vec::new(), ConfigSettings::new(), None)
        };
        let launch_server = plugins::for_agent(key).launch_server(agent_spec.server());
        let agent = Self::acp_agent_from_server(&launch_server)?;
        let (callback_router, callback_routes, shared_connection, shared_mcp_capabilities) = self
            .provider_supervisors
            .get(key)
            .and_then(|supervisor| match &supervisor.resources {
                ProviderSupervisorResources::Shared {
                    connection: Some(connection),
                    callback_router,
                    callback_routes,
                    mcp_acp,
                    mcp_http,
                    ..
                } => Some((
                    callback_router.clone(),
                    callback_routes.clone(),
                    Some(connection.clone()),
                    Some((*mcp_acp, *mcp_http)),
                )),
                ProviderSupervisorResources::Shared { .. } | ProviderSupervisorResources::Isolated { .. } => None,
            })
            .unwrap_or_else(|| (Arc::new(Mutex::new(CallbackRouter::default())), None, None, None));
        let configured_defaults = config_settings_from_value(agent_spec.default_session_config());
        let conn = self.persistence.acquire_connection().await?;
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
        let setup = if reconnect_acp_session_id.is_some() {
            team_leader_id.map(|leader_id| DiscussSessionSetup::Team { leader_id })
        } else {
            setup
        };
        let (assigned_agent_name, setup_settings) = self.resolve_session_setup(key, setup).await?;
        if reconnect_acp_session_id.is_none() {
            session_settings.extend(setup_settings);
        }
        session_settings.extend(session_config_overrides);
        if !transient_connectivity {
            persist_session_anchor_inner(
                &self.persistence,
                &sluice_session_id.to_string(),
                super::session::SessionAnchorMetadata {
                    agent_name: Some(key),
                    session_name: Some(&session_name),
                    team_leader_id: assigned_agent_name.as_deref(),
                    is_task_session: node_token.is_some(),
                    ..Default::default()
                },
            )
            .await?;
        }
        let sluice_mcp_enabled = requested_sluice_mcp_enabled.unwrap_or_else(|| agent_spec.sluice_mcp_enabled());
        let stream_tx = self.stream_tx.clone();
        let broadcast_tx = self.broadcast_tx.clone();
        let persistence = self.persistence.clone();
        let capabilities_update_tx = self.capabilities_update_tx.clone();
        let worker_ready_tx = self.worker_ready_tx.clone();
        let pending_permission_tx = self.pending_permission_tx.clone();
        let pending_permission_cancel_tx = self.pending_permission_cancel_tx.clone();
        let pending_elicitation_tx = self.pending_elicitation_tx.clone();
        let pending_elicitation_cancel_tx = self.pending_elicitation_cancel_tx.clone();
        let agents_workspace = session_cwd.unwrap_or_else(|| self.agents_workspace.clone());
        let bus_service = self.bus_service.clone();
        let auth_service = self.auth_service.clone();
        let (worker_jwt, worker_jti) = if let Some(node_token) = node_token {
            (node_token, None)
        } else {
            let worker_auth = issue_worker_jwt(self.auth_service.as_ref(), SESSION_WORKER_JWT_EXPIRATION).await?;
            (worker_auth.token, Some(worker_auth.jti))
        };
        let root_context = self.root_context.clone();
        let mcp_domains = self.mcp_domains;
        let agent_name = key.to_string();
        let worker_agent_name = agent_name.clone();
        let worker_name_for_reporting = agent_name.clone();
        let workspace_for_reporting = agents_workspace.clone();

        // Create the worker command channel keyed by `sluice_session_id`.
        let (worker_command_tx, worker_command_rx) = mpsc::channel::<WorkerCommand>(DEFAULT_BUFFER_SIZE);

        tracing::debug!(
            target: "application_acp",
            sluice_session_id = %sluice_session_id,
            session_name = %session_name,
            agent_name = %agent_name,
            reconnect = reconnect_acp_session_id.is_some(),
            restored_persisted_transcript,
            next_turn_position,
            "spawning session worker"
        );

        let transient_worker_tx_for_failure = transient_connectivity.then(|| self.transient_worker_tx.clone());
        let stream_tx_for_failure = self.stream_tx.clone();
        let pending_elicitation_cancel_tx_for_failure = self.pending_elicitation_cancel_tx.clone();
        let worker_params = WorkerSessionParams {
            stream_tx,
            sluice_session_id,
            auth_launch_server: launch_server,
            session_name,
            agent_name,
            assigned_agent_name,
            reconnect_acp_session_id,
            persisted_config_options,
            session_settings,
            restored_persisted_transcript,
            next_turn_position,
            agents_workspace,
            sluice_mcp_enabled,
            bus_service,
            flow_mcp_endpoints,
            worker_jwt,
            root_context,
            mcp_domains,
            broadcast_tx,
            persistence,
            worker_command_rx,
            capabilities_update_tx,
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
        };
        let join_handle = tokio::spawn(run_session_worker_with_reporting(
            agent,
            worker_params,
            sluice_session_id,
            worker_name_for_reporting,
            workspace_for_reporting,
            auth_service,
            worker_jti,
            transient_worker_tx_for_failure,
            pending_elicitation_cancel_tx_for_failure,
            stream_tx_for_failure,
        ));

        self.worker_registry.insert(
            sluice_session_id,
            WorkerEntry {
                agent_name: worker_agent_name,
                worker_jti,
                command_tx: worker_command_tx,
                join_handle,
                acp_session_id: None,
                desktop_association_requested: announce_desktop_association,
            },
        );
        if !transient_connectivity && let Some(supervisor) = self.provider_supervisors.get_mut(key) {
            supervisor.associate_chat(sluice_session_id);
            if let ProviderSupervisorResources::Isolated { child_sessions, .. } = &mut supervisor.resources {
                child_sessions.insert(sluice_session_id);
            }
        }

        Ok(DiscussResponse::Success)
    }

    pub(super) async fn resolve_session_setup(
        &self,
        provider_name: &str,
        setup: Option<DiscussSessionSetup>,
    ) -> Result<(Option<String>, ConfigSettings), DiscussError> {
        match setup {
            None => Ok((None, ConfigSettings::new())),
            Some(DiscussSessionSetup::Provider { config_overrides }) => Ok((None, config_overrides)),
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
        sluice_session_id: SluiceSessionId,
        requested_acp_session_id: Option<String>,
    ) -> Result<Option<String>, DiscussError> {
        if let Some(acp_session_id) = present_acp_session_id(requested_acp_session_id.as_deref()) {
            return Ok(Some(acp_session_id));
        }

        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_session(&conn, &sluice_session_id.to_string()).await?;
        Ok(row.and_then(|session| present_acp_session_id(session.acp_session_id.as_deref())))
    }

    pub(super) async fn announce_desktop_association(
        &self,
        sluice_session_id: SluiceSessionId,
    ) -> Result<(), DiscussError> {
        self.broadcast_tx
            .send(DiscussResponse::NewSession {
                session_id: sluice_session_id,
            })
            .await
            .map_err(|error| channel_send_error("announce_desktop_association.broadcast", error.to_string()))?;
        Ok(())
    }

    // ── Additive request handlers ──────────────────────────────────────────

    /// List local Sluice ACP chat sessions ordered by most recent interaction.
    ///
    /// Reads local `acp_agent_sessions` rows only as the session identity
    /// source. For every local row that has a non-empty `acp_session_id`,
    /// checks the matching worker's server-side capability state and, when the
    /// `list` bit is set, forwards a `WorkerCommand::ListProviderSessions` to
    /// the live worker so provider `SessionInfo` can refresh freshness metadata
    /// (`last_interaction_at`) on **already-local** rows matched by
    /// `acp_session_id`. Provider titles remain metadata-only; local Sluice
    /// display labels are user-controlled.
    ///
    /// Provider-listed sessions that do not already have a local
    /// `discuss_sessions` row are **not** materialized. A provider list row
    /// is a candidate only; local identity is created solely by explicit
    /// Sluice create / load / import / persist paths.
    pub(super) async fn handle_list_sessions(&mut self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let local_rows = list_recent_discuss_sessions(&conn).await?;

        let mut sessions: Vec<DiscussSessionInfo> = local_rows
            .iter()
            .filter_map(|row| {
                Some(DiscussSessionInfo {
                    is_task_session: row.is_task_session,
                    team_leader_id: row.team_leader_id.clone(),
                    sluice_session_id: SluiceSessionId::from_str(&row.sluice_session_id).ok()?,
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
            let Ok(sluice_session_id) = SluiceSessionId::from_str(&row.sluice_session_id) else {
                continue;
            };
            if let Some(entry) = self.worker_registry.get(&sluice_session_id)
                && let Some(caps) = self.worker_capabilities.get(&sluice_session_id)
                && caps.list
            {
                let command_tx = entry.command_tx.clone();
                let (reply, reply_rx) = oneshot::channel();
                if command_tx
                    .send(WorkerCommand::ListProviderSessions { reply })
                    .await
                    .is_err()
                {
                    self.worker_registry.remove(&sluice_session_id);
                    self.worker_capabilities.remove(&sluice_session_id);
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
                                &row.sluice_session_id,
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

    /// Remove a local Sluice Discuss chat session anchor and clean live worker state.
    ///
    /// Deletes the `discuss_sessions` row for `sluice_session_id`.
    /// Related `discuss_chat_turns` cascade via FK. Dropping the worker registry
    /// sender closes the command channel so the session worker exits; the
    /// capability cache entry is removed so subsequent config/list calls fail
    /// closed for the removed identity.
    ///
    /// Idempotent: a missing DB row still cleans live worker state and returns
    /// `Success`.
    pub(super) async fn handle_remove_session(
        &mut self,
        sluice_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        delete_discuss_session(&conn, &sluice_session_id.to_string()).await?;

        if let Some(entry) = self.worker_registry.remove(&sluice_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
            if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                supervisor.disassociate_chat(&sluice_session_id);
            }
        }
        if let Some(pending) = self.pending_provider_tests.remove(&sluice_session_id) {
            let reply = self.restore_provider_test_snapshot(pending, false).await;
            let _ = reply.send(Err(DiscussError::SessionCancelled { sluice_session_id }));
        }
        self.worker_capabilities.remove(&sluice_session_id);
        self.cancel_pending_permissions_for_session(&sluice_session_id);
        self.cancel_pending_elicitations_for_session(&sluice_session_id);

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
        sluice_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let key = sluice_session_id;
        tracing::debug!(
            target: "application_acp",
            sluice_session_id = %sluice_session_id,
            has_live_worker = self.worker_registry.contains_key(&key),
            "loading session (metadata + bounded transcript window)"
        );
        let conn = self.persistence.acquire_connection().await?;
        let mut row = load_discuss_session(&conn, &sluice_session_id.to_string())
            .await?
            .ok_or(DiscussError::SessionNotFound { sluice_session_id })?;

        let turns = load_discuss_chat_turns_window(&conn, &sluice_session_id.to_string()).await?;
        let subagents = load_discuss_subagents(&conn, &sluice_session_id.to_string()).await?;
        let turn_records: Vec<DiscussChatTurnRecord> = turns
            .iter()
            .filter_map(|t| {
                Some(DiscussChatTurnRecord {
                    sluice_session_id: SluiceSessionId::from_str(&t.sluice_session_id).ok()?,
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
                            sluice_session_id,
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
                return Err(DiscussError::SessionNotFound { sluice_session_id });
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
                        sluice_session_id,
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
            is_task_session: row.is_task_session,
            team_leader_id: row.team_leader_id,
            sluice_session_id: SluiceSessionId::from_str(&row.sluice_session_id).map_err(|_| {
                DiscussError::Serialization {
                    message: "invalid sluice_session_id in Discuss session row".to_string(),
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
        sluice_session_id: SluiceSessionId,
        refresh: LoadedSessionRefresh,
    ) -> Result<DiscussSessionRow, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let sluice_session_id_string = sluice_session_id.to_string();
        merge_refresh_discuss_session(&conn, &sluice_session_id_string, &refresh).await?;
        let refreshed = load_discuss_session(&conn, &sluice_session_id_string)
            .await?
            .ok_or(DiscussError::SessionNotFound { sluice_session_id })?;
        Ok(refreshed)
    }

    /// Get the database-backed provider menu for a session.
    ///
    /// Workers eagerly replace the persisted snapshot whenever the provider
    /// publishes a complete menu. Reads never synchronously query a provider.
    pub(super) async fn handle_get_config_options(
        &mut self,
        sluice_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_session(&conn, &sluice_session_id.to_string()).await?;
        let row = row.ok_or(DiscussError::SessionNotFound { sluice_session_id })?;
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
        sluice_session_id: SluiceSessionId,
        config_id: String,
        value: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let key = sluice_session_id;

        let command_tx = self
            .worker_registry
            .get(&key)
            .map(|e| e.command_tx.clone())
            .ok_or(DiscussError::SessionNotFound { sluice_session_id })?;

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

    /// Rename the local Sluice display label for a chat anchor.
    ///
    /// The stable `sluice_session_id` key and any provider `acp_session_id`
    /// are preserved; this updates only the user-controlled local label stored
    /// on the anchor row.
    pub(super) async fn handle_rename_session(
        &mut self,
        sluice_session_id: SluiceSessionId,
        display_title: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        let title = display_title.trim();
        if title.is_empty() {
            return Err(DiscussError::Validation {
                message: "session display title cannot be empty".to_string(),
            });
        }

        let conn = self.persistence.acquire_connection().await?;
        rename_discuss_session(&conn, sluice_session_id, title).await?;
        Ok(DiscussResponse::Success)
    }

    // ── New control request handlers ──────────────────────────────────────

    /// Send a user turn to a live session worker.
    pub(super) async fn handle_send_user_turn(
        &mut self,
        sluice_session_id: SluiceSessionId,
        text: String,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        validate_user_turn(&text)?;
        let entry = self
            .worker_registry
            .get(&sluice_session_id)
            .ok_or(DiscussError::SessionNotFound { sluice_session_id })?;
        if !self
            .provider_supervisors
            .get(&entry.agent_name)
            .is_some_and(ProviderSupervisor::accepts_work)
        {
            return Err(DiscussError::ProviderNotRunning {
                provider_name: entry.agent_name.clone(),
            });
        }
        let command_tx = entry.command_tx.clone();
        let (reply, reply_rx) = oneshot::channel();
        command_tx
            .send(WorkerCommand::SendUserTurn { text, reply })
            .await
            .map_err(|error| channel_send_error("handle_send_user_turn.worker_command", error.to_string()))?;
        let result = reply_rx.await.map_err(|_| DiscussError::OneshotReply)?;
        result?;
        Ok(DiscussResponse::Success)
    }

    /// Answer a pending permission request.
    ///
    /// Validates session ownership, liveness, uniqueness, and that the selected
    /// option was actually offered before atomically removing and resolving the
    /// pending entry. An invalid option does NOT consume the pending entry.
    pub(super) async fn handle_answer_permission(
        &mut self,
        sluice_session_id: SluiceSessionId,
        permission_request_id: PermissionRequestId,
        option: DiscussPermissionOption,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let key = (sluice_session_id, permission_request_id);

        // Validate the option against the offered options before removing
        // the pending entry. An invalid option must NOT consume the entry.
        if let DiscussPermissionOption::Select { ref option_id } = option {
            let pending = self
                .pending_permissions
                .get(&key)
                .ok_or(DiscussError::PermissionNotFound {
                    sluice_session_id,
                    permission_request_id: permission_request_id.to_string(),
                })?;
            let selected_option_id = PermissionOptionId::new(option_id.clone());
            if !pending
                .offered_option_ids
                .iter()
                .any(|id| id == &selected_option_id)
            {
                return Err(DiscussError::InvalidOption {
                    option_id: option_id.clone(),
                });
            }
        }

        // Atomically remove and resolve.
        let pending = self
            .pending_permissions
            .remove(&key)
            .ok_or(DiscussError::PermissionNotFound {
                sluice_session_id,
                permission_request_id: permission_request_id.to_string(),
            })?;

        let _ = pending.responder_tx.send(option);
        Ok(DiscussResponse::Success)
    }

    /// Answer a pending elicitation request.
    ///
    /// Validation happens before removal so malformed accepted content leaves
    /// the pending responder available for correction and resubmission.
    pub(super) async fn handle_answer_elicitation(
        &mut self,
        sluice_session_id: SluiceSessionId,
        elicitation_request_id: ElicitationRequestId,
        answer: DiscussElicitationAnswer,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let key = (sluice_session_id, elicitation_request_id);
        let pending = self
            .pending_elicitations
            .get(&key)
            .ok_or(DiscussError::ElicitationRequestNotPending {
                sluice_session_id,
                elicitation_request_id,
            })?;

        if let DiscussElicitationAnswer::Accept { content } = &answer {
            validate_elicitation_content(&pending.classification, content)?;
        }

        let pending = self
            .pending_elicitations
            .remove(&key)
            .ok_or(DiscussError::ElicitationAlreadyAnswered {
                sluice_session_id,
                elicitation_request_id,
            })?;
        self.persist_elicitation_turn(sluice_session_id, elicitation_request_id, &pending, &answer);
        let _ = pending.responder_tx.send(answer);
        Ok(DiscussResponse::Success)
    }

    /// Cancel one pending elicitation request.
    pub(super) async fn handle_cancel_elicitation(
        &mut self,
        sluice_session_id: SluiceSessionId,
        elicitation_request_id: ElicitationRequestId,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        self.cancel_pending_elicitation_entry(sluice_session_id, elicitation_request_id);
        Ok(DiscussResponse::Success)
    }

    /// Cancel the active turn for a session.
    pub(super) async fn handle_cancel_turn(
        &mut self,
        sluice_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        self.cancel_pending_permissions_for_session(&sluice_session_id);
        self.cancel_pending_elicitations_for_session(&sluice_session_id);

        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let Some(entry) = self.worker_registry.get(&sluice_session_id) else {
            return Ok(DiscussResponse::Success);
        };
        let command_tx = entry.command_tx.clone();
        let (reply, reply_rx) = oneshot::channel();
        if command_tx
            .send(WorkerCommand::CancelTurn { reply })
            .await
            .is_err()
        {
            self.recover_cancel_turn_worker(
                sluice_session_id,
                channel_send_error("handle_cancel_turn.worker_command", "worker command channel closed"),
            )
            .await;
            return Ok(DiscussResponse::Success);
        }
        match tokio::time::timeout(CANCEL_TURN_DEADLINE, reply_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => return Err(error),
            Ok(Err(_)) => {
                self.recover_cancel_turn_worker(sluice_session_id, DiscussError::OneshotReply)
                    .await;
            }
            Err(_) => {
                self.recover_cancel_turn_worker(
                    sluice_session_id,
                    DiscussError::WorkerStopTimeout { sluice_session_id },
                )
                .await;
            }
        }

        Ok(DiscussResponse::Success)
    }

    /// Cancel and close a session.
    pub(super) async fn handle_cancel_session(
        &mut self,
        sluice_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let cancel_result = if self.worker_registry.contains_key(&sluice_session_id) {
            self.handle_cancel_turn(sluice_session_id).await.map(|_| ())
        } else {
            Ok(())
        };
        if let Some(entry) = self.worker_registry.remove(&sluice_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "session cancellation").await;
        }
        self.worker_capabilities.remove(&sluice_session_id);

        self.cancel_pending_permissions_for_session(&sluice_session_id);
        self.cancel_pending_elicitations_for_session(&sluice_session_id);

        cancel_result?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_reserve_inspector(
        &mut self,
        parent_session_id: SluiceSessionId,
        child_session_id: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let parent = self
            .worker_registry
            .get(&parent_session_id)
            .ok_or(DiscussError::SessionNotFound {
                sluice_session_id: parent_session_id,
            })?;
        let provider_name = parent.agent_name.clone();
        if !self
            .provider_supervisors
            .get(&provider_name)
            .is_some_and(ProviderSupervisor::accepts_work)
        {
            return Err(DiscussError::ProviderNotRunning { provider_name });
        }
        let plugin = plugins::for_agent(&provider_name);
        if !plugin.supports_child_session_load() {
            return Err(DiscussError::InspectorUnavailable { provider_name });
        }
        let viewer_session_id = SluiceSessionId::new();
        let binding = InspectorBinding {
            viewer_session_id,
            parent_session_id,
            child_session_id: child_session_id.clone(),
        };
        self.inspector_bindings
            .insert(viewer_session_id, binding.clone());
        Ok(DiscussResponse::ReserveInspector { binding })
    }

    pub(super) async fn handle_start_inspector(
        &mut self,
        viewer_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let binding = self
            .inspector_bindings
            .get(&viewer_session_id)
            .cloned()
            .ok_or(DiscussError::SessionNotFound {
                sluice_session_id: viewer_session_id,
            })?;
        let parent = self
            .worker_registry
            .get(&binding.parent_session_id)
            .ok_or(DiscussError::SessionNotFound {
                sluice_session_id: binding.parent_session_id,
            })?;
        if !self
            .provider_supervisors
            .get(&parent.agent_name)
            .is_some_and(ProviderSupervisor::accepts_work)
        {
            return Err(DiscussError::ProviderNotRunning {
                provider_name: parent.agent_name.clone(),
            });
        }
        let (reply, reply_rx) = oneshot::channel();
        parent
            .command_tx
            .clone()
            .send(WorkerCommand::AttachInspector {
                viewer_session_id,
                child_session_id: SessionId::from(binding.child_session_id),
                reply,
            })
            .await
            .map_err(|error| channel_send_error("handle_start_inspector.worker_command", error.to_string()))?;
        reply_rx.await.map_err(|_| DiscussError::OneshotReply)??;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_dispose_inspector(
        &mut self,
        viewer_session_id: SluiceSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let Some(binding) = self.inspector_bindings.remove(&viewer_session_id) else {
            return Err(DiscussError::SessionNotFound {
                sluice_session_id: viewer_session_id,
            });
        };
        self.disposing_inspectors.insert(binding.viewer_session_id);
        self.cancel_pending_permissions_for_session(&viewer_session_id);
        self.cancel_pending_elicitations_for_session(&viewer_session_id);
        self.worker_capabilities.remove(&viewer_session_id);
        let detach_result = if let Some(parent) = self.worker_registry.get(&binding.parent_session_id) {
            let (reply, reply_rx) = oneshot::channel();
            let send_result = parent
                .command_tx
                .clone()
                .send(WorkerCommand::DisposeInspector {
                    viewer_session_id,
                    reply,
                })
                .await
                .map_err(|error| channel_send_error("handle_dispose_inspector.worker_command", error.to_string()));
            match send_result {
                Ok(()) => reply_rx
                    .await
                    .map_err(|_| DiscussError::OneshotReply)
                    .and_then(|result| result),
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        self.disposing_inspectors.remove(&binding.viewer_session_id);
        detach_result?;
        Ok(DiscussResponse::Success)
    }
    /// Remove a cancellation worker that can no longer acknowledge commands and
    /// publish the same terminal failure path used by normal worker failures.
    async fn recover_cancel_turn_worker(&mut self, sluice_session_id: SluiceSessionId, error: DiscussError) {
        if let Some(entry) = self.worker_registry.remove(&sluice_session_id) {
            entry.join_handle.abort();
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "cancel-turn recovery").await;
        }
        self.worker_capabilities.remove(&sluice_session_id);
        self.cancel_pending_permissions_for_session(&sluice_session_id);

        if let Err(send_error) = self
            .stream_tx
            .send(DiscussStreamEvent::SessionFailed {
                sluice_session_id,
                error,
                terminal_auth_methods: Vec::new(),
            })
            .await
        {
            tracing::warn!(
                target: "application_acp",
                ?send_error,
                "cancellation worker recovery could not reach the desktop bridge"
            );
        }
    }

    /// Cancel all pending permissions for a given session.
    pub(super) fn cancel_pending_permissions_for_session(&mut self, sluice_session_id: &SluiceSessionId) {
        let keys_to_remove: Vec<_> = self
            .pending_permissions
            .keys()
            .filter(|(sid, _)| sid == sluice_session_id)
            .cloned()
            .collect();
        for key in keys_to_remove {
            if let Some(perm) = self.pending_permissions.remove(&key) {
                let _ = perm.responder_tx.send(DiscussPermissionOption::Cancel);
            }
        }
    }

    fn cancel_pending_elicitation_entry(
        &mut self,
        sluice_session_id: SluiceSessionId,
        elicitation_request_id: ElicitationRequestId,
    ) {
        let key = (sluice_session_id, elicitation_request_id);
        if let Some(pending) = self.pending_elicitations.remove(&key) {
            self.persist_cancelled_elicitation(sluice_session_id, elicitation_request_id, &pending);
            let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
        }
    }

    fn persist_elicitation_turn(
        &self,
        sluice_session_id: SluiceSessionId,
        elicitation_request_id: ElicitationRequestId,
        pending: &PendingElicitation,
        answer: &DiscussElicitationAnswer,
    ) {
        let persistence = self.persistence.clone();
        let pending = ElicitationPersistenceInput {
            position: pending.position,
            message: pending.raw_request.message.clone(),
        };
        let session_id = sluice_session_id.to_string();
        let request_id = elicitation_request_id.to_string();
        let answer = answer.clone();
        tokio::spawn(async move {
            if let Err(error) =
                persist_elicitation_turn_inner(&persistence, &session_id, &request_id, pending, answer).await
            {
                tracing::warn!(?error, "Discuss worker: failed to persist elicitation turn");
            }
        });
    }

    pub(super) fn persist_cancelled_elicitation(
        &self,
        sluice_session_id: SluiceSessionId,
        elicitation_request_id: ElicitationRequestId,
        pending: &PendingElicitation,
    ) {
        self.persist_elicitation_turn(
            sluice_session_id,
            elicitation_request_id,
            pending,
            &DiscussElicitationAnswer::Cancel,
        );
    }

    /// Cancel all pending elicitations for a session.
    pub(super) fn cancel_pending_elicitations_for_session(&mut self, sluice_session_id: &SluiceSessionId) {
        let keys_to_remove: Vec<_> = self
            .pending_elicitations
            .keys()
            .filter(|(sid, _)| sid == sluice_session_id)
            .cloned()
            .collect();
        for (session_id, request_id) in keys_to_remove {
            self.cancel_pending_elicitation_entry(session_id, request_id);
        }
    }

    pub(super) fn cancel_pending_elicitations_for_session_all(&mut self) {
        let keys_to_remove: Vec<_> = self.pending_elicitations.keys().cloned().collect();
        for (session_id, request_id) in keys_to_remove {
            self.cancel_pending_elicitation_entry(session_id, request_id);
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the moved session-worker reporting helper's inputs unchanged during the module refactor."
)]
async fn run_session_worker_with_reporting(
    agent: AcpAgent,
    params: WorkerSessionParams,
    sluice_session_id: SluiceSessionId,
    agent_name: String,
    agents_workspace: PathBuf,
    auth_service: Option<ServiceReader<dyn AuthSudoService>>,
    worker_jti: Option<uuid::Uuid>,
    transient_worker_tx: Option<mpsc::Sender<TransientWorkerEvent>>,
    pending_elicitation_cancel_tx: mpsc::Sender<PendingElicitationCancel>,
    stream_tx: mpsc::Sender<DiscussStreamEvent>,
) {
    tracing::info!(%sluice_session_id, provider = %agent_name, cwd = %agents_workspace.display(), "Discuss session worker started");
    let result = run_session_worker(agent, params).await;

    tracing::info!(%sluice_session_id, failed = result.is_err(), "Discuss session worker stopped");

    if let Err(failure) = &result {
        tracing::debug!(%sluice_session_id, error = %failure.error, "Discuss session worker failure");
    }
    revoke_worker_jti(auth_service.as_ref(), worker_jti, "transient").await;

    if let Err(error) = result {
        let SessionWorkerFailure {
            error,
            terminal_auth_methods,
        } = error;
        tracing::warn!(?error, "Discuss session worker stopped");
        if let Some(transient_worker_tx) = transient_worker_tx {
            let _ = transient_worker_tx
                .send(TransientWorkerEvent {
                    sluice_session_id,
                    result: Err(error),
                })
                .await;
        } else {
            let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
            if pending_elicitation_cancel_tx
                .send(PendingElicitationCancel {
                    sluice_session_id,
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
                    sluice_session_id,
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
    let sluice_mcp_enabled = agent_spec.sluice_mcp_enabled();
    let agent_key = serde_json::to_string(agent_spec.server())?;
    let agent = AcpAgent::from_str(&agent_key)?;

    Client
        .builder()
        .name("sluice-acp-session-load")
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

            if sluice_mcp_enabled && !init_response.agent_capabilities.mcp_capabilities.acp {
                tracing::debug!(
                    target: "application_acp",
                    "short-lived ACP session load skipped Sluice MCP because the agent lacks MCP-over-ACP support"
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
    let mut seen_identity = HashSet::<SluiceSessionId>::new();
    let mut seen_acp_id = HashSet::<String>::new();
    let mut out = Vec::with_capacity(sessions.len());

    for session in sessions {
        let identity = session.sluice_session_id;
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

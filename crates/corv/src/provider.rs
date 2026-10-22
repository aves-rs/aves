use std::process::Stdio;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::McpServer;
use agent_client_protocol::{Agent, ByteStreams, Client, ConnectionTo};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use turso::Connection;

use aves::utils::retry::{RetryBackoff, sleep_with_backoff};

use super::actor::{
    Actor, PROVIDER_CHILD_REAP_DEADLINE, PendingProviderTest, ProviderRestartReady, SharedConnectionReady,
    SharedProviderEvent, SpawnSessionOptions, channel_send_error,
};
use super::config::{config_options_from_json, config_settings_from_json, config_settings_from_value};
use super::database::{cleanup_warning, persistence_context, retained_team_assets_by_scope};
use super::library::AgentId;
use super::messages::DiscussResponse;
use super::plugins;
use super::session::{initialize_request, revoke_worker_jti};
use super::storage::{
    DISCUSS_AGENT_CONFIG_UPSERT_SQL, DiscussAgentConfigRow, delete_discuss_agent_config, delete_discuss_session,
    delete_discuss_team_installation, list_discuss_team_installations, list_discuss_team_installations_by_provider,
    list_recent_discuss_sessions, load_discuss_agent_config, present_acp_session_id, upsert_discuss_agent_config,
};
use super::supervisor::{CallbackRouteRegistry, CallbackRouter, ProviderSupervisor, ProviderSupervisorResources};
use super::types::{
    AcpProvider, CleanupWarning, DiscussConfigOption, DiscussError, DiscussProviderConfig, ProviderRuntimeState,
    SluiceSessionId, WorkerCommand,
};

type SharedConnectionResult = Result<(ConnectionTo<Agent>, bool, bool), agent_client_protocol::Error>;

impl Actor {
    /// Add a provider to the ACP server.
    pub(super) async fn handle_add_provider(
        &mut self,
        provider_name: String,
        provider_spec: AcpProvider,
    ) -> Result<DiscussResponse, DiscussError> {
        provider_spec.validate_workspace_copy_sources()?;
        let plugin = plugins::for_agent(&provider_name);
        plugin.install_config(&self.agents_workspace)?;
        let defaults = config_settings_from_value(provider_spec.default_session_config());
        let previous_provider = self.agents.insert(provider_name.clone(), provider_spec);
        let provider_was_new = previous_provider.is_none();
        if let Err(error) = self.persist_agents().await {
            match previous_provider {
                Some(provider) => {
                    self.agents.insert(provider_name, provider);
                }
                None => {
                    self.agents.remove(&provider_name);
                }
            }
            if provider_was_new && let Err(cleanup_error) = plugin.remove_config(&self.agents_workspace) {
                return Err(persistence_context(
                    error,
                    format!("Provider config compensation failed: {cleanup_error}"),
                ));
            }
            return Err(error);
        }
        let conn = self.persistence.acquire_connection().await?;
        if let Err(error) = upsert_discuss_agent_config(&conn, &provider_name, None, &defaults).await {
            match previous_provider {
                Some(provider) => {
                    self.agents.insert(provider_name.clone(), provider);
                }
                None => {
                    self.agents.remove(&provider_name);
                }
            }
            let _ = self.persist_agents().await;
            if provider_was_new && let Err(cleanup_error) = plugin.remove_config(&self.agents_workspace) {
                return Err(persistence_context(
                    error,
                    format!("Provider config compensation failed: {cleanup_error}"),
                ));
            }
            return Err(error);
        }
        if provider_was_new {
            self.provider_supervisors.insert(
                provider_name.clone(),
                ProviderSupervisor::new(provider_name.clone(), plugin.connection_strategy()),
            );
            if self.server_running {
                match self.start_provider_supervisor(&provider_name, false).await {
                    Ok(true) => self.publish_provider_runtime(&provider_name).await,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(
                            target: "application_acp",
                            ?error,
                            provider_name,
                            "new Provider was persisted but its runtime could not start"
                        );
                        self.publish_provider_runtime(&provider_name).await;
                    }
                }
            }
        }
        Ok(DiscussResponse::Success)
    }

    pub(super) fn handle_install_provider_config(&self, provider_name: &str) -> Result<DiscussResponse, DiscussError> {
        if !self.agents.contains_key(provider_name) {
            return Err(DiscussError::Validation {
                message: format!("provider `{provider_name}` is not installed"),
            });
        }
        plugins::for_agent(provider_name).install_config(&self.agents_workspace)?;
        Ok(DiscussResponse::Success)
    }

    pub(super) fn handle_remove_provider_config(&self, provider_name: &str) -> Result<DiscussResponse, DiscussError> {
        if !self.agents.contains_key(provider_name) {
            return Err(DiscussError::Validation {
                message: format!("provider `{provider_name}` is not installed"),
            });
        }
        plugins::for_agent(provider_name).remove_config(&self.agents_workspace)?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_add_provider_request(
        &mut self,
        provider_name: String,
        provider_spec: AcpProvider,
        reply_to: oneshot::Sender<Result<DiscussResponse, DiscussError>>,
        force_connectivity: bool,
    ) -> Result<(), DiscussError> {
        provider_spec.validate_workspace_copy_sources()?;
        let previous_provider = self.agents.get(&provider_name).cloned();
        let requires_connectivity = force_connectivity
            || previous_provider.as_ref().is_none_or(|previous| {
                serde_json::to_value(previous.server()).ok() != serde_json::to_value(provider_spec.server()).ok()
            });

        if requires_connectivity {
            let conn = self.persistence.acquire_connection().await?;
            let previous_config = load_discuss_agent_config(&conn, &provider_name).await?;
            let sluice_session_id = SluiceSessionId::new();
            self.agents
                .insert(provider_name.clone(), provider_spec.clone());
            self.pending_provider_tests.insert(
                sluice_session_id,
                PendingProviderTest {
                    provider_name: provider_name.clone(),
                    candidate: provider_spec,
                    previous_provider,
                    previous_config,
                    reply_to,
                },
            );
            if let Err(error) = self
                .spawn_session(
                    sluice_session_id,
                    &provider_name,
                    SpawnSessionOptions {
                        name: Some("Provider connectivity check".to_string()),
                        setup: None,
                        session_config_overrides: std::collections::BTreeMap::new(),
                        acp_session_id: None,
                        announce_desktop_association: false,
                        transient_connectivity: true,
                        session_cwd: None,
                        sluice_mcp_enabled: None,
                        node_token: None,
                        flow_mcp_endpoints: None,
                    },
                )
                .await
            {
                if let Some(pending) = self.pending_provider_tests.remove(&sluice_session_id) {
                    let original = error.clone();
                    let reply = self.restore_provider_test_snapshot(pending, false).await;
                    let _ = reply.send(Err(original));
                }
                return Ok(());
            }
            return Ok(());
        }

        let result = self.handle_add_provider(provider_name, provider_spec).await;
        let _ = reply_to.send(result);
        Ok(())
    }

    pub(super) async fn restore_provider_test_snapshot(
        &mut self,
        pending: PendingProviderTest,
        persist_agents: bool,
    ) -> oneshot::Sender<Result<DiscussResponse, DiscussError>> {
        let PendingProviderTest {
            provider_name,
            previous_provider,
            previous_config,
            reply_to,
            ..
        } = pending;

        match previous_provider {
            Some(provider) => {
                self.agents.insert(provider_name.clone(), provider);
            }
            None => {
                self.agents.remove(&provider_name);
            }
        }
        if persist_agents && let Err(error) = self.persist_agents().await {
            tracing::warn!(target: "application_acp", ?error, "could not restore Provider config after transient connectivity failure");
        }
        if let Err(error) = self
            .restore_provider_config(&provider_name, previous_config.as_ref())
            .await
        {
            tracing::warn!(target: "application_acp", ?error, "could not restore Provider menu after transient connectivity failure");
        }
        reply_to
    }

    async fn restore_provider_config(
        &self,
        provider_name: &str,
        previous_config: Option<&DiscussAgentConfigRow>,
    ) -> Result<(), DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        match previous_config {
            Some(row) => {
                conn.execute(
                    DISCUSS_AGENT_CONFIG_UPSERT_SQL,
                    (
                        provider_name.to_string(),
                        row.config_options_json.clone(),
                        row.default_config_json.clone(),
                        chrono::Utc::now().timestamp_millis(),
                    ),
                )
                .await?;
            }
            None => {
                delete_discuss_agent_config(&conn, provider_name).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn finish_provider_test(
        &mut self,
        sluice_session_id: SluiceSessionId,
        result: Result<Option<Vec<DiscussConfigOption>>, DiscussError>,
    ) {
        let Some(pending) = self.pending_provider_tests.remove(&sluice_session_id) else {
            return;
        };
        if let Some(entry) = self.worker_registry.remove(&sluice_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
        }

        match result {
            Ok(config_options) => {
                let defaults = config_settings_from_value(pending.candidate.default_session_config());
                let persist_result = async {
                    self.persist_agents().await?;
                    let conn = self.persistence.acquire_connection().await?;
                    upsert_discuss_agent_config(&conn, &pending.provider_name, config_options.as_deref(), &defaults)
                        .await
                }
                .await;
                if let Err(error) = persist_result {
                    let reply = self.restore_provider_test_snapshot(pending, true).await;
                    let _ = reply.send(Err(error));
                } else {
                    let provider_was_new = pending.previous_provider.is_none();
                    if provider_was_new {
                        let provider_name = pending.provider_name.clone();
                        let plugin = plugins::for_agent(&provider_name);
                        self.provider_supervisors.insert(
                            provider_name.clone(),
                            ProviderSupervisor::new(provider_name.clone(), plugin.connection_strategy()),
                        );
                        if self.server_running {
                            match self.start_provider_supervisor(&provider_name, false).await {
                                Ok(true) => self.publish_provider_runtime(&provider_name).await,
                                Ok(false) => {}
                                Err(error) => tracing::warn!(
                                    target: "application_acp",
                                    ?error,
                                    provider_name,
                                    "new Provider was persisted but its runtime could not start"
                                ),
                            }
                        }
                    }
                    let _ = pending.reply_to.send(Ok(DiscussResponse::Success));
                }
            }
            Err(error) => {
                let reply = self.restore_provider_test_snapshot(pending, false).await;
                let _ = reply.send(Err(error));
            }
        }
    }

    /// Simple list for all installed providers.
    pub(super) async fn handle_list_providers(&self) -> Result<DiscussResponse, DiscussError> {
        Ok(DiscussResponse::ProvidersList {
            providers: self.provider_runtime_states(),
            agents_workspace: self.agents_workspace.to_string_lossy().into_owned(),
        })
    }

    fn provider_runtime_states(&self) -> Vec<ProviderRuntimeState> {
        let mut providers = self
            .provider_supervisors
            .values()
            .map(ProviderSupervisor::projection)
            .collect::<Vec<_>>();
        providers.sort_by(|left, right| left.provider_name.cmp(&right.provider_name));
        providers
    }

    pub(super) async fn start_provider_supervisor(
        &mut self,
        provider_name: &str,
        reset_restart_budget: bool,
    ) -> Result<bool, DiscussError> {
        let claimed = self
            .provider_supervisors
            .get_mut(provider_name)
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor is unavailable".to_string(),
            })?
            .claim_start()?;
        if !claimed {
            return Ok(false);
        }
        self.cancel_provider_restart(provider_name);
        let supervisor = self
            .provider_supervisors
            .get_mut(provider_name)
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor is unavailable".to_string(),
            })?;
        if reset_restart_budget {
            supervisor.reset_restart_budget();
        }
        if let Err(error) = self.ensure_shared_provider_connection(provider_name).await {
            let supervisor = self
                .provider_supervisors
                .get_mut(provider_name)
                .ok_or_else(|| DiscussError::ProviderRuntime {
                    provider_name: provider_name.to_string(),
                    message: "Provider runtime supervisor disappeared during connection setup".to_string(),
                })?;
            supervisor.record_failure(&error);
            if supervisor.claim_stop() {
                let _ = supervisor.complete_stop();
            }
            return Err(error);
        }
        let supervisor = self
            .provider_supervisors
            .get_mut(provider_name)
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor disappeared during start".to_string(),
            })?;
        if let Err(error) = supervisor.complete_start() {
            supervisor.record_failure(&error);
            if supervisor.claim_stop() {
                let _ = supervisor.complete_stop();
            }
            return Err(error);
        }
        Ok(true)
    }

    /// Establish the single ACP connection for a shared Provider.
    ///
    /// The returned handle is a clone of the connection owned by the ACP
    /// event-loop task.  The task remains alive until the connection closes;
    /// callers must never create an `AcpAgent` for an individual shared
    /// session.
    async fn open_shared_provider_connection(
        &self,
        provider_name: &str,
    ) -> Result<SharedConnectionReady, DiscussError> {
        let mut child = self.spawn_shared_provider_process(provider_name)?;
        let child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider process stdin is unavailable".to_string(),
            })?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider process stdout is unavailable".to_string(),
            })?;
        let mut child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider process stderr is unavailable".to_string(),
            })?;
        let transport = ByteStreams::new(child_stdin.compat_write(), child_stdout.compat());
        let callback_router = self.shared_provider_callback_router(provider_name)?;
        let callback_routes = Arc::new(Mutex::new(CallbackRouteRegistry::new(callback_router)));
        let callback_routes_for_connection = callback_routes.clone();
        let (ready_tx, ready_rx) =
            oneshot::channel::<Result<(ConnectionTo<Agent>, bool, bool), agent_client_protocol::Error>>();
        let (exit_tx, exit_rx) = oneshot::channel();
        let owner = tokio::spawn(async move {
            let stderr_task = tokio::spawn(async move {
                let mut sink = tokio::io::sink();
                let _ = tokio::io::copy(&mut child_stderr, &mut sink).await;
            });
            let connection =
                Client
                    .builder()
                    .connect_with(transport, move |connection: ConnectionTo<Agent>| async move {
                        let _callback_handler = connection.add_dynamic_handler(
                            super::session::SessionCallbackRouter::new(callback_routes_for_connection.clone()),
                        )?;
                        let init_response = connection
                            .send_request(initialize_request())
                            .block_task()
                            .await?;
                        let _ = ready_tx.send(Ok((
                            connection.clone(),
                            init_response.agent_capabilities.mcp_capabilities.acp,
                            init_response.agent_capabilities.mcp_capabilities.http,
                        )));
                        connection.incoming_closed().await;
                        Ok(())
                    });
            tokio::pin!(connection);
            tokio::select! {
                _ = &mut connection => {}
                _ = child.wait() => {}
            }
            stderr_task.abort();
            let _ = exit_tx.send(());
        });
        let (connection, mcp_acp, mcp_http) = Self::shared_connection_result(provider_name, ready_rx, &owner).await?;
        Ok(SharedConnectionReady {
            connection,
            owner,
            exit_rx,
            mcp_acp,
            mcp_http,
            callback_routes,
        })
    }

    fn spawn_shared_provider_process(&self, provider_name: &str) -> Result<tokio::process::Child, DiscussError> {
        let provider = self
            .agents
            .get(provider_name)
            .ok_or_else(|| DiscussError::UnknownAgent {
                agent_key: provider_name.to_string(),
            })?;
        let McpServer::Stdio(server) = plugins::for_agent(provider_name).launch_server(provider.server()) else {
            return Err(DiscussError::Validation {
                message: "Shared Providers require a stdio ACP server".to_string(),
            });
        };
        let mut command = tokio::process::Command::new(&server.command);
        command
            .args(&server.args)
            .envs(
                server
                    .env
                    .iter()
                    .map(|variable| (&variable.name, &variable.value)),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
            .spawn()
            .map_err(|error| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: format!("failed to start Provider process: {error}"),
            })
    }

    fn shared_provider_callback_router(&self, provider_name: &str) -> Result<Arc<Mutex<CallbackRouter>>, DiscussError> {
        match self.provider_supervisors.get(provider_name) {
            Some(supervisor) => match &supervisor.resources {
                ProviderSupervisorResources::Shared { callback_router, .. } => Ok(callback_router.clone()),
                ProviderSupervisorResources::Isolated { .. } => Err(DiscussError::ProviderRuntime {
                    provider_name: provider_name.to_string(),
                    message: "shared callback router requested for isolated Provider".to_string(),
                }),
            },
            None => Err(DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor is unavailable".to_string(),
            }),
        }
    }

    async fn shared_connection_result(
        provider_name: &str,
        ready_rx: oneshot::Receiver<SharedConnectionResult>,
        owner: &JoinHandle<()>,
    ) -> Result<(ConnectionTo<Agent>, bool, bool), DiscussError> {
        let ready_result = ready_rx.await;
        let (connection, mcp_acp, mcp_http) = match ready_result {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => {
                owner.abort();
                return Err(DiscussError::from(error));
            }
            Err(error) => {
                owner.abort();
                return Err(DiscussError::ProviderRuntime {
                    provider_name: provider_name.to_string(),
                    message: format!("shared Provider connection did not initialize: {error}"),
                });
            }
        };
        Ok((connection, mcp_acp, mcp_http))
    }

    async fn ensure_shared_provider_connection(&mut self, provider_name: &str) -> Result<(), DiscussError> {
        let needs_connection = self
            .provider_supervisors
            .get(provider_name)
            .is_some_and(|supervisor| {
                matches!(
                    &supervisor.resources,
                    ProviderSupervisorResources::Shared { connection: None, .. }
                )
            });
        if !needs_connection {
            return Ok(());
        }
        let ready = self.open_shared_provider_connection(provider_name).await?;
        let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) else {
            ready.owner.abort();
            return Err(DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor disappeared during connection setup".to_string(),
            });
        };
        let exit_rx = ready.exit_rx;
        if let ProviderSupervisorResources::Shared {
            connection,
            connection_owner,
            mcp_acp,
            mcp_http,
            callback_routes,
            ..
        } = &mut supervisor.resources
        {
            *connection = Some(ready.connection);
            *mcp_acp = ready.mcp_acp;
            *mcp_http = ready.mcp_http;
            *callback_routes = Some(ready.callback_routes.clone());
            *connection_owner = Some(ready.owner);
        } else {
            ready.owner.abort();
            return Err(DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider connection strategy changed during startup".to_string(),
            });
        }
        self.watch_shared_provider_connection(provider_name, exit_rx);
        Ok(())
    }

    /// Install an exit reporter for a shared Provider connection.
    ///
    /// The supervisor retains the actual ACP owner task. This reporter only
    /// observes its completion through a oneshot and never owns the child.
    /// Replaced owners are distinguished by a generation so late exit
    /// notifications cannot restart a stopped runtime.
    fn watch_shared_provider_connection(&mut self, provider_name: &str, exit_rx: oneshot::Receiver<()>) {
        let generation = self
            .shared_provider_generations
            .entry(provider_name.to_string())
            .and_modify(|value| *value = value.saturating_add(1))
            .or_insert(0);
        let generation = *generation;
        let event_tx = self.shared_provider_event_tx.clone();
        let provider_name_for_event = provider_name.to_string();
        let watcher = tokio::spawn(async move {
            let _ = exit_rx.await;
            let _ = event_tx
                .send(SharedProviderEvent {
                    provider_name: provider_name_for_event,
                    generation,
                })
                .await;
        });
        if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name)
            && let ProviderSupervisorResources::Shared {
                connection_watcher: watcher_slot,
                ..
            } = &mut supervisor.resources
            && let Some(previous) = watcher_slot.replace(watcher)
        {
            previous.abort();
        }
    }

    pub(super) fn cancel_provider_restart(&mut self, provider_name: &str) {
        if let Some(cancel) = self.provider_restart_cancellations.remove(provider_name) {
            let _ = cancel.send(());
        }
        if let Some(task) = self.provider_restart_tasks.remove(provider_name) {
            task.abort();
        }
    }

    pub(super) async fn handle_shared_provider_exit(&mut self, event: SharedProviderEvent) {
        if self
            .shared_provider_generations
            .get(&event.provider_name)
            .copied()
            != Some(event.generation)
        {
            return;
        }
        self.clear_dead_shared_provider_generation(&event.provider_name, event.generation)
            .await;
        let claim = self
            .provider_supervisors
            .get_mut(&event.provider_name)
            .and_then(|supervisor| match &supervisor.resources {
                ProviderSupervisorResources::Shared { .. } => Some(supervisor.claim_auto_restart()),
                ProviderSupervisorResources::Isolated { .. } => None,
            });
        let Some(claim) = claim else { return };
        match claim {
            Ok(true) => {
                if let Some(supervisor) = self.provider_supervisors.get_mut(&event.provider_name) {
                    supervisor.record_failure(&DiscussError::ProviderProcessExit {
                        provider_name: event.provider_name.clone(),
                        message: "The shared Provider connection exited unexpectedly".to_string(),
                    });
                }
                let (cancel, mut cancel_rx) = oneshot::channel();
                self.cancel_provider_restart(&event.provider_name);
                let event_tx = self.provider_restart_ready_tx.clone();
                let provider_name = event.provider_name.clone();
                let generation = event.generation;
                let task = tokio::spawn(async move {
                    let mut backoff = RetryBackoff::new(Duration::from_millis(500), Duration::from_secs(30));
                    if sleep_with_backoff(&mut backoff, &mut cancel_rx).await {
                        let _ = event_tx
                            .send(ProviderRestartReady {
                                provider_name,
                                generation,
                            })
                            .await;
                    }
                });
                self.provider_restart_cancellations
                    .insert(event.provider_name.clone(), cancel);
                self.provider_restart_tasks
                    .insert(event.provider_name.clone(), task);
                self.publish_provider_runtime(&event.provider_name).await;
            }
            Ok(false) => {}
            Err(error) => {
                if let Some(supervisor) = self.provider_supervisors.get_mut(&event.provider_name) {
                    supervisor.record_failure(&error);
                }
                self.publish_provider_runtime(&event.provider_name).await;
            }
        }
    }

    /// Remove every actor-owned resource belonging to an exited shared
    /// connection before automatic recovery creates its replacement.
    async fn clear_dead_shared_provider_generation(&mut self, provider_name: &str, generation: u64) {
        if self.shared_provider_generations.get(provider_name).copied() != Some(generation) {
            return;
        }

        let session_ids = self
            .worker_registry
            .iter()
            .filter(|(_, entry)| entry.agent_name == provider_name)
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        for session_id in &session_ids {
            if let Some(entry) = self.worker_registry.remove(session_id) {
                entry.join_handle.abort();
                let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, entry.join_handle).await;
                revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
            }
            if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) {
                supervisor.disassociate_chat(session_id);
            }
            self.worker_capabilities.remove(session_id);
            self.cancel_pending_permissions_for_session(session_id);
            self.cancel_pending_elicitations_for_session(session_id);
        }

        let inspector_ids = self
            .inspector_bindings
            .iter()
            .filter(|(_, binding)| session_ids.contains(&binding.parent_session_id))
            .map(|(viewer_id, _)| *viewer_id)
            .collect::<Vec<_>>();
        for viewer_id in &inspector_ids {
            self.inspector_bindings.remove(viewer_id);
            self.worker_capabilities.remove(viewer_id);
            self.cancel_pending_permissions_for_session(viewer_id);
            self.cancel_pending_elicitations_for_session(viewer_id);
        }
        self.disposing_inspectors
            .retain(|id| !inspector_ids.contains(id));

        let (connection_owner, connection_watcher, callback_routes) = self
            .provider_supervisors
            .get_mut(provider_name)
            .map(|supervisor| match &mut supervisor.resources {
                ProviderSupervisorResources::Shared {
                    connection,
                    connection_owner,
                    connection_watcher,
                    callback_router,
                    callback_routes,
                    mcp_acp,
                    mcp_http,
                } => {
                    supervisor.chat_sessions.clear();
                    connection.take();
                    *callback_router = Arc::new(Mutex::new(CallbackRouter::default()));
                    *mcp_acp = false;
                    *mcp_http = false;
                    (
                        connection_owner.take(),
                        connection_watcher.take(),
                        callback_routes.take(),
                    )
                }
                ProviderSupervisorResources::Isolated { .. } => (None, None, None),
            })
            .unwrap_or((None, None, None));

        if let Some(callback_routes) = callback_routes {
            callback_routes.lock().await.clear();
        }
        if let Some(owner) = connection_owner {
            owner.abort();
            let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, owner).await;
        }
        if let Some(watcher) = connection_watcher {
            watcher.abort();
            let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, watcher).await;
        }
    }

    pub(super) async fn handle_shared_provider_restart_ready(&mut self, event: ProviderRestartReady) {
        self.provider_restart_cancellations
            .remove(&event.provider_name);
        self.provider_restart_tasks.remove(&event.provider_name);
        if self
            .shared_provider_generations
            .get(&event.provider_name)
            .copied()
            != Some(event.generation)
        {
            return;
        }
        if let Err(error) = self
            .ensure_shared_provider_connection(&event.provider_name)
            .await
        {
            if let Some(supervisor) = self.provider_supervisors.get_mut(&event.provider_name) {
                supervisor.record_failure(&error);
                let _ = supervisor.claim_stop();
                let _ = supervisor.complete_stop();
            }
            self.publish_provider_runtime(&event.provider_name).await;
            return;
        }
        let Some(supervisor) = self.provider_supervisors.get_mut(&event.provider_name) else {
            return;
        };
        if supervisor.complete_auto_restart().is_err() {
            return;
        }
        self.recover_provider_sessions(&event.provider_name).await;
        self.publish_provider_runtime(&event.provider_name).await;
    }

    pub(super) async fn handle_start_provider(&mut self, provider_name: &str) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        if !self.agents.contains_key(provider_name) {
            return Err(DiscussError::UnknownAgent {
                agent_key: provider_name.to_string(),
            });
        }
        if !self.start_provider_supervisor(provider_name, true).await? {
            return Ok(DiscussResponse::Success);
        }
        self.publish_provider_runtime(provider_name).await;
        self.recover_provider_sessions(provider_name).await;
        self.publish_provider_runtime(provider_name).await;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_stop_provider(&mut self, provider_name: &str) -> Result<DiscussResponse, DiscussError> {
        if !self.agents.contains_key(provider_name) {
            return Err(DiscussError::UnknownAgent {
                agent_key: provider_name.to_string(),
            });
        }
        let should_stop = self
            .provider_supervisors
            .get_mut(provider_name)
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor is unavailable".to_string(),
            })?
            .claim_stop();
        if !should_stop {
            return Ok(DiscussResponse::Success);
        }

        self.cancel_provider_restart(provider_name);
        self.stop_provider_resources(provider_name).await;
        self.provider_supervisors
            .get_mut(provider_name)
            .ok_or_else(|| DiscussError::ProviderRuntime {
                provider_name: provider_name.to_string(),
                message: "Provider runtime supervisor disappeared during stop".to_string(),
            })?
            .complete_stop()?;
        self.publish_provider_runtime(provider_name).await;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn recover_provider_sessions(&mut self, provider_name: &str) {
        let Ok(conn) = self.persistence.acquire_connection().await else {
            return;
        };
        let Ok(rows) = list_recent_discuss_sessions(&conn).await else {
            return;
        };

        for row in rows
            .into_iter()
            .filter(|row| !row.is_task_session && row.agent_name == provider_name)
        {
            let Some(acp_session_id) = present_acp_session_id(row.acp_session_id.as_deref()) else {
                continue;
            };
            let Ok(sluice_session_id) = SluiceSessionId::from_str(&row.sluice_session_id) else {
                continue;
            };
            if let Err(error) = self
                .spawn_session(
                    sluice_session_id,
                    provider_name,
                    SpawnSessionOptions {
                        name: Some(row.session_name),
                        setup: None,
                        session_config_overrides: std::collections::BTreeMap::new(),
                        acp_session_id: Some(acp_session_id),
                        announce_desktop_association: false,
                        transient_connectivity: false,
                        session_cwd: None,
                        sluice_mcp_enabled: None,
                        node_token: None,
                        flow_mcp_endpoints: None,
                    },
                )
                .await
            {
                tracing::warn!(target: "application_acp", ?error, provider_name, "skipping unrecoverable Provider session");
            }
        }
    }

    async fn stop_provider_resources(&mut self, provider_name: &str) {
        let session_ids = self
            .worker_registry
            .iter()
            .filter(|(_, entry)| entry.agent_name == provider_name)
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();

        for session_id in &session_ids {
            if let Some(entry) = self.worker_registry.remove(session_id) {
                entry.join_handle.abort();
                let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, entry.join_handle).await;
                revoke_worker_jti(
                    self.auth_service.as_ref(),
                    entry.worker_jti,
                    "provider stop forced abort",
                )
                .await;
            }
            if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) {
                supervisor.disassociate_chat(session_id);
            }
            self.worker_capabilities.remove(session_id);
            self.cancel_pending_permissions_for_session(session_id);
            self.cancel_pending_elicitations_for_session(session_id);
        }

        let inspector_ids = self
            .inspector_bindings
            .iter()
            .filter(|(_, binding)| session_ids.contains(&binding.parent_session_id))
            .map(|(viewer_id, _)| *viewer_id)
            .collect::<Vec<_>>();
        for viewer_id in &inspector_ids {
            self.inspector_bindings.remove(viewer_id);
            self.worker_capabilities.remove(viewer_id);
            self.cancel_pending_permissions_for_session(viewer_id);
            self.cancel_pending_elicitations_for_session(viewer_id);
        }
        self.disposing_inspectors
            .retain(|id| !inspector_ids.contains(id));

        let pending_tests = self
            .pending_provider_tests
            .iter()
            .filter(|(_, pending)| pending.provider_name == provider_name)
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        for session_id in pending_tests {
            if let Some(pending) = self.pending_provider_tests.remove(&session_id) {
                let _ = pending.reply_to.send(Err(DiscussError::ProviderNotRunning {
                    provider_name: provider_name.to_string(),
                }));
            }
        }

        self.stop_provider_supervisor_resources(provider_name).await;
    }

    pub(super) async fn stop_provider_supervisor_resources(&mut self, provider_name: &str) {
        let (connection_owner, connection_watcher, callback_routes) = self
            .provider_supervisors
            .get_mut(provider_name)
            .map(|supervisor| match &mut supervisor.resources {
                ProviderSupervisorResources::Shared {
                    connection,
                    connection_owner,
                    connection_watcher,
                    callback_routes,
                    ..
                } => {
                    connection.take();
                    (
                        connection_owner.take(),
                        connection_watcher.take(),
                        callback_routes.take(),
                    )
                }
                ProviderSupervisorResources::Isolated { child_sessions } => {
                    child_sessions.clear();
                    (None, None, None)
                }
            })
            .unwrap_or((None, None, None));
        if let Some(callback_routes) = callback_routes {
            callback_routes.lock().await.clear();
        }
        if let Some(owner) = connection_owner {
            owner.abort();
            let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, owner).await;
        }
        if let Some(watcher) = connection_watcher {
            watcher.abort();
            let _ = tokio::time::timeout(PROVIDER_CHILD_REAP_DEADLINE, watcher).await;
        }
        if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) {
            supervisor.chat_sessions.clear();
        }
    }

    pub(super) async fn publish_provider_runtime(&self, provider_name: &str) {
        let Some(supervisor) = self.provider_supervisors.get(provider_name) else {
            return;
        };
        if let Err(error) = self
            .broadcast_tx
            .send(DiscussResponse::ProviderRuntimeChanged {
                state: supervisor.projection(),
            })
            .await
        {
            tracing::warn!(target: "application_acp", ?error, provider_name, "could not publish Provider runtime state");
        }
    }

    pub(super) async fn handle_get_provider_config(
        &self,
        provider_name: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        let provider = self
            .agents
            .get(provider_name)
            .ok_or_else(|| DiscussError::UnknownAgent {
                agent_key: provider_name.to_string(),
            })?;
        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_agent_config(&conn, provider_name).await?;
        let config = match row {
            Some(row) => DiscussProviderConfig {
                options_discovered: row.config_options_json.is_some(),
                options: row
                    .config_options_json
                    .as_deref()
                    .map(config_options_from_json)
                    .transpose()?
                    .unwrap_or_default(),
                defaults: config_settings_from_json(&row.default_config_json)?,
            },
            None => DiscussProviderConfig::default(),
        };
        Ok(DiscussResponse::ProviderConfig {
            config,
            workspace_copy_sources: effective_workspace_copy_sources(provider_name, provider),
        })
    }

    pub(super) async fn handle_remove_provider(
        &mut self,
        provider_name: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        let Some(removed_provider) = self.agents.get(provider_name).cloned() else {
            return Ok(DiscussResponse::ProviderRemoveSuccess { warnings: Vec::new() });
        };
        let should_stop = self
            .provider_supervisors
            .get_mut(provider_name)
            .is_some_and(ProviderSupervisor::claim_stop);
        if should_stop {
            self.stop_provider_resources(provider_name).await;
            if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) {
                let _ = supervisor.complete_stop();
            }
        }
        let conn = self.persistence.acquire_connection().await?;
        self.remove_provider_sessions(&conn, provider_name).await?;
        let removed_team_installations = list_discuss_team_installations_by_provider(&conn, provider_name).await?;
        let surviving_team_installations = list_discuss_team_installations(&conn)
            .await?
            .into_iter()
            .filter(|installation| installation.provider_name != provider_name)
            .collect::<Vec<_>>();
        let (retained_agents_by_scope, retained_skills_by_scope) =
            retained_team_assets_by_scope(self.library, &surviving_team_installations);
        let mut warnings = Vec::new();

        for installation in &removed_team_installations {
            let plugin = plugins::for_agent(provider_name);
            let cleanup_result = AgentId::try_from(installation.leader_id.clone())
                .map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                    asset_id: installation.leader_id.clone(),
                    violation: error.to_string(),
                })
                .and_then(|leader_id| {
                    let Some(team) = self
                        .library
                        .teams_for()
                        .find(|team| team.leader == leader_id)
                    else {
                        return Ok(());
                    };
                    let retain_agents = retained_agents_by_scope
                        .get(plugin.agent_scope_key())
                        .cloned()
                        .unwrap_or_default();
                    let retain_skills = retained_skills_by_scope
                        .get(plugin.skill_scope_key())
                        .cloned()
                        .unwrap_or_default();
                    plugin.destroy_team(
                        self.library,
                        &self.agents_workspace,
                        team,
                        &retain_agents,
                        &retain_skills,
                    )
                });
            if let Err(error) = cleanup_result {
                warnings.push(cleanup_warning(provider_name, &installation.leader_id, &error));
            }
        }

        if let Err(error) = plugins::for_agent(provider_name).remove_config(&self.agents_workspace) {
            warnings.push(CleanupWarning {
                kind: "workspace_cleanup".to_string(),
                provider_name: provider_name.to_string(),
                agent_id: None,
                path: None,
                message: error.to_string(),
            });
        }

        for (index, installation) in removed_team_installations.iter().enumerate() {
            if let Err(error) = delete_discuss_team_installation(&conn, &installation.leader_id).await {
                let remaining = removed_team_installations[index..]
                    .iter()
                    .map(|row| row.leader_id.as_str())
                    .collect::<Vec<_>>();
                return Err(persistence_context(
                    error,
                    format!("removing Provider `{provider_name}` left Team installation records: {remaining:?}"),
                ));
            }
        }

        if let Err(error) = delete_discuss_agent_config(&conn, provider_name).await {
            return Err(persistence_context(
                error,
                format!("Provider `{provider_name}` config row could not be deleted before Provider removal"),
            ));
        }

        self.agents.remove(provider_name);
        if let Err(error) = self.persist_agents().await {
            self.agents
                .insert(provider_name.to_string(), removed_provider);
            return Err(persistence_context(
                error,
                format!("Provider `{provider_name}` remains installed after cascade record deletion"),
            ));
        }
        self.provider_supervisors.remove(provider_name);

        Ok(DiscussResponse::ProviderRemoveSuccess { warnings })
    }

    /// Stop and remove every persisted session owned by a Provider before the
    /// Provider's configuration and installation records are removed.
    async fn remove_provider_sessions(&mut self, conn: &Connection, provider_name: &str) -> Result<(), DiscussError> {
        let sessions = list_recent_discuss_sessions(conn)
            .await?
            .into_iter()
            .filter(|session| session.agent_name == provider_name)
            .collect::<Vec<_>>();

        let live_session_ids = self
            .worker_registry
            .iter()
            .filter(|(_, entry)| entry.agent_name == provider_name)
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();

        for session_id in &live_session_ids {
            if let Some(entry) = self.worker_registry.get(session_id) {
                entry
                    .command_tx
                    .send(WorkerCommand::Stop)
                    .await
                    .map_err(|error| channel_send_error("handle_remove_provider.worker_command", error.to_string()))?;
            }
        }

        for session in &sessions {
            delete_discuss_session(conn, &session.sluice_session_id).await?;
        }

        for session_id in live_session_ids {
            self.worker_registry.remove(&session_id);
            if let Some(supervisor) = self.provider_supervisors.get_mut(provider_name) {
                supervisor.disassociate_chat(&session_id);
            }
            self.worker_capabilities.remove(&session_id);
            if let Some(pending) = self.pending_provider_tests.remove(&session_id) {
                let reply = self.restore_provider_test_snapshot(pending, false).await;
                let _ = reply.send(Err(DiscussError::SessionCancelled {
                    sluice_session_id: session_id,
                }));
            }
            self.cancel_pending_permissions_for_session(&session_id);
        }

        Ok(())
    }

    async fn persist_agents(&self) -> Result<(), DiscussError> {
        super::boot::persist_discuss_agents(&self.root_context, &self.agents).await
    }
}

pub(super) fn effective_workspace_copy_sources(provider_name: &str, provider: &AcpProvider) -> Vec<String> {
    let mut sources = vec![".agents".to_string()];
    if let Some(configured) = provider.workspace_copy_sources() {
        sources.extend(configured.iter().cloned());
    } else if let Some(default) = plugins::default_workspace_copy_source(provider_name) {
        sources.push(default.to_string());
    }
    sources
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::{McpServer, McpServerStdio};

    use super::{AcpProvider, effective_workspace_copy_sources};

    fn provider(sources: Option<Vec<&str>>) -> AcpProvider {
        let base = AcpProvider::from(McpServer::Stdio(McpServerStdio::new("test", "test")));
        let mut value = serde_json::to_value(base).expect("provider JSON");
        if let Some(sources) = sources {
            value["workspace_copy_sources"] = serde_json::json!(sources);
        }
        serde_json::from_value(value).expect("provider source config")
    }

    #[test]
    fn absent_sources_use_builtin_defaults_and_mandatory_agents() {
        for (name, expected) in [
            ("opencode", Some(".opencode")),
            ("claude-acp", Some(".claude")),
            ("codex-acp", Some(".codex")),
            ("grok-build", Some(".grok")),
            ("custom-provider", None),
        ] {
            let mut sources = vec![".agents".to_string()];
            sources.extend(expected.map(str::to_string));
            assert_eq!(effective_workspace_copy_sources(name, &provider(None)), sources);
        }
    }

    #[test]
    fn explicit_empty_sources_disable_builtin_defaults_but_not_agents() {
        assert_eq!(
            effective_workspace_copy_sources("opencode", &provider(Some(vec![]))),
            vec![".agents"]
        );
        assert_eq!(
            effective_workspace_copy_sources("opencode", &provider(Some(vec!["opencode.jsonc", ".custom"]))),
            vec![".agents", "opencode.jsonc", ".custom"]
        );
    }
}

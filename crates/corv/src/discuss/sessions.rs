use std::collections::BTreeMap;
use std::path::PathBuf;

use agent_client_protocol::v1::schema::DeleteSessionRequest;
use serde_json::{from_str as json_from_str, to_string as json_to_string};
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::actor::{Actor, PROVIDER_CHILD_REAP_DEADLINE, SpawnSessionOptions, channel_send_error, validate_user_turn};
use super::agent_nodes::TaskSessionStart;
use super::error::DiscussError;
use super::identity::{Binding, flow_node_subject, persist_binding};
use super::mcp::clear_mcp_scope;
use super::messages::{DiscussResponse, EnsureAttachedSessionOutcome};
use super::session::revoke_worker_jti;
use super::storage::{
    delete_discuss_session, load_discuss_session, present_acp_session_id, rename_discuss_session,
    update_session_launch_context, update_session_setup_kind,
};
use super::supervisor::ProviderSupervisorResources;
use super::types::{CustomMcpScope, DiscussSessionId, DiscussSessionSetup, TaskSessionStarted, WorkerCommand};

impl Actor {
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
        Ok(TaskSessionStarted {
            session_id,
            terminal: terminal_rx,
        })
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

    /// Reload an existing session at the requested working directory.
    /// An anchor without a provider session id uses the normal first-attachment path.
    pub(super) async fn handle_change_working_dir(
        &mut self,
        discuss_session_id: DiscussSessionId,
        session_cwd: PathBuf,
    ) -> Result<DiscussResponse, DiscussError> {
        self.ensure_session_working_dir(discuss_session_id, session_cwd, false)
            .await?;
        Ok(DiscussResponse::Success)
    }

    /// Check the stored attachment and apply the requested directory without loading chat history.
    pub(super) async fn ensure_session_working_dir(
        &mut self,
        discuss_session_id: DiscussSessionId,
        session_cwd: PathBuf,
        attached_only: bool,
    ) -> Result<EnsureAttachedSessionOutcome, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let session_id = discuss_session_id.to_string();
        let Some(row) = load_discuss_session(&conn, &session_id).await? else {
            return if attached_only {
                Ok(EnsureAttachedSessionOutcome::NeedsLaunch)
            } else {
                Err(DiscussError::SessionNotFound { discuss_session_id })
            };
        };
        if attached_only && present_acp_session_id(row.acp_session_id.as_deref()).is_none() {
            return Ok(EnsureAttachedSessionOutcome::NeedsLaunch);
        }
        let stored_cwd = row
            .session_cwd_json
            .as_deref()
            .map(json_from_str::<PathBuf>)
            .transpose()?;
        let live_worker = self
            .worker_registry
            .get(&discuss_session_id)
            .filter(|entry| !entry.join_handle.is_finished() && !entry.command_tx.is_closed());
        let cwd_changed = stored_cwd.as_ref() != Some(&session_cwd);
        if !cwd_changed && live_worker.is_some() {
            return Ok(EnsureAttachedSessionOutcome::Ensured);
        }
        if cwd_changed && live_worker.is_some_and(|entry| entry.active_turn) {
            return Err(DiscussError::SessionBusy { discuss_session_id });
        }
        if let Some(entry) = self.remove_worker(&discuss_session_id).await {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            drop(entry.command_tx);
            let mut join_handle = entry.join_handle;
            if timeout(PROVIDER_CHILD_REAP_DEADLINE, &mut join_handle)
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
            update_session_launch_context(&conn, &session_id, Some(json_to_string(&session_cwd)?), None).await?;
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
                session_cwd: None,
                application_mcp_enabled: None,
                node_token: None,
                flow_mcp_endpoints: None,
            },
        )
        .await?;
        Ok(EnsureAttachedSessionOutcome::Ensured)
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
        if let Some(entry) = self.worker_registry.get(&discuss_session_id) {
            let (reply, response) = oneshot::channel();
            if matches!(
                timeout(
                    PROVIDER_CHILD_REAP_DEADLINE,
                    entry
                        .command_tx
                        .send(WorkerCommand::DeleteSession { reply })
                )
                .await,
                Ok(Ok(()))
            ) {
                match timeout(PROVIDER_CHILD_REAP_DEADLINE, response).await {
                    Ok(Ok(Ok(()))) => {}
                    result => tracing::warn!(%discuss_session_id, ?result, "ACP session delete failed"),
                }
            } else {
                tracing::warn!(%discuss_session_id, "ACP session delete could not be queued");
            }
        } else {
            let conn = self.persistence.acquire_connection().await?;
            if let Some(row) = load_discuss_session(&conn, &discuss_session_id.to_string()).await?
                && let Some(session_id) = present_acp_session_id(row.acp_session_id.as_deref())
            {
                if let Some(ProviderSupervisorResources::Shared {
                    connection: Some(connection),
                    capabilities,
                    ..
                }) = self
                    .provider_supervisors
                    .get(&row.agent_name)
                    .map(|supervisor| &supervisor.resources)
                {
                    if capabilities.session_capabilities.delete.is_some() {
                        let request = connection.send_request(DeleteSessionRequest::new(session_id));
                        if let result @ (Err(_) | Ok(Err(_))) =
                            timeout(PROVIDER_CHILD_REAP_DEADLINE, request.block_task()).await
                        {
                            tracing::warn!(%discuss_session_id, ?result, "ACP session delete failed");
                        }
                    }
                } else {
                    tracing::debug!(%discuss_session_id, provider = %row.agent_name, "No running provider connection; the provider keeps its copy of the removed chat");
                }
            }
        }
        let mut conn = self.persistence.acquire_connection().await?;
        let tx = conn.transaction().await?;
        clear_mcp_scope(&tx, CustomMcpScope::Session, &discuss_session_id.to_string()).await?;
        delete_discuss_session(&tx, &discuss_session_id.to_string()).await?;
        tx.commit().await?;

        if let Some(entry) = self.remove_worker(&discuss_session_id).await {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
            if let Some(supervisor) = self.provider_supervisors.get_mut(&entry.agent_name) {
                supervisor.disassociate_chat(&discuss_session_id);
            }
        }
        self.worker_capabilities.remove(&discuss_session_id);
        self.cancel_pending_permissions_for_session(&discuss_session_id);
        self.cancel_pending_elicitations_for_session(&discuss_session_id);

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

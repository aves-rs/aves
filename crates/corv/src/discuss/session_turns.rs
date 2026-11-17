use agent_client_protocol::v1::schema::{PermissionOptionId, SessionId};
use tokio::sync::oneshot;

use super::actor::{Actor, CANCEL_TURN_DEADLINE, channel_send_error, validate_user_turn};
use super::database::persist_elicitation_turn_inner;
use super::elicitation::validate_elicitation_content;
use super::messages::DiscussResponse;
use super::plugins;
use super::session::revoke_worker_jti;
use super::storage::ElicitationPersistenceInput;
use super::supervisor::ProviderSupervisor;
use super::types::{
    DiscussElicitationAnswer, DiscussError, DiscussImageAttachment, DiscussPermissionOption, DiscussSessionId,
    DiscussStreamEvent, ElicitationRequestId, InspectorBinding, PendingElicitation, PermissionRequestId, WorkerCommand,
};

impl Actor {
    // ── New control request handlers ──────────────────────────────────────

    /// Send a user turn to a live session worker.
    pub(super) async fn handle_send_user_turn(
        &mut self,
        discuss_session_id: DiscussSessionId,
        text: String,
        images: Vec<DiscussImageAttachment>,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        validate_user_turn(&text, &images)?;
        let entry = self
            .worker_registry
            .get(&discuss_session_id)
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
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
            .send(WorkerCommand::SendUserTurn { text, images, reply })
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
        discuss_session_id: DiscussSessionId,
        permission_request_id: PermissionRequestId,
        option: DiscussPermissionOption,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let key = (discuss_session_id, permission_request_id);

        // Validate the option against the offered options before removing
        // the pending entry. An invalid option must NOT consume the entry.
        if let DiscussPermissionOption::Select { ref option_id } = option {
            let pending = self
                .pending_permissions
                .get(&key)
                .ok_or(DiscussError::PermissionNotFound {
                    discuss_session_id,
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
                discuss_session_id,
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
        discuss_session_id: DiscussSessionId,
        elicitation_request_id: ElicitationRequestId,
        answer: DiscussElicitationAnswer,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let key = (discuss_session_id, elicitation_request_id);
        let pending = self
            .pending_elicitations
            .get(&key)
            .ok_or(DiscussError::ElicitationRequestNotPending {
                discuss_session_id,
                elicitation_request_id,
            })?;

        if let DiscussElicitationAnswer::Accept { content } = &answer {
            validate_elicitation_content(&pending.classification, content)?;
        }

        let pending = self
            .pending_elicitations
            .remove(&key)
            .ok_or(DiscussError::ElicitationAlreadyAnswered {
                discuss_session_id,
                elicitation_request_id,
            })?;
        self.persist_elicitation_turn(discuss_session_id, elicitation_request_id, &pending, &answer);
        let _ = pending.responder_tx.send(answer);
        Ok(DiscussResponse::Success)
    }

    /// Cancel one pending elicitation request.
    pub(super) async fn handle_cancel_elicitation(
        &mut self,
        discuss_session_id: DiscussSessionId,
        elicitation_request_id: ElicitationRequestId,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        self.cancel_pending_elicitation_entry(discuss_session_id, elicitation_request_id);
        Ok(DiscussResponse::Success)
    }

    /// Cancel the active turn for a session.
    pub(super) async fn handle_cancel_turn(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        self.cancel_pending_permissions_for_session(&discuss_session_id);
        self.cancel_pending_elicitations_for_session(&discuss_session_id);

        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let Some(entry) = self.worker_registry.get(&discuss_session_id) else {
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
                discuss_session_id,
                channel_send_error("handle_cancel_turn.worker_command", "worker command channel closed"),
            )
            .await;
            return Ok(DiscussResponse::Success);
        }
        match tokio::time::timeout(CANCEL_TURN_DEADLINE, reply_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => return Err(error),
            Ok(Err(_)) => {
                self.recover_cancel_turn_worker(discuss_session_id, DiscussError::OneshotReply)
                    .await;
            }
            Err(_) => {
                self.recover_cancel_turn_worker(
                    discuss_session_id,
                    DiscussError::WorkerStopTimeout { discuss_session_id },
                )
                .await;
            }
        }

        Ok(DiscussResponse::Success)
    }

    /// Cancel and close a session.
    pub(super) async fn handle_cancel_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let cancel_result = if self.worker_registry.contains_key(&discuss_session_id) {
            self.handle_cancel_turn(discuss_session_id)
                .await
                .map(|_| ())
        } else {
            Ok(())
        };
        if let Some(entry) = self.worker_registry.remove(&discuss_session_id) {
            let _ = entry.command_tx.send(WorkerCommand::Stop).await;
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "session cancellation").await;
        }
        self.worker_capabilities.remove(&discuss_session_id);

        self.cancel_pending_permissions_for_session(&discuss_session_id);
        self.cancel_pending_elicitations_for_session(&discuss_session_id);

        cancel_result?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_reserve_inspector(
        &mut self,
        parent_session_id: DiscussSessionId,
        child_session_id: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let parent = self
            .worker_registry
            .get(&parent_session_id)
            .ok_or(DiscussError::SessionNotFound {
                discuss_session_id: parent_session_id,
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
        let viewer_session_id = DiscussSessionId::new();
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
        viewer_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let binding = self
            .inspector_bindings
            .get(&viewer_session_id)
            .cloned()
            .ok_or(DiscussError::SessionNotFound {
                discuss_session_id: viewer_session_id,
            })?;
        let parent = self
            .worker_registry
            .get(&binding.parent_session_id)
            .ok_or(DiscussError::SessionNotFound {
                discuss_session_id: binding.parent_session_id,
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
        viewer_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        let Some(binding) = self.inspector_bindings.remove(&viewer_session_id) else {
            return Err(DiscussError::SessionNotFound {
                discuss_session_id: viewer_session_id,
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
    async fn recover_cancel_turn_worker(&mut self, discuss_session_id: DiscussSessionId, error: DiscussError) {
        if let Some(entry) = self.worker_registry.remove(&discuss_session_id) {
            entry.join_handle.abort();
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "cancel-turn recovery").await;
        }
        self.worker_capabilities.remove(&discuss_session_id);
        self.cancel_pending_permissions_for_session(&discuss_session_id);

        if let Err(send_error) = self
            .stream_tx
            .send(DiscussStreamEvent::SessionFailed {
                discuss_session_id,
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
    pub(super) fn cancel_pending_permissions_for_session(&mut self, discuss_session_id: &DiscussSessionId) {
        let keys_to_remove: Vec<_> = self
            .pending_permissions
            .keys()
            .filter(|(sid, _)| sid == discuss_session_id)
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
        discuss_session_id: DiscussSessionId,
        elicitation_request_id: ElicitationRequestId,
    ) {
        let key = (discuss_session_id, elicitation_request_id);
        if let Some(pending) = self.pending_elicitations.remove(&key) {
            self.persist_cancelled_elicitation(discuss_session_id, elicitation_request_id, &pending);
            let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
        }
    }

    fn persist_elicitation_turn(
        &self,
        discuss_session_id: DiscussSessionId,
        elicitation_request_id: ElicitationRequestId,
        pending: &PendingElicitation,
        answer: &DiscussElicitationAnswer,
    ) {
        let persistence = self.persistence.clone();
        let pending = ElicitationPersistenceInput {
            position: pending.position,
            message: pending.raw_request.message.clone(),
        };
        let session_id = discuss_session_id.to_string();
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
        discuss_session_id: DiscussSessionId,
        elicitation_request_id: ElicitationRequestId,
        pending: &PendingElicitation,
    ) {
        self.persist_elicitation_turn(
            discuss_session_id,
            elicitation_request_id,
            pending,
            &DiscussElicitationAnswer::Cancel,
        );
    }

    /// Cancel all pending elicitations for a session.
    pub(super) fn cancel_pending_elicitations_for_session(&mut self, discuss_session_id: &DiscussSessionId) {
        let keys_to_remove: Vec<_> = self
            .pending_elicitations
            .keys()
            .filter(|(sid, _)| sid == discuss_session_id)
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

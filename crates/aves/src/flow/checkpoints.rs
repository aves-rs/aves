use std::collections::HashMap;

use tokio::time::Instant;

use super::actor::Actor;
use super::actor::terminal_retention_retry_due;
use super::hook::{
    FlowAppCheckpointCommittedContext, FlowAppCheckpointContext, FlowAppHookContext, FlowAppLandingContext,
};
use super::mailbox::TerminalResult;
use super::messages::FlowBroadcast;
use super::tick::{FlowDispatch, FlowTreeStatus, TickResult};
use super::types::{
    FlowError, FlowExecutionId, FlowExecutionSnapshot, FlowExecutionStatus, FlowFailure, FlowId, FlowTimestamp,
    PendingTerminalPersistence, PreparedCheckpointCommit,
};

impl Actor {
    /// Persists and finalizes a terminal tick recovered during runtime startup.
    pub(super) async fn persist_recovered_terminal(
        &mut self,
        execution_id: FlowExecutionId,
        tick: TickResult,
        terminal: TerminalResult,
    ) -> Result<(), FlowError> {
        let state = self
            .execution
            .get(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        let status = match tick.status {
            FlowTreeStatus::Running => FlowExecutionStatus::Active,
            FlowTreeStatus::Succeeded => FlowExecutionStatus::Succeeded,
            FlowTreeStatus::Failed => FlowExecutionStatus::Failed,
        };
        let snapshot = state.snapshot(execution_id, status, tick.reconstruction, None);
        self.snapshot_store.save(&snapshot).await?;
        self.finalize_execution_transition(
            execution_id,
            vec![PendingTerminalPersistence {
                correlation: terminal.correlation,
                failure_message: None,
            }],
            snapshot.execution,
            tick.dispatches,
        )
        .await
    }

    /// Persists the current live projection before notifying observers.
    pub(super) async fn persist_active_snapshot(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let state = self
            .execution
            .get(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        let durable = state.snapshot(
            execution_id,
            FlowExecutionStatus::Active,
            state.tick_loop.reconstruction_state(),
            None,
        );
        let snapshot = durable.execution.clone();
        self.snapshot_store.save(&durable).await?;
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::ExecutionStatusChanged {
                flow_name: self
                    .definitions
                    .get(&snapshot.flow_id)
                    .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone()),
                snapshot,
            })
            .await;
        Ok(())
    }

    /// Captures one tick's durable checkpoint before dispatching its successors.
    pub(super) async fn prepare_execution_transition(
        &mut self,
        execution_id: FlowExecutionId,
        tick: TickResult,
    ) -> Result<(), FlowError> {
        let pending = self
            .execution
            .get(&execution_id)
            .map(|state| {
                state
                    .pending_terminal_persistence
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .ok_or(FlowError::ExecutionNotFound)?;
        let (previous_entries, superseded_sequence, sequence) = {
            let state = self
                .execution
                .get_mut(&execution_id)
                .ok_or(FlowError::ExecutionNotFound)?;
            let previous_entries = state.workspace_entries.clone();
            let superseded_sequence = (state.checkpoint_sequence > 0).then_some(state.checkpoint_sequence);
            state.checkpoint_sequence += 1;
            (previous_entries, superseded_sequence, state.checkpoint_sequence)
        };
        let status = match tick.status {
            FlowTreeStatus::Running => FlowExecutionStatus::Active,
            FlowTreeStatus::Succeeded => FlowExecutionStatus::Succeeded,
            FlowTreeStatus::Failed => FlowExecutionStatus::Failed,
        };
        if status != FlowExecutionStatus::Active {
            Box::pin(self.request_owner_cancel(execution_id)).await?;
        }
        let state = self
            .execution
            .get(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        let execution_root = state.workspace.root();
        let checkpoint = self
            .workspace_hook
            .checkpoint(FlowAppCheckpointContext {
                execution: FlowAppHookContext {
                    execution_id,
                    flow_id: state.active.flow_id(),
                    definition_revision: state.active.revision(),
                    execution_root,
                    invocation_kind: state.active.invocation_kind(),
                },
                previous_entries,
                sequence,
            })
            .await
            .map_err(|source| FlowError::AppWorkspaceCheckpoint { source })?;
        let dispatches = tick.dispatches;
        let failure_message = pending.iter().find_map(|item| item.failure_message.clone());
        let state = self
            .execution
            .get_mut(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        if status != FlowExecutionStatus::Active {
            state.tick_loop.mark_pending_failed();
        }
        let mut snapshot = state.snapshot(execution_id, status, tick.reconstruction, failure_message);
        snapshot.workspace_checkpoint = checkpoint.workspace_checkpoint;
        snapshot.workspace_entries = checkpoint.workspace_entries;
        state.prepared_checkpoint_commit = Some(PreparedCheckpointCommit {
            snapshot,
            superseded_sequence,
            pending,
            dispatches,
            snapshot_saved: false,
        });
        self.commit_execution_transition(execution_id).await
    }

    /// Retries a prepared commit before advancing the controller again.
    pub(super) async fn advance_after_node_result(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        if self
            .execution
            .get(&execution_id)
            .is_some_and(|state| state.prepared_checkpoint_commit.is_some())
        {
            return self.commit_execution_transition(execution_id).await;
        }
        let tick = self
            .execution
            .get_mut(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?
            .tick_loop
            .tick()?;
        self.prepare_execution_transition(execution_id, tick).await
    }

    /// Applies the committed transition and emits its ordered observer events.
    pub(super) async fn finalize_execution_transition(
        &mut self,
        execution_id: FlowExecutionId,
        pending: Vec<PendingTerminalPersistence>,
        snapshot: FlowExecutionSnapshot,
        dispatches: Vec<FlowDispatch>,
    ) -> Result<(), FlowError> {
        let status = snapshot.status.clone();
        if let Some(state) = self.execution.get_mut(&execution_id) {
            state.pending_terminal_persistence.clear();
        }
        if status == FlowExecutionStatus::Active {
            self.spawn_dispatches(dispatches);
        } else {
            self.land_execution_best_effort(
                &snapshot,
                pending
                    .iter()
                    .map(|item| item.correlation.clone())
                    .collect(),
            )
            .await;
            self.terminal_executions.push(snapshot.clone());
            self.revoke_execution_tokens_best_effort(execution_id).await;
            self.mcp_ports.finish(execution_id).await;
            self.execution.remove(&execution_id);
            self.prune_terminal_executions().await?;
        }
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::ExecutionStatusChanged {
                flow_name: self
                    .definitions
                    .get(&snapshot.flow_id)
                    .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone()),
                snapshot,
            })
            .await;
        let representative = pending.first().ok_or(FlowError::ExecutionNotFound)?;
        match status {
            FlowExecutionStatus::Succeeded => {
                let _ = self
                    .broadcast_tx
                    .send(FlowBroadcast::Succeeded {
                        correlation: representative.correlation.clone(),
                    })
                    .await;
            }
            FlowExecutionStatus::Failed => {
                let _ = self
                    .broadcast_tx
                    .send(FlowBroadcast::Failed {
                        correlation: representative.correlation.clone(),
                        failure: FlowFailure {
                            code: "OwnerExecutionFailed".to_owned(),
                            message: pending
                                .iter()
                                .find_map(|item| item.failure_message.clone())
                                .unwrap_or_else(|| "Flow node execution failed".to_owned()),
                        },
                    })
                    .await;
            }
            FlowExecutionStatus::Active | FlowExecutionStatus::Aborting | FlowExecutionStatus::Aborted => {}
        }
        Ok(())
    }

    /// Saves, projects, and releases one prepared checkpoint commit in order.
    pub(super) async fn commit_execution_transition(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let mut prepared = self
            .execution
            .get_mut(&execution_id)
            .and_then(|state| state.prepared_checkpoint_commit.take())
            .ok_or(FlowError::ExecutionNotFound)?;
        if !prepared.snapshot_saved {
            if let Err(error) = self.snapshot_store.save(&prepared.snapshot).await {
                self.execution
                    .get_mut(&execution_id)
                    .ok_or(FlowError::ExecutionNotFound)?
                    .prepared_checkpoint_commit = Some(prepared);
                return Err(error.into());
            }
            prepared.snapshot_saved = true;
        }

        let state = self
            .execution
            .get(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        let execution_root = prepared.snapshot.workspace.root();
        let context = FlowAppCheckpointCommittedContext {
            execution: FlowAppHookContext {
                execution_id,
                flow_id: prepared.snapshot.execution.flow_id,
                definition_revision: prepared.snapshot.execution.revision,
                execution_root,
                invocation_kind: prepared.snapshot.execution.invocation_kind,
            },
            sequence: state.checkpoint_sequence,
            superseded_sequence: prepared.superseded_sequence,
        };
        if let Err(source) = self.workspace_hook.checkpoint_committed(context).await {
            self.execution
                .get_mut(&execution_id)
                .ok_or(FlowError::ExecutionNotFound)?
                .prepared_checkpoint_commit = Some(prepared);
            return Err(FlowError::AppWorkspaceCheckpoint { source });
        }
        prepared.superseded_sequence = None;
        let state = self
            .execution
            .get_mut(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        state.workspace_entries = prepared.snapshot.workspace_entries;
        state.workspace_checkpoint = prepared.snapshot.workspace_checkpoint;
        let snapshot = prepared.snapshot.execution.clone();
        let pending = prepared.pending;
        let dispatches = prepared.dispatches;
        self.finalize_execution_transition(execution_id, pending, snapshot, dispatches)
            .await
    }

    /// Retries pending execution commits and independently throttled retention.
    pub(super) async fn retry_pending_execution_work(&mut self) {
        let execution_ids = self
            .execution
            .iter()
            .filter(|(_, state)| {
                !state.abort_pending
                    && (!state.pending_terminal_persistence.is_empty() || state.prepared_checkpoint_commit.is_some())
            })
            .map(|(execution_id, _)| *execution_id)
            .collect::<Vec<_>>();
        for execution_id in execution_ids {
            if let Err(error) = self.advance_after_node_result(execution_id).await {
                tracing::warn!(%execution_id, %error, "Flow terminal snapshot persistence retry failed");
            }
        }
        if terminal_retention_retry_due(Instant::now(), &mut self.next_terminal_retention_retry)
            && let Err(error) = self.prune_terminal_executions().await
        {
            tracing::warn!(%error, "Flow terminal retention retry failed");
        }
    }

    /// Prunes excess terminal history while retaining active executions and failed cleanup anchors.
    pub(super) async fn prune_terminal_executions(&mut self) -> Result<(), FlowError> {
        let mut by_flow = HashMap::<FlowId, Vec<&FlowExecutionSnapshot>>::new();
        for snapshot in &self.terminal_executions {
            by_flow.entry(snapshot.flow_id).or_default().push(snapshot);
        }
        let mut candidates = Vec::new();
        for executions in by_flow.values_mut() {
            executions.sort_by(|left, right| {
                right
                    .completed_at
                    .as_ref()
                    .map(FlowTimestamp::as_datetime)
                    .cmp(&left.completed_at.as_ref().map(FlowTimestamp::as_datetime))
            });
            candidates.extend(
                executions
                    .iter()
                    .skip(self.retention_completed_runs)
                    .filter(|snapshot| !self.execution.contains_key(&snapshot.execution_id))
                    .map(|snapshot| snapshot.execution_id),
            );
        }

        let mut first_error = None;
        let mut cleaned = Vec::new();
        for execution_id in candidates {
            match self.cleanup_terminal_execution(execution_id).await {
                Ok(()) => cleaned.push(execution_id),
                Err(error) => {
                    tracing::warn!(%execution_id, %error, "Flow terminal retention cleanup failed; retaining cleanup anchor");
                    if !matches!(&error, FlowError::AppWorkspaceCleanup { .. }) {
                        first_error.get_or_insert(FlowError::RetentionCleanup {
                            execution_id,
                            source: Box::new(error),
                        });
                    }
                }
            }
        }
        if !cleaned.is_empty() {
            self.terminal_executions
                .retain(|snapshot| !cleaned.contains(&snapshot.execution_id));
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    /// Removes one terminal execution's workspace projection before its durable row.
    pub(super) async fn cleanup_terminal_execution(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let durable = self
            .snapshot_store
            .load_all()
            .await?
            .into_iter()
            .find(|snapshot| snapshot.execution.execution_id == execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        let snapshot = &durable.execution;
        let execution_root = durable.workspace.root();
        let mut context = FlowAppHookContext {
            execution_id,
            flow_id: snapshot.flow_id,
            definition_revision: snapshot.revision,
            execution_root,
            invocation_kind: snapshot.invocation_kind,
        };
        context.execution_root = self.workspace_hook.execution_root(context.clone()).await?;
        let workspace_mappings = self
            .store
            .load_definition_revision(snapshot.flow_id, snapshot.revision)
            .await?
            .workspace_mappings;
        let landing = FlowAppLandingContext {
            execution: context.clone(),
            status: snapshot.status.clone(),
            correlations: Vec::new(),
            workspace_mappings,
            imported_workspace_root: durable.imported_workspace_root,
        };
        self.workspace_hook
            .landing(landing)
            .await
            .map_err(|source| FlowError::AppWorkspaceCleanup { source })?;
        self.workspace_hook
            .release_artifacts(context)
            .await
            .map_err(|source| FlowError::AppWorkspaceCleanup { source })?;
        self.store.delete_execution(execution_id).await?;
        Ok(())
    }
}

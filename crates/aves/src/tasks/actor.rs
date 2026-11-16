use std::collections::HashMap;
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::auth::Jwt;
use crate::bus::BusIngress;
use crate::flow::{LifecycleCorrelation, NodeExecutionId, resolve_script_limits};

use super::client::{GuestRpcBindingFactoryHandle, GuestRpcBindingsHandle};
use super::compiler::TypeScriptCompiler;
use super::messages::{TasksRequest, TasksRequestPayload, TasksResponse};
use super::sandbox::{CompiledScript, ScriptAbortHandle, ScriptExecution, ScriptSandbox};
use super::shell::{join_shell_commands, run_shell};
use super::types::{
    MAX_CONCURRENT_SHELL_EXECUTIONS, SHELL_SUPERVISION_DEADLINE, ScriptNodeCompletion, ScriptNodeFailure,
    ScriptNodeTerminalResult, ScriptNodeTickResult, ShellNodeCompletion, ShellNodeFailure, ShellNodeTerminalResult,
    ShellNodeTickResult, ShellRunSpec, ShellTerminal, ShellTerminalCause, TasksError, resolve_shell_timeout,
};

/// The maximum number of concurrent script executions allowed.
const MAX_CONCURRENT_EXECUTIONS: usize = 4;

/// Represents a pending script execution request.
struct Pending {
    /// The compiled script to execute.
    compiled: String,
    /// The filename of the script.
    filename: String,
    /// The fuel limit for the script execution.
    fuel: u64,
    /// The timeout for the script execution.
    timeout: u64,
    /// The input data for the script execution.
    input: Value,
    /// The workspace path for the script execution.
    workspace_path: PathBuf,
    /// The guest RPC bindings for the script execution.
    guest_rpc: GuestRpcBindingsHandle,
}

/// Represents a running script execution.
struct Running {
    /// The correlation data for the script execution.
    correlation: LifecycleCorrelation,
    /// The input data for the script execution.
    input: Value,
    /// The abort handle for the script execution.
    abort: ScriptAbortHandle,
}

/// Represents a completed script execution.
struct Completion {
    /// The correlation data for the script execution.
    correlation: LifecycleCorrelation,
    /// The permit used to release the execution slot.
    _permit: OwnedSemaphorePermit,
    /// The result of the script execution.
    result: Result<ScriptNodeCompletion, TasksError>,
}

/// A validated shell request awaiting an execution slot.
struct ShellPending {
    interpreter: String,
    commands: Vec<String>,
    cwd: PathBuf,
    timeout: Duration,
    input: Value,
}

/// A shell runner and any callers awaiting its terminal acknowledgement.
struct ShellRunning {
    correlation: LifecycleCorrelation,
    input: Value,
    terminate: Option<oneshot::Sender<()>>,
    runner: JoinHandle<()>,
    deadline: Option<Instant>,
    cancel_waiters: Vec<oneshot::Sender<Result<TasksResponse, TasksError>>>,
}

/// Runner acknowledgement carrying its capacity permit until reconciliation.
struct ShellCompletion {
    correlation: LifecycleCorrelation,
    terminal: ShellTerminal,
    _permit: OwnedSemaphorePermit,
}

/// Tasks actor. Its execution state is process-local and resets on restart.
pub(super) struct Actor {
    /// The receiver for incoming tasks requests.
    receiver: mpsc::Receiver<TasksRequest>,
    /// The compiler for TypeScript scripts.
    compiler: TypeScriptCompiler,
    /// The sandbox for script execution.
    sandbox: ScriptSandbox,
    /// The factory for guest RPC bindings.
    guest_rpc_factory: GuestRpcBindingFactoryHandle,
    /// The pending tasks waiting to be executed.
    pending: HashMap<NodeExecutionId, Pending>,
    /// The running tasks currently being executed.
    running: HashMap<NodeExecutionId, Running>,
    /// The completed tasks that have been executed.
    completed: HashMap<NodeExecutionId, ScriptNodeTerminalResult>,
    /// The semaphore for managing execution slots.
    execution_slots: Arc<Semaphore>,
    /// Shell lifecycle state and capacity are independent of ScriptNode.
    shell_pending: HashMap<NodeExecutionId, ShellPending>,
    /// Shell running state.
    shell_running: HashMap<NodeExecutionId, ShellRunning>,
    /// Shell completed state.
    shell_completed: HashMap<NodeExecutionId, ShellNodeTerminalResult>,
    /// The semaphore for managing shell slots.
    shell_slots: Arc<Semaphore>,
    /// Whether old shell runners are being reconciled after a stop.
    shell_stopping: bool,
    /// Stop requests waiting for the current shell cleanup window.
    stop_waiters: Vec<oneshot::Sender<Result<TasksResponse, TasksError>>>,
    /// Runners force-expired during the current stop window.
    shell_stop_unacknowledged: usize,
    /// Whether the actor is running.
    runtime_started: bool,
}

impl Actor {
    pub(super) fn new(
        receiver: mpsc::Receiver<TasksRequest>,
        sandbox: ScriptSandbox,
        guest_rpc_factory: GuestRpcBindingFactoryHandle,
    ) -> Self {
        Self {
            receiver,
            compiler: TypeScriptCompiler,
            sandbox,
            guest_rpc_factory,
            pending: HashMap::new(),
            running: HashMap::new(),
            completed: HashMap::new(),
            execution_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_EXECUTIONS)),
            shell_pending: HashMap::new(),
            shell_running: HashMap::new(),
            shell_completed: HashMap::new(),
            shell_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_SHELL_EXECUTIONS)),
            shell_stopping: false,
            stop_waiters: Vec::new(),
            shell_stop_unacknowledged: 0,
            runtime_started: false,
        }
    }

    async fn handle_message(
        &mut self,
        payload: TasksRequestPayload,
        reply_to: oneshot::Sender<Result<TasksResponse, TasksError>>,
        completion_tx: &mpsc::Sender<Completion>,
        shell_completion_tx: &mpsc::Sender<ShellCompletion>,
    ) {
        let result = match payload {
            TasksRequestPayload::StartRuntime => {
                self.runtime_started = true;
                Ok(TasksResponse::Success)
            }
            TasksRequestPayload::StartScript {
                correlation,
                source,
                filename,
                input,
                fuel_limit,
                timeout_secs,
                workspace_path,
                node_token,
                node_bus,
            } => self.start_script(
                correlation,
                source,
                filename,
                input,
                fuel_limit,
                timeout_secs,
                workspace_path,
                node_token,
                node_bus,
            ),
            TasksRequestPayload::TickScript { correlation, input } => {
                self.tick_script(correlation, input, completion_tx)
            }
            TasksRequestPayload::CancelScriptExecution { node_execution_id } => {
                self.cancel_script(node_execution_id);
                Ok(TasksResponse::Success)
            }
            TasksRequestPayload::StartShell {
                correlation,
                interpreter,
                commands,
                cwd,
                timeout_secs,
                input,
            } => self.start_shell(correlation, interpreter, commands, cwd, timeout_secs, input),
            TasksRequestPayload::TickShell { correlation, input } => {
                self.tick_shell(correlation, input, shell_completion_tx)
            }
            TasksRequestPayload::CancelShellExecution { node_execution_id } => {
                self.cancel_shell(node_execution_id, reply_to);
                return;
            }
            TasksRequestPayload::StopRuntime => {
                self.runtime_started = false;
                self.pending.clear();
                if self.shell_stopping {
                    self.stop_waiters.push(reply_to);
                    return;
                }
                for (id, _) in self.shell_pending.drain() {
                    self.shell_completed
                        .insert(id, Self::empty_shell_failure(TasksError::ShellCancelled));
                }
                let deadline = Instant::now() + SHELL_SUPERVISION_DEADLINE;
                for running in self.shell_running.values_mut() {
                    if running.deadline.is_none() {
                        if let Some(terminate) = running.terminate.take() {
                            let _ = terminate.send(());
                        }
                        running.deadline = Some(deadline);
                    }
                }
                if self.shell_running.is_empty() {
                    Ok(TasksResponse::Success)
                } else {
                    self.shell_stopping = true;
                    self.stop_waiters.push(reply_to);
                    return;
                }
            }
        };

        let _ = reply_to.send(result);
    }

    // The argument list mirrors the wire request payload fields.
    #[allow(clippy::too_many_arguments)]
    fn start_script(
        &mut self,
        correlation: LifecycleCorrelation,
        source: String,
        filename: String,
        input: Value,
        fuel_limit: u64,
        timeout_secs: u64,
        workspace_path: PathBuf,
        node_token: Option<Jwt>,
        node_bus: Option<BusIngress>,
    ) -> Result<TasksResponse, TasksError> {
        if !self.runtime_started {
            return Err(TasksError::RuntimeStopped);
        }
        let id = correlation.node_execution_id;
        if self.completed.contains_key(&id) {
            return Ok(TasksResponse::Started { correlation });
        }
        if self.pending.contains_key(&id) || self.running.contains_key(&id) {
            return Err(TasksError::IdempotencyConflict { node_execution_id: id });
        }
        let compiled = self.compiler.transpile(&source, &filename)?;
        let (fuel, timeout) = resolve_script_limits(fuel_limit, timeout_secs);
        let node_token = node_token.ok_or(TasksError::MissingNodeToken)?;
        let node_bus = node_bus.ok_or(TasksError::MissingNodeBus)?;
        Self::ensure_execution_workspace(&workspace_path)?;
        let guest_rpc = self.guest_rpc_factory.bind(node_bus, node_token);
        self.pending.insert(
            id,
            Pending {
                compiled,
                filename,
                fuel,
                timeout,
                input,
                workspace_path,
                guest_rpc,
            },
        );
        Ok(TasksResponse::Started { correlation })
    }

    fn tick_script(
        &mut self,
        correlation: LifecycleCorrelation,
        input: Value,
        completion_tx: &mpsc::Sender<Completion>,
    ) -> Result<TasksResponse, TasksError> {
        if !self.runtime_started {
            return Err(TasksError::RuntimeStopped);
        }
        let id = correlation.node_execution_id;
        if let Some(result) = self.completed.get(&id) {
            return Ok(TasksResponse::ScriptNodeTicked {
                correlation,
                result: result.clone().into(),
            });
        }
        if let Some(running) = self.running.get(&id) {
            return if running.input == input {
                Ok(TasksResponse::ScriptNodeTicked {
                    correlation,
                    result: ScriptNodeTickResult::Running,
                })
            } else {
                Err(TasksError::IdempotencyConflict { node_execution_id: id })
            };
        }
        let Some(pending) = self.pending.get(&id) else {
            return Err(TasksError::ScriptNotStarted { node_execution_id: id });
        };
        if pending.input != input {
            return Err(TasksError::IdempotencyConflict { node_execution_id: id });
        }
        let Some(permit) = self.execution_slots.clone().try_acquire_owned().ok() else {
            return Ok(TasksResponse::ScriptNodeTicked {
                correlation,
                result: ScriptNodeTickResult::Running,
            });
        };
        self.dispatch_script(correlation.clone(), input, permit, completion_tx);
        Ok(TasksResponse::ScriptNodeTicked {
            correlation,
            result: ScriptNodeTickResult::Running,
        })
    }

    fn dispatch_script(
        &mut self,
        correlation: LifecycleCorrelation,
        input: Value,
        permit: OwnedSemaphorePermit,
        completion_tx: &mpsc::Sender<Completion>,
    ) {
        let id = correlation.node_execution_id;
        let pending = self.pending.remove(&id).expect("pending checked above");
        let abort = ScriptAbortHandle::new();
        self.running.insert(
            id,
            Running {
                correlation: correlation.clone(),
                input,
                abort: abort.clone(),
            },
        );
        let sandbox = self.sandbox;
        let completion_tx = completion_tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                sandbox.run(
                    ScriptExecution::new(
                        correlation.clone(),
                        CompiledScript::new(pending.compiled, pending.filename),
                        pending.input,
                        pending.fuel,
                        pending.timeout,
                        pending.workspace_path,
                        pending.guest_rpc,
                    ),
                    abort,
                )
            }))
            .unwrap_or_else(|_| {
                Err(TasksError::Execution {
                    message: String::from("sandbox worker panicked"),
                })
            });
            let _ = completion_tx.blocking_send(Completion {
                correlation,
                _permit: permit,
                result,
            });
        });
    }

    fn cancel_script(&mut self, node_execution_id: NodeExecutionId) {
        self.pending.remove(&node_execution_id);
        if let Some(running) = self.running.remove(&node_execution_id) {
            running.abort.abort();
        }
        self.completed.insert(
            node_execution_id,
            ScriptNodeTerminalResult::Failed {
                failure: ScriptNodeFailure {
                    error: TasksError::Cancelled,
                    outputs: Default::default(),
                    logs: Value::Null,
                },
            },
        );
    }

    async fn complete(&mut self, completion: Completion) {
        let node_execution_id = completion.correlation.node_execution_id;
        let Some(running) = self.running.remove(&node_execution_id) else {
            return;
        };
        if running.correlation != completion.correlation {
            return;
        }
        let result = Self::terminal_result(completion.correlation, completion.result);
        self.completed.insert(node_execution_id, result);
    }

    fn terminal_result(
        correlation: LifecycleCorrelation,
        result: Result<ScriptNodeCompletion, TasksError>,
    ) -> ScriptNodeTerminalResult {
        match result {
            Ok(mut completion) => {
                completion.correlation = correlation;
                if completion.errors.is_empty() {
                    ScriptNodeTerminalResult::Succeeded { completion }
                } else {
                    ScriptNodeTerminalResult::Failed {
                        failure: ScriptNodeFailure {
                            error: TasksError::ScriptReported {
                                message: completion.errors.join("\n"),
                            },
                            outputs: completion.outputs,
                            logs: completion.logs,
                        },
                    }
                }
            }
            Err(error) => ScriptNodeTerminalResult::Failed {
                failure: ScriptNodeFailure {
                    error,
                    outputs: Default::default(),
                    logs: Value::Null,
                },
            },
        }
    }

    fn start_shell(
        &mut self,
        correlation: LifecycleCorrelation,
        interpreter: String,
        commands: Vec<String>,
        cwd: PathBuf,
        timeout_secs: u64,
        input: Value,
    ) -> Result<TasksResponse, TasksError> {
        if !self.runtime_started {
            return Err(TasksError::RuntimeStopped);
        }
        if self.shell_stopping {
            return Err(TasksError::ShellStopping);
        }
        let id = correlation.node_execution_id;
        if self.shell_completed.contains_key(&id) {
            return Ok(TasksResponse::Started { correlation });
        }
        if self.shell_pending.contains_key(&id) || self.shell_running.contains_key(&id) {
            return Err(TasksError::IdempotencyConflict { node_execution_id: id });
        }
        if !fs::metadata(&cwd).is_ok_and(|metadata| metadata.is_dir()) {
            return Err(TasksError::ShellInvalidWorkingDirectory {
                path: cwd.to_string_lossy().into_owned(),
            });
        }
        self.shell_pending.insert(
            id,
            ShellPending {
                interpreter,
                commands,
                cwd,
                timeout: resolve_shell_timeout(timeout_secs),
                input,
            },
        );
        Ok(TasksResponse::Started { correlation })
    }

    fn tick_shell(
        &mut self,
        correlation: LifecycleCorrelation,
        input: Value,
        completion_tx: &mpsc::Sender<ShellCompletion>,
    ) -> Result<TasksResponse, TasksError> {
        if !self.runtime_started {
            return Err(TasksError::RuntimeStopped);
        }
        let id = correlation.node_execution_id;
        if let Some(result) = self.shell_completed.get(&id) {
            return Ok(TasksResponse::ShellNodeTicked {
                correlation,
                result: result.clone().into(),
            });
        }
        if let Some(running) = self.shell_running.get(&id) {
            return if running.input == input {
                Ok(TasksResponse::ShellNodeTicked {
                    correlation,
                    result: ShellNodeTickResult::Running,
                })
            } else {
                Err(TasksError::IdempotencyConflict { node_execution_id: id })
            };
        }
        let Some(pending) = self.shell_pending.get(&id) else {
            return Err(TasksError::ShellNotStarted { node_execution_id: id });
        };
        if pending.input != input {
            return Err(TasksError::IdempotencyConflict { node_execution_id: id });
        }
        if let Ok(permit) = self.shell_slots.clone().try_acquire_owned() {
            let pending = self
                .shell_pending
                .remove(&id)
                .expect("pending checked above");
            let spec = ShellRunSpec {
                interpreter: pending.interpreter,
                script: join_shell_commands(&pending.commands),
                cwd: pending.cwd,
                timeout: pending.timeout,
            };
            let (terminate, terminate_rx) = oneshot::channel();
            let tx = completion_tx.clone();
            let runner_correlation = correlation.clone();
            let runner = tokio::spawn(async move {
                let terminal = run_shell(spec, terminate_rx).await;
                let _ = tx
                    .send(ShellCompletion {
                        correlation: runner_correlation,
                        terminal,
                        _permit: permit,
                    })
                    .await;
            });
            self.shell_running.insert(
                id,
                ShellRunning {
                    correlation: correlation.clone(),
                    input,
                    terminate: Some(terminate),
                    runner,
                    deadline: None,
                    cancel_waiters: Vec::new(),
                },
            );
        }
        Ok(TasksResponse::ShellNodeTicked {
            correlation,
            result: ShellNodeTickResult::Running,
        })
    }

    fn cancel_shell(&mut self, id: NodeExecutionId, reply: oneshot::Sender<Result<TasksResponse, TasksError>>) {
        if let Some(result) = self.shell_completed.get(&id) {
            let _ = reply.send(Ok(TasksResponse::ShellNodeCancelled {
                node_execution_id: id,
                result: result.clone().into(),
            }));
            return;
        }
        if let Some(running) = self.shell_running.get_mut(&id) {
            if running.deadline.is_none() {
                if let Some(terminate) = running.terminate.take() {
                    let _ = terminate.send(());
                }
                running.deadline = Some(Instant::now() + SHELL_SUPERVISION_DEADLINE);
            }
            running.cancel_waiters.push(reply);
            return;
        }
        self.shell_pending.remove(&id);
        let result = Self::empty_shell_failure(TasksError::ShellCancelled);
        self.shell_completed.insert(id, result.clone());
        let _ = reply.send(Ok(TasksResponse::ShellNodeCancelled {
            node_execution_id: id,
            result: result.into(),
        }));
    }

    fn empty_shell_failure(error: TasksError) -> ShellNodeTerminalResult {
        ShellNodeTerminalResult::Failed {
            failure: ShellNodeFailure {
                error,
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                stdout_dropped_bytes: 0,
                stderr_dropped_bytes: 0,
                stdout_abandoned_tail: false,
                stderr_abandoned_tail: false,
            },
        }
    }

    async fn complete_shell(&mut self, completion: ShellCompletion) {
        let id = completion.correlation.node_execution_id;
        if !self
            .shell_running
            .get(&id)
            .is_some_and(|running| running.correlation == completion.correlation)
        {
            return;
        }
        let running = self
            .shell_running
            .remove(&id)
            .expect("correlation checked above");
        // The acknowledgement send can wake the actor before the runner returns.
        let _ = running.runner.await;
        let result = Self::shell_terminal_result(completion.correlation, completion.terminal);
        self.shell_completed.insert(id, result.clone());
        for waiter in running.cancel_waiters {
            let _ = waiter.send(Ok(TasksResponse::ShellNodeCancelled {
                node_execution_id: id,
                result: result.clone().into(),
            }));
        }
        self.finish_shell_stop();
    }

    fn shell_terminal_result(correlation: LifecycleCorrelation, terminal: ShellTerminal) -> ShellNodeTerminalResult {
        let ShellTerminal {
            cause,
            stdout,
            stderr,
            stdout_dropped_bytes,
            stderr_dropped_bytes,
            stdout_abandoned_tail,
            stderr_abandoned_tail,
        } = terminal;
        let (error, exit_code) = match cause {
            ShellTerminalCause::Exited { code: 0 } => {
                return ShellNodeTerminalResult::Succeeded {
                    completion: ShellNodeCompletion {
                        correlation,
                        exit_code: 0,
                        stdout,
                        stderr,
                        stdout_dropped_bytes,
                        stderr_dropped_bytes,
                        stdout_abandoned_tail,
                        stderr_abandoned_tail,
                    },
                };
            }
            ShellTerminalCause::Exited { code } => (TasksError::ShellExitedNonZero { code }, Some(code)),
            ShellTerminalCause::Signaled { description } => (TasksError::ShellSignaled { description }, None),
            ShellTerminalCause::TimedOut { seconds } => (TasksError::ShellTimeout { seconds }, None),
            ShellTerminalCause::Cancelled => (TasksError::ShellCancelled, None),
            ShellTerminalCause::SpawnFailed { interpreter } => (TasksError::ShellSpawnFailed { interpreter }, None),
            ShellTerminalCause::PostSpawnFailed { kind } => (TasksError::ShellPostSpawnFailed { kind }, None),
        };
        ShellNodeTerminalResult::Failed {
            failure: ShellNodeFailure {
                error,
                exit_code,
                stdout,
                stderr,
                stdout_dropped_bytes,
                stderr_dropped_bytes,
                stdout_abandoned_tail,
                stderr_abandoned_tail,
            },
        }
    }

    fn earliest_shell_deadline(&self) -> Option<Instant> {
        self.shell_running
            .values()
            .filter_map(|running| running.deadline)
            .min()
    }

    fn expire_shell_supervision(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .shell_running
            .iter()
            .filter_map(|(id, running)| {
                running
                    .deadline
                    .filter(|deadline| *deadline <= now)
                    .map(|_| *id)
            })
            .collect();
        for id in expired {
            let running = self
                .shell_running
                .remove(&id)
                .expect("expired runner exists");
            running.runner.abort();
            if self.shell_stopping {
                self.shell_stop_unacknowledged += 1;
            }
            let error = TasksError::ShellStopCleanupFailed { unacknowledged: 1 };
            self.shell_completed
                .insert(id, Self::empty_shell_failure(error.clone()));
            for waiter in running.cancel_waiters {
                let _ = waiter.send(Err(error.clone()));
            }
        }
        self.finish_shell_stop();
    }

    fn finish_shell_stop(&mut self) {
        if !self.shell_stopping || !self.shell_running.is_empty() {
            return;
        }
        let unacknowledged = std::mem::take(&mut self.shell_stop_unacknowledged);
        self.shell_stopping = false;
        for waiter in self.stop_waiters.drain(..) {
            let result = if unacknowledged == 0 {
                Ok(TasksResponse::Success)
            } else {
                Err(TasksError::ShellStopCleanupFailed { unacknowledged })
            };
            let _ = waiter.send(result);
        }
    }

    fn ensure_execution_workspace(path: &Path) -> Result<(), TasksError> {
        fs::create_dir_all(path).map_err(|error| TasksError::Execution {
            message: format!("failed to create ScriptNode execution workspace: {error}"),
        })?;
        let metadata = fs::metadata(path).map_err(|error| TasksError::Execution {
            message: format!("failed to validate ScriptNode execution workspace: {error}"),
        })?;
        if !metadata.is_dir() {
            return Err(TasksError::Execution {
                message: String::from("ScriptNode execution workspace is not a directory"),
            });
        }
        Ok(())
    }
}

pub(super) async fn run_actor(mut actor: Actor) {
    let (completion_tx, mut completion_rx) = mpsc::channel(MAX_CONCURRENT_EXECUTIONS);
    let (shell_completion_tx, mut shell_completion_rx) = mpsc::channel(MAX_CONCURRENT_SHELL_EXECUTIONS);
    loop {
        let deadline = actor.earliest_shell_deadline();
        tokio::select! {
            request = actor.receiver.recv() => match request {
                Some(request) => actor.handle_message(request.payload, request.reply_to, &completion_tx, &shell_completion_tx).await,
                None => break,
            },
            completion = completion_rx.recv() => match completion {
                Some(completion) => actor.complete(completion).await,
                None => break,
            },
            completion = shell_completion_rx.recv() => match completion {
                Some(completion) => actor.complete_shell(completion).await,
                None => break,
            },
            _ = async { sleep_until(deadline.expect("deadline is armed")).await }, if deadline.is_some() => {
                // Drain acknowledgements already queued before treating a runner as lost.
                while let Ok(completion) = shell_completion_rx.try_recv() {
                    actor.complete_shell(completion).await;
                }
                actor.expire_shell_supervision(Instant::now());
            },
        }
    }
    for running in actor.shell_running.values() {
        running.runner.abort();
    }
}

#[cfg(test)]
mod tests;

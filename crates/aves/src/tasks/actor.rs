use std::collections::HashMap;
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::auth::Jwt;
use crate::bus::BusIngress;
use crate::flow::{LifecycleCorrelation, NodeExecutionId, resolve_script_limits};

use super::client::{GuestRpcBindingFactoryHandle, GuestRpcBindingsHandle};
use super::compiler::TypeScriptCompiler;
use super::messages::{TasksRequest, TasksRequestPayload, TasksResponse};
use super::sandbox::{CompiledScript, ScriptAbortHandle, ScriptExecution, ScriptSandbox};
use super::types::{
    ScriptNodeCompletion, ScriptNodeFailure, ScriptNodeTerminalResult, ScriptNodeTickResult, TasksError,
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
            runtime_started: false,
        }
    }

    async fn handle_message(
        &mut self,
        payload: TasksRequestPayload,
        reply_to: oneshot::Sender<Result<TasksResponse, TasksError>>,
        completion_tx: &mpsc::Sender<Completion>,
    ) {
        let result = match payload {
            TasksRequestPayload::StartRuntime => {
                self.runtime_started = true;
                Ok(TasksResponse::Success)
            }
            TasksRequestPayload::StopRuntime => {
                self.runtime_started = false;
                self.pending.clear();
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
    loop {
        tokio::select! {
            request = actor.receiver.recv() => match request {
                Some(request) => actor.handle_message(request.payload, request.reply_to, &completion_tx).await,
                None => break,
            },
            completion = completion_rx.recv() => match completion {
                Some(completion) => actor.complete(completion).await,
                None => break,
            },
        }
    }
}

#[cfg(test)]
mod tests;

use std::{collections::BTreeMap, num::TryFromIntError, path::PathBuf, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Map, Value, json};
use specta::Type;
use thiserror::Error;
use wasmtime::{Error as WasmtimeError, MemoryAccessError};

use aves::ErrorCode;

use crate::flow::{LifecycleCorrelation, NodeExecutionId};

pub(crate) const DEFAULT_SHELL_TIMEOUT_SECS: u64 = 300;
pub(crate) const MAX_CONCURRENT_SHELL_EXECUTIONS: usize = 4;
pub(crate) const SHELL_OUTPUT_CAP_BYTES: usize = 1024 * 1024;
pub(crate) const SHELL_REAP_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const SHELL_DRAIN_WINDOW: Duration = Duration::from_secs(5);
/// The runner's 2s reap and 5s drain bounds fit inside this deadline.
pub(crate) const SHELL_SUPERVISION_DEADLINE: Duration = Duration::from_secs(10);

/// Tasks-owned configuration for a single shell session.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShellNodeConfig {
    #[serde(default = "default_shell_interpreter")]
    pub interpreter: String,
    pub commands: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub timeout_secs: u64,
}

fn default_shell_interpreter() -> String {
    String::from("sh")
}

/// Resolves zero to the finite shell timeout default.
pub(crate) fn resolve_shell_timeout(timeout_secs: u64) -> Duration {
    Duration::from_secs(if timeout_secs == 0 {
        DEFAULT_SHELL_TIMEOUT_SECS
    } else {
        timeout_secs
    })
}

/// Input to the infallible runner entry in shell.rs:
/// `pub(crate) async fn run_shell(spec: ShellRunSpec, terminate: tokio::sync::oneshot::Receiver<()>) -> ShellTerminal`.
/// Every path yields exactly one terminal cause.
#[derive(Debug, Clone)]
pub(crate) struct ShellRunSpec {
    pub interpreter: String,
    pub script: String,
    pub cwd: PathBuf,
    pub timeout: Duration,
}

/// Acknowledged shell outcome and bounded output capture.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ShellTerminal {
    pub cause: ShellTerminalCause,
    pub stdout: String,
    pub stderr: String,
    pub stdout_dropped_bytes: u64,
    pub stderr_dropped_bytes: u64,
    pub stdout_abandoned_tail: bool,
    pub stderr_abandoned_tail: bool,
}

/// Terminal cause of a shell execution.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum ShellTerminalCause {
    Exited { code: i32 },
    Signaled { description: String },
    TimedOut { seconds: u64 },
    Cancelled,
    SpawnFailed { interpreter: String },
    PostSpawnFailed { kind: ShellPostSpawnFailureKind },
}

/// Runner failure after the interpreter has spawned.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Type, JsonSchema)]
pub enum ShellPostSpawnFailureKind {
    PipeRead,
    Kill,
    Reap,
}

/// Successful shell outputs and capture completeness markers.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ShellNodeCompletion {
    pub correlation: LifecycleCorrelation,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub stdout_dropped_bytes: u64,
    pub stderr_dropped_bytes: u64,
    pub stdout_abandoned_tail: bool,
    pub stderr_abandoned_tail: bool,
}

/// Failed shell outcome, including any captured output and known exit status.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ShellNodeFailure {
    pub error: TasksError,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_dropped_bytes: u64,
    pub stderr_dropped_bytes: u64,
    pub stdout_abandoned_tail: bool,
    pub stderr_abandoned_tail: bool,
}

/// Immediate wire result returned by every ShellNode tick.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum ShellNodeTickResult {
    Running,
    Succeeded { completion: ShellNodeCompletion },
    Failed { failure: ShellNodeFailure },
}

#[derive(Debug, Clone)]
pub(super) enum ShellNodeTerminalResult {
    Succeeded { completion: ShellNodeCompletion },
    Failed { failure: ShellNodeFailure },
}

impl From<ShellNodeTerminalResult> for ShellNodeTickResult {
    fn from(terminal: ShellNodeTerminalResult) -> Self {
        match terminal {
            ShellNodeTerminalResult::Succeeded { completion } => Self::Succeeded { completion },
            ShellNodeTerminalResult::Failed { failure } => Self::Failed { failure },
        }
    }
}

/// Opaque values emitted by a ScriptNode under script-declared output names.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize, Type, JsonSchema)]
#[serde(transparent)]
pub struct ScriptNodeOutputs(BTreeMap<String, Value>);

impl ScriptNodeOutputs {
    /// Creates named opaque ScriptNode output values.
    #[cfg(test)]
    pub(super) fn new(values: BTreeMap<String, Value>) -> Self {
        Self(values)
    }

    /// Returns the value emitted for a script-declared output name.
    pub(super) fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }
}

/// Generic completion emitted by a ScriptNode.
///
/// Outputs remain opaque to Tasks so the Flow host can bind and interpret them
/// for its own node configuration without coupling ScriptNode execution to a
/// particular domain.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ScriptNodeCompletion {
    pub correlation: LifecycleCorrelation,
    pub outputs: ScriptNodeOutputs,
    pub errors: Vec<String>,
    pub logs: Value,
}

/// Failure payload returned by a terminal ScriptNode tick.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ScriptNodeFailure {
    pub error: TasksError,
    pub outputs: ScriptNodeOutputs,
    pub logs: Value,
}

/// Immediate wire result returned by every ScriptNode tick.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum ScriptNodeTickResult {
    Running,
    Succeeded { completion: ScriptNodeCompletion },
    Failed { failure: ScriptNodeFailure },
}

#[derive(Debug, Clone)]
pub(super) enum ScriptNodeTerminalResult {
    Succeeded { completion: ScriptNodeCompletion },
    Failed { failure: ScriptNodeFailure },
}

impl From<ScriptNodeTerminalResult> for ScriptNodeTickResult {
    fn from(terminal: ScriptNodeTerminalResult) -> Self {
        match terminal {
            ScriptNodeTerminalResult::Succeeded { completion } => Self::Succeeded { completion },
            ScriptNodeTerminalResult::Failed { failure } => Self::Failed { failure },
        }
    }
}

/// Typed failures at the Tasks execution boundary.
#[derive(Debug, Error, ErrorCode, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum TasksError {
    #[error("TypeScript compilation failed: {message}")]
    Compilation { message: String },
    #[error("script execution failed: {message}")]
    Execution { message: String },
    #[error("script reported failure: {message}")]
    ScriptReported { message: String },
    #[error("script exceeded the fuel budget")]
    FuelExhausted,
    #[error("script timed out after {seconds}s")]
    Timeout { seconds: u64 },
    #[error("script execution cancelled")]
    Cancelled,
    #[error("script execution capacity is full")]
    ExecutionCapacity,
    #[error("Tasks runtime is stopped")]
    RuntimeStopped,
    #[error("failed to spawn shell interpreter: {interpreter}")]
    ShellSpawnFailed { interpreter: String },
    #[error("shell exited with nonzero status: {code}")]
    ShellExitedNonZero { code: i32 },
    #[error("shell terminated by signal: {description}")]
    ShellSignaled { description: String },
    #[error("invalid shell working directory: {path}")]
    ShellInvalidWorkingDirectory { path: String },
    #[error("shell timed out after {seconds}s")]
    ShellTimeout { seconds: u64 },
    #[error("shell execution cancelled")]
    ShellCancelled,
    #[error("shell post-spawn runner failure: {kind:?}")]
    ShellPostSpawnFailed { kind: ShellPostSpawnFailureKind },
    #[error("shell runtime is stopping")]
    ShellStopping,
    #[error("ShellNode owner state unavailable")]
    ShellOwnerStateUnavailable,
    #[error("shell stop cleanup failed for {unacknowledged} unacknowledged executions")]
    ShellStopCleanupFailed { unacknowledged: usize },
    #[error("ScriptNode start requires a Flow-scoped token")]
    MissingNodeToken,
    #[error("ScriptNode start requires runtime bus ingress for guest RPC")]
    MissingNodeBus,
    #[error("ScriptNode guest RPC protocol failed: {message}")]
    GuestRpcProtocol { message: String },
    #[error("ScriptNode guest RPC relay failed: {message}")]
    GuestRpcRelay { message: String },
    #[error("script emitted invalid output: {message}")]
    InvalidOutput { message: String },
    #[error("QuickJS export is missing: {0}")]
    MissingExport(String),
    #[error("node execution {node_execution_id} was already started with different input")]
    IdempotencyConflict { node_execution_id: NodeExecutionId },
    #[error("node execution {node_execution_id} must be started before it can be ticked")]
    ScriptNotStarted { node_execution_id: NodeExecutionId },
    #[error("shell node execution {node_execution_id} must be started before it can be ticked")]
    ShellNotStarted { node_execution_id: NodeExecutionId },
    #[error("Tasks request channel is closed")]
    ChannelClosed,
    #[error("Tasks request channel send failed for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    #[error("Tasks response channel receive failed for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
}

impl TasksError {
    /// Safe structured fields for the public Tasks error envelope.
    pub fn rpc_details(&self) -> Map<String, Value> {
        let details = match self {
            Self::Timeout { seconds } => json!({"seconds": seconds}),
            Self::ShellSpawnFailed { interpreter } => json!({"interpreter": interpreter}),
            Self::ShellExitedNonZero { code } => json!({"code": code}),
            Self::ShellSignaled { description } => json!({"description": description}),
            Self::ShellInvalidWorkingDirectory { path } => json!({"path": path}),
            Self::ShellTimeout { seconds } => json!({"seconds": seconds}),
            Self::ShellPostSpawnFailed { kind } => json!({"kind": kind}),
            Self::ShellStopCleanupFailed { unacknowledged } => json!({"unacknowledged": unacknowledged}),
            Self::IdempotencyConflict { node_execution_id }
            | Self::ScriptNotStarted { node_execution_id }
            | Self::ShellNotStarted { node_execution_id } => {
                json!({"node_execution_id": node_execution_id.to_string()})
            }
            Self::ChannelSend {
                target_domain,
                operation,
                ..
            }
            | Self::ChannelRecv {
                target_domain,
                operation,
                ..
            } => json!({"target_domain": target_domain, "operation": operation}),
            _ => return Map::new(),
        };
        match details {
            Value::Object(fields) => fields,
            _ => unreachable!("detail projections are objects"),
        }
    }
}

impl From<WasmtimeError> for TasksError {
    fn from(error: WasmtimeError) -> Self {
        Self::Execution {
            message: error.to_string(),
        }
    }
}
impl From<MemoryAccessError> for TasksError {
    fn from(error: MemoryAccessError) -> Self {
        Self::Execution {
            message: error.to_string(),
        }
    }
}
impl From<TryFromIntError> for TasksError {
    fn from(error: TryFromIntError) -> Self {
        Self::Execution {
            message: error.to_string(),
        }
    }
}

impl From<JsonError> for TasksError {
    fn from(error: JsonError) -> Self {
        Self::InvalidOutput {
            message: error.to_string(),
        }
    }
}

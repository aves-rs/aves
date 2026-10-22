use std::{collections::BTreeMap, num::TryFromIntError};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Map, Value, json};
use specta::Type;
use thiserror::Error;
use wasmtime::{Error as WasmtimeError, MemoryAccessError};

use aves::ErrorCode;

use crate::flow::{LifecycleCorrelation, NodeExecutionId};

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
            Self::IdempotencyConflict { node_execution_id } | Self::ScriptNotStarted { node_execution_id } => {
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

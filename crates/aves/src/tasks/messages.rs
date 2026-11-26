use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::auth::Jwt;
use crate::bus::{BusIngress, BusRpcContext};
use crate::flow::{LifecycleCorrelation, NodeExecutionId};

use super::types::{ScriptNodeTickResult, ShellNodeTickResult, ShellOutputFiles, TasksError};

pub struct TasksRequest {
    pub payload: TasksRequestPayload,
    pub reply_to: oneshot::Sender<Result<TasksResponse, TasksError>>,
}

#[aves::bus_requests(context = BusRpcContext, root_error = TasksError, domain_error = TasksError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TasksRequestPayload {
    /// Starts the tasks runtime.
    #[privileged(reason = "Tasks runtime lifecycle is owned by Aves")]
    StartRuntime,

    /// Starts a script node execution.
    #[privileged(reason = "ScriptNode lifecycle is admitted only from Aves Flow")]
    StartScript {
        correlation: LifecycleCorrelation,
        source: String,
        filename: String,
        input: Value,
        fuel_limit: u64,
        timeout_secs: u64,
        workspace_path: PathBuf,
        /// Flow-scoped bearer used only by this ScriptNode's guest RPC bridge.
        #[serde(skip)]
        node_token: Option<Jwt>,
        /// Runtime bus ingress paired with the scoped token for generated guest RPC.
        #[serde(skip)]
        node_bus: Option<BusIngress>,
    },

    /// Ticks a script node execution.
    #[privileged(reason = "ScriptNode lifecycle is admitted only from Aves Flow")]
    TickScript {
        correlation: LifecycleCorrelation,
        input: Value,
    },

    /// Cancels a script node execution.
    #[privileged(reason = "ScriptNode lifecycle is admitted only from Aves Flow")]
    CancelScriptExecution { node_execution_id: NodeExecutionId },

    /// Starts a shell node execution.
    #[privileged(reason = "ShellNode lifecycle is admitted only from Aves Flow")]
    StartShell {
        correlation: LifecycleCorrelation,
        interpreter: String,
        commands: Vec<String>,
        cwd: PathBuf,
        /// Configured execution timeout in seconds; zero selects the Tasks default.
        timeout_secs: u64,
        output: Option<ShellOutputFiles>,
        input: Value,
    },

    /// Ticks a shell node execution.
    #[privileged(reason = "ShellNode lifecycle is admitted only from Aves Flow")]
    TickShell {
        correlation: LifecycleCorrelation,
        input: Value,
    },

    /// Cancels a shell node execution and returns its terminal snapshot.
    #[privileged(reason = "ShellNode lifecycle is admitted only from Aves Flow")]
    CancelShellExecution { node_execution_id: NodeExecutionId },

    /// Stops the tasks runtime.
    #[privileged(reason = "Tasks runtime lifecycle is owned by Aves")]
    StopRuntime,
}

#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TasksResponse {
    Started {
        correlation: LifecycleCorrelation,
    },
    ScriptNodeTicked {
        correlation: LifecycleCorrelation,
        result: ScriptNodeTickResult,
    },
    ShellNodeTicked {
        correlation: LifecycleCorrelation,
        result: ShellNodeTickResult,
    },
    /// Terminal snapshot after cancellation; `Running` is never returned.
    ///
    /// A completed success is replayed. Failures retain their typed error,
    /// known exit code, captured output, and truncation or abandoned-tail markers.
    /// A lost runner exceeding the supervision deadline instead returns
    /// `Err(TasksError::ShellStopCleanupFailed { .. })` to the caller.
    ShellNodeCancelled {
        node_execution_id: NodeExecutionId,
        result: ShellNodeTickResult,
    },
    Success,
    Error {
        error: TasksError,
    },
}

/// Tasks-domain broadcast contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TasksBroadcast {}

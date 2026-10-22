use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::auth::Jwt;
use crate::bus::{BusIngress, BusRpcContext};
use crate::flow::{LifecycleCorrelation, NodeExecutionId};

use super::types::{ScriptNodeTickResult, TasksError};

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

    /// Stops the tasks runtime.
    #[privileged(reason = "Tasks runtime lifecycle is owned by Aves")]
    StopRuntime,

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
    Success,
    Error {
        error: TasksError,
    },
}

/// Tasks-domain broadcast contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TasksBroadcast {}

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use super::messages::LifecycleCorrelation;
use super::types::{FlowExecutionId, FlowExecutionStatus, FlowId, FlowInvocationKind};

/// Complete application-facing context for one Flow execution hook step.
#[derive(Clone, Debug)]
pub struct FlowAppHookContext {
    pub execution_id: FlowExecutionId,
    pub flow_id: FlowId,
    pub definition_revision: i64,
    pub execution_root: PathBuf,
    pub invocation_kind: FlowInvocationKind,
}

/// Context delivered when an execution reaches a terminal outcome.
#[derive(Clone, Debug)]
pub struct FlowAppLandingContext {
    pub execution: FlowAppHookContext,
    pub status: FlowExecutionStatus,
    pub correlations: Vec<LifecycleCorrelation>,
}

/// Async, opt-in lifecycle integration supplied by an embedding application.
pub trait FlowAppWorkspaceHook: Send + Sync {
    /// Captures the execution workspace at a checkpoint. Defaults to Flow's own capture.
    fn checkpoint(&self, _context: FlowAppHookContext) -> FlowAppCheckpointFuture<'_> {
        Box::pin(async { Ok(FlowAppCheckpoint::Flow) })
    }
    /// Restores the execution workspace after a restart, once Flow has restored its own state.
    fn restore(&self, _context: FlowAppHookContext) -> FlowAppHookFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn takeoff(&self, context: FlowAppHookContext) -> FlowAppHookFuture<'_>;
    fn landing(&self, context: FlowAppLandingContext) -> FlowAppHookFuture<'_>;
    fn release_artifacts(&self, context: FlowAppHookContext) -> FlowAppHookFuture<'_>;
}

/// Who captured a workspace checkpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FlowAppCheckpoint {
    /// Flow captures its own checkpoint, as without a hook.
    #[default]
    Flow,
    /// The application captured the checkpoint; Flow records none.
    App,
}

/// Future returned by the checkpoint step.
pub type FlowAppCheckpointFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlowAppCheckpoint, FlowAppHookError>> + Send + 'a>>;

/// Future returned by an application workspace-hook step.
pub type FlowAppHookFuture<'a> = Pin<Box<dyn Future<Output = Result<(), FlowAppHookError>> + Send + 'a>>;

/// Failure returned by an application workspace-hook step.
pub type FlowAppHookError = Box<dyn std::error::Error + Send + Sync + 'static>;

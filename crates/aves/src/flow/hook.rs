use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use crate::auth::MachineIssuerId;

use super::ast::{ContentAddressedPath, ContentAddressedRoot, ContentHash};
use super::messages::LifecycleCorrelation;
use super::mobility::FlowMobilitySnapshot;
use super::snapshot::NodeExecutionSnapshot;
use super::types::{
    FlowDefinition, FlowError, FlowExecutionId, FlowExecutionStatus, FlowId, FlowInvocationKind, FlowWorkspaceMapping,
    ScriptSourceLocation, WorkspaceError,
};

/// Complete application-facing context for one Flow execution hook step.
#[derive(Clone, Debug)]
pub struct FlowAppHookContext {
    pub execution_id: FlowExecutionId,
    pub flow_id: FlowId,
    pub definition_revision: i64,
    pub execution_root: PathBuf,
    pub invocation_kind: FlowInvocationKind,
}

/// Inputs for preparing a newly started execution workspace.
#[derive(Clone, Debug)]
pub struct FlowAppTakeoffContext {
    pub execution: FlowAppHookContext,
    pub definition: FlowDefinition,
    pub pulse: serde_json::Value,
}

/// Inputs for capturing one execution workspace checkpoint.
#[derive(Clone, Debug)]
pub struct FlowAppCheckpointContext {
    /// Execution and workspace identity.
    pub execution: FlowAppHookContext,
    /// Path-to-hash projection from the previous committed checkpoint.
    pub previous_entries: BTreeMap<String, ContentHash>,
    /// Sequence number of the checkpoint being captured.
    pub sequence: u64,
}

/// Inputs for restoring an execution workspace after a restart.
#[derive(Clone, Debug)]
pub struct FlowAppRestoreContext {
    pub execution: FlowAppHookContext,
    pub definition: FlowDefinition,
    pub workspace_checkpoint: Option<ContentAddressedRoot>,
    pub workspace_entries: BTreeMap<String, ContentHash>,
    pub imported_workspace_root: Option<ContentAddressedRoot>,
}

/// Identity of a checkpoint that has been durably saved.
#[derive(Clone, Debug)]
pub struct FlowAppCheckpointCommittedContext {
    pub execution: FlowAppHookContext,
    pub sequence: u64,
    /// Previous durable checkpoint pin to retire, if one exists.
    pub superseded_sequence: Option<u64>,
}

/// Context delivered when an execution reaches a terminal outcome.
#[derive(Clone, Debug)]
pub struct FlowAppLandingContext {
    pub execution: FlowAppHookContext,
    pub status: FlowExecutionStatus,
    pub correlations: Vec<LifecycleCorrelation>,
    pub workspace_mappings: Vec<FlowWorkspaceMapping>,
    pub imported_workspace_root: Option<ContentAddressedRoot>,
}

/// Workspace selected by an application for a newly started execution.
#[derive(Clone, Debug)]
pub struct FlowAppTakeoffResult {
    /// Directory used as the execution root by Flow nodes.
    pub execution_root: PathBuf,
    /// Optional imported workspace content associated with the execution.
    pub imported_workspace_root: Option<ContentAddressedRoot>,
}

/// Async, opt-in lifecycle integration supplied by an embedding application.
pub trait FlowAppWorkspaceHook: Send + Sync {
    /// Imports host-local content through the selected workspace implementation.
    fn import_path(&self, _path: PathBuf) -> FlowWorkspaceFuture<'_, ContentAddressedRoot> {
        Box::pin(async { Err(WorkspaceError::ProviderUnavailable.into()) })
    }
    /// Downloads immutable content through the selected workspace implementation.
    fn download_collection_ticket(&self, _ticket: String) -> FlowWorkspaceFuture<'_, ContentAddressedRoot> {
        Box::pin(async { Err(WorkspaceError::ProviderUnavailable.into()) })
    }
    /// Saves script text using implementation-owned storage.
    fn save_script_source(
        &self,
        _location: ScriptSourceLocation,
        _path: String,
        _source: String,
    ) -> FlowWorkspaceFuture<'_, Option<ContentAddressedPath>> {
        Box::pin(async { Err(WorkspaceError::ProviderUnavailable.into()) })
    }
    /// Reads script text using implementation-owned storage.
    fn read_script_source(&self, _location: ScriptSourceLocation, _path: String) -> FlowWorkspaceFuture<'_, String> {
        Box::pin(async { Err(WorkspaceError::ProviderUnavailable.into()) })
    }
    /// Enriches execution detail with workspace-owned artifact observations.
    fn project_artifacts<'a>(
        &'a self,
        _execution_id: FlowExecutionId,
        _nodes: &'a mut [NodeExecutionSnapshot],
    ) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Checks whether a persisted start completed workspace preparation.
    fn recoverable(&self, _context: FlowAppHookContext, _status: FlowExecutionStatus) -> FlowWorkspaceFuture<'_, bool> {
        Box::pin(async { Ok(true) })
    }
    /// Resolves the workspace root without preparing its contents.
    fn execution_root(&self, context: FlowAppHookContext) -> FlowWorkspaceFuture<'_, PathBuf> {
        Box::pin(async move { Ok(context.execution_root) })
    }
    /// Releases implementation-owned runtime resources without deleting executions.
    fn shutdown(&self) -> FlowWorkspaceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Adds implementation-owned content references to a transfer snapshot.
    fn export_mobility<'a>(&'a self, snapshot: &'a mut FlowMobilitySnapshot) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move { unsupported_mobility(snapshot) })
    }
    /// Stages transferred content before destination activation.
    fn import_mobility<'a>(
        &'a self,
        snapshot: &'a FlowMobilitySnapshot,
        _source: MachineIssuerId,
    ) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move { unsupported_mobility(snapshot) })
    }
    /// Releases transferred content after destination activation.
    fn release_mobility<'a>(&'a self, snapshot: &'a FlowMobilitySnapshot) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move { unsupported_mobility(snapshot) })
    }
    /// Captures the execution workspace at a checkpoint. Defaults to an empty capture.
    fn checkpoint(&self, _context: FlowAppCheckpointContext) -> FlowAppCheckpointFuture<'_> {
        Box::pin(async { Ok(FlowAppCheckpointResult::default()) })
    }
    /// Restores the execution workspace and selects the root for node dispatch.
    fn restore(&self, context: FlowAppRestoreContext) -> FlowAppRestoreFuture<'_> {
        Box::pin(async move { Ok(context.execution.execution_root) })
    }
    /// Finalizes a checkpoint after its snapshot has been durably saved.
    fn checkpoint_committed(&self, _context: FlowAppCheckpointCommittedContext) -> FlowAppHookFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Prepares the execution workspace and returns the root to use for node dispatch.
    fn takeoff(&self, context: FlowAppTakeoffContext) -> FlowAppTakeoffFuture<'_>;
    fn landing(&self, context: FlowAppLandingContext) -> FlowAppHookFuture<'_>;
    fn release_artifacts(&self, context: FlowAppHookContext) -> FlowAppHookFuture<'_>;
}

/// Captured workspace root and its path-to-content projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FlowAppCheckpointResult {
    /// Optional content-addressed checkpoint root.
    pub workspace_checkpoint: Option<ContentAddressedRoot>,
    /// Content hashes indexed by workspace-relative path.
    pub workspace_entries: BTreeMap<String, ContentHash>,
}

/// Future returned by the checkpoint step.
pub type FlowAppCheckpointFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlowAppCheckpointResult, FlowAppHookError>> + Send + 'a>>;

/// Future returned by the workspace restore step.
pub type FlowAppRestoreFuture<'a> = Pin<Box<dyn Future<Output = Result<PathBuf, FlowAppHookError>> + Send + 'a>>;

/// Future returned by the application takeoff step.
pub type FlowAppTakeoffFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlowAppTakeoffResult, FlowAppHookError>> + Send + 'a>>;

/// Future returned by an application workspace-hook step.
pub type FlowAppHookFuture<'a> = Pin<Box<dyn Future<Output = Result<(), FlowAppHookError>> + Send + 'a>>;

/// Failure returned by an application workspace-hook step.
pub type FlowAppHookError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Future for workspace operations using Flow's typed errors.
pub type FlowWorkspaceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, FlowError>> + Send + 'a>>;

fn unsupported_mobility(snapshot: &FlowMobilitySnapshot) -> Result<(), FlowError> {
    match snapshot.executions.first() {
        Some(execution) => Err(FlowError::AppWorkspaceMobilityUnsupported {
            execution_id: execution.execution.execution_id,
        }),
        None => Ok(()),
    }
}

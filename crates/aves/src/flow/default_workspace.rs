use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};

use crate::auth::MachineIssuerId;
use crate::machine::MachinePeerServices;
use crate::runtime::ServiceReader;

use self::native::WorkspaceStore;
use super::ast::{ContentAddressedPath, ContentAddressedRoot};
use super::database::{ExecutionDocumentLink, FlowStore};
use super::graph::FlowGraph;
use super::hook::{
    FlowAppCheckpointCommittedContext, FlowAppCheckpointContext, FlowAppCheckpointFuture, FlowAppCheckpointResult,
    FlowAppHookContext, FlowAppHookFuture, FlowAppLandingContext, FlowAppRestoreContext, FlowAppRestoreFuture,
    FlowAppTakeoffContext, FlowAppTakeoffFuture, FlowAppTakeoffResult, FlowAppWorkspaceHook, FlowWorkspaceFuture,
};
use super::mobility::FlowMobilitySnapshot;
use super::registration::{FlowNodeRegistry, WorkspaceSeed};
use super::snapshot::{ExecutionSnapshotStore, NodeExecutionSnapshot};
use super::types::{
    FlowError, FlowExecutionId, FlowExecutionStatus, FlowWorkspaceMapping, FlowWorkspaceSourceService,
    ScriptSourceLocation, WorkspaceError,
};

mod files;
mod native;
mod replication;
mod storage;

/// Native lifecycle implementation shared by all executions.
pub(super) struct DefaultWorkspace {
    workspace: Arc<WorkspaceStore>,
    registry: Arc<FlowNodeRegistry>,
    source_service: ServiceReader<dyn FlowWorkspaceSourceService>,
    store: FlowStore,
    snapshots: ExecutionSnapshotStore,
    executions: Mutex<HashMap<FlowExecutionId, Arc<NativeExecution>>>,
}

#[derive(Default)]
struct NativeExecution {
    takeoff: OnceCell<NativeTakeoff>,
    document_committed: OnceCell<()>,
}

struct NativeTakeoff {
    result: FlowAppTakeoffResult,
    document: ExecutionDocumentLink,
}

impl DefaultWorkspace {
    #[cfg(test)]
    pub(super) fn with_native_fixture(mut self, fixture: super::fixtures::NativeDocsFixture) -> Self {
        Arc::get_mut(&mut self.workspace)
            .expect("test workspace is not shared yet")
            .attach_native_fixture(fixture);
        self
    }
    pub(super) async fn new(
        data_root: PathBuf,
        machine: &MachinePeerServices,
        registry: Arc<FlowNodeRegistry>,
        source_service: ServiceReader<dyn FlowWorkspaceSourceService>,
        store: FlowStore,
        snapshots: ExecutionSnapshotStore,
    ) -> Result<Self, FlowError> {
        Ok(Self {
            workspace: Arc::new(
                WorkspaceStore::open(
                    &data_root,
                    machine.blobs.clone(),
                    machine.native.clone(),
                    machine.connectivity.clone(),
                )
                .await?,
            ),
            registry,
            source_service,
            store,
            snapshots,
            executions: Mutex::new(HashMap::new()),
        })
    }

    async fn execution(&self, id: FlowExecutionId) -> Arc<NativeExecution> {
        self.executions.lock().await.entry(id).or_default().clone()
    }

    async fn prepare(&self, context: FlowAppTakeoffContext) -> Result<FlowAppTakeoffResult, FlowError> {
        let state = self.execution(context.execution.execution_id).await;
        let prepared = state.takeoff.get_or_try_init(|| async {
            let graph = FlowGraph::load_json(&serde_json::to_string(&context.definition.graph)?)?;
            let seeds = self.seeds_for_graph(&graph).await?;
            let execution_id = context.execution.execution_id;
            let prepared = self.workspace.prepare_execution_workspace(
                execution_id,
                &graph,
                &context.definition.workspace_mappings,
                &context.pulse,
                context.execution.invocation_kind,
                &self.source_service,
                &self.registry,
            ).await?;
            if let Err(error) = files::seed_workspace_seeds(&seeds, &prepared.0) {
                if let Err(cleanup_error) = self.workspace.discard_unlinked_execution(execution_id).await {
                    tracing::warn!(error = ?cleanup_error, "failed to discard unlinked Flow execution after seeding failure");
                }
                return Err(error.into());
            }
            let document = self.workspace.create_execution_document().await?;
            Ok::<NativeTakeoff, FlowError>(NativeTakeoff {
                result: FlowAppTakeoffResult {
                    execution_root: prepared.0,
                    imported_workspace_root: prepared.1,
                },
                document,
            })
        }).await?;
        Ok(prepared.result.clone())
    }

    async fn capture(&self, context: FlowAppCheckpointContext) -> Result<FlowAppCheckpointResult, FlowError> {
        let state = self.execution(context.execution.execution_id).await;
        let document = if state.document_committed.get().is_none() {
            match state.takeoff.get() {
                Some(prepared) => prepared.document.clone(),
                None => {
                    self.store
                        .load_execution_document(context.execution.execution_id)
                        .await?
                }
            }
        } else {
            self.store
                .load_execution_document(context.execution.execution_id)
                .await?
        };
        let (checkpoint, entries) = self
            .workspace
            .capture_checkpoint(
                context.execution.execution_id,
                &context.execution.execution_root,
                &document,
                &context.previous_entries,
                context.sequence,
            )
            .await?;
        Ok(FlowAppCheckpointResult {
            workspace_checkpoint: Some(checkpoint),
            workspace_entries: entries,
        })
    }

    async fn commit(&self, context: FlowAppCheckpointCommittedContext) -> Result<(), FlowError> {
        let state = self.execution(context.execution.execution_id).await;
        if let Some(prepared) = state.takeoff.get() {
            state
                .document_committed
                .get_or_try_init(|| async {
                    if let Err(cause) = self
                        .store
                        .create_execution_document(context.execution.execution_id, &prepared.document)
                        .await
                    {
                        self.workspace
                            .compensate_failed_start(
                                context.execution.execution_id,
                                &prepared.result.execution_root,
                                &prepared.document,
                            )
                            .await;
                        return Err(cause);
                    }
                    Ok::<(), FlowError>(())
                })
                .await?;
        }
        if let Some(sequence) = context.superseded_sequence {
            self.workspace
                .release_checkpoint_pin(context.execution.execution_id, sequence)
                .await?;
        }
        Ok(())
    }

    async fn restore_native(&self, context: FlowAppRestoreContext) -> Result<PathBuf, FlowError> {
        let graph = FlowGraph::load_json(&serde_json::to_string(&context.definition.graph)?)?;
        let document = self
            .store
            .load_execution_document(context.execution.execution_id)
            .await?;
        let mut snapshot = self
            .snapshots
            .load_all()
            .await?
            .into_iter()
            .find(|snapshot| snapshot.execution.execution_id == context.execution.execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        snapshot.workspace_checkpoint = context.workspace_checkpoint;
        snapshot.workspace_entries = context.workspace_entries;
        snapshot.imported_workspace_root = context.imported_workspace_root;
        self.workspace
            .restore_execution_workspace(&snapshot, &graph, &document, &self.registry)
            .await
    }

    async fn seeds_for_graph(&self, graph: &FlowGraph) -> Result<Vec<WorkspaceSeed>, FlowError> {
        let mut seeds = Vec::new();
        for registration in self.registry.iter() {
            let mut configurations = Vec::new();
            graph.ast().root.for_each_leaf(&mut |node| {
                if node.node_kind == registration.descriptor.kind {
                    configurations.push(node.configuration.clone());
                }
            });
            seeds.extend(registration.graph_seeds(configurations).await?);
        }
        Ok(seeds)
    }
}

impl FlowAppWorkspaceHook for DefaultWorkspace {
    fn import_path(&self, path: PathBuf) -> FlowWorkspaceFuture<'_, ContentAddressedRoot> {
        Box::pin(async move { Ok(self.workspace.import_path(path).await?) })
    }

    fn download_collection_ticket(&self, ticket: String) -> FlowWorkspaceFuture<'_, ContentAddressedRoot> {
        Box::pin(async move { Ok(self.workspace.download_collection_ticket(ticket).await?) })
    }

    fn save_script_source(
        &self,
        location: ScriptSourceLocation,
        path: String,
        source: String,
    ) -> FlowWorkspaceFuture<'_, Option<ContentAddressedPath>> {
        Box::pin(async move {
            self.workspace
                .save_script_source(location, path, source, &self.source_service)
                .await
        })
    }

    fn read_script_source(&self, location: ScriptSourceLocation, path: String) -> FlowWorkspaceFuture<'_, String> {
        Box::pin(async move {
            self.workspace
                .read_script_source(location, path, &self.source_service)
                .await
        })
    }

    fn project_artifacts<'a>(
        &'a self,
        execution_id: FlowExecutionId,
        nodes: &'a mut [NodeExecutionSnapshot],
    ) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move {
            Ok(WorkspaceStore::project_artifacts(
                &self.workspace.execution_root(execution_id),
                nodes,
            )?)
        })
    }

    fn execution_root(&self, context: FlowAppHookContext) -> FlowWorkspaceFuture<'_, PathBuf> {
        Box::pin(async move { Ok(self.workspace.execution_root(context.execution_id)) })
    }

    fn recoverable(&self, context: FlowAppHookContext, status: FlowExecutionStatus) -> FlowWorkspaceFuture<'_, bool> {
        Box::pin(async move {
            if status.is_terminal()
                || self
                    .store
                    .load_execution_document_optional(context.execution_id)
                    .await?
                    .is_some()
            {
                return Ok(true);
            }
            self.workspace
                .discard_unlinked_execution(context.execution_id)
                .await?;
            Ok(false)
        })
    }

    fn shutdown(&self) -> FlowWorkspaceFuture<'_, ()> {
        Box::pin(async move { Ok(self.workspace.shutdown().await?) })
    }

    fn export_mobility<'a>(&'a self, snapshot: &'a mut FlowMobilitySnapshot) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move {
            snapshot.native_references.clear();
            for execution in &snapshot.executions {
                let document = self
                    .store
                    .load_execution_document(execution.execution.execution_id)
                    .await?;
                snapshot
                    .native_references
                    .push(WorkspaceStore::native_references(execution, &document));
            }
            Ok(())
        })
    }

    fn import_mobility<'a>(
        &'a self,
        snapshot: &'a FlowMobilitySnapshot,
        source: MachineIssuerId,
    ) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move { self.import_transferred_workspace(snapshot, source).await })
    }

    fn release_mobility<'a>(&'a self, snapshot: &'a FlowMobilitySnapshot) -> FlowWorkspaceFuture<'a, ()> {
        Box::pin(async move {
            for reference in &snapshot.native_references {
                self.release_transferred_workspace(reference).await?;
            }
            Ok(())
        })
    }
    fn takeoff(&self, context: FlowAppTakeoffContext) -> FlowAppTakeoffFuture<'_> {
        Box::pin(async move { self.prepare(context).await.map_err(Into::into) })
    }

    fn checkpoint(&self, context: FlowAppCheckpointContext) -> FlowAppCheckpointFuture<'_> {
        Box::pin(async move { self.capture(context).await.map_err(Into::into) })
    }

    fn checkpoint_committed(&self, context: FlowAppCheckpointCommittedContext) -> FlowAppHookFuture<'_> {
        Box::pin(async move { self.commit(context).await.map_err(Into::into) })
    }

    fn restore(&self, context: FlowAppRestoreContext) -> FlowAppRestoreFuture<'_> {
        Box::pin(async move { self.restore_native(context).await.map_err(Into::into) })
    }

    fn landing(&self, context: FlowAppLandingContext) -> FlowAppHookFuture<'_> {
        Box::pin(async move {
            if context.status == FlowExecutionStatus::Aborted {
                return Ok(());
            }
            self.workspace
                .project_mapped_workspace(
                    context.execution.execution_id,
                    &context.workspace_mappings,
                    context.imported_workspace_root.as_ref(),
                    &self.source_service,
                )
                .await
                .map_err(Into::into)
        })
    }

    fn release_artifacts(&self, context: FlowAppHookContext) -> FlowAppHookFuture<'_> {
        Box::pin(async move {
            let state = self.execution(context.execution_id).await;
            let document = match self
                .store
                .load_execution_document_optional(context.execution_id)
                .await?
            {
                Some(document) => Some(document),
                None => state
                    .takeoff
                    .get()
                    .map(|prepared| prepared.document.clone()),
            };
            self.workspace
                .cleanup_execution(context.execution_id, document.as_ref())
                .await?;
            self.executions.lock().await.remove(&context.execution_id);
            Ok(())
        })
    }
}

pub(super) fn validate_mappings(mappings: &[FlowWorkspaceMapping]) -> Result<(), WorkspaceError> {
    let mut destinations: Vec<PathBuf> = Vec::new();
    for mapping in mappings {
        let destination = files::mapping_destination(&mapping.destination)?;
        for previous in &destinations {
            if destination.starts_with(previous) || previous.starts_with(&destination) {
                return Err(WorkspaceError::OverlappingMappings {
                    first: previous.display().to_string(),
                    second: destination.display().to_string(),
                });
            }
        }
        destinations.push(destination);
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn seed_workspace_seeds(seeds: &[WorkspaceSeed], root: &std::path::Path) -> Result<(), WorkspaceError> {
    files::seed_workspace_seeds(seeds, root)
}

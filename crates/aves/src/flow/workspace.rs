use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iroh_blobs::store::fs::FsStore;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::bus::PeerConnectivity;
use crate::runtime::ServiceReader;

use super::ast::{ContentAddressedPath, ContentAddressedRoot, NodeDataSpace};
use super::database::ExecutionDocumentLink;
use super::files::{collect_artifact_paths, collect_files, mapping_destination, validate_mappings};
use super::graph::FlowGraph;
use super::mobility::FlowExecutionNativeReferences;
use super::registration::FlowNodeRegistry;
use super::snapshot::{ExecutionSnapshot, NodeExecutionSnapshot};
use super::storage::{EXECUTION_PATH, anyhow_document_api_error};
use super::sync::{FlowNativeServices, FlowSyncServices};
use super::types::{
    FlowError, FlowExecutionId, FlowInvocationKind, FlowNodeId, FlowWorkspaceMapping, FlowWorkspaceMappingMode,
    FlowWorkspaceSourceError, FlowWorkspaceSourceService, ScriptSourceLocation, WorkspaceError,
};

#[cfg(test)]
use super::fixtures::NativeDocsFixture;

/// Owns execution paths and coordinates storage, native sync, and projection services.
pub(super) struct WorkspaceStore {
    data_root: PathBuf,
    pub(super) store: FsStore,
    pub(super) native: Arc<RwLock<FlowNativeServices>>,
    pub(super) connectivity: PeerConnectivity,
    #[cfg(test)]
    _native_fixture: Option<NativeDocsFixture>,
}

/// A content root or selected path to project into one execution directory.
pub(super) enum WorkspaceInput {
    Root {
        root: ContentAddressedRoot,
        role: &'static str,
    },
    Path {
        root: ContentAddressedRoot,
        path: String,
        role: &'static str,
    },
}

pub(super) fn roots(
    graph: &FlowGraph,
    registry: &FlowNodeRegistry,
) -> Result<(Vec<ContentAddressedRoot>, Vec<ContentAddressedRoot>), FlowError> {
    let mut code = BTreeMap::new();
    let mut state = BTreeMap::new();
    let mut error = None;
    graph.ast().root.for_each_leaf(&mut |node| {
        match registry.get(&node.node_kind) {
            Ok(registration) => {
                for root in registration.node_content_roots(&node.configuration) {
                    code.insert(root.digest.as_str().to_owned(), root);
                }
            }
            Err(failure) => error = Some(FlowError::from(failure)),
        }
        let data_space = node.data_space();
        for_each_data_space_path(data_space, |role, path| {
            let roots = if role == "code" { &mut code } else { &mut state };
            roots.insert(path.root.digest.as_str().to_owned(), path.root.clone());
        });
    });
    error.map_or_else(
        || Ok((code.into_values().collect(), state.into_values().collect())),
        Err,
    )
}

pub(super) fn inputs(graph: &FlowGraph, registry: &FlowNodeRegistry) -> Result<Vec<WorkspaceInput>, FlowError> {
    let mut inputs = Vec::new();
    let mut error = None;
    graph.ast().root.for_each_leaf(&mut |node| {
        match registry.get(&node.node_kind) {
            Ok(registration) => inputs.extend(
                registration
                    .node_content_roots(&node.configuration)
                    .into_iter()
                    .map(|root| WorkspaceInput::Root { root, role: "code" }),
            ),
            Err(failure) => error = Some(FlowError::from(failure)),
        }
        for_each_data_space_path(node.data_space(), |role, path| {
            inputs.push(WorkspaceInput::Path {
                root: path.root.clone(),
                path: path.path.clone(),
                role,
            });
        });
    });
    error.map_or_else(|| Ok(inputs), Err)
}

fn for_each_data_space_path(data_space: &NodeDataSpace, mut visit: impl FnMut(&'static str, &ContentAddressedPath)) {
    for path in &data_space.code_paths {
        visit("code", path);
    }
    for path in &data_space.state_paths {
        visit("state", path);
    }
}

impl WorkspaceStore {
    pub(super) fn native_references(
        snapshot: &ExecutionSnapshot,
        document: &ExecutionDocumentLink,
    ) -> FlowExecutionNativeReferences {
        FlowExecutionNativeReferences {
            execution_id: snapshot.execution.execution_id,
            document_namespace_id: document.namespace_id.clone(),
            document_author_id: document.author_id.clone(),
            code_roots: snapshot.code_roots.clone(),
            state_roots: snapshot.state_roots.clone(),
            workspace_checkpoint: snapshot.workspace_checkpoint.clone(),
            workspace_entries: snapshot.workspace_entries.clone(),
            imported_workspace_root: snapshot.imported_workspace_root.clone(),
        }
    }

    pub(super) async fn open(data_root: &Path, sync_services: FlowSyncServices) -> Result<Self, WorkspaceError> {
        Ok(Self {
            data_root: data_root.to_path_buf(),
            store: sync_services.blobs,
            native: sync_services.native,
            connectivity: sync_services.connectivity,
            #[cfg(test)]
            _native_fixture: None,
        })
    }

    pub(super) fn execution_root(&self, execution_id: FlowExecutionId) -> PathBuf {
        self.data_root
            .join("executions")
            .join(execution_id.to_string())
    }

    // This setup boundary receives the graph, mappings, trigger, and owner resolver as distinct inputs.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_execution_workspace(
        &self,
        execution_id: FlowExecutionId,
        graph: &FlowGraph,
        mappings: &[FlowWorkspaceMapping],
        pulse: &Value,
        invocation_kind: FlowInvocationKind,
        source_service: &ServiceReader<dyn FlowWorkspaceSourceService>,
        registry: &FlowNodeRegistry,
    ) -> Result<(PathBuf, Option<ContentAddressedRoot>), FlowError> {
        validate_mappings(mappings)?;
        let imported_workspace_root = if !mappings.is_empty() {
            let resolver = source_service
                .get()
                .ok_or(FlowWorkspaceSourceError::BackendUnavailable)?;
            let mut entries = Vec::new();
            for mapping in mappings {
                let source = resolver.resolve_known_directory(&mapping.directory)?;
                let destination = mapping_destination(&mapping.destination)?;
                let mut files = Vec::new();
                collect_files(&source, &source, &mut files)?;
                entries.extend(
                    files
                        .into_iter()
                        .map(|(relative, file)| (destination.join(relative), file)),
                );
            }
            Some(
                self.import_entries_with_tag(&entries, &format!("{EXECUTION_PATH}/{execution_id}/mapped"))
                    .await?,
            )
        } else {
            None
        };
        let execution_root = self.execution_root(execution_id);
        tokio::fs::create_dir_all(&execution_root)
            .await
            .map_err(|source| FlowError::ExecutionDirectory {
                path: execution_root.clone(),
                source,
            })?;
        self.materialize_execution_inputs(
            execution_id,
            &execution_root,
            graph,
            imported_workspace_root.as_ref(),
            registry,
        )
        .await?;
        for mapping in mappings {
            let path = execution_root.join(mapping_destination(&mapping.destination)?);
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|source| WorkspaceError::io(path, source))?;
        }
        self.create_artifact_directories(&execution_root, graph)?;
        self.write_trigger(&execution_root, pulse, invocation_kind)
            .await?;
        Ok((execution_root, imported_workspace_root))
    }

    async fn write_trigger(
        &self,
        execution_root: &Path,
        pulse: &Value,
        invocation_kind: FlowInvocationKind,
    ) -> Result<(), FlowError> {
        let data = if pulse.is_object() {
            pulse.clone()
        } else {
            serde_json::json!({ "value": pulse })
        };
        let workflow = execution_root.join("workflow");
        tokio::fs::create_dir_all(&workflow)
            .await
            .map_err(|source| FlowError::Workspace(WorkspaceError::io(workflow.clone(), source)))?;
        let trigger = workflow.join("trigger.json");
        tokio::fs::write(
            &trigger,
            serde_json::to_vec(&serde_json::json!({
                "kind": invocation_kind,
                "triggeredAt": chrono::Utc::now().to_rfc3339(),
                "data": data,
            }))?,
        )
        .await
        .map_err(|source| FlowError::Workspace(WorkspaceError::io(trigger, source)))?;
        Ok(())
    }

    pub(super) async fn restore_execution_workspace(
        &self,
        snapshot: &ExecutionSnapshot,
        graph: &FlowGraph,
        document: &ExecutionDocumentLink,
        registry: &FlowNodeRegistry,
    ) -> Result<PathBuf, FlowError> {
        self.reopen_execution_document(document).await?;
        let execution_root = self.execution_root(snapshot.execution.execution_id);
        if let Some(checkpoint) = &snapshot.workspace_checkpoint {
            self.materialize_clean(
                checkpoint,
                &execution_root,
                &format!(
                    "{EXECUTION_PATH}/{}/checkpoint/recovery",
                    snapshot.execution.execution_id
                ),
            )
            .await?;
        } else {
            self.materialize_inputs_clean(
                snapshot.execution.execution_id,
                &execution_root,
                &inputs(graph, registry)?,
            )
            .await?;
        }
        self.create_artifact_directories(&execution_root, graph)?;
        Ok(execution_root)
    }

    fn create_artifact_directories(&self, execution_root: &Path, graph: &FlowGraph) -> Result<(), FlowError> {
        let artifacts_root = execution_root.join("artifacts");
        let mut node_ids = Vec::<FlowNodeId>::new();
        graph
            .ast()
            .root
            .for_each_leaf(&mut |node| node_ids.push(node.id()));
        for node_id in node_ids {
            let path = artifacts_root.join(node_id.to_string());
            std::fs::create_dir_all(&path).map_err(|source| WorkspaceError::io(path, source))?;
        }
        Ok(())
    }

    pub(super) fn project_artifacts(
        &self,
        execution_root: &Path,
        nodes: &mut [NodeExecutionSnapshot],
    ) -> Result<(), WorkspaceError> {
        for node in nodes {
            let directory = execution_root
                .join("artifacts")
                .join(node.flow_node_id.to_string());
            let mut paths = Vec::new();
            collect_artifact_paths(execution_root, &directory, &mut paths)?;
            node.artifacts = paths;
        }
        Ok(())
    }

    pub(super) async fn compensate_failed_start(
        &self,
        execution_id: FlowExecutionId,
        execution_root: &Path,
        document: &ExecutionDocumentLink,
    ) {
        if let Err(error) = self.cleanup_execution_directory(execution_root).await {
            tracing::warn!(error = ?error, "failed to clean up Flow execution directory");
        }
        if let Err(error) = self.remove_execution_document(document).await {
            tracing::warn!(error = ?error, "failed to remove Flow execution document");
        }
        if let Err(error) = self.release_execution_pins(execution_id).await {
            tracing::warn!(error = ?error, "failed to release Flow execution pins");
        }
    }

    pub(super) async fn discard_unlinked_execution(&self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        self.cleanup_execution_directory(&self.execution_root(execution_id))
            .await?;
        self.release_execution_pins(execution_id).await?;
        Ok(())
    }

    pub(super) async fn cleanup_execution(
        &self,
        execution_id: FlowExecutionId,
        document: Option<&ExecutionDocumentLink>,
    ) -> Result<(), FlowError> {
        self.cleanup_execution_directory(&self.execution_root(execution_id))
            .await?;
        if let Some(document) = document {
            self.remove_execution_document(document).await?;
        }
        self.release_execution_pins(execution_id).await?;
        Ok(())
    }

    pub(super) async fn read_script_source(
        &self,
        location: ScriptSourceLocation,
        path: String,
        source_service: &ServiceReader<dyn FlowWorkspaceSourceService>,
    ) -> Result<String, FlowError> {
        match location {
            ScriptSourceLocation::New => Err(WorkspaceError::SourceNotStored.into()),
            ScriptSourceLocation::Mapped { directory } => {
                let root = source_service
                    .get()
                    .ok_or(FlowWorkspaceSourceError::BackendUnavailable)?
                    .resolve_known_directory(&directory)?;
                Ok(self.read_source_text(&root, &path)?)
            }
            ScriptSourceLocation::Stored { root } => Ok(self.read_collection_text(&root, &path).await?),
        }
    }

    pub(super) async fn save_script_source(
        &self,
        location: ScriptSourceLocation,
        path: String,
        source: String,
        source_service: &ServiceReader<dyn FlowWorkspaceSourceService>,
    ) -> Result<Option<ContentAddressedPath>, FlowError> {
        match location {
            ScriptSourceLocation::Mapped { directory } => {
                let root = source_service
                    .get()
                    .ok_or(FlowWorkspaceSourceError::BackendUnavailable)?
                    .resolve_known_directory(&directory)?;
                self.write_source_text(&root, &path, &source)?;
                Ok(None)
            }
            ScriptSourceLocation::New => Ok(Some(self.import_text_at(&path, &source).await?)),
            ScriptSourceLocation::Stored { root } => {
                Ok(Some(self.replace_collection_text(&root, &path, &source).await?))
            }
        }
    }

    pub(super) async fn materialize_execution_inputs(
        &self,
        execution_id: FlowExecutionId,
        execution_root: &Path,
        graph: &FlowGraph,
        imported_workspace_root: Option<&ContentAddressedRoot>,
        registry: &FlowNodeRegistry,
    ) -> Result<(), FlowError> {
        let mut inputs = inputs(graph, registry)?;
        if let Some(root) = imported_workspace_root {
            inputs.push(WorkspaceInput::Root {
                root: root.clone(),
                role: "mapped",
            });
        }
        self.materialize_inputs(execution_id, execution_root, &inputs)
            .await?;
        Ok(())
    }

    pub(super) async fn project_mapped_workspace(
        &self,
        execution_id: FlowExecutionId,
        mappings: &[FlowWorkspaceMapping],
        imported_workspace_root: Option<&ContentAddressedRoot>,
        source_service: &ServiceReader<dyn FlowWorkspaceSourceService>,
    ) -> Result<(), FlowError> {
        validate_mappings(mappings)?;
        if mappings
            .iter()
            .all(|mapping| mapping.mode == FlowWorkspaceMappingMode::ReadOnly)
        {
            return Ok(());
        }
        let imported_paths = match imported_workspace_root {
            Some(root) => self.collection_paths(root).await?,
            None => std::collections::BTreeSet::new(),
        };
        for mapping in mappings {
            if mapping.mode == FlowWorkspaceMappingMode::ReadOnly {
                continue;
            }
            let source = source_service
                .get()
                .ok_or(FlowWorkspaceSourceError::BackendUnavailable)?
                .resolve_known_directory(&mapping.directory)?;
            let destination = mapping_destination(&mapping.destination)?;
            let mut output = self.execution_root(execution_id);
            for component in destination.components() {
                output.push(component.as_os_str());
                if std::fs::symlink_metadata(&output).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
                    return Err(WorkspaceError::SymbolicLink { path: output }.into());
                }
            }
            let baseline = imported_paths
                .iter()
                .filter_map(|path| path.strip_prefix(&destination).ok().map(Path::to_path_buf))
                .collect();
            self.project_directory(
                &output,
                &source,
                &baseline,
                mapping.mode == FlowWorkspaceMappingMode::ReadWriteDelete,
            )?;
        }
        Ok(())
    }

    pub(super) async fn cleanup_execution_directory(&self, execution_root: &Path) -> Result<(), WorkspaceError> {
        if execution_root.exists() {
            std::fs::remove_dir_all(execution_root)
                .map_err(|source| WorkspaceError::io(execution_root.to_path_buf(), source))?;
        }
        Ok(())
    }

    /// Flushes and closes the local blob store before its containing Flow actor exits.
    pub(super) async fn shutdown(&self) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        let documents = native
            .open_documents
            .lock()
            .await
            .drain()
            .map(|(_, document)| document)
            .collect::<Vec<_>>();
        for document in documents {
            document.close().await.map_err(anyhow_document_api_error)?;
        }
        self.store
            .wait_idle()
            .await
            .map_err(|source| WorkspaceError::Blob {
                source: Box::new(source),
            })?;
        self.store
            .sync_db()
            .await
            .map_err(|source| WorkspaceError::Blob {
                source: Box::new(source),
            })?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn attach_native_fixture(&mut self, fixture: super::fixtures::NativeDocsFixture) {
        self._native_fixture = Some(fixture);
    }
}

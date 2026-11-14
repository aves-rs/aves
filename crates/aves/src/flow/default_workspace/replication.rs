use std::collections::{HashMap, HashSet};

use iroh::EndpointAddr;

use crate::auth::MachineIssuerId;

use super::super::database::ExecutionDocumentLink;
use super::super::mobility::{FlowExecutionNativeReferences, FlowMobilitySnapshot};
use super::super::snapshot::ExecutionSnapshot;
use super::super::types::{FlowError, FlowExecutionId};
use super::DefaultWorkspace;

impl DefaultWorkspace {
    pub(super) async fn import_transferred_workspace(
        &self,
        snapshot: &FlowMobilitySnapshot,
        source: MachineIssuerId,
    ) -> Result<(), FlowError> {
        let references = references_by_execution(snapshot)?;
        for execution in &snapshot.executions {
            let reference = references
                .get(&execution.execution.execution_id)
                .expect("validated native reference exists");
            if !reference_matches_snapshot(execution, reference) {
                return Err(FlowError::MobilitySnapshotMismatch {
                    flow_id: snapshot.flow_id,
                });
            }
        }
        let source = EndpointAddr::from(source.public_key());
        for execution in &snapshot.executions {
            let reference = references
                .get(&execution.execution.execution_id)
                .expect("validated native reference exists");
            let link = ExecutionDocumentLink {
                namespace_id: reference.document_namespace_id.clone(),
                author_id: reference.document_author_id.clone(),
            };
            self.workspace
                .import_execution_document(&link, source.clone())
                .await?;
            self.workspace
                .import_native_content(reference, source.clone())
                .await?;
            self.workspace
                .materialize_imported_execution(reference)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn release_transferred_workspace(
        &self,
        reference: &FlowExecutionNativeReferences,
    ) -> Result<(), FlowError> {
        if self
            .store
            .document_is_exclusive(
                &reference.document_namespace_id,
                &reference.document_author_id,
                reference.execution_id,
            )
            .await?
        {
            self.workspace
                .remove_execution_document(&ExecutionDocumentLink {
                    namespace_id: reference.document_namespace_id.clone(),
                    author_id: reference.document_author_id.clone(),
                })
                .await?;
        }
        let root = self.workspace.execution_root(reference.execution_id);
        self.workspace.cleanup_execution_directory(&root).await?;
        self.workspace
            .release_execution_pins(reference.execution_id)
            .await?;
        Ok(())
    }
}

fn reference_matches_snapshot(execution: &ExecutionSnapshot, reference: &FlowExecutionNativeReferences) -> bool {
    execution.execution.execution_id == reference.execution_id
        && execution.code_roots == reference.code_roots
        && execution.state_roots == reference.state_roots
        && execution.workspace_checkpoint == reference.workspace_checkpoint
        && execution.workspace_entries == reference.workspace_entries
        && execution.imported_workspace_root == reference.imported_workspace_root
}

fn references_by_execution(
    snapshot: &FlowMobilitySnapshot,
) -> Result<HashMap<FlowExecutionId, &FlowExecutionNativeReferences>, FlowError> {
    if snapshot.validate_selected_flow().is_err() {
        return Err(FlowError::MobilitySnapshotMismatch {
            flow_id: snapshot.flow_id,
        });
    }
    let execution_ids = snapshot
        .executions
        .iter()
        .map(|execution| execution.execution.execution_id)
        .collect::<HashSet<_>>();
    let mut references = HashMap::new();
    for reference in &snapshot.native_references {
        if !execution_ids.contains(&reference.execution_id)
            || references
                .insert(reference.execution_id, reference)
                .is_some()
        {
            return Err(FlowError::MobilitySnapshotMismatch {
                flow_id: snapshot.flow_id,
            });
        }
    }
    if references.len() != execution_ids.len() {
        return Err(FlowError::MobilitySnapshotMismatch {
            flow_id: snapshot.flow_id,
        });
    }
    Ok(references)
}

#[cfg(test)]
mod tests {
    use crate::flow::graph::{FlowGraphAst, GraphNode};
    use crate::flow::types::{FlowDefinition, FlowId};

    use super::*;

    #[test]
    fn selected_flow_rejects_a_definition_for_another_flow() {
        let flow_id = FlowId(uuid::Uuid::new_v4());
        let other_flow_id = FlowId(uuid::Uuid::new_v4());
        let snapshot = FlowMobilitySnapshot {
            flow_id,
            definition: FlowDefinition {
                flow_id: other_flow_id,
                name: "Replica validation".to_owned(),
                revision: 1,
                graph: FlowGraphAst {
                    version: 1,
                    root: GraphNode::Sequence { children: Vec::new() },
                },
                blackboard_defaults: serde_json::json!({}),
                editor_metadata: serde_json::json!({}),
                automatic_enabled: false,
                workspace_mappings: Vec::new(),
            },
            triggers: Vec::new(),
            executions: Vec::new(),
            native_references: Vec::new(),
        };
        assert!(
            matches!(references_by_execution(&snapshot), Err(FlowError::MobilitySnapshotMismatch { flow_id: actual }) if actual == flow_id)
        );
    }
}

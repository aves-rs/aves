use std::collections::{HashMap, HashSet};

use iroh::EndpointAddr;

use crate::auth::MachineIssuerId;

use super::actor::Actor;
use super::database::{ExecutionDocumentLink, FlowStore};
use super::messages::FlowResponse;
use super::mobility::{
    FlowAuthorityState, FlowExecutionNativeReferences, FlowMobilityCommitAcknowledgement, FlowMobilitySnapshot,
    FlowYieldAcknowledgement, mobility_snapshot_generation,
};
use super::snapshot::ExecutionSnapshot;
use super::types::{FlowError, FlowExecutionId, FlowId};
use super::workspace::WorkspaceStore;

impl Actor {
    /// Finalizes source cleanup only after destination activation is acknowledged.
    pub(super) async fn finalize_flow_transfer(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        let authority = self.store.authority_state(flow_id).await?;
        if !matches!(
            authority,
            FlowAuthorityState::Yielded | FlowAuthorityState::CleanupPending
        ) {
            return Err(FlowError::MobilityPromotionRejected {
                state: authority.as_sql().to_owned(),
            });
        }
        let snapshot = self
            .store
            .load_mobility_snapshot(flow_id)
            .await?
            .ok_or_else(|| FlowError::MobilityPromotionRejected {
                state: "snapshot_missing".to_owned(),
            })?;
        let generation = mobility_snapshot_generation(snapshot.definition.revision)?;
        let destination = self
            .mobility
            .destination(flow_id)
            .cloned()
            .ok_or(FlowError::MobilityDestinationNotSelected)?;
        let yield_acknowledgement = FlowYieldAcknowledgement {
            flow_id,
            snapshot_generation: generation,
        };
        let acknowledgement = self
            .mobility_service
            .promote_replica(destination, yield_acknowledgement)
            .await?;
        if acknowledgement.flow_id != flow_id || acknowledgement.snapshot_generation != generation {
            return Err(FlowError::MobilityPromotionRejected {
                state: "activation_acknowledgement_generation_mismatch".to_owned(),
            });
        }
        self.store
            .save_mobility_activation(flow_id, &acknowledgement)
            .await?;
        if authority == FlowAuthorityState::Yielded {
            self.store
                .set_authority_state(flow_id, FlowAuthorityState::CleanupPending)
                .await?;
            self.mobility
                .set_authority_state(flow_id, FlowAuthorityState::CleanupPending);
        }
        for native in snapshot.native_references {
            self.cleanup_native_reference(native).await?;
        }
        self.store.delete_transferred_flow(flow_id).await?;
        self.definitions.remove(&flow_id);
        self.triggers
            .retain(|_, trigger| trigger.flow_id != flow_id);
        Ok(FlowResponse::FlowTransferFinalized { flow_id })
    }

    pub(super) async fn import_selected_flow_native_content(
        &mut self,
        snapshot: FlowMobilitySnapshot,
        source_peer_key: MachineIssuerId,
    ) -> Result<FlowResponse, FlowError> {
        let flow_id = snapshot.flow_id;
        for trigger in &snapshot.triggers {
            self.trigger_registry.validate_configuration(trigger)?;
        }
        if let Some(existing_snapshot) = self.store.load_mobility_snapshot(flow_id).await? {
            if serde_json::to_value(&existing_snapshot)? != serde_json::to_value(&snapshot)? {
                return Err(FlowError::MobilitySnapshotMismatch { flow_id });
            }
            let authority = self.store.authority_state(flow_id).await?;
            if authority != FlowAuthorityState::InactiveCommitted {
                return Err(FlowError::MobilityPromotionRejected {
                    state: authority.as_sql().to_owned(),
                });
            }
            let acknowledgement = FlowMobilityCommitAcknowledgement {
                flow_id,
                snapshot_generation: mobility_snapshot_generation(existing_snapshot.definition.revision)?,
            };
            self.record_import_acknowledgement(acknowledgement.clone());
            return Ok(FlowResponse::FlowMobilityCommitAcknowledgement { acknowledgement });
        }
        NativeFlowReplication::new(&self.workspace, &self.store)
            .import_selected_flow(&snapshot, EndpointAddr::from(source_peer_key.public_key()))
            .await?;
        self.definitions
            .insert(flow_id, snapshot.definition.clone());
        self.triggers.extend(
            snapshot
                .triggers
                .iter()
                .cloned()
                .map(|trigger| (trigger.trigger_id, trigger)),
        );
        self.mobility
            .set_authority_state(flow_id, FlowAuthorityState::InactiveCommitted);
        let acknowledgement = FlowMobilityCommitAcknowledgement {
            flow_id,
            snapshot_generation: mobility_snapshot_generation(snapshot.definition.revision)?,
        };
        self.mobility
            .record_commit_acknowledgement(acknowledgement.clone());
        Ok(FlowResponse::FlowMobilityCommitAcknowledgement { acknowledgement })
    }

    async fn cleanup_native_reference(&self, native: FlowExecutionNativeReferences) -> Result<(), FlowError> {
        if self
            .store
            .document_is_exclusive(
                &native.document_namespace_id,
                &native.document_author_id,
                native.execution_id,
            )
            .await?
        {
            self.workspace
                .remove_execution_document(&ExecutionDocumentLink {
                    namespace_id: native.document_namespace_id,
                    author_id: native.document_author_id,
                })
                .await?;
        }
        let execution_root = self.workspace.execution_root(native.execution_id);
        self.workspace
            .cleanup_execution_directory(&execution_root)
            .await?;
        self.workspace
            .release_execution_pins(native.execution_id)
            .await?;
        Ok(())
    }

    fn record_import_acknowledgement(&mut self, acknowledgement: FlowMobilityCommitAcknowledgement) {
        self.mobility
            .set_authority_state(acknowledgement.flow_id, FlowAuthorityState::InactiveCommitted);
        self.mobility.record_commit_acknowledgement(acknowledgement);
    }
}

/// Stages native execution documents and immutable content for one selected Flow.
///
/// This deliberately does not select a peer, change authority, or activate the
/// imported Flow. Those control-plane decisions remain with the Flow actor.
struct NativeFlowReplication<'a> {
    workspace: &'a WorkspaceStore,
    store: &'a FlowStore,
}

impl<'a> NativeFlowReplication<'a> {
    fn new(workspace: &'a WorkspaceStore, store: &'a FlowStore) -> Self {
        Self { workspace, store }
    }

    /// Imports exactly the native references named by a selected Flow snapshot.
    ///
    /// Every referenced namespace is joined using the shared Docs engine and
    /// every immutable identity is fetched through the shared BlobsProtocol.
    /// Only after all content has arrived is each execution directory replaced
    /// with a clean materialized projection.
    async fn import_selected_flow(
        &self,
        snapshot: &FlowMobilitySnapshot,
        source: EndpointAddr,
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
        for execution in &snapshot.executions {
            let reference = references
                .get(&execution.execution.execution_id)
                .expect("validated native reference exists");
            self.import_execution(reference, source.clone()).await?;
        }
        self.store.commit_imported_flow(snapshot).await?;
        Ok(())
    }

    async fn import_execution(
        &self,
        reference: &FlowExecutionNativeReferences,
        source: EndpointAddr,
    ) -> Result<(), FlowError> {
        let link = ExecutionDocumentLink {
            namespace_id: reference.document_namespace_id.clone(),
            author_id: reference.document_author_id.clone(),
        };
        self.workspace
            .import_execution_document(&link, source.clone())
            .await?;
        self.workspace
            .import_native_content(reference, source)
            .await?;
        self.workspace
            .materialize_imported_execution(reference)
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
    use super::super::graph::{FlowGraphAst, GraphNode};
    use super::super::types::{FlowDefinition, FlowId};

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

        assert!(matches!(
            references_by_execution(&snapshot),
            Err(FlowError::MobilitySnapshotMismatch { flow_id: actual }) if actual == flow_id
        ));
    }
}

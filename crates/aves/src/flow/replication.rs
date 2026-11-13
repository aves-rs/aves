use crate::auth::MachineIssuerId;

use super::actor::Actor;
use super::messages::FlowResponse;
use super::mobility::{
    FlowAuthorityState, FlowMobilityCommitAcknowledgement, FlowMobilitySnapshot, FlowYieldAcknowledgement,
    mobility_snapshot_generation,
};
use super::types::{FlowError, FlowId};

impl Actor {
    /// Finalizes source cleanup only after destination activation is acknowledged.
    pub(super) async fn finalize_flow_transfer(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        self.require_peer_service()?;
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
        self.workspace_hook.release_mobility(&snapshot).await?;
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
        self.require_peer_service()?;
        if let Some(execution) = snapshot
            .executions
            .iter()
            .find(|execution| execution.workspace.is_application())
        {
            return Err(FlowError::AppWorkspaceMobilityUnsupported {
                execution_id: execution.execution.execution_id,
            });
        }
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
        self.workspace_hook
            .import_mobility(&snapshot, source_peer_key)
            .await?;
        self.store.commit_imported_flow(&snapshot).await?;
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

    fn record_import_acknowledgement(&mut self, acknowledgement: FlowMobilityCommitAcknowledgement) {
        self.mobility
            .set_authority_state(acknowledgement.flow_id, FlowAuthorityState::InactiveCommitted);
        self.mobility.record_commit_acknowledgement(acknowledgement);
    }
}

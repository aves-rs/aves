use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
};

use serde::{Deserialize, Serialize};
use specta::Type;

use super::actor::Actor;
use super::ast::{ContentAddressedRoot, ContentHash};
use super::cron::FlowTriggerRecord;
use super::database::PersistedMobilityState;
use super::messages::FlowResponse;
use super::snapshot::ExecutionSnapshot;
use super::types::{
    FlowDefinition, FlowError, FlowExecutionId, FlowFailure, FlowId, FlowMobilityDestination, FlowMobilityReadiness,
};
use super::workspace::WorkspaceStore;

impl Actor {
    pub(super) async fn yield_flow_authority(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        if self.has_active_execution(flow_id) {
            return Err(FlowError::MobilityPromotionRejected {
                state: "active_execution".to_owned(),
            });
        }
        self.mobility
            .destination(flow_id)
            .ok_or(FlowError::MobilityDestinationNotSelected)?;
        let authority = self.store.authority_state(flow_id).await?;
        let snapshot = self
            .store
            .load_mobility_snapshot(flow_id)
            .await?
            .ok_or_else(|| FlowError::MobilityPromotionRejected {
                state: "snapshot_missing".to_owned(),
            })?;
        let snapshot_generation = mobility_snapshot_generation(snapshot.definition.revision)?;
        let acknowledgement =
            self.mobility
                .destination_commit(flow_id)
                .ok_or_else(|| FlowError::MobilityPromotionRejected {
                    state: "destination_commit_acknowledgement_missing".to_owned(),
                })?;
        if acknowledgement.snapshot_generation != snapshot_generation {
            return Err(FlowError::MobilityPromotionRejected {
                state: "destination_commit_acknowledgement_stale".to_owned(),
            });
        }
        let yield_acknowledgement = FlowYieldAcknowledgement {
            flow_id,
            snapshot_generation,
        };
        if authority != FlowAuthorityState::Yielded {
            if !matches!(
                authority,
                FlowAuthorityState::Authoritative | FlowAuthorityState::Synchronizing
            ) {
                return Err(FlowError::MobilityPromotionRejected {
                    state: authority.as_sql().to_owned(),
                });
            }
            self.store
                .fence_and_yield_flow(flow_id, snapshot_generation)
                .await?;
            self.mobility
                .set_authority_state(flow_id, FlowAuthorityState::Yielded);
        }
        Ok(FlowResponse::FlowAuthorityYielded {
            acknowledgement: yield_acknowledgement,
        })
    }

    pub(super) async fn promote_flow_replica(
        &mut self,
        flow_id: FlowId,
        acknowledgement: FlowYieldAcknowledgement,
    ) -> Result<FlowResponse, FlowError> {
        if acknowledgement.flow_id != flow_id {
            return Err(FlowError::MobilityPromotionRejected {
                state: "acknowledgement_flow_mismatch".to_owned(),
            });
        }
        let authority = self.store.authority_state(flow_id).await?;
        if authority == FlowAuthorityState::Active {
            return self.mobility_readiness_response(flow_id).await;
        }
        if authority != FlowAuthorityState::InactiveCommitted {
            return Err(FlowError::MobilityPromotionRejected {
                state: authority.as_sql().to_owned(),
            });
        }
        let readiness = self.mobility_readiness(flow_id).await?;
        let readiness_generation = readiness
            .snapshot_generation
            .map(mobility_snapshot_generation)
            .transpose()?;
        if readiness_generation != Some(acknowledgement.snapshot_generation)
            || !readiness.history_complete
            || !readiness.documents_complete
            || !readiness.blobs_complete
        {
            return Err(FlowError::MobilityPromotionRejected {
                state: FlowAuthorityState::InactiveCommitted.as_sql().to_owned(),
            });
        }
        self.store
            .set_authority_state(flow_id, FlowAuthorityState::Active)
            .await?;
        self.mobility
            .set_authority_state(flow_id, FlowAuthorityState::Active);
        Ok(FlowResponse::FlowReplicaPromoted {
            acknowledgement: FlowActivationAcknowledgement {
                flow_id,
                snapshot_generation: acknowledgement.snapshot_generation,
            },
        })
    }

    pub(super) async fn select_mobility_destination(
        &mut self,
        flow_id: FlowId,
        destinations: Vec<FlowMobilityDestination>,
    ) -> Result<FlowResponse, FlowError> {
        if !self.definitions.contains_key(&flow_id) {
            return Err(FlowError::DefinitionNotFound);
        }
        let [destination] = destinations.as_slice() else {
            return Err(FlowError::MobilityDestinationCount);
        };
        self.mobility
            .select_destination(flow_id, destination.clone());
        self.store
            .save_mobility_destination(flow_id, destination)
            .await?;
        Ok(FlowResponse::MobilityDestinationSelected {
            flow_id,
            destination: destination.clone(),
        })
    }

    pub(super) async fn synchronize_flow(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        if !self.definitions.contains_key(&flow_id) {
            return Err(FlowError::DefinitionNotFound);
        }
        self.mobility
            .destination(flow_id)
            .ok_or(FlowError::MobilityDestinationNotSelected)?;
        let authority = self.store.authority_state(flow_id).await?;
        if !matches!(
            authority,
            FlowAuthorityState::Authoritative | FlowAuthorityState::Synchronizing
        ) {
            return Err(FlowError::MobilityPromotionRejected {
                state: authority.as_sql().to_owned(),
            });
        }
        let snapshot = self.mobility_snapshot(flow_id).await?;
        let destination = self
            .mobility
            .destination(flow_id)
            .cloned()
            .ok_or(FlowError::MobilityDestinationNotSelected)?;
        let acknowledgement = self
            .mobility_service
            .import_snapshot(destination.clone(), snapshot)
            .await?;
        self.store
            .save_mobility_commit(flow_id, &destination, &acknowledgement)
            .await?;
        self.mobility.record_commit_acknowledgement(acknowledgement);
        if authority == FlowAuthorityState::Authoritative {
            self.store
                .set_authority_state(flow_id, FlowAuthorityState::Synchronizing)
                .await?;
            self.mobility
                .set_authority_state(flow_id, FlowAuthorityState::Synchronizing);
        }
        self.mobility_readiness_response(flow_id).await
    }

    pub(super) async fn mobility_readiness(&mut self, flow_id: FlowId) -> Result<FlowMobilityReadiness, FlowError> {
        let authority = self.store.authority_state(flow_id).await?;
        let snapshot = self.store.load_mobility_snapshot(flow_id).await?;
        let inactive = authority == FlowAuthorityState::InactiveCommitted;
        let Some(snapshot) = snapshot else {
            return Ok(FlowMobilityReadiness {
                flow_id,
                snapshot_generation: None,
                history_complete: false,
                documents_complete: false,
                blobs_complete: false,
                inactive,
                failure: Some(FlowFailure {
                    code: "MobilitySnapshotMissing".to_owned(),
                    message: "no durable selected-Flow snapshot has committed".to_owned(),
                }),
            });
        };
        let mut documents_complete = true;
        for execution in &snapshot.executions {
            if self
                .store
                .load_execution_document_optional(execution.execution.execution_id)
                .await?
                .is_none()
            {
                documents_complete = false;
                break;
            }
        }
        let history_complete =
            snapshot.validate_selected_flow().is_ok() && snapshot.native_references.len() == snapshot.executions.len();
        let destination_committed = matches!(
            authority,
            FlowAuthorityState::InactiveCommitted | FlowAuthorityState::Active
        );
        Ok(FlowMobilityReadiness {
            flow_id,
            snapshot_generation: Some(snapshot.definition.revision),
            history_complete,
            documents_complete,
            blobs_complete: destination_committed && history_complete && documents_complete,
            inactive,
            failure: (!destination_committed).then(|| FlowFailure {
                code: "MobilityDestinationNotCommitted".to_owned(),
                message: "destination native content has not committed".to_owned(),
            }),
        })
    }

    pub(super) async fn mobility_readiness_response(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        Ok(FlowResponse::FlowMobilityReadiness {
            readiness: self.mobility_readiness(flow_id).await?,
        })
    }

    pub(super) async fn mobility_snapshot(&mut self, flow_id: FlowId) -> Result<FlowMobilitySnapshot, FlowError> {
        self.ensure_local_work(flow_id)?;
        let definition = self
            .definitions
            .get(&flow_id)
            .cloned()
            .ok_or(FlowError::DefinitionNotFound)?;
        let triggers = self.store.list_triggers(flow_id).await?;
        let executions = self.snapshot_store.load_for_flow(flow_id).await?;
        let mut native_references = Vec::with_capacity(executions.len());
        for snapshot in &executions {
            let document = self
                .store
                .load_execution_document(snapshot.execution.execution_id)
                .await?;
            native_references.push(WorkspaceStore::native_references(snapshot, &document));
        }
        let snapshot = FlowMobilitySnapshot {
            flow_id,
            definition,
            triggers,
            executions,
            native_references,
        };
        self.store.save_mobility_snapshot(&snapshot).await?;
        Ok(snapshot)
    }

    pub(super) fn ensure_local_work(&self, flow_id: FlowId) -> Result<(), FlowError> {
        if self.mobility.permits_local_work(flow_id) {
            Ok(())
        } else {
            Err(FlowError::InactiveReplica { flow_id })
        }
    }
}

/// Narrow runtime-owned seam for Flow mobility.
pub(crate) trait FlowMobilityService: Send + Sync {
    fn import_snapshot(
        &self,
        destination: FlowMobilityDestination,
        snapshot: FlowMobilitySnapshot,
    ) -> Pin<Box<dyn Future<Output = Result<FlowMobilityCommitAcknowledgement, FlowError>> + Send + '_>>;

    fn promote_replica(
        &self,
        destination: FlowMobilityDestination,
        acknowledgement: FlowYieldAcknowledgement,
    ) -> Pin<Box<dyn Future<Output = Result<FlowActivationAcknowledgement, FlowError>> + Send + '_>>;
}

/// Native execution document and immutable blob references for one Flow execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub(super) struct FlowExecutionNativeReferences {
    pub(super) execution_id: FlowExecutionId,
    pub(super) document_namespace_id: String,
    pub(super) document_author_id: String,
    pub(super) code_roots: Vec<ContentAddressedRoot>,
    pub(super) state_roots: Vec<ContentAddressedRoot>,
    pub(super) workspace_checkpoint: Option<ContentAddressedRoot>,
    pub(super) workspace_entries: BTreeMap<String, ContentHash>,
    pub(super) imported_workspace_root: Option<ContentAddressedRoot>,
}

/// Durable ownership and transfer position for one Flow.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlowAuthorityState {
    /// This node is the authoritative Flow owner.
    Authoritative,
    /// This node is synchronizing a replica while retaining authority.
    Synchronizing,
    /// The replica is committed but is not yet active.
    InactiveCommitted,
    /// The former authority has fenced local Flow work.
    Yielded,
    /// This replica has been promoted to authority.
    Active,
    /// Source cleanup remains pending after transfer.
    CleanupPending,
}

impl FlowAuthorityState {
    pub(super) fn permits_local_work(self) -> bool {
        matches!(self, Self::Authoritative | Self::Synchronizing | Self::Active)
    }

    pub(super) fn as_sql(self) -> &'static str {
        match self {
            Self::Authoritative => "authoritative",
            Self::Synchronizing => "synchronizing",
            Self::InactiveCommitted => "inactive_committed",
            Self::Yielded => "yielded",
            Self::Active => "active",
            Self::CleanupPending => "cleanup_pending",
        }
    }

    pub(super) fn from_sql(value: &str) -> Option<Self> {
        match value {
            "authoritative" => Some(Self::Authoritative),
            "synchronizing" => Some(Self::Synchronizing),
            "inactive_committed" => Some(Self::InactiveCommitted),
            "yielded" => Some(Self::Yielded),
            "active" => Some(Self::Active),
            "cleanup_pending" => Some(Self::CleanupPending),
            _ => None,
        }
    }
}

/// One complete, selected-Flow durability unit for mobility transfer.
#[derive(Clone, Debug, Deserialize, Serialize, Type, schemars::JsonSchema)]
pub struct FlowMobilitySnapshot {
    pub(super) flow_id: FlowId,
    pub(super) definition: FlowDefinition,
    pub(super) triggers: Vec<FlowTriggerRecord>,
    pub(super) executions: Vec<ExecutionSnapshot>,
    pub(super) native_references: Vec<FlowExecutionNativeReferences>,
}

impl FlowMobilitySnapshot {
    /// Rejects a transfer unit that contains a definition or execution from another Flow.
    pub(super) fn validate_selected_flow(&self) -> Result<(), FlowId> {
        (self.definition.flow_id == self.flow_id
            && self
                .executions
                .iter()
                .all(|execution| execution.execution.flow_id == self.flow_id))
        .then_some(())
        .ok_or(self.flow_id)
    }
}

/// Tracks each Flow's mobility authority and transfer acknowledgements.
pub(super) struct MobilityCoordinator {
    authority_states: HashMap<FlowId, FlowAuthorityState>,
    selected_destinations: HashMap<FlowId, FlowMobilityDestination>,
    destination_commits: HashMap<FlowId, FlowMobilityCommitAcknowledgement>,
    activation_acknowledgements: HashMap<FlowId, FlowActivationAcknowledgement>,
}

impl MobilityCoordinator {
    pub(super) fn new(authority_states: HashMap<FlowId, FlowAuthorityState>) -> Self {
        Self {
            authority_states,
            selected_destinations: HashMap::new(),
            destination_commits: HashMap::new(),
            activation_acknowledgements: HashMap::new(),
        }
    }

    pub(super) fn restore(states: Vec<PersistedMobilityState>) -> Result<Self, FlowError> {
        let mut coordinator = Self::new(HashMap::new());
        for state in states {
            if let Some(commit) = &state.commit
                && (commit.flow_id != state.flow_id || commit.snapshot_generation == 0)
            {
                return Err(FlowError::MobilityPeer {
                    message: format!("mobility commit flow mismatch for {}", state.flow_id),
                });
            }
            if let Some(activation) = &state.activation
                && (activation.flow_id != state.flow_id || activation.snapshot_generation == 0)
            {
                return Err(FlowError::MobilityPeer {
                    message: format!("mobility activation flow mismatch for {}", state.flow_id),
                });
            }
            coordinator
                .authority_states
                .insert(state.flow_id, state.authority_state);
            if let Some(destination) = state.destination {
                coordinator
                    .selected_destinations
                    .insert(state.flow_id, destination);
            }
            if let Some(commit) = state.commit {
                coordinator
                    .destination_commits
                    .insert(state.flow_id, commit);
            }
            if let Some(activation) = state.activation {
                coordinator
                    .activation_acknowledgements
                    .insert(state.flow_id, activation);
            }
        }
        Ok(coordinator)
    }

    pub(super) fn permits_local_work(&self, flow_id: FlowId) -> bool {
        self.authority_states
            .get(&flow_id)
            .copied()
            .is_some_and(FlowAuthorityState::permits_local_work)
    }

    pub(super) fn authority_state(&self, flow_id: FlowId) -> Option<FlowAuthorityState> {
        self.authority_states.get(&flow_id).copied()
    }

    pub(super) fn set_authority_state(&mut self, flow_id: FlowId, state: FlowAuthorityState) {
        self.authority_states.insert(flow_id, state);
    }

    pub(super) fn destination(&self, flow_id: FlowId) -> Option<&FlowMobilityDestination> {
        self.selected_destinations.get(&flow_id)
    }

    pub(super) fn destination_commit(&self, flow_id: FlowId) -> Option<&FlowMobilityCommitAcknowledgement> {
        self.destination_commits.get(&flow_id)
    }

    fn select_destination(&mut self, flow_id: FlowId, destination: FlowMobilityDestination) {
        self.selected_destinations.insert(flow_id, destination);
        self.destination_commits.remove(&flow_id);
    }

    pub(super) fn record_commit_acknowledgement(&mut self, acknowledgement: FlowMobilityCommitAcknowledgement) {
        self.destination_commits
            .insert(acknowledgement.flow_id, acknowledgement);
    }
}

/// Coordination acknowledgement that a former authority fenced a Flow before hand-off.
#[derive(Clone, Debug, Deserialize, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowYieldAcknowledgement {
    pub flow_id: FlowId,
    pub snapshot_generation: u64,
}

/// Confirmation returned by the destination after it durably becomes active.
#[derive(Clone, Debug, Deserialize, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowActivationAcknowledgement {
    pub flow_id: FlowId,
    pub snapshot_generation: u64,
}

/// Durable destination acknowledgement for an imported, inactive replica.
#[derive(Clone, Debug, Deserialize, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowMobilityCommitAcknowledgement {
    pub flow_id: FlowId,
    pub snapshot_generation: u64,
}

pub(super) fn mobility_snapshot_generation(revision: i64) -> Result<u64, FlowError> {
    revision
        .try_into()
        .map_err(|_| FlowError::MobilityPromotionRejected {
            state: "snapshot_generation_out_of_range".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::super::graph::{FlowGraphAst, GraphNode};
    use super::super::types::{FlowDefinition, FlowId};

    use super::{FlowAuthorityState, FlowMobilitySnapshot};

    #[test]
    fn authority_state_round_trips_and_inactive_replica_cannot_work() {
        let serialized = serde_json::to_string(&FlowAuthorityState::InactiveCommitted).unwrap();
        let restored: FlowAuthorityState = serde_json::from_str(&serialized).unwrap();

        assert_eq!(restored, FlowAuthorityState::InactiveCommitted);
        assert!(!restored.permits_local_work());
        assert!(!FlowAuthorityState::Yielded.permits_local_work());
        assert!(!FlowAuthorityState::CleanupPending.permits_local_work());
        assert!(FlowAuthorityState::Authoritative.permits_local_work());
        assert!(FlowAuthorityState::Synchronizing.permits_local_work());
        assert!(FlowAuthorityState::Active.permits_local_work());
    }

    #[test]
    fn mobility_snapshot_round_trips_selected_flow_definition_and_history() {
        let flow_id = FlowId(uuid::Uuid::new_v4());
        let snapshot = FlowMobilitySnapshot {
            flow_id,
            definition: FlowDefinition {
                flow_id,
                name: "Mobility test".to_owned(),
                revision: 3,
                graph: FlowGraphAst {
                    version: 1,
                    root: GraphNode::Sequence { children: Vec::new() },
                },
                blackboard_defaults: serde_json::json!({"currency": "USD"}),
                editor_metadata: serde_json::json!({"collapsed": false}),
                automatic_enabled: true,
                workspace_mappings: Vec::new(),
            },
            triggers: Vec::new(),
            executions: Vec::new(),
            native_references: Vec::new(),
        };

        let encoded = serde_json::to_vec(&snapshot).unwrap();
        let restored: FlowMobilitySnapshot = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored.flow_id, flow_id);
        assert_eq!(restored.definition.revision, 3);
        assert_eq!(
            restored.definition.blackboard_defaults,
            serde_json::json!({"currency": "USD"})
        );
    }

    #[test]
    fn yield_acknowledgement_is_flow_and_generation_bound() {
        let acknowledgement = super::FlowYieldAcknowledgement {
            flow_id: FlowId(uuid::Uuid::nil()),
            snapshot_generation: 7,
        };
        let restored: super::FlowYieldAcknowledgement =
            serde_json::from_str(&serde_json::to_string(&acknowledgement).unwrap()).unwrap();
        assert_eq!(restored.flow_id, acknowledgement.flow_id);
        assert_eq!(restored.snapshot_generation, 7);
    }

    #[test]
    fn activation_acknowledgement_is_flow_and_generation_bound() {
        let acknowledgement = super::FlowActivationAcknowledgement {
            flow_id: FlowId(uuid::Uuid::nil()),
            snapshot_generation: 7,
        };
        let restored: super::FlowActivationAcknowledgement =
            serde_json::from_str(&serde_json::to_string(&acknowledgement).unwrap()).unwrap();
        assert_eq!(restored.flow_id, acknowledgement.flow_id);
        assert_eq!(restored.snapshot_generation, 7);
    }
}

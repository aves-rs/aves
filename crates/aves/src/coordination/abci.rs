//! Actor-owned coordination application state, queries, persistence, and recovery.

use std::collections::{BTreeMap, HashMap, HashSet};

use arc_malachitebft_app_channel::app::types::sync::RawDecidedValue;
use arc_malachitebft_core_types::{ThresholdParams, ValidatorSet as _};
use arc_malachitebft_signing::VerifierExt;
use sha2::{Digest, Sha256};

use super::persistence::{CoordinationStore, DecidedRow, RecoveryState};
use super::signing::CoordinationVerifier;
use super::transaction::{
    ApplicationRecovery, IdleValueReply, PendingTransaction, ProposalPartAssembly, ProposalRecovery,
};
use super::types::{
    ChainRecord, CommittedSnapshot, ConsensusValue, CoordinationContext, CoordinationError, CoordinationEventId,
    CoordinationExecutionId, DomainId, ExecutionRecord, Height, MachineId, MachineRecord, PublicKey, RecordHash,
    Regenesis, SchemaActivated, SignedBody, SnapshotDecided, UserId, UserRecord,
};

#[derive(Clone)]
pub(super) struct CommittedState {
    pub(super) users: BTreeMap<UserId, UserRecord>,
    pub(super) machines: BTreeMap<MachineId, MachineRecord>,
    pub(super) executions: BTreeMap<[u8; 16], ExecutionRecord>,
    pub(super) schemas: Vec<SchemaActivated>,
    pub(super) regenesis: Option<Regenesis>,
    pub(super) height: Height,
    pub(super) applied_events: HashSet<CoordinationEventId>,
}

impl Default for CommittedState {
    fn default() -> Self {
        Self {
            users: BTreeMap::new(),
            machines: BTreeMap::new(),
            executions: BTreeMap::new(),
            schemas: Vec::new(),
            regenesis: None,
            height: Height(0),
            applied_events: HashSet::new(),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ApplicationState {
    pub(super) domain: Option<DomainId>,
    pub(super) genesis_validators: Option<Vec<(PublicKey, u64)>>,
    pub(super) committed: CommittedState,
    pub(super) pending: Vec<(CoordinationEventId, PendingTransaction)>,
    pub(super) proposals: BTreeMap<(u64, Vec<u8>), ProposalRecovery>,
    pub(super) committed_values: BTreeMap<u64, RawDecidedValue<CoordinationContext>>,
    pub(super) proposal_parts: HashMap<(String, Vec<u8>), ProposalPartAssembly>,
    pub(super) restream_nonce: u64,
}

/// Actor-owned application projection and pending admission queue.
///
/// Committed fields represent only applied values. The store persists projection, admissions,
/// decided values, and proposal replay data; consensus and transport remain engine-owned.
pub(super) struct CoordinationApplication {
    pub(super) state: ApplicationState,
    store: CoordinationStore,
    pub(super) idle_value_replies: Vec<IdleValueReply>,
}

impl CoordinationApplication {
    /// Prepares a successor from the committed tip without requiring predecessor consensus.
    pub(super) async fn begin_successor(
        &mut self,
        record: SignedBody<Regenesis>,
    ) -> Result<CoordinationEventId, CoordinationError> {
        let transaction = PendingTransaction::Regenesis(record.clone());
        let event_id = transaction.event_id()?;
        let body = &record.body;
        transaction.verify_signature(body.predecessor)?;
        if self.state.domain == Some(body.successor) {
            if self.state.committed.regenesis.as_ref() == Some(body)
                && self.state.committed.applied_events.contains(&event_id)
            {
                return Ok(event_id);
            }
            return Err(CoordinationError::InvalidSnapshot);
        }
        let snapshot = self.export_snapshot(None)?;
        if body.predecessor != snapshot.domain
            || body.predecessor == body.successor
            || body.predecessor_height != snapshot.height
            || body.snapshot_hash != snapshot.hash
        {
            return Err(CoordinationError::InvalidSnapshot);
        }
        if self.store.load_retired(body.successor).await?.is_some() {
            return Err(CoordinationError::InvalidSnapshot);
        }
        let authorized = self.state.committed.users.values().any(|user| {
            user.revoked.is_none()
                && user
                    .root_rotations
                    .last()
                    .map_or(user.registered.root_key, |rotation| rotation.new_root)
                    == record.signer
        });
        if !authorized {
            return Err(CoordinationError::SignatureInvalid);
        }
        Self::validator_set(&body.validators.0)?;
        let retained = self
            .state
            .genesis_validators
            .as_deref()
            .ok_or(CoordinationError::InvalidValidatorSet)?;
        if body
            .validators
            .0
            .iter()
            .any(|(key, power)| !retained.contains(&(*key, *power)))
        {
            return Err(CoordinationError::InvalidValidatorSet);
        }
        let previous = Self::recovery_state(&self.state)?;
        let mut candidate = ApplicationState {
            domain: Some(body.successor),
            genesis_validators: Some(body.validators.0.clone()),
            committed: self.state.committed.clone(),
            ..ApplicationState::default()
        };
        candidate.committed.height = Height(0);
        candidate.committed.regenesis = Some(body.clone());
        candidate.committed.applied_events.insert(event_id);
        let successor = Self::recovery_state(&candidate)?;
        self.store
            .save_successor(body.predecessor, &previous, &successor)
            .await?;
        self.state = candidate;
        self.cancel_value_requests();
        Ok(event_id)
    }

    /// Exports only committed state. The hash requires an independently trusted anchor.
    pub(super) fn export_snapshot(&self, domain: Option<DomainId>) -> Result<CommittedSnapshot, CoordinationError> {
        Self::snapshot_from_state(&self.state, domain)
    }

    /// Exports either the active chain or an archived predecessor.
    pub(super) async fn export_chain_snapshot(
        &self,
        domain: Option<DomainId>,
    ) -> Result<CommittedSnapshot, CoordinationError> {
        if domain.is_none() || domain == self.state.domain {
            return self.export_snapshot(domain);
        }
        let requested = domain.ok_or(CoordinationError::InvalidSnapshot)?;
        let recovery = self
            .store
            .load_retired(requested)
            .await?
            .ok_or(CoordinationError::InvalidSnapshot)?;
        let state = Self::restore_recovery(recovery)?;
        Self::snapshot_from_state(&state, Some(requested))
    }

    fn snapshot_from_state(
        state: &ApplicationState,
        domain: Option<DomainId>,
    ) -> Result<CommittedSnapshot, CoordinationError> {
        let active = state.domain.ok_or(CoordinationError::InvalidSnapshot)?;
        if domain.is_some_and(|requested| requested != active) {
            return Err(CoordinationError::InvalidSnapshot);
        }
        let recovery = Self::recovery_state(state)?;
        let mut application: ApplicationRecovery =
            serde_json::from_str(&recovery.state_json).map_err(|_| CoordinationError::Encoding)?;
        application.restream_nonce = 0;
        application
            .committed
            .applied_events
            .sort_by_key(|event| *event.as_bytes());
        let application_json = serde_json::to_string(&application).map_err(|_| CoordinationError::Encoding)?;
        let decided = recovery
            .decided
            .into_iter()
            .map(|row| SnapshotDecided {
                height: Height(row.height),
                value_bytes: row.value_bytes,
                certificate_json: row.certificate_json,
            })
            .collect();
        let mut snapshot = CommittedSnapshot {
            domain: active,
            height: state.committed.height,
            application_json,
            decided,
            hash: RecordHash([0; 32]),
        };
        snapshot.hash = Self::snapshot_hash(&snapshot)?;
        Ok(snapshot)
    }

    /// Adopts caller-trusted state after integrity, recovery and certificate validation.
    pub(super) async fn adopt_snapshot(
        &mut self,
        snapshot: CommittedSnapshot,
        expected_hash: RecordHash,
    ) -> Result<ChainRecord, CoordinationError> {
        if snapshot.hash != expected_hash || Self::snapshot_hash(&snapshot)? != expected_hash {
            return Err(CoordinationError::InvalidSnapshot);
        }
        let application: ApplicationRecovery =
            serde_json::from_str(&snapshot.application_json).map_err(|_| CoordinationError::InvalidSnapshot)?;
        if application.domain != Some(snapshot.domain)
            || application.committed.height != snapshot.height
            || application.restream_nonce != 0
        {
            return Err(CoordinationError::InvalidSnapshot);
        }
        let recovery = RecoveryState {
            state_json: snapshot.application_json.clone(),
            pending: Vec::new(),
            proposals: Vec::new(),
            decided: snapshot
                .decided
                .iter()
                .map(|row| DecidedRow {
                    height: row.height.0,
                    value_bytes: row.value_bytes.clone(),
                    certificate_json: row.certificate_json.clone(),
                })
                .collect(),
        };
        let candidate = Self::restore_recovery(recovery)?;
        let roster = candidate
            .genesis_validators
            .as_deref()
            .ok_or(CoordinationError::InvalidSnapshot)?;
        let validators = Self::validator_set(roster)?;
        // Malachite's threshold arithmetic multiplies total power by three.
        if validators.total_voting_power() > u64::MAX / 3 {
            return Err(CoordinationError::InvalidValidatorSet);
        }
        let verifier = CoordinationVerifier::new(snapshot.domain);
        for value in candidate.committed_values.values() {
            verifier
                .verify_commit_certificate(
                    &CoordinationContext,
                    &value.certificate.trim_vote_extensions(),
                    &validators,
                    ThresholdParams::default(),
                )
                .await
                .map_err(|_| CoordinationError::SignatureInvalid)?;
        }
        if self.state.domain.is_some() {
            let current = self.export_snapshot(None)?;
            if current.domain == snapshot.domain && current.height == snapshot.height && current.hash == snapshot.hash {
                return Ok(ChainRecord {
                    domain: snapshot.domain,
                    height: snapshot.height,
                });
            }
            return Err(CoordinationError::SnapshotHostNotEmpty);
        }
        let existing = &self.state;
        if existing.genesis_validators.is_some()
            || existing.committed.height != Height(0)
            || !existing.committed.users.is_empty()
            || !existing.committed.machines.is_empty()
            || !existing.committed.executions.is_empty()
            || !existing.committed.schemas.is_empty()
            || existing.committed.regenesis.is_some()
            || !existing.committed.applied_events.is_empty()
            || !existing.pending.is_empty()
            || !existing.proposals.is_empty()
            || !existing.committed_values.is_empty()
            || !existing.proposal_parts.is_empty()
            || existing.restream_nonce != 0
            || !self.idle_value_replies.is_empty()
        {
            return Err(CoordinationError::SnapshotHostNotEmpty);
        }
        self.save_state(&candidate).await?;
        self.state = candidate;
        Ok(ChainRecord {
            domain: snapshot.domain,
            height: snapshot.height,
        })
    }

    fn snapshot_hash(snapshot: &CommittedSnapshot) -> Result<RecordHash, CoordinationError> {
        let values: Vec<_> = snapshot
            .decided
            .iter()
            .map(|row| (row.height, &row.value_bytes))
            .collect();
        let bytes = serde_json::to_vec(&(snapshot.domain, snapshot.height, &snapshot.application_json, values))
            .map_err(|_| CoordinationError::Encoding)?;
        Ok(RecordHash(Sha256::digest(bytes).into()))
    }

    /// Restores one application instance with the actor-opened store and its recovery snapshot.
    pub(super) fn with_store(
        store: CoordinationStore,
        recovery: Option<RecoveryState>,
    ) -> Result<Self, CoordinationError> {
        let state = recovery
            .map(Self::restore_recovery)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            state,
            store,
            idle_value_replies: Vec::new(),
        })
    }

    pub(super) fn get_user(&self, user: UserId) -> Result<Option<UserRecord>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.users.get(&user).cloned())
    }

    pub(super) fn get_machine(&self, machine: MachineId) -> Result<Option<MachineRecord>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.machines.get(&machine).cloned())
    }

    pub(super) fn list_machines(&self) -> Result<Vec<MachineRecord>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.machines.values().cloned().collect())
    }

    pub(super) fn get_execution(
        &self,
        execution: CoordinationExecutionId,
    ) -> Result<Option<ExecutionRecord>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self
            .state
            .committed
            .executions
            .get(execution.0.0.as_bytes())
            .cloned())
    }

    pub(super) fn list_executions(&self) -> Result<Vec<ExecutionRecord>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.executions.values().cloned().collect())
    }

    pub(super) fn get_schema(&self, at: Height) -> Result<Option<SchemaActivated>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self
            .state
            .committed
            .schemas
            .iter()
            .filter(|schema| schema.active_from <= at)
            .max_by_key(|schema| schema.active_from)
            .cloned())
    }

    pub(super) fn get_schema_history(&self) -> Result<Vec<SchemaActivated>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.schemas.clone())
    }

    pub(super) fn get_chain(&self) -> Result<ChainRecord, CoordinationError> {
        Ok(ChainRecord {
            domain: self.state.domain.ok_or(CoordinationError::NotImplemented)?,
            height: self.state.committed.height,
        })
    }

    pub(super) fn get_regenesis(&self) -> Result<Option<Regenesis>, CoordinationError> {
        self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok(self.state.committed.regenesis.clone())
    }

    pub(super) async fn save_state(&self, state: &ApplicationState) -> Result<(), CoordinationError> {
        let recovery = Self::recovery_state(state)?;
        self.store.save(&recovery).await
    }

    /// Builds a JSON application value only when admitted transactions exist.
    pub(super) fn build_application_value(&self) -> Result<Option<ConsensusValue>, CoordinationError> {
        let state = &self.state;
        if state.pending.is_empty() {
            return Ok(None);
        }
        let transactions: Vec<_> = state
            .pending
            .iter()
            .map(|(_, transaction)| transaction)
            .collect();
        serde_json::to_string(&transactions)
            .map(|json| Some(ConsensusValue(json)))
            .map_err(|_| CoordinationError::Encoding)
    }

    pub(super) fn validate_application_value_in_state(
        state: &ApplicationState,
        value: &ConsensusValue,
        height: Height,
    ) -> Result<(), CoordinationError> {
        let transactions = Self::decode_transactions(value)?;
        let domain = state.domain.ok_or(CoordinationError::NotImplemented)?;
        let mut committed = state.committed.clone();
        committed.apply_transactions(domain, transactions, height)?;
        Ok(())
    }
}

pub(super) fn unique_map<K: Ord, V>(entries: Vec<(K, V)>) -> Result<BTreeMap<K, V>, CoordinationError> {
    let expected_len = entries.len();
    let values: BTreeMap<_, _> = entries.into_iter().collect();
    if values.len() != expected_len {
        return Err(CoordinationError::Encoding);
    }
    Ok(values)
}

#[cfg(test)]
#[path = "actor/snapshot_tests.rs"]
mod snapshot_tests;

#[cfg(test)]
#[path = "actor/recovery_tests.rs"]
mod recovery_tests;

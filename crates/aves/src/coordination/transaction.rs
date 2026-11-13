//! Signed transactions, application state, and recovery records.

use std::collections::{BTreeMap, HashSet};

use arc_malachitebft_app_channel::Reply;
use arc_malachitebft_app_channel::app::types::LocallyProposedValue;
use arc_malachitebft_app_channel::app::types::core::Round;
use arc_malachitebft_engine::host::HostMsg;
use ractor::{Actor as RactorActor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::abci::{CommittedState, CoordinationApplication};
use super::signing::{ChainSigningScheme, ChainVerifier};
use super::types::{
    ConsensusProposalPart, CoordinationContext, CoordinationError, CoordinationEventId, DomainId, ExecutionCancelled,
    ExecutionCreated, ExecutionLanded, ExecutionRecord, Height, MachineBound, MachineId, MachineRecord,
    MachineRegistered, MachineUnbound, PublicKey, Regenesis, RegisteredTransaction, SchemaActivated, SignedBody,
    UserEncRotated, UserId, UserRecord, UserRegistered, UserRevoked, UserRootRotated, UserSuccessorLinked,
};

/// Forwards Malachite's application requests into the coordination actor's channel.
pub(super) struct ConsensusMessageAdapter;

#[async_trait::async_trait]
impl RactorActor for ConsensusMessageAdapter {
    type Msg = HostMsg<CoordinationContext>;
    type State = mpsc::Sender<Self::Msg>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        sender: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(sender)
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        sender: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        sender.send(message).await.map_err(Into::into)
    }
}

/// A signed record awaiting commitment.
///
/// Admission persists before acknowledgement; application occurs only after Malachite decides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum PendingTransaction {
    UserRegistered(SignedBody<UserRegistered>),
    UserEncRotated(SignedBody<UserEncRotated>),
    UserRootRotated(SignedBody<UserRootRotated>),
    UserSuccessorLinked(SignedBody<UserSuccessorLinked>),
    UserRevoked(SignedBody<UserRevoked>),
    MachineRegistered(SignedBody<MachineRegistered>),
    MachineBound(SignedBody<MachineBound>),
    MachineUnbound(SignedBody<MachineUnbound>),
    ExecutionCreated(SignedBody<ExecutionCreated>),
    ExecutionLanded(SignedBody<ExecutionLanded>),
    ExecutionCancelled(SignedBody<ExecutionCancelled>),
    SchemaActivated(SignedBody<SchemaActivated>),
    Regenesis(SignedBody<Regenesis>),
}

impl PendingTransaction {
    pub(super) fn body_json(&self) -> Result<String, CoordinationError> {
        match self {
            Self::UserRegistered(body) => serde_json::to_string(body),
            Self::UserEncRotated(body) => serde_json::to_string(body),
            Self::UserRootRotated(body) => serde_json::to_string(body),
            Self::UserSuccessorLinked(body) => serde_json::to_string(body),
            Self::UserRevoked(body) => serde_json::to_string(body),
            Self::MachineRegistered(body) => serde_json::to_string(body),
            Self::MachineBound(body) => serde_json::to_string(body),
            Self::MachineUnbound(body) => serde_json::to_string(body),
            Self::ExecutionCreated(body) => serde_json::to_string(body),
            Self::ExecutionLanded(body) => serde_json::to_string(body),
            Self::ExecutionCancelled(body) => serde_json::to_string(body),
            Self::SchemaActivated(body) => serde_json::to_string(body),
            Self::Regenesis(body) => serde_json::to_string(body),
        }
        .map_err(|_| CoordinationError::Encoding)
    }

    pub(super) fn event_id(&self) -> Result<CoordinationEventId, CoordinationError> {
        serde_json::to_vec(self)
            .map(|bytes| CoordinationEventId::from_bytes(Sha256::digest(bytes).into()))
            .map_err(|_| CoordinationError::Encoding)
    }

    pub(super) fn verify_signature(&self, domain: DomainId) -> Result<(), CoordinationError> {
        match self {
            Self::UserRegistered(body) => verify_body(body, domain),
            Self::UserEncRotated(body) => verify_body(body, domain),
            Self::UserRootRotated(body) => verify_body(body, domain),
            Self::UserSuccessorLinked(body) => verify_body(body, domain),
            Self::UserRevoked(body) => verify_body(body, domain),
            Self::MachineRegistered(body) => verify_body(body, domain),
            Self::MachineBound(body) => verify_body(body, domain),
            Self::MachineUnbound(body) => verify_body(body, domain),
            Self::ExecutionCreated(body) => verify_body(body, domain),
            Self::ExecutionLanded(body) => verify_body(body, domain),
            Self::ExecutionCancelled(body) => verify_body(body, domain),
            Self::SchemaActivated(body) => verify_body(body, domain),
            Self::Regenesis(body) => verify_body(body, domain),
        }
    }

    pub(super) fn validate_payload(&self) -> Result<(), CoordinationError> {
        match self {
            Self::SchemaActivated(signed) => {
                serde_json::from_str::<serde_json::Value>(&signed.body.schema)
                    .map_err(|_| CoordinationError::InvalidJson)?;
            }
            Self::Regenesis(signed) => {
                CoordinationApplication::validator_set(&signed.body.validators.0)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Authorizes and applies one transaction to the supplied committed projection.
    pub(super) fn apply(self, committed: &mut CommittedState, height: Height) -> Result<(), CoordinationError> {
        match self {
            PendingTransaction::UserRegistered(signed) => {
                let user = signed.body.user;
                if committed.users.contains_key(&user) || signed.signer != signed.body.root_key {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed.users.insert(
                    user,
                    UserRecord {
                        user,
                        registered: signed.body,
                        encryption_rotations: Vec::new(),
                        root_rotations: Vec::new(),
                        successors: Vec::new(),
                        revoked: None,
                    },
                );
            }
            PendingTransaction::UserEncRotated(signed) => {
                let record = committed
                    .users
                    .get_mut(&signed.body.user)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.revoked.is_some()
                    || signed.signer != current_root(record)
                    || record
                        .encryption_rotations
                        .last()
                        .map_or(record.registered.enc_epoch, |r| r.enc_epoch)
                        != signed.body.previous_epoch
                    || signed.body.previous_epoch.checked_add(1) != Some(signed.body.enc_epoch)
                {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.encryption_rotations.push(signed.body);
            }
            PendingTransaction::UserRootRotated(signed) => {
                let record = committed
                    .users
                    .get_mut(&signed.body.user)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.revoked.is_some()
                    || signed.signer != current_root(record)
                    || signed.body.previous_root != current_root(record)
                {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.root_rotations.push(signed.body);
            }
            PendingTransaction::UserSuccessorLinked(signed) => {
                if committed.users.contains_key(&signed.body.successor) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                let record = committed
                    .users
                    .get_mut(&signed.body.predecessor)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.revoked.is_some()
                    || signed.signer != current_root(record)
                    || signed.body.predecessor_root != current_root(record)
                    || record
                        .successors
                        .iter()
                        .any(|link| link.successor == signed.body.successor)
                {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.successors.push(signed.body);
            }
            PendingTransaction::UserRevoked(signed) => {
                let record = committed
                    .users
                    .get_mut(&signed.body.user)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.revoked.is_some()
                    || signed.signer != current_root(record)
                    || signed.body.root_key != current_root(record)
                {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.revoked = Some(signed.body);
            }
            PendingTransaction::MachineRegistered(signed) => {
                if signed.signer != signed.body.machine_key {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed
                    .machines
                    .entry(signed.body.machine)
                    .or_insert_with(|| MachineRecord {
                        machine: signed.body.machine,
                        registrations: Vec::new(),
                        bindings: Vec::new(),
                        unbindings: Vec::new(),
                    })
                    .registrations
                    .push(signed.body);
            }
            PendingTransaction::MachineBound(signed) => {
                let user = committed
                    .users
                    .get(&signed.body.user)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if user.revoked.is_some() || signed.signer != current_root(user) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed
                    .machines
                    .get_mut(&signed.body.machine)
                    .ok_or(CoordinationError::SignatureInvalid)?
                    .bindings
                    .push(signed.body);
            }
            PendingTransaction::MachineUnbound(signed) => {
                let user = committed
                    .users
                    .get(&signed.body.user)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if user.revoked.is_some() || signed.signer != current_root(user) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed
                    .machines
                    .get_mut(&signed.body.machine)
                    .ok_or(CoordinationError::SignatureInvalid)?
                    .unbindings
                    .push(signed.body);
            }
            PendingTransaction::ExecutionCreated(signed) => {
                let body = signed.body;
                let key = *body.execution_id.0.0.as_bytes();
                if committed.executions.contains_key(&key) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed.executions.insert(
                    key,
                    ExecutionRecord {
                        execution_id: body.execution_id,
                        flow_id: body.flow_id,
                        machine: body.machine,
                        landed: None,
                        cancelled: None,
                    },
                );
            }
            PendingTransaction::ExecutionLanded(signed) => {
                let key = *signed.body.execution_id.0.0.as_bytes();
                let record = committed
                    .executions
                    .get_mut(&key)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.landed.is_some() || record.cancelled.is_some() {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.landed = Some(signed.body);
            }
            PendingTransaction::ExecutionCancelled(signed) => {
                let key = *signed.body.execution_id.0.0.as_bytes();
                let record = committed
                    .executions
                    .get_mut(&key)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.landed.is_some() || record.cancelled.is_some() {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.cancelled = Some(signed.body);
            }
            PendingTransaction::SchemaActivated(signed) => committed.schemas.push(signed.body),
            PendingTransaction::Regenesis(signed) => committed.regenesis = Some(signed.body),
        }
        committed.height = height;
        Ok(())
    }
}

impl CommittedState {
    /// Verifies and applies a batch, retaining the existing replay protection.
    pub(super) fn apply_transactions(
        &mut self,
        domain: DomainId,
        transactions: Vec<PendingTransaction>,
        height: Height,
    ) -> Result<HashSet<CoordinationEventId>, CoordinationError> {
        let mut applied = HashSet::new();
        for transaction in transactions {
            if matches!(transaction, PendingTransaction::Regenesis(_)) {
                return Err(CoordinationError::SignatureInvalid);
            }
            transaction.validate_payload()?;
            transaction.verify_signature(domain)?;
            let event_id = transaction.event_id()?;
            if self.applied_events.insert(event_id) {
                transaction.apply(self, height)?;
            }
            applied.insert(event_id);
        }
        Ok(applied)
    }
}

#[derive(Clone, Default)]
pub(super) struct ProposalPartAssembly {
    pub(super) next_sequence: u64,
    pub(super) queued_parts: BTreeMap<u64, ConsensusProposalPart>,
    pub(super) proposal_bytes: Vec<u8>,
    pub(super) ordered_parts: Vec<ConsensusProposalPart>,
    pub(super) started: bool,
}

#[derive(Serialize, Deserialize)]
pub(super) struct ApplicationRecovery {
    pub(super) domain: Option<DomainId>,
    pub(super) genesis_validators: Option<Vec<(PublicKey, u64)>>,
    pub(super) committed: CommittedRecovery,
    pub(super) restream_nonce: u64,
}

pub(super) struct IdleValueReply {
    pub(super) height: Height,
    pub(super) round: Round,
    pub(super) reply: Reply<LocallyProposedValue<CoordinationContext>>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct CommittedRecovery {
    pub(super) users: Vec<(UserId, UserRecord)>,
    pub(super) machines: Vec<(MachineId, MachineRecord)>,
    pub(super) executions: Vec<([u8; 16], ExecutionRecord)>,
    pub(super) schemas: Vec<SchemaActivated>,
    pub(super) regenesis: Option<Regenesis>,
    pub(super) height: Height,
    pub(super) applied_events: Vec<CoordinationEventId>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ProposalRecovery {
    pub(super) application_bytes: Vec<u8>,
    pub(super) metadata: Option<ProposalMetadata>,
    pub(super) parts: Vec<ConsensusProposalPart>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ProposalMetadata {
    pub(super) height: Height,
    pub(super) round: i64,
    pub(super) valid_round: i64,
    pub(super) proposer: PublicKey,
}

#[derive(Serialize, Deserialize)]
pub(super) struct PersistedCertificate {
    pub(super) peer: [u8; 32],
    pub(super) network_response: Vec<u8>,
}

fn current_root(record: &UserRecord) -> PublicKey {
    record
        .root_rotations
        .last()
        .map_or(record.registered.root_key, |rotation| rotation.new_root)
}

fn verify_body<T: Serialize + RegisteredTransaction>(
    body: &SignedBody<T>,
    domain: DomainId,
) -> Result<(), CoordinationError> {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(T::PURPOSE.as_bytes());
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body.body).map_err(|_| CoordinationError::Encoding)?;
    let key = ChainSigningScheme::decode_public_key(&body.signer.0).map_err(|_| CoordinationError::SignatureInvalid)?;
    let signature = ChainSigningScheme::decode_signature(&body.sig).map_err(|_| CoordinationError::SignatureInvalid)?;
    if !ChainVerifier.verify(&key, &preimage, &signature) {
        return Err(CoordinationError::SignatureInvalid);
    }
    Ok(())
}

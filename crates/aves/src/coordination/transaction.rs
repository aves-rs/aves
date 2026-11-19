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
use super::authority::apply_authority_update;
use super::membership::{OwnerAction, apply_member_action, apply_owner_action};
use super::notices::apply_sharing_notice;
use super::signing::{ChainSigningScheme, ChainVerifier};
use super::types::{
    Acceptance, ChainKind, ChainPolicyChanged, ChainPolicyRecord, ConsensusProposalPart, CoordinationContext,
    CoordinationError, CoordinationEventId, DomainId, EnvelopeAddition, ExecutionCancelled, ExecutionCreated,
    ExecutionLanded, ExecutionRecord, Height, Invitation, InvitationId, InvitationRecord, MachineBound, MachineId,
    MachineRecord, MachineRegistered, MachineUnbound, MemberAuthority, MemberAuthorityUpdated, MemberMachineCertUpdate,
    MemberPowerChanged, MemberPowerRecord, MemberRecord, MemberRemoved, MemberValidatorSplitChanged,
    MemberValidatorSplitRecord, MembershipDecision, OwnerRegistration, PublicKey, PublicationRecord, Regenesis,
    RegisteredTransaction, SchemaActivated, ShareGrantChanged, SharingNotice, SharingNoticeKey, SignedBody,
    StoryPublished, UserEncRotated, UserId, UserRecord, UserRegistered, UserRevoked, UserRootRotated,
    UserSuccessorLinked, VerificationReport,
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
    SharingNotice(Box<SignedBody<SharingNotice>>),
    ShareGrantChanged(SignedBody<ShareGrantChanged>),
    StoryPublished(SignedBody<StoryPublished>),
    EnvelopeAddition(SignedBody<EnvelopeAddition>),
    VerificationReport(SignedBody<VerificationReport>),
    OwnerRegistration(SignedBody<OwnerRegistration>),
    Invitation(Box<SignedBody<Invitation>>),
    MembershipDecision(Box<SignedBody<MembershipDecision>>),
    MemberRemoved(SignedBody<MemberRemoved>),
    MemberPowerChanged(SignedBody<MemberPowerChanged>),
    MemberValidatorSplitChanged(SignedBody<MemberValidatorSplitChanged>),
    ChainPolicyChanged(SignedBody<ChainPolicyChanged>),
    Acceptance(Box<SignedBody<Acceptance>>),
    MemberMachineCertUpdate(Box<SignedBody<MemberMachineCertUpdate>>),
    MemberAuthorityUpdated(SignedBody<MemberAuthorityUpdated>),
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
            Self::SharingNotice(body) => serde_json::to_string(body),
            Self::ShareGrantChanged(body) => serde_json::to_string(body),
            Self::StoryPublished(body) => serde_json::to_string(body),
            Self::EnvelopeAddition(body) => serde_json::to_string(body),
            Self::VerificationReport(body) => serde_json::to_string(body),
            Self::OwnerRegistration(body) => serde_json::to_string(body),
            Self::Invitation(body) => serde_json::to_string(body),
            Self::MembershipDecision(body) => serde_json::to_string(body),
            Self::MemberRemoved(body) => serde_json::to_string(body),
            Self::MemberPowerChanged(body) => serde_json::to_string(body),
            Self::MemberValidatorSplitChanged(body) => serde_json::to_string(body),
            Self::ChainPolicyChanged(body) => serde_json::to_string(body),
            Self::Acceptance(body) => serde_json::to_string(body),
            Self::MemberMachineCertUpdate(body) => serde_json::to_string(body),
            Self::MemberAuthorityUpdated(body) => serde_json::to_string(body),
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
            Self::SharingNotice(body) if body.body.domain == domain => verify_body(body, domain),
            Self::SharingNotice(_) => Err(CoordinationError::SignatureInvalid),
            Self::ShareGrantChanged(body) if body.body.domain == domain => verify_body(body, domain),
            Self::StoryPublished(body) if body.body.key.domain == domain => verify_body(body, domain),
            Self::EnvelopeAddition(body) if body.body.key.domain == domain => verify_body(body, domain),
            Self::VerificationReport(body) if body.body.domain == domain => verify_body(body, domain),
            Self::ShareGrantChanged(_)
            | Self::StoryPublished(_)
            | Self::EnvelopeAddition(_)
            | Self::VerificationReport(_) => Err(CoordinationError::SignatureInvalid),
            Self::OwnerRegistration(body) if body.body.domain == domain => verify_body(body, domain),
            Self::Invitation(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MembershipDecision(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MemberRemoved(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MemberPowerChanged(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MemberValidatorSplitChanged(body) if body.body.domain == domain => verify_body(body, domain),
            Self::ChainPolicyChanged(body) if body.body.domain == domain => verify_body(body, domain),
            Self::Acceptance(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MemberMachineCertUpdate(body) if body.body.domain == domain => verify_body(body, domain),
            Self::MemberAuthorityUpdated(body) if body.body.domain == domain => verify_body(body, domain),
            Self::OwnerRegistration(_)
            | Self::Invitation(_)
            | Self::MembershipDecision(_)
            | Self::MemberRemoved(_)
            | Self::MemberPowerChanged(_)
            | Self::MemberValidatorSplitChanged(_)
            | Self::ChainPolicyChanged(_)
            | Self::Acceptance(_)
            | Self::MemberMachineCertUpdate(_)
            | Self::MemberAuthorityUpdated(_) => Err(CoordinationError::SignatureInvalid),
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

    /// Applies a transaction and converts deterministic state conflicts to rejection.
    pub(super) fn apply(
        self,
        committed: &mut CommittedState,
        height: Height,
    ) -> Result<ApplyOutcome, CoordinationError> {
        if matches!(
            self,
            PendingTransaction::MemberPowerChanged(_) | PendingTransaction::MemberValidatorSplitChanged(_)
        ) {
            let mut candidate = committed.clone();
            let outcome = apply_member_action(&self, &mut candidate)
                .or_else(|| apply_owner_action(&self, &mut candidate))
                .unwrap_or(ApplyOutcome::Rejected(RejectionReason::InvalidPayload));
            if outcome != ApplyOutcome::Applied {
                return Ok(outcome);
            }
            let member = match &self {
                PendingTransaction::MemberPowerChanged(signed) => signed.body.member,
                PendingTransaction::MemberValidatorSplitChanged(signed) => signed.body.member,
                _ => unreachable!(),
            };
            if candidate
                .member_power
                .get(&member)
                .is_some_and(|power| power.total > 0)
                && !candidate.member_splits.get(&member).is_some_and(|split| {
                    super::authority::current_member_authority(&candidate, member).is_some_and(|authority| {
                        !split.splits.is_empty()
                            && split.splits.iter().all(|part| {
                                authority
                                    .certified_machines
                                    .iter()
                                    .any(|machine| machine.machine == part.machine)
                            })
                    })
                })
            {
                return Ok(ApplyOutcome::Rejected(RejectionReason::InvalidPayload));
            }
            let roster = match super::roster::derive_roster(&candidate) {
                Ok(super::roster::RosterOutcome::Validators(roster)) => roster,
                _ => return Ok(ApplyOutcome::Rejected(RejectionReason::InvalidPayload)),
            };
            if CoordinationApplication::validator_set(&roster).is_err() {
                return Ok(ApplyOutcome::Rejected(RejectionReason::InvalidPayload));
            }
            candidate.height = height;
            *committed = candidate;
            return Ok(ApplyOutcome::Applied);
        }
        if let PendingTransaction::MemberAuthorityUpdated(ref signed) = self {
            let outcome = apply_authority_update(committed, signed);
            if outcome == ApplyOutcome::Applied {
                committed.height = height;
            }
            return Ok(outcome);
        }
        if let PendingTransaction::SharingNotice(ref signed) = self {
            let outcome = apply_sharing_notice(committed, signed);
            if outcome == ApplyOutcome::Applied {
                committed.height = height;
            }
            return Ok(outcome);
        }
        if let Some(outcome) = super::sharing::apply_sharing(&self, committed, height) {
            if outcome == ApplyOutcome::Applied {
                committed.height = height;
            }
            return Ok(outcome);
        }
        if let Some(outcome) = apply_member_action(&self, committed) {
            if outcome == ApplyOutcome::Applied {
                committed.height = height;
            }
            return Ok(outcome);
        }
        if let Some(outcome) = apply_owner_action(&self, committed) {
            if outcome == ApplyOutcome::Applied {
                committed.height = height;
            }
            return Ok(outcome);
        }
        match self.apply_authorized(committed, height) {
            Ok(()) => Ok(ApplyOutcome::Applied),
            Err(CoordinationError::SignatureInvalid) => Ok(ApplyOutcome::Rejected(RejectionReason::UnauthorizedSigner)),
            Err(error) => Err(error),
        }
    }

    /// Authorizes and applies one transaction to the supplied committed projection.
    fn apply_authorized(self, committed: &mut CommittedState, height: Height) -> Result<(), CoordinationError> {
        match self {
            PendingTransaction::SharingNotice(_) => return Err(CoordinationError::SignatureInvalid),
            PendingTransaction::ShareGrantChanged(_)
            | PendingTransaction::StoryPublished(_)
            | PendingTransaction::EnvelopeAddition(_)
            | PendingTransaction::VerificationReport(_) => return Err(CoordinationError::SignatureInvalid),
            PendingTransaction::OwnerRegistration(_)
            | PendingTransaction::Invitation(_)
            | PendingTransaction::MembershipDecision(_)
            | PendingTransaction::MemberRemoved(_)
            | PendingTransaction::MemberPowerChanged(_)
            | PendingTransaction::MemberValidatorSplitChanged(_)
            | PendingTransaction::ChainPolicyChanged(_)
            | PendingTransaction::Acceptance(_)
            | PendingTransaction::MemberMachineCertUpdate(_) => return Err(CoordinationError::SignatureInvalid),
            PendingTransaction::MemberAuthorityUpdated(_) => return Err(CoordinationError::SignatureInvalid),
            PendingTransaction::UserRegistered(signed) => {
                let user = signed.body.user;
                if committed.kind == Some(ChainKind::Collective)
                    && (!committed
                        .owner
                        .as_ref()
                        .is_some_and(|owner| owner.user == user)
                        || !chain_owner_authorized(committed, signed.signer))
                {
                    return Err(CoordinationError::SignatureInvalid);
                }
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
                if !execution_signer_authorized(committed, signed.body.machine, signed.signer, false) {
                    return Err(CoordinationError::SignatureInvalid);
                }
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
                let machine = committed
                    .executions
                    .get(&key)
                    .ok_or(CoordinationError::SignatureInvalid)?
                    .machine;
                if !execution_signer_authorized(committed, machine, signed.signer, true) {
                    return Err(CoordinationError::SignatureInvalid);
                }
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
                let machine = committed
                    .executions
                    .get(&key)
                    .ok_or(CoordinationError::SignatureInvalid)?
                    .machine;
                if !execution_signer_authorized(committed, machine, signed.signer, true) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                let record = committed
                    .executions
                    .get_mut(&key)
                    .ok_or(CoordinationError::SignatureInvalid)?;
                if record.landed.is_some() || record.cancelled.is_some() {
                    return Err(CoordinationError::SignatureInvalid);
                }
                record.cancelled = Some(signed.body);
            }
            PendingTransaction::SchemaActivated(signed) => {
                if !chain_owner_authorized(committed, signed.signer) {
                    return Err(CoordinationError::SignatureInvalid);
                }
                committed.schemas.push(signed.body);
            }
            PendingTransaction::Regenesis(signed) => committed.regenesis = Some(signed.body),
        }
        committed.height = height;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum RejectionReason {
    UnauthorizedSigner,
    InvalidPayload,
    ConflictingAction,
    StaleRevision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApplyOutcome {
    Applied,
    Rejected(RejectionReason),
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
            let event_id = transaction.event_id()?;
            if !self.applied_events.contains(&event_id) && !self.rejected_events.contains_key(&event_id) {
                let outcome = if matches!(transaction, PendingTransaction::Regenesis(_)) {
                    ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
                } else if let Err(error) = transaction.validate_payload() {
                    match error {
                        CoordinationError::InvalidJson
                        | CoordinationError::InvalidValidatorSet
                        | CoordinationError::ZeroVotingPower => ApplyOutcome::Rejected(RejectionReason::InvalidPayload),
                        error => return Err(error),
                    }
                } else if let Err(error) = transaction.verify_signature(domain) {
                    match error {
                        CoordinationError::SignatureInvalid => {
                            ApplyOutcome::Rejected(RejectionReason::UnauthorizedSigner)
                        }
                        error => return Err(error),
                    }
                } else {
                    transaction.apply(self, height)?
                };
                match outcome {
                    ApplyOutcome::Applied => {
                        self.applied_events.insert(event_id);
                    }
                    ApplyOutcome::Rejected(reason) => {
                        self.rejected_events.insert(event_id, reason);
                    }
                }
                self.event_heights.insert(event_id, height);
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
    #[serde(default)]
    pub(super) legacy: bool,
    #[serde(default)]
    pub(super) genesis: Option<super::abci::StoredGenesis>,
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
    #[serde(default)]
    pub(super) roster_history: Vec<(u64, Vec<(PublicKey, u64)>)>,
    #[serde(default)]
    pub(super) roster_owners: Vec<(u64, Vec<(PublicKey, UserId)>)>,
    #[serde(default)]
    pub(super) sharing_notices: Vec<(SharingNoticeKey, Vec<SharingNotice>)>,
    #[serde(default)]
    pub(super) sharing_notice_signers: Vec<(SharingNoticeKey, Vec<Option<PublicKey>>)>,
    #[serde(default)]
    pub(super) share_grants: Vec<ShareGrantChanged>,
    #[serde(default)]
    pub(super) publications: Vec<PublicationRecord>,
    #[serde(default)]
    pub(super) envelope_additions: Vec<EnvelopeAddition>,
    #[serde(default)]
    pub(super) verification_reports: Vec<VerificationReport>,
    #[serde(default)]
    pub(super) owner_actions: Vec<OwnerAction>,
    #[serde(default)]
    pub(super) owner_registration: Option<OwnerRegistration>,
    #[serde(default)]
    pub(super) invitations: Vec<(InvitationId, InvitationRecord)>,
    #[serde(default)]
    pub(super) members: Vec<(UserId, MemberRecord)>,
    #[serde(default)]
    pub(super) member_power: Vec<(UserId, MemberPowerRecord)>,
    #[serde(default)]
    pub(super) member_splits: Vec<(UserId, MemberValidatorSplitRecord)>,
    #[serde(default)]
    pub(super) member_machine_certs: Vec<((UserId, MachineId), MemberMachineCertUpdate)>,
    #[serde(default)]
    pub(super) member_authorities: Vec<(UserId, MemberAuthority)>,
    #[serde(default)]
    pub(super) policy: Option<ChainPolicyRecord>,
    pub(super) users: Vec<(UserId, UserRecord)>,
    pub(super) machines: Vec<(MachineId, MachineRecord)>,
    pub(super) executions: Vec<([u8; 16], ExecutionRecord)>,
    pub(super) schemas: Vec<SchemaActivated>,
    pub(super) regenesis: Option<Regenesis>,
    pub(super) height: Height,
    pub(super) applied_events: Vec<CoordinationEventId>,
    #[serde(default)]
    pub(super) rejected_events: Vec<(CoordinationEventId, RejectionReason)>,
    #[serde(default)]
    pub(super) event_heights: Vec<(CoordinationEventId, Height)>,
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

fn execution_signer_authorized(
    committed: &CommittedState,
    machine_id: MachineId,
    signer: PublicKey,
    terminal: bool,
) -> bool {
    let Some(machine) = committed.machines.get(&machine_id) else {
        return false;
    };
    machine.bindings.iter().any(|binding| {
        let Some(user) = committed.users.get(&binding.user) else {
            return false;
        };
        let revoked = machine
            .unbindings
            .iter()
            .any(|unbinding| unbinding.user == binding.user);
        user.revoked.is_none()
            && machine.registrations.iter().any(|registration| {
                registration.machine_key == binding.machine_key
                    && registration.sonya_key == binding.sonya_key
                    && registration.enc_key == binding.enc_key
            })
            && ((!revoked && signer == binding.machine_key) || ((!revoked || terminal) && signer == current_root(user)))
    })
}

pub(super) fn chain_owner_authorized(committed: &CommittedState, signer: PublicKey) -> bool {
    let Some(owner) = &committed.owner else {
        return false;
    };
    match committed.users.get(&owner.user) {
        Some(user) if user.registered.root_key == owner.root && user.revoked.is_none() => signer == current_root(user),
        Some(_) => false,
        None => signer == owner.root,
    }
}

pub(super) fn verify_body<T: Serialize + RegisteredTransaction>(
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

#[cfg(test)]
pub(super) fn test_genesis(
    domain: DomainId,
    roster: Vec<(PublicKey, u64)>,
) -> SignedBody<super::types::GenesisDescriptor> {
    use super::signing::ChainSigner;
    use super::types::{AUTHORITY_RULES_VERSION, ChainKind, GenesisDescriptor};

    let signer = ChainSigner::new(iroh::SecretKey::from_bytes(&[42; 32]));
    let owner_root = PublicKey::from(signer.public_key());
    let body = GenesisDescriptor {
        domain,
        kind: ChainKind::Personal,
        owner_user: UserId([1; 32]),
        owner_root,
        creator_machine: roster.first().map_or(owner_root, |entry| entry.0),
        initial_roster: roster,
        rules_version: AUTHORITY_RULES_VERSION,
    };
    let mut preimage = GenesisDescriptor::PURPOSE.as_bytes().to_vec();
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).unwrap();
    SignedBody {
        body,
        signer: owner_root,
        sig: signer.sign(&preimage).0,
    }
}

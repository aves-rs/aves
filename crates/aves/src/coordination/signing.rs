//! Byte-oriented Ed25519 signing primitives for coordination.

use arc_malachitebft_core_types::{
    PublicKey as MalachitePublicKey, Signature as MalachiteSignature, SignedMessage, ValidatorProof, VoteExtensionScope,
};
use arc_malachitebft_signing::{Error as SigningError, Signer, VerificationResult, Verifier};
use async_trait::async_trait;
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use specta::Type;

use crate::machine::MachinePeerService;

use super::abci::CommittedState;
use super::authority::current_member_authority;
use super::transaction::{ApplyOutcome, PendingTransaction, verify_body};

use super::types::{
    Acceptance, ConsensusProposal, ConsensusVote, CoordinationContext, CoordinationError, CoordinationEventId,
    DomainId, EnvelopeAddition, ExecutionCancelled, ExecutionCreated, ExecutionLanded, Invitation, MachineId,
    MachineRegistered, MemberAuthorityUpdated, MemberMachineSelfRegistration, MemberValidatorSplitChanged,
    PublicKey as CoordinationPublicKey, PublicationEnvelopeRef, RegisteredTransaction, ScopedPublicationKey,
    ShareScope, SharingNotice, SignedBody, StoryPublished, UserId, VerificationReport,
};

/// Closed set of records authorized for local machine-key signing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub enum MachineRecord {
    ExecutionCreated(ExecutionCreated),
    ExecutionLanded(ExecutionLanded),
    ExecutionCancelled(ExecutionCancelled),
    Acceptance {
        body: Acceptance,
        invitation: Box<SignedBody<Invitation>>,
    },
    MachineRegistered(MachineRegistered),
    MachineSelfRegistration(MemberMachineSelfRegistration),
    StoryPublished(StoryPublished),
    EnvelopeAddition(EnvelopeAddition),
    VerificationReport(VerificationReport),
    MemberValidatorSplitChanged(MemberValidatorSplitChanged),
    SharingNotice(Box<SharingNotice>),
    MemberAuthorityUpdated(MemberAuthorityUpdated),
}

/// Signs a typed body using the current native machine transport key.
pub(super) async fn sign_machine_record(
    services: Option<&dyn MachinePeerService>,
    domain: DomainId,
    record: MachineRecord,
    committed: Option<&CommittedState>,
    pending: &[(CoordinationEventId, PendingTransaction)],
) -> Result<(Vec<u8>, CoordinationEventId), CoordinationError> {
    let services = services.ok_or(CoordinationError::PeerServicesPending)?;
    let endpoint = services.transport().await.0;
    let signer = ChainSigner::new(endpoint.secret_key().clone());
    sign_machine_record_with_signer(domain, record, committed, pending, &signer)
}

fn sign_machine_record_with_signer(
    domain: DomainId,
    record: MachineRecord,
    committed: Option<&CommittedState>,
    pending: &[(CoordinationEventId, PendingTransaction)],
    signer: &ChainSigner,
) -> Result<(Vec<u8>, CoordinationEventId), CoordinationError> {
    if let MachineRecord::Acceptance { body, invitation } = &record {
        ensure_acceptance_invitation(domain, body, invitation, signer.public_key().into())?;
    }
    if let Some(committed) = committed {
        ensure_machine_identity(committed, domain, &record, signer.public_key().into())?;
        ensure_no_conflict(committed, pending, &record)?;
    } else if !matches!(record, MachineRecord::Acceptance { .. }) {
        return Err(CoordinationError::ChainNotHosted { domain });
    }

    macro_rules! sign_transaction {
        ($body:expr, $variant:ident) => {{
            let signed = sign_body(domain, $body, signer)?;
            let bytes = serde_json::to_vec(&signed).map_err(|_| CoordinationError::Encoding)?;
            let event_id = PendingTransaction::$variant(signed).event_id()?;
            Ok((bytes, event_id))
        }};
    }

    match record {
        MachineRecord::ExecutionCreated(body) => sign_transaction!(body, ExecutionCreated),
        MachineRecord::ExecutionLanded(body) => sign_transaction!(body, ExecutionLanded),
        MachineRecord::ExecutionCancelled(body) => sign_transaction!(body, ExecutionCancelled),
        MachineRecord::Acceptance { body, .. } => {
            let signed = sign_body(domain, body, signer)?;
            let bytes = serde_json::to_vec(&signed).map_err(|_| CoordinationError::Encoding)?;
            let event_id = PendingTransaction::Acceptance(Box::new(signed)).event_id()?;
            Ok((bytes, event_id))
        }
        MachineRecord::MachineRegistered(body) => sign_transaction!(body, MachineRegistered),
        MachineRecord::MachineSelfRegistration(body) => {
            let signed = sign_body(domain, body, signer)?;
            let bytes = serde_json::to_vec(&signed).map_err(|_| CoordinationError::Encoding)?;
            let event_id = CoordinationEventId::from_bytes(Sha256::digest(&bytes).into());
            Ok((bytes, event_id))
        }
        MachineRecord::StoryPublished(body) => sign_transaction!(body, StoryPublished),
        MachineRecord::EnvelopeAddition(body) => sign_transaction!(body, EnvelopeAddition),
        MachineRecord::VerificationReport(body) => sign_transaction!(body, VerificationReport),
        MachineRecord::MemberValidatorSplitChanged(body) => sign_transaction!(body, MemberValidatorSplitChanged),
        MachineRecord::SharingNotice(body) => {
            let signed = sign_body(domain, *body, signer)?;
            let bytes = serde_json::to_vec(&signed).map_err(|_| CoordinationError::Encoding)?;
            let event_id = PendingTransaction::SharingNotice(Box::new(signed)).event_id()?;
            Ok((bytes, event_id))
        }
        MachineRecord::MemberAuthorityUpdated(body) => sign_transaction!(body, MemberAuthorityUpdated),
    }
}

fn ensure_acceptance_invitation(
    domain: DomainId,
    body: &Acceptance,
    invitation: &SignedBody<Invitation>,
    key: CoordinationPublicKey,
) -> Result<(), CoordinationError> {
    if body.domain != domain
        || invitation.body.domain != domain
        || body.invitation != invitation.body.invitation
        || body.revision != invitation.body.revision
        || body.proof != invitation.body.proof
        || body.proof.certificate.body.machine_key != key
    {
        return Err(CoordinationError::SignatureInvalid);
    }
    verify_body(invitation, domain)
}

fn ensure_no_conflict(
    state: &CommittedState,
    pending: &[(CoordinationEventId, PendingTransaction)],
    record: &MachineRecord,
) -> Result<(), CoordinationError> {
    macro_rules! pending_conflict {
        ($variant:ident, $body:expr, $same:expr) => {
            pending.iter().any(|(_, transaction)| match transaction {
                PendingTransaction::$variant(previous) => ($same)(&previous.body, $body) && &previous.body != $body,
                _ => false,
            })
        };
    }
    let conflict = match record {
        MachineRecord::ExecutionCreated(body) => state.executions.get(body.execution_id.0.0.as_bytes()).is_some_and(|old|
            old.machine != body.machine || old.flow_id != body.flow_id)
            || pending_conflict!(ExecutionCreated, body, |a: &ExecutionCreated, b: &ExecutionCreated| a.execution_id == b.execution_id),
        MachineRecord::ExecutionLanded(body) => state.executions.get(body.execution_id.0.0.as_bytes())
            .and_then(|old| old.landed.as_ref()).is_some_and(|old| old != body)
            || pending_conflict!(ExecutionLanded, body, |a: &ExecutionLanded, b: &ExecutionLanded| a.execution_id == b.execution_id),
        MachineRecord::ExecutionCancelled(body) => state.executions.get(body.execution_id.0.0.as_bytes())
            .and_then(|old| old.cancelled.as_ref()).is_some_and(|old| old != body)
            || pending_conflict!(ExecutionCancelled, body, |a: &ExecutionCancelled, b: &ExecutionCancelled| a.execution_id == b.execution_id),
        MachineRecord::Acceptance { body, .. } => state.invitations.get(&body.invitation).and_then(|old| old.acceptance.as_ref())
            .is_some_and(|old| old.revision == body.revision && old != body)
            || pending_conflict!(Acceptance, body, |a: &Acceptance, b: &Acceptance| a.invitation == b.invitation && a.revision == b.revision),
        MachineRecord::MachineRegistered(body) => state.machines.get(&body.machine).is_some_and(|old|
            old.registrations.iter().any(|registration| registration.machine_key != body.machine_key
                || registration.sonya_key != body.sonya_key || registration.enc_key != body.enc_key))
            || pending.iter().any(|(_, tx)| matches!(tx, PendingTransaction::MachineRegistered(old)
                if old.body.machine == body.machine && old.body != *body)),
        MachineRecord::MachineSelfRegistration(body) => state.member_machine_certs.get(&(body.member, body.machine))
            .is_some_and(|old| old.revision == body.revision && old.registration.body != *body)
            || state.owner_actions.iter().any(|old| matches!(old, super::membership::OwnerAction::Certificate(old)
                if old.member == body.member && old.machine == body.machine && old.revision == body.revision
                    && old.registration.body != *body))
            || pending.iter().any(|(_, tx)| matches!(tx, PendingTransaction::MemberMachineCertUpdate(old)
                if old.body.member == body.member && old.body.machine == body.machine && old.body.revision == body.revision
                    && old.body.registration.body != *body)),
        MachineRecord::StoryPublished(body) => state.publications.iter().any(|old| old.publication.key == body.key && old.publication != *body)
            || pending_conflict!(StoryPublished, body, |a: &StoryPublished, b: &StoryPublished| a.key == b.key),
        MachineRecord::EnvelopeAddition(body) => state.envelope_additions.iter().any(|old| old.key == body.key
                && old.envelope.record_root == body.envelope.record_root && old.envelope.target_machine == body.envelope.target_machine && old != body)
            || state.publications.iter().any(|old| old.publication.key == body.key && old.publication.records.iter().any(|record|
                record.root == body.envelope.record_root && record.envelopes.iter().any(|envelope| envelope.target_machine == body.envelope.target_machine)))
            || pending.iter().any(|(_, transaction)| matches!(transaction, PendingTransaction::StoryPublished(old)
                if old.body.key == body.key && old.body.records.iter().any(|record|
                    record.root == body.envelope.record_root && record.envelopes.iter().any(|envelope| envelope.target_machine == body.envelope.target_machine))))
            || pending_conflict!(EnvelopeAddition, body, |a: &EnvelopeAddition, b: &EnvelopeAddition| a.key == b.key
                && a.envelope.record_root == b.envelope.record_root && a.envelope.target_machine == b.envelope.target_machine),
        MachineRecord::VerificationReport(body) => state.verification_reports.iter().any(|old| old.execution_id == body.execution_id
                && old.revision == body.revision && old != body)
            || pending_conflict!(VerificationReport, body, |a: &VerificationReport, b: &VerificationReport| a.execution_id == b.execution_id && a.revision == b.revision),
        MachineRecord::MemberValidatorSplitChanged(body) => state.owner_actions.iter().any(|old| matches!(old,
            super::membership::OwnerAction::Split(old) if old.member == body.member && old.revision == body.revision && old != body))
            || pending_conflict!(MemberValidatorSplitChanged, body, |a: &MemberValidatorSplitChanged, b: &MemberValidatorSplitChanged| a.member == b.member && a.revision == b.revision),
        MachineRecord::SharingNotice(body) => state.sharing_notices.get(&body.key).is_some_and(|history|
                history.iter().any(|old| old.revision == body.revision && old != body.as_ref()))
            || pending_conflict!(SharingNotice, body.as_ref(), |a: &SharingNotice, b: &SharingNotice| a.key == b.key && a.revision == b.revision),
        MachineRecord::MemberAuthorityUpdated(body) => state.member_authorities.get(&body.member).is_some_and(|authority|
                authority.history.iter().any(|old| old.body.new_revision == body.new_revision && old.body != *body))
            || pending_conflict!(MemberAuthorityUpdated, body, |a: &MemberAuthorityUpdated, b: &MemberAuthorityUpdated| a.member == b.member && a.new_revision == b.new_revision),
    };
    if conflict {
        Err(CoordinationError::SignatureInvalid)
    } else {
        Ok(())
    }
}

fn machine_belongs_to(state: &CommittedState, user: UserId, machine: MachineId, key: CoordinationPublicKey) -> bool {
    if current_member_authority(state, user).is_some_and(|authority| {
        authority
            .certified_machines
            .iter()
            .any(|binding| binding.machine == machine && binding.machine_key == key)
    }) {
        return true;
    }
    if !state.owner.as_ref().is_some_and(|owner| owner.user == user) {
        return false;
    }
    let Some(record) = state.machines.get(&machine) else {
        return false;
    };
    if !state
        .users
        .get(&user)
        .is_some_and(|record| record.revoked.is_none())
        || record
            .unbindings
            .iter()
            .any(|unbinding| unbinding.user == user)
    {
        return false;
    }
    record.bindings.iter().any(|binding| {
        binding.user == user
            && binding.machine_key == key
            && record.registrations.iter().any(|registration| {
                registration.machine_key == key
                    && registration.sonya_key == binding.sonya_key
                    && registration.enc_key == binding.enc_key
            })
    })
}

fn envelope_is_granted(state: &CommittedState, key: ScopedPublicationKey, envelope: &PublicationEnvelopeRef) -> bool {
    let certified = if let Some(authority) = current_member_authority(state, envelope.recipient) {
        authority.proof_revision == envelope.certificate_revision
            && authority
                .certified_machines
                .iter()
                .any(|machine| machine.machine == envelope.target_machine)
    } else {
        envelope.certificate_revision == 0
            && state
                .machines
                .get(&envelope.target_machine)
                .is_some_and(|machine| {
                    machine.bindings.iter().any(|binding| {
                        machine_belongs_to(state, envelope.recipient, envelope.target_machine, binding.machine_key)
                    })
                })
    };
    if !certified {
        return false;
    }
    state.share_grants.iter().any(|grant| {
        grant.domain == key.domain
            && grant.grantor == key.origin_owner_user
            && grant.recipient == envelope.recipient
            && grant.active
            && (grant.scope == ShareScope::Ticket(key.source_ticket_id)
                || grant.scope == ShareScope::Project(key.source_project_id))
            && !state.share_grants.iter().any(|later| {
                later.domain == grant.domain
                    && later.grantor == grant.grantor
                    && later.recipient == grant.recipient
                    && later.scope == grant.scope
                    && later.revision > grant.revision
            })
    })
}

fn ensure_machine_identity(
    state: &CommittedState,
    domain: DomainId,
    record: &MachineRecord,
    key: CoordinationPublicKey,
) -> Result<(), CoordinationError> {
    let valid = match record {
        MachineRecord::ExecutionCreated(body) => state.machines.get(&body.machine).is_some_and(|machine| {
            machine
                .bindings
                .iter()
                .any(|binding| machine_belongs_to(state, binding.user, body.machine, key))
        }),
        MachineRecord::ExecutionLanded(body) => state
            .executions
            .get(body.execution_id.0.0.as_bytes())
            .is_some_and(|execution| {
                state
                    .machines
                    .get(&execution.machine)
                    .is_some_and(|machine| {
                        machine
                            .bindings
                            .iter()
                            .any(|binding| machine_belongs_to(state, binding.user, execution.machine, key))
                    })
            }),
        MachineRecord::ExecutionCancelled(body) => state
            .executions
            .get(body.execution_id.0.0.as_bytes())
            .is_some_and(|execution| {
                state
                    .machines
                    .get(&execution.machine)
                    .is_some_and(|machine| {
                        machine
                            .bindings
                            .iter()
                            .any(|binding| machine_belongs_to(state, binding.user, execution.machine, key))
                    })
            }),
        MachineRecord::Acceptance { body, .. } => {
            body.domain == domain
                && state
                    .invitations
                    .get(&body.invitation)
                    .is_some_and(|invitation| {
                        body.revision == invitation.invitation.revision
                            && body.proof == invitation.invitation.proof
                            && body.proof.certificate.body.machine_key == key
                    })
        }
        MachineRecord::MachineRegistered(body) => body.machine_key == key,
        MachineRecord::MachineSelfRegistration(body) => {
            body.domain == domain
                && body.machine_key == key
                && (current_member_authority(state, body.member).is_some()
                    || state
                        .owner
                        .as_ref()
                        .is_some_and(|owner| owner.user == body.member))
        }
        MachineRecord::StoryPublished(body) => {
            body.key.domain == domain
                && machine_belongs_to(state, body.key.origin_owner_user, body.publisher, key)
                && super::sharing::records_digest(&body.records) == Some(body.digest)
                && body.records.iter().all(|record| {
                    super::sharing::valid_attestation(record)
                        && record
                            .envelopes
                            .iter()
                            .all(|envelope| super::sharing::covered_ref(state, body.key, body.publisher, envelope))
                })
        }
        MachineRecord::EnvelopeAddition(body) => {
            body.key.domain == domain
                && machine_belongs_to(state, body.key.origin_owner_user, body.publisher, key)
                && state.publications.iter().any(|publication| {
                    publication.publication.key == body.key
                        && publication
                            .publication
                            .records
                            .iter()
                            .any(|record| record.root == body.envelope.record_root)
                })
                && envelope_is_granted(state, body.key, &body.envelope)
        }
        MachineRecord::VerificationReport(body) => {
            body.domain == domain
                && state.kind == Some(super::types::ChainKind::Personal)
                && state
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.user == body.user)
                && machine_belongs_to(state, body.user, body.reporting_machine, key)
        }
        MachineRecord::MemberValidatorSplitChanged(body) => {
            body.domain == domain
                && current_member_authority(state, body.member).is_some_and(|authority| {
                    authority
                        .certified_machines
                        .iter()
                        .any(|binding| binding.machine_key == key)
                })
        }
        MachineRecord::SharingNotice(body) => {
            body.domain == domain
                && state.kind == Some(super::types::ChainKind::Personal)
                && state
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.user == body.key.target_user)
                && state
                    .machines
                    .values()
                    .any(|machine| machine_belongs_to(state, body.key.target_user, machine.machine, key))
                && super::notices::apply_sharing_notice(
                    &mut state.clone(),
                    &SignedBody {
                        body: body.as_ref().clone(),
                        signer: key,
                        sig: [0; 64],
                    },
                ) == ApplyOutcome::Applied
        }
        MachineRecord::MemberAuthorityUpdated(body) => {
            body.domain == domain
                && current_member_authority(state, body.member).is_some_and(|authority| {
                    authority
                        .certified_machines
                        .iter()
                        .any(|binding| binding.machine_key == key)
                        && (authority.history.iter().any(|entry| entry.body == *body)
                            || super::authority::apply_authority_update(
                                &mut state.clone(),
                                &SignedBody {
                                    body: body.clone(),
                                    signer: key,
                                    sig: [0; 64],
                                },
                            ) == ApplyOutcome::Applied)
                })
        }
    };
    if valid {
        Ok(())
    } else {
        Err(CoordinationError::SignatureInvalid)
    }
}

fn sign_body<T: RegisteredTransaction + Serialize>(
    domain: DomainId,
    body: T,
    signer: &ChainSigner,
) -> Result<SignedBody<T>, CoordinationError> {
    let mut preimage = T::PURPOSE.as_bytes().to_vec();
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).map_err(|_| CoordinationError::Encoding)?;
    Ok(SignedBody {
        body,
        signer: signer.public_key().into(),
        sig: signer.sign(&preimage).0,
    })
}

pub type ChainPrivateKey = SecretKey;

type VoteScope = VoteExtensionScope<CoordinationContext>;
type VoteSignature = SignedMessage<CoordinationContext, ConsensusVote>;
type ProposalSignature = SignedMessage<CoordinationContext, ConsensusProposal>;
type Vote = ConsensusVote;
type Proposal = ConsensusProposal;
type ConsensusSig = MalachiteSignature<CoordinationContext>;
type ConsensusKey = MalachitePublicKey<CoordinationContext>;
type ConsensusProof = ValidatorProof<CoordinationContext>;
type ExtensionSig = SignedMessage<CoordinationContext, Vec<u8>>;
type VerifyResult = Result<VerificationResult, SigningError>;

const VOTE_PURPOSE: &[u8] = b"aves/coordination/consensus/vote/v1";
const PROPOSAL_PURPOSE: &[u8] = b"aves/coordination/consensus/proposal/v1";

/// Ed25519 public key bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainPublicKey(pub [u8; 32]);

impl TryFrom<CoordinationPublicKey> for ChainPublicKey {
    type Error = KeyEncodingError;

    fn try_from(public_key: CoordinationPublicKey) -> Result<Self, Self::Error> {
        ChainSigningScheme::decode_public_key(&public_key.0)
    }
}

impl From<ChainPublicKey> for CoordinationPublicKey {
    fn from(public_key: ChainPublicKey) -> Self {
        Self(public_key.0)
    }
}

/// Raw Ed25519 signature bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChainSignature(pub [u8; 64]);

/// Key or signature bytes have an invalid length or key encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid Ed25519 key or signature encoding")]
pub struct KeyEncodingError;

/// Ed25519 key and signature byte conversion helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainSigningScheme;

impl ChainSigningScheme {
    /// Decodes an exact-length Ed25519 signature.
    pub fn decode_signature(bytes: &[u8]) -> Result<ChainSignature, KeyEncodingError> {
        <[u8; 64]>::try_from(bytes)
            .map(ChainSignature)
            .map_err(|_| KeyEncodingError)
    }

    /// Encodes a signature as its 64 raw bytes.
    pub fn encode_signature(signature: &ChainSignature) -> Vec<u8> {
        signature.0.to_vec()
    }

    /// Decodes an exact-length, valid Ed25519 public key.
    pub fn decode_public_key(bytes: &[u8]) -> Result<ChainPublicKey, KeyEncodingError> {
        let key = <[u8; 32]>::try_from(bytes).map_err(|_| KeyEncodingError)?;
        PublicKey::from_bytes(&key).map_err(|_| KeyEncodingError)?;
        Ok(ChainPublicKey(key))
    }

    /// Encodes a public key as its 32 raw bytes.
    pub fn encode_public_key(public_key: &ChainPublicKey) -> Vec<u8> {
        public_key.0.to_vec()
    }
}

/// Signs caller-supplied bytes; Malachite adapters encode typed messages separately.
#[derive(Debug, Clone)]
pub struct ChainSigner {
    key: ChainPrivateKey,
}

impl ChainSigner {
    /// Creates a signer for the given private key.
    pub fn new(key: ChainPrivateKey) -> Self {
        Self { key }
    }

    /// Returns the signer's public key.
    pub fn public_key(&self) -> ChainPublicKey {
        ChainPublicKey(*self.key.public().as_bytes())
    }

    /// Signs the supplied bytes exactly as provided.
    pub fn sign(&self, message: &[u8]) -> ChainSignature {
        ChainSignature(self.key.sign(message).to_bytes())
    }
}

/// Verifies caller-supplied bytes without interpreting or encoding them.
#[derive(Debug, Clone, Copy)]
pub struct ChainVerifier;

impl ChainVerifier {
    /// Returns whether the signature is valid for the exact supplied message bytes.
    pub fn verify(&self, public_key: &ChainPublicKey, message: &[u8], signature: &ChainSignature) -> bool {
        PublicKey::from_bytes(&public_key.0).is_ok_and(|key| {
            key.verify(message, &Signature::from_bytes(&signature.0))
                .is_ok()
        })
    }
}

/// Malachite signer bound to one coordination chain.
#[derive(Debug, Clone)]
pub(super) struct CoordinationSigner {
    signer: ChainSigner,
    domain: DomainId,
}

impl CoordinationSigner {
    /// Creates a signer whose consensus preimages are bound to `domain`.
    pub(super) fn new(key: ChainPrivateKey, domain: DomainId) -> Self {
        Self {
            signer: ChainSigner::new(key),
            domain,
        }
    }
}

/// Malachite verifier bound to one coordination chain.
#[derive(Debug, Clone, Copy)]
pub(super) struct CoordinationVerifier(DomainId);

impl CoordinationVerifier {
    /// Creates a verifier bound to `domain`.
    pub(super) const fn new(domain: DomainId) -> Self {
        Self(domain)
    }
}

#[async_trait]
impl Signer<CoordinationContext> for CoordinationSigner {
    async fn sign_vote(&self, vote: ConsensusVote) -> Result<VoteSignature, SigningError> {
        let signature = self
            .signer
            .sign(&preimage(VOTE_PURPOSE, self.domain, &vote)?);
        Ok(SignedMessage::new(vote, signature))
    }

    async fn sign_proposal(&self, proposal: ConsensusProposal) -> Result<ProposalSignature, SigningError> {
        let signature = self
            .signer
            .sign(&preimage(PROPOSAL_PURPOSE, self.domain, &proposal)?);
        Ok(SignedMessage::new(proposal, signature))
    }

    async fn sign_vote_extension(&self, _: VoteScope, _: Vec<u8>) -> Result<ExtensionSig, SigningError> {
        Err(SigningError::new())
    }

    async fn sign_validator_proof(
        &self,
        public_key: Vec<u8>,
        peer_id: Vec<u8>,
    ) -> Result<ConsensusProof, SigningError> {
        let decoded = ChainSigningScheme::decode_public_key(&public_key).map_err(SigningError::from_source)?;
        if decoded != self.signer.public_key() {
            return Err(SigningError::new());
        }
        let bytes = ConsensusProof::signing_bytes(&public_key, &peer_id);
        let signature = self.signer.sign(&bytes);
        Ok(ValidatorProof::new(public_key, peer_id, signature))
    }
}

#[async_trait]
impl Verifier<CoordinationContext> for CoordinationVerifier {
    async fn verify_signed_vote(
        &self,
        vote: &Vote,
        signature: &ConsensusSig,
        public_key: &ConsensusKey,
    ) -> VerifyResult {
        verify_message(VOTE_PURPOSE, self.0, vote, signature, public_key)
    }

    async fn verify_signed_proposal(
        &self,
        proposal: &Proposal,
        signature: &ConsensusSig,
        public_key: &ConsensusKey,
    ) -> VerifyResult {
        verify_message(PROPOSAL_PURPOSE, self.0, proposal, signature, public_key)
    }

    async fn verify_signed_vote_extension(
        &self,
        _: &VoteScope,
        _: &Vec<u8>,
        _: &ConsensusSig,
        _: &ConsensusKey,
    ) -> VerifyResult {
        Err(SigningError::new())
    }

    async fn verify_validator_proof(&self, proof: &ConsensusProof) -> VerifyResult {
        let Ok(public_key) = proof.decoded_public_key() else {
            return Ok(VerificationResult::Invalid);
        };
        let bytes = ConsensusProof::signing_bytes(&proof.public_key, &proof.peer_id);
        let valid = ChainVerifier.verify(&public_key, &bytes, &proof.signature);
        Ok(VerificationResult::from_bool(valid))
    }
}

fn preimage<T: Serialize>(purpose: &[u8], domain: DomainId, message: &T) -> Result<Vec<u8>, SigningError> {
    let mut bytes = purpose.to_vec();
    bytes.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut bytes, message).map_err(SigningError::from_source)?;
    Ok(bytes)
}

fn verify_message<T: Serialize>(
    purpose: &[u8],
    domain: DomainId,
    message: &T,
    signature: &ChainSignature,
    public_key: &ChainPublicKey,
) -> VerifyResult {
    let valid = ChainVerifier.verify(public_key, &preimage(purpose, domain, message)?, signature);
    Ok(VerificationResult::from_bool(valid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::types::{ConsensusValue, ConsensusVoteKind, Height};
    use crate::coordination::types::{
        EncKey, InvitationId, MachineBound, MemberIdentityProof, MemberRecord, MemberStatus,
    };
    use arc_malachitebft_core_types::NilOrVal;

    const DOMAIN: DomainId = DomainId::from_bytes([31; 32]);

    fn acceptance_fixture() -> (ChainSigner, Acceptance, SignedBody<Invitation>) {
        let machine = ChainSigner::new(SecretKey::from_bytes(&[17; 32]));
        let owner = ChainSigner::new(SecretKey::from_bytes(&[18; 32]));
        let key = machine.public_key().into();
        let proof = MemberIdentityProof {
            source_domain: DomainId::from_bytes([32; 32]),
            certificate: SignedBody {
                body: MachineBound {
                    user: UserId([1; 32]),
                    machine: MachineId([2; 32]),
                    machine_key: key,
                    sonya_key: key,
                    enc_key: EncKey([3; 32]),
                    enc_epoch: 1,
                },
                signer: key,
                sig: [0; 64],
            },
        };
        let invitation = Invitation {
            domain: DOMAIN,
            invitation: InvitationId([4; 32]),
            revision: 1,
            target_user: UserId([1; 32]),
            target_root: key,
            proof: proof.clone(),
        };
        let acceptance = Acceptance {
            domain: DOMAIN,
            invitation: invitation.invitation,
            revision: invitation.revision,
            proof,
        };
        (machine, acceptance, sign_body(DOMAIN, invitation, &owner).unwrap())
    }

    #[test]
    fn unhosted_invitee_signs_the_delivered_invitation_deterministically() {
        let (machine, body, invitation) = acceptance_fixture();
        let record = MachineRecord::Acceptance {
            body,
            invitation: Box::new(invitation),
        };
        let first = sign_machine_record_with_signer(DOMAIN, record.clone(), None, &[], &machine).unwrap();
        let second = sign_machine_record_with_signer(DOMAIN, record, None, &[], &machine).unwrap();
        assert_eq!(first, second);
        let signed: SignedBody<Acceptance> = serde_json::from_slice(&first.0).unwrap();
        assert_eq!(signed.signer, CoordinationPublicKey::from(machine.public_key()));
        assert!(verify_body(&signed, DOMAIN).is_ok());
    }

    #[test]
    fn unhosted_acceptance_rejects_mismatched_proof_key_and_signature() {
        let (machine, body, invitation) = acceptance_fixture();
        let mut changed = body.clone();
        changed.proof.certificate.body.enc_epoch += 1;
        let record = MachineRecord::Acceptance {
            body: changed,
            invitation: Box::new(invitation.clone()),
        };
        assert!(sign_machine_record_with_signer(DOMAIN, record, None, &[], &machine).is_err());
        let other = ChainSigner::new(SecretKey::from_bytes(&[19; 32]));
        let record = MachineRecord::Acceptance {
            body: body.clone(),
            invitation: Box::new(invitation.clone()),
        };
        assert!(sign_machine_record_with_signer(DOMAIN, record, None, &[], &other).is_err());
        let mut tampered = invitation;
        tampered.sig[0] ^= 1;
        let record = MachineRecord::Acceptance {
            body,
            invitation: Box::new(tampered),
        };
        assert!(sign_machine_record_with_signer(DOMAIN, record, None, &[], &machine).is_err());
    }

    #[tokio::test]
    async fn missing_machine_service_is_pending() {
        let result = sign_machine_record(
            None,
            DOMAIN,
            MachineRecord::MachineSelfRegistration(MemberMachineSelfRegistration {
                domain: DOMAIN,
                member: super::super::types::UserId([1; 32]),
                machine: super::super::types::MachineId([2; 32]),
                machine_key: CoordinationPublicKey([3; 32]),
                revision: 1,
            }),
            Some(&CommittedState::default()),
            &[],
        )
        .await;
        assert!(matches!(result, Err(CoordinationError::PeerServicesPending)));
    }

    #[test]
    fn typed_body_signatures_are_deterministic() {
        let signer = ChainSigner::new(SecretKey::from_bytes(&[17; 32]));
        let body = MemberMachineSelfRegistration {
            domain: DOMAIN,
            member: super::super::types::UserId([1; 32]),
            machine: super::super::types::MachineId([2; 32]),
            machine_key: signer.public_key().into(),
            revision: 1,
        };
        assert_eq!(
            sign_body(DOMAIN, body.clone(), &signer).unwrap(),
            sign_body(DOMAIN, body, &signer).unwrap()
        );
    }

    #[test]
    fn wrong_machine_key_is_refused() {
        let signer = ChainSigner::new(SecretKey::from_bytes(&[17; 32]));
        let key = CoordinationPublicKey::from(signer.public_key());
        let user = UserId([1; 32]);
        let machine = MachineId([2; 32]);
        let binding = MachineBound {
            user,
            machine,
            machine_key: key,
            sonya_key: key,
            enc_key: EncKey([4; 32]),
            enc_epoch: 1,
        };
        let mut state = CommittedState::default();
        state.members.insert(
            user,
            MemberRecord {
                member: user,
                status: MemberStatus::Active,
                revision: 1,
                root: CoordinationPublicKey([5; 32]),
                proof: MemberIdentityProof {
                    source_domain: DOMAIN,
                    certificate: SignedBody {
                        body: binding,
                        signer: CoordinationPublicKey([5; 32]),
                        sig: [0; 64],
                    },
                },
            },
        );
        let body = MemberMachineSelfRegistration {
            domain: DOMAIN,
            member: user,
            machine,
            machine_key: CoordinationPublicKey([3; 32]),
            revision: 1,
        };
        assert!(
            ensure_machine_identity(
                &state,
                DOMAIN,
                &MachineRecord::MachineSelfRegistration(body),
                signer.public_key().into()
            )
            .is_err()
        );
    }

    #[test]
    fn non_member_machine_is_refused() {
        let signer = ChainSigner::new(SecretKey::from_bytes(&[17; 32]));
        let body = MemberMachineSelfRegistration {
            domain: DOMAIN,
            member: super::super::types::UserId([1; 32]),
            machine: super::super::types::MachineId([2; 32]),
            machine_key: signer.public_key().into(),
            revision: 1,
        };
        assert!(
            ensure_machine_identity(
                &CommittedState::default(),
                DOMAIN,
                &MachineRecord::MachineSelfRegistration(body),
                signer.public_key().into()
            )
            .is_err()
        );
    }

    #[test]
    fn member_machine_self_registration_needs_membership_but_not_prior_certification() {
        let signer = ChainSigner::new(SecretKey::from_bytes(&[21; 32]));
        let key: CoordinationPublicKey = signer.public_key().into();
        let member = UserId([1; 32]);
        let mut state = CommittedState::default();
        state.members.insert(
            member,
            MemberRecord {
                member,
                status: MemberStatus::Active,
                revision: 0,
                root: CoordinationPublicKey([5; 32]),
                proof: MemberIdentityProof {
                    source_domain: DOMAIN,
                    certificate: SignedBody {
                        body: MachineBound {
                            user: member,
                            machine: MachineId([2; 32]),
                            machine_key: CoordinationPublicKey([3; 32]),
                            sonya_key: CoordinationPublicKey([3; 32]),
                            enc_key: EncKey([4; 32]),
                            enc_epoch: 1,
                        },
                        signer: CoordinationPublicKey([5; 32]),
                        sig: [0; 64],
                    },
                },
            },
        );
        let body = MemberMachineSelfRegistration {
            domain: DOMAIN,
            member,
            machine: MachineId([6; 32]),
            machine_key: key,
            revision: 1,
        };
        assert!(
            ensure_machine_identity(
                &state,
                DOMAIN,
                &MachineRecord::MachineSelfRegistration(body.clone()),
                key
            )
            .is_ok()
        );
        assert!(
            ensure_machine_identity(
                &CommittedState::default(),
                DOMAIN,
                &MachineRecord::MachineSelfRegistration(body),
                key
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn machine_registration_bootstraps_from_the_local_key_and_is_deterministic() {
        let signer = ChainSigner::new(SecretKey::from_bytes(&[19; 32]));
        let key: CoordinationPublicKey = signer.public_key().into();
        let body = super::super::types::MachineRegistered {
            machine: MachineId([22; 32]),
            name: "bootstrap".to_owned(),
            machine_key: key,
            sonya_key: key,
            enc_key: super::super::types::EncKey([23; 32]),
        };
        assert!(
            ensure_machine_identity(
                &CommittedState::default(),
                DOMAIN,
                &MachineRecord::MachineRegistered(body.clone()),
                key
            )
            .is_ok()
        );
        let first = sign_body(DOMAIN, body.clone(), &signer).unwrap();
        let second = sign_body(DOMAIN, body, &signer).unwrap();
        assert_eq!(first, second);
    }

    fn make_signer(seed: u8) -> CoordinationSigner {
        CoordinationSigner::new(SecretKey::from_bytes(&[seed; 32]), DOMAIN)
    }

    fn vote(address: CoordinationPublicKey) -> ConsensusVote {
        ConsensusVote {
            height: Height(4),
            round: 2,
            value: NilOrVal::Nil,
            kind: ConsensusVoteKind::Precommit,
            address,
            extension: None,
        }
    }

    fn proposal(address: CoordinationPublicKey) -> ConsensusProposal {
        ConsensusProposal {
            height: Height(4),
            round: 2,
            value: ConsensusValue("[{\"entry\":1}]".to_owned()),
            pol_round: 1,
            proposer: address,
        }
    }

    #[tokio::test]
    async fn vote_signing_binds_message_domain_key_and_signature() {
        let signer = make_signer(41);
        let address = signer.signer.public_key().into();
        let signed = Signer::sign_vote(&signer, vote(address)).await.unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        let key = signer.signer.public_key();

        assert!(
            verifier
                .verify_signed_vote(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_vote = signed.message.clone();
        changed_vote.round += 1;
        assert!(
            verifier
                .verify_signed_vote(&changed_vote, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            CoordinationVerifier::new(DomainId::from_bytes([32; 32]))
                .verify_signed_vote(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );

        let other_key = make_signer(42).signer.public_key();
        assert!(
            verifier
                .verify_signed_vote(&signed.message, &signed.signature, &other_key)
                .await
                .unwrap()
                .is_invalid()
        );
        let mut changed_signature = signed.signature;
        changed_signature.0[0] ^= 1;
        assert!(
            verifier
                .verify_signed_vote(&signed.message, &changed_signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
    }

    #[tokio::test]
    async fn proposal_signing_binds_message_domain_and_key() {
        let signer = make_signer(51);
        let address = signer.signer.public_key().into();
        let signed = Signer::sign_proposal(&signer, proposal(address))
            .await
            .unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        let key = signer.signer.public_key();

        assert!(
            verifier
                .verify_signed_proposal(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_proposal = signed.message.clone();
        changed_proposal.round += 1;
        assert!(
            verifier
                .verify_signed_proposal(&changed_proposal, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            CoordinationVerifier::new(DomainId::from_bytes([32; 32]))
                .verify_signed_proposal(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        let other_key = make_signer(52).signer.public_key();
        assert!(
            verifier
                .verify_signed_proposal(&signed.message, &signed.signature, &other_key)
                .await
                .unwrap()
                .is_invalid()
        );
    }

    #[tokio::test]
    async fn validator_proof_binds_key_and_peer_identity() {
        let signer = make_signer(61);
        let public_key = ChainSigningScheme::encode_public_key(&signer.signer.public_key());
        let proof = Signer::sign_validator_proof(&signer, public_key.clone(), b"peer-a".to_vec())
            .await
            .unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        assert!(
            verifier
                .verify_validator_proof(&proof)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_peer = proof.clone();
        changed_peer.peer_id = b"peer-b".to_vec();
        assert!(
            verifier
                .verify_validator_proof(&changed_peer)
                .await
                .unwrap()
                .is_invalid()
        );

        let other_key = ChainSigningScheme::encode_public_key(&make_signer(62).signer.public_key());
        let mut changed_key = proof.clone();
        changed_key.public_key = other_key.clone();
        assert!(
            verifier
                .verify_validator_proof(&changed_key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            Signer::sign_validator_proof(&signer, other_key, b"peer-a".to_vec())
                .await
                .is_err()
        );
    }
}

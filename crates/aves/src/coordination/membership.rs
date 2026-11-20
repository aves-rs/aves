//! Deterministic application of collective membership records.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::abci::CommittedState;
use super::authority::current_member_authority;
use super::transaction::{ApplyOutcome, PendingTransaction, RejectionReason, chain_owner_authorized, verify_body};
use super::types::{
    AUTHORITY_RULES_VERSION, Acceptance, ChainPolicyChanged, ChainPolicyRecord, Invitation, InvitationRecord,
    MachineId, MemberIdentityProof, MemberMachineCertUpdate, MemberPowerChanged, MemberPowerRecord, MemberRecord,
    MemberRemoved, MemberStatus, MemberValidatorSplitChanged, MemberValidatorSplitRecord, MembershipDecision,
    OwnerRegistration, PublicKey, UserId,
};

/// Retains each logical revision so retries and conflicts remain distinguishable after updates.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum OwnerAction {
    Registration(OwnerRegistration),
    Invitation(Invitation),
    Decision(MembershipDecision),
    Removal(MemberRemoved),
    Power(MemberPowerChanged),
    Policy(ChainPolicyChanged),
    Acceptance(Acceptance),
    Split(MemberValidatorSplitChanged),
    Certificate(Box<MemberMachineCertUpdate>),
}

impl OwnerAction {
    fn same_revision(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Registration(a), Self::Registration(b)) => a.owner == b.owner && a.revision == b.revision,
            (Self::Invitation(a), Self::Invitation(b)) => a.invitation == b.invitation && a.revision == b.revision,
            (Self::Decision(a), Self::Decision(b)) => a.invitation == b.invitation && a.revision == b.revision,
            (Self::Removal(a), Self::Removal(b)) => a.member == b.member && a.revision == b.revision,
            (Self::Power(a), Self::Power(b)) => a.member == b.member && a.revision == b.revision,
            (Self::Policy(a), Self::Policy(b)) => a.domain == b.domain && a.version == b.version,
            (Self::Acceptance(a), Self::Acceptance(b)) => a.invitation == b.invitation && a.revision == b.revision,
            (Self::Split(a), Self::Split(b)) => a.member == b.member && a.revision == b.revision,
            (Self::Certificate(a), Self::Certificate(b)) => {
                a.member == b.member && a.machine == b.machine && a.revision == b.revision
            }
            _ => false,
        }
    }
}

/// Applies the six owner record families, leaving other transaction families to their owner.
pub(super) fn apply_owner_action(transaction: &PendingTransaction, state: &mut CommittedState) -> Option<ApplyOutcome> {
    let (action, signer) = match transaction {
        PendingTransaction::OwnerRegistration(s) => (OwnerAction::Registration(s.body.clone()), s.signer),
        PendingTransaction::Invitation(s) => (OwnerAction::Invitation(s.body.clone()), s.signer),
        PendingTransaction::MembershipDecision(s) => (OwnerAction::Decision(s.body.clone()), s.signer),
        PendingTransaction::MemberRemoved(s) => (OwnerAction::Removal(s.body.clone()), s.signer),
        PendingTransaction::MemberPowerChanged(s) => (OwnerAction::Power(s.body.clone()), s.signer),
        PendingTransaction::ChainPolicyChanged(s) => (OwnerAction::Policy(s.body.clone()), s.signer),
        _ => return None,
    };
    Some(match apply_action(state, action, signer) {
        Ok(()) => ApplyOutcome::Applied,
        Err(reason) => ApplyOutcome::Rejected(reason),
    })
}

fn apply_action(state: &mut CommittedState, action: OwnerAction, signer: PublicKey) -> Result<(), RejectionReason> {
    if !chain_owner_authorized(state, signer) {
        return Err(RejectionReason::UnauthorizedSigner);
    }
    if let Some(previous) = state
        .owner_actions
        .iter()
        .find(|previous| previous.same_revision(&action))
    {
        return if *previous == action {
            Ok(())
        } else {
            Err(RejectionReason::ConflictingAction)
        };
    }
    match &action {
        OwnerAction::Registration(body) => {
            let owner = state
                .owner
                .as_ref()
                .ok_or(RejectionReason::UnauthorizedSigner)?;
            if body.owner != owner.user
                || body.root != signer
                || body.revision != 1
                || state.owner_registration.is_some()
            {
                return Err(RejectionReason::StaleRevision);
            }
            state.owner_registration = Some(body.clone());
        }
        OwnerAction::Invitation(body) => apply_invitation(state, body)?,
        OwnerAction::Decision(body) => apply_decision(state, body)?,
        OwnerAction::Removal(body) => {
            let member = state
                .members
                .get_mut(&body.member)
                .ok_or(RejectionReason::InvalidPayload)?;
            if member.status != MemberStatus::Active
                || body.previous_revision != member.revision
                || body.previous_revision.checked_add(1) != Some(body.revision)
            {
                return Err(RejectionReason::StaleRevision);
            }
            member.status = MemberStatus::Removed;
            member.revision = body.revision;
        }
        OwnerAction::Power(body) => {
            if current_member_authority(state, body.member).is_none() {
                return Err(RejectionReason::InvalidPayload);
            }
            let previous = state
                .member_power
                .get(&body.member)
                .map_or(0, |record| record.revision);
            if body.previous_revision != previous || previous.checked_add(1) != Some(body.revision) {
                return Err(RejectionReason::StaleRevision);
            }
            state.member_power.insert(
                body.member,
                MemberPowerRecord {
                    member: body.member,
                    revision: body.revision,
                    total: body.total,
                },
            );
        }
        OwnerAction::Policy(body) => {
            if body.rules_version != AUTHORITY_RULES_VERSION {
                return Err(RejectionReason::InvalidPayload);
            }
            let previous = state.policy.as_ref().map_or(0, |record| record.version);
            if body.previous_version != previous || previous.checked_add(1) != Some(body.version) {
                return Err(RejectionReason::StaleRevision);
            }
            state.policy = Some(ChainPolicyRecord {
                version: body.version,
                rules_version: body.rules_version,
            });
        }
        OwnerAction::Acceptance(_) | OwnerAction::Split(_) | OwnerAction::Certificate(_) => {
            return Err(RejectionReason::UnauthorizedSigner);
        }
    }
    state.owner_actions.push(action);
    Ok(())
}

/// Applies member-signed records after signature verification at the transaction boundary.
pub(super) fn apply_member_action(
    transaction: &PendingTransaction,
    state: &mut CommittedState,
) -> Option<ApplyOutcome> {
    let (action, signer) = match transaction {
        PendingTransaction::Acceptance(s) => (OwnerAction::Acceptance(s.body.clone()), s.signer),
        PendingTransaction::MemberValidatorSplitChanged(s) => (OwnerAction::Split(s.body.clone()), s.signer),
        PendingTransaction::MemberMachineCertUpdate(s) => {
            (OwnerAction::Certificate(Box::new(s.body.clone())), s.signer)
        }
        _ => return None,
    };
    Some(match apply_member(state, action, signer) {
        Ok(()) => ApplyOutcome::Applied,
        Err(reason) => ApplyOutcome::Rejected(reason),
    })
}

fn apply_member(state: &mut CommittedState, action: OwnerAction, signer: PublicKey) -> Result<(), RejectionReason> {
    match &action {
        OwnerAction::Acceptance(body) => {
            let invitation = state
                .invitations
                .get(&body.invitation)
                .ok_or(RejectionReason::InvalidPayload)?;
            if !valid_proof(
                &invitation.invitation.proof,
                invitation.invitation.target_user,
                invitation.invitation.target_root,
            ) || invitation.invitation.proof.certificate.body.machine_key != signer
            {
                return Err(RejectionReason::UnauthorizedSigner);
            }
        }
        OwnerAction::Split(body) => {
            let authority = current_member_authority(state, body.member).ok_or(RejectionReason::InvalidPayload)?;
            if signer != authority.root && !certified_signer(state, body.member, signer) {
                return Err(RejectionReason::UnauthorizedSigner);
            }
        }
        OwnerAction::Certificate(body) => {
            let authority = current_member_authority(state, body.member).ok_or(RejectionReason::InvalidPayload)?;
            if signer != authority.root {
                return Err(RejectionReason::UnauthorizedSigner);
            }
        }
        _ => return Err(RejectionReason::InvalidPayload),
    }
    if let Some(previous) = state
        .owner_actions
        .iter()
        .find(|previous| previous.same_revision(&action))
    {
        return if *previous == action {
            Ok(())
        } else {
            Err(RejectionReason::ConflictingAction)
        };
    }
    match &action {
        OwnerAction::Acceptance(body) => {
            let invitation = state
                .invitations
                .get_mut(&body.invitation)
                .ok_or(RejectionReason::InvalidPayload)?;
            if invitation.decision.is_some()
                || body.revision != invitation.invitation.revision
                || invitation.acceptance.is_some()
            {
                return Err(RejectionReason::StaleRevision);
            }
            if body.proof != invitation.invitation.proof
                || !valid_proof(
                    &body.proof,
                    invitation.invitation.target_user,
                    invitation.invitation.target_root,
                )
            {
                return Err(RejectionReason::InvalidPayload);
            }
            invitation.acceptance = Some(body.clone());
        }
        OwnerAction::Split(body) => apply_split(state, body)?,
        OwnerAction::Certificate(body) => apply_certificate(state, body)?,
        _ => return Err(RejectionReason::InvalidPayload),
    }
    state.owner_actions.push(action);
    Ok(())
}

fn valid_proof(proof: &MemberIdentityProof, user: UserId, root: PublicKey) -> bool {
    proof.certificate.signer == root
        && proof.certificate.body.user == user
        && verify_body(&proof.certificate, proof.source_domain).is_ok()
}

fn active_member(state: &CommittedState, user: UserId) -> Result<&MemberRecord, RejectionReason> {
    state
        .members
        .get(&user)
        .filter(|member| member.status == MemberStatus::Active)
        .ok_or(RejectionReason::InvalidPayload)
}

fn certified_machine(state: &CommittedState, user: UserId, machine: MachineId) -> Option<PublicKey> {
    current_member_authority(state, user)?
        .certified_machines
        .into_iter()
        .find(|entry| entry.machine == machine)
        .map(|entry| entry.machine_key)
}

fn certified_signer(state: &CommittedState, user: UserId, signer: PublicKey) -> bool {
    current_member_authority(state, user).is_some_and(|authority| {
        authority
            .certified_machines
            .iter()
            .any(|machine| machine.machine_key == signer)
    })
}

fn apply_split(state: &mut CommittedState, body: &MemberValidatorSplitChanged) -> Result<(), RejectionReason> {
    let previous = state
        .member_splits
        .get(&body.member)
        .map_or(0, |record| record.revision);
    if body.previous_revision != previous || previous.checked_add(1) != Some(body.revision) {
        return Err(RejectionReason::StaleRevision);
    }
    let mut machines = BTreeSet::new();
    if body.splits.iter().any(|part| {
        part.parts == 0
            || !machines.insert(part.machine)
            || certified_machine(state, body.member, part.machine).is_none()
    }) {
        return Err(RejectionReason::InvalidPayload);
    }
    state.member_splits.insert(
        body.member,
        MemberValidatorSplitRecord {
            member: body.member,
            revision: body.revision,
            splits: body.splits.clone(),
        },
    );
    Ok(())
}

fn apply_certificate(state: &mut CommittedState, body: &MemberMachineCertUpdate) -> Result<(), RejectionReason> {
    let member = active_member(state, body.member)?;
    let authority = current_member_authority(state, body.member).ok_or(RejectionReason::InvalidPayload)?;
    let previous = state
        .member_machine_certs
        .get(&(body.member, body.machine))
        .map_or(0, |record| record.revision);
    if body.previous_revision != previous || previous.checked_add(1) != Some(body.revision) {
        return Err(RejectionReason::StaleRevision);
    }
    let registration = &body.registration;
    if body.proof.source_domain != member.proof.source_domain
        || body.proof.source_domain == body.domain
        || !valid_proof(&body.proof, body.member, authority.root)
        || (state.member_authorities.contains_key(&body.member)
            && !authority
                .certified_machines
                .iter()
                .any(|machine| machine == &body.proof.certificate.body))
        || body.proof.certificate.body.machine != body.machine
        || registration.body.domain != body.domain
        || registration.body.member != body.member
        || registration.body.machine != body.machine
        || registration.body.revision != body.revision
        || registration.body.machine_key != body.proof.certificate.body.machine_key
        || registration.signer != registration.body.machine_key
        || verify_body(registration, body.domain).is_err()
    {
        return Err(RejectionReason::InvalidPayload);
    }
    state
        .member_machine_certs
        .insert((body.member, body.machine), body.clone());
    if let Some(authority) = state.member_authorities.get_mut(&body.member) {
        authority
            .certified_machines
            .retain(|machine| machine.machine != body.machine);
        authority
            .certified_machines
            .push(body.proof.certificate.body.clone());
        authority
            .certified_machines
            .sort_by_key(|machine| machine.machine);
    }
    Ok(())
}

fn apply_invitation(state: &mut CommittedState, body: &Invitation) -> Result<(), RejectionReason> {
    let previous = state.invitations.get(&body.invitation);
    let revision = previous.map_or(0, |record| record.invitation.revision);
    if revision.checked_add(1) != Some(body.revision) || previous.is_some_and(|record| record.decision.is_some()) {
        return Err(RejectionReason::StaleRevision);
    }
    if body.proof.source_domain == body.domain
        || body.proof.certificate.signer != body.target_root
        || body.proof.certificate.body.user != body.target_user
        || verify_body(&body.proof.certificate, body.proof.source_domain).is_err()
    {
        return Err(RejectionReason::InvalidPayload);
    }
    state.invitations.insert(
        body.invitation,
        InvitationRecord {
            invitation: body.clone(),
            acceptance: None,
            decision: None,
        },
    );
    Ok(())
}

fn apply_decision(state: &mut CommittedState, body: &MembershipDecision) -> Result<(), RejectionReason> {
    let invitation = state
        .invitations
        .get(&body.invitation)
        .ok_or(RejectionReason::InvalidPayload)?;
    if body.revision != 1 || invitation.decision.is_some() {
        return Err(RejectionReason::StaleRevision);
    }
    if body.member != invitation.invitation.target_user
        || body.root != invitation.invitation.target_root
        || body.proof != invitation.invitation.proof
    {
        return Err(RejectionReason::InvalidPayload);
    }
    if body.approved {
        let acceptance = invitation
            .acceptance
            .as_ref()
            .ok_or(RejectionReason::InvalidPayload)?;
        if acceptance.proof != body.proof || state.members.contains_key(&body.member) {
            return Err(RejectionReason::InvalidPayload);
        }
        state.members.insert(
            body.member,
            MemberRecord {
                member: body.member,
                status: MemberStatus::Active,
                revision: 0,
                root: body.root,
                proof: acceptance.proof.clone(),
            },
        );
        state.member_power.insert(
            body.member,
            MemberPowerRecord {
                member: body.member,
                revision: 0,
                total: 0,
            },
        );
    }
    state
        .invitations
        .get_mut(&body.invitation)
        .ok_or(RejectionReason::InvalidPayload)?
        .decision = Some(body.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::abci::ChainOwner;
    use crate::coordination::signing::ChainSigner;
    use crate::coordination::types::{
        ChainKind, DomainId, EncKey, Height, InvitationId, MachineBound, RegisteredTransaction, SignedBody,
        UserRegistered,
    };

    fn signed<T: serde::Serialize + RegisteredTransaction>(
        body: T,
        signer: &ChainSigner,
        domain: DomainId,
    ) -> SignedBody<T> {
        let mut bytes = T::PURPOSE.as_bytes().to_vec();
        bytes.extend_from_slice(domain.as_bytes());
        serde_json::to_writer(&mut bytes, &body).unwrap();
        SignedBody {
            body,
            signer: PublicKey::from(signer.public_key()),
            sig: signer.sign(&bytes).0,
        }
    }

    fn submit(state: &mut CommittedState, domain: DomainId, tx: PendingTransaction) -> Result<(), RejectionReason> {
        let event = tx.event_id().unwrap();
        state
            .apply_transactions(domain, vec![tx], Height(1))
            .unwrap();
        state
            .rejected_events
            .get(&event)
            .copied()
            .map_or(Ok(()), Err)
    }

    #[test]
    fn invitation_acceptance_and_approval_are_authorized_and_idempotent() {
        let domain = DomainId([1; 32]);
        let source = DomainId([2; 32]);
        let owner = ChainSigner::new(iroh::SecretKey::from_bytes(&[3; 32]));
        let root = ChainSigner::new(iroh::SecretKey::from_bytes(&[4; 32]));
        let machine = ChainSigner::new(iroh::SecretKey::from_bytes(&[5; 32]));
        let user = UserId([6; 32]);
        let machine_key = PublicKey::from(machine.public_key());
        let proof = MemberIdentityProof {
            source_domain: source,
            certificate: signed(
                MachineBound {
                    user,
                    machine: MachineId([7; 32]),
                    machine_key,
                    sonya_key: machine_key,
                    enc_key: EncKey([8; 32]),
                    enc_epoch: 1,
                },
                &root,
                source,
            ),
        };
        let invitation = Invitation {
            domain,
            invitation: InvitationId([9; 32]),
            revision: 1,
            target_user: user,
            target_root: PublicKey::from(root.public_key()),
            proof: proof.clone(),
        };
        let mut state = CommittedState::default();
        state.owner = Some(ChainOwner {
            user: UserId([10; 32]),
            root: PublicKey::from(owner.public_key()),
        });
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Invitation(Box::new(signed(invitation.clone(), &root, domain)))
            ),
            Err(RejectionReason::UnauthorizedSigner)
        );
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Invitation(Box::new(signed(invitation.clone(), &owner, domain)))
            ),
            Ok(())
        );
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Invitation(Box::new(signed(invitation.clone(), &owner, domain)))
            ),
            Ok(())
        );
        let mut conflict = invitation.clone();
        conflict.target_user = UserId([11; 32]);
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Invitation(Box::new(signed(conflict, &owner, domain)))
            ),
            Err(RejectionReason::ConflictingAction)
        );
        let mut bad_proof = proof.clone();
        bad_proof.certificate.sig[0] ^= 1;
        let bad = Acceptance {
            domain,
            invitation: invitation.invitation,
            revision: 1,
            proof: bad_proof,
        };
        assert!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Acceptance(Box::new(signed(bad, &machine, domain)))
            )
            .is_err()
        );
        let acceptance = Acceptance {
            domain,
            invitation: invitation.invitation,
            revision: 1,
            proof: proof.clone(),
        };
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::Acceptance(Box::new(signed(acceptance, &machine, domain)))
            ),
            Ok(())
        );
        assert!(!state.members.contains_key(&user));
        let decision = MembershipDecision {
            domain,
            invitation: invitation.invitation,
            revision: 1,
            approved: true,
            member: user,
            root: invitation.target_root,
            proof,
        };
        assert_eq!(
            submit(
                &mut state,
                domain,
                PendingTransaction::MembershipDecision(Box::new(signed(decision, &owner, domain)))
            ),
            Ok(())
        );
        assert_eq!(state.members.get(&user).unwrap().status, MemberStatus::Active);
        assert_eq!(state.member_power.get(&user).unwrap().total, 0);
    }

    #[test]
    fn collective_user_registration_is_owner_only() {
        let domain = DomainId([12; 32]);
        let owner = ChainSigner::new(iroh::SecretKey::from_bytes(&[13; 32]));
        let other = ChainSigner::new(iroh::SecretKey::from_bytes(&[14; 32]));
        let owner_user = UserId([15; 32]);
        let other_user = UserId([16; 32]);
        let owner_key = PublicKey::from(owner.public_key());
        let other_key = PublicKey::from(other.public_key());
        let other_registration = UserRegistered {
            user: other_user,
            root_key: other_key,
            enc_key: EncKey([17; 32]),
            enc_epoch: 1,
        };
        let mut collective = CommittedState::default();
        collective.kind = Some(ChainKind::Collective);
        collective.owner = Some(ChainOwner {
            user: owner_user,
            root: owner_key,
        });
        assert_eq!(
            submit(
                &mut collective,
                domain,
                PendingTransaction::UserRegistered(signed(other_registration.clone(), &other, domain))
            ),
            Err(RejectionReason::UnauthorizedSigner)
        );
        assert!(!collective.users.contains_key(&other_user));
        let owner_registration = UserRegistered {
            user: owner_user,
            root_key: owner_key,
            enc_key: EncKey([18; 32]),
            enc_epoch: 1,
        };
        assert_eq!(
            submit(
                &mut collective,
                domain,
                PendingTransaction::UserRegistered(signed(owner_registration, &owner, domain))
            ),
            Ok(())
        );
        let mut personal = CommittedState::default();
        personal.kind = Some(ChainKind::Personal);
        personal.owner = Some(ChainOwner {
            user: owner_user,
            root: owner_key,
        });
        assert_eq!(
            submit(
                &mut personal,
                domain,
                PendingTransaction::UserRegistered(signed(other_registration, &other, domain))
            ),
            Ok(())
        );
    }
}

//! Deterministic personal-chain sharing notice application.

use sha2::{Digest, Sha256};

use super::abci::CommittedState;
use super::serialization::canonical_json;
use super::transaction::{ApplyOutcome, RejectionReason, verify_body};
use super::types::{
    ChainKind, GenesisFingerprint, PublicKey, SharingDecision, SharingNotice, SharingNoticeState, SignedBody, UserId,
};

/// Applies an addressed receipt after the outer signature has been verified.
pub(super) fn apply_sharing_notice(state: &mut CommittedState, signed: &SignedBody<SharingNotice>) -> ApplyOutcome {
    match apply(state, signed) {
        Ok(()) => ApplyOutcome::Applied,
        Err(reason) => ApplyOutcome::Rejected(reason),
    }
}

fn certified(state: &CommittedState, user: UserId, signer: PublicKey) -> bool {
    if state.kind != Some(ChainKind::Personal)
        || !state.owner.as_ref().is_some_and(|owner| owner.user == user)
        || !state
            .users
            .get(&user)
            .is_some_and(|record| record.revoked.is_none())
    {
        return false;
    }
    state.machines.values().any(|machine| {
        !machine
            .unbindings
            .iter()
            .any(|unbinding| unbinding.user == user)
            && machine.bindings.iter().any(|binding| {
                binding.user == user
                    && binding.machine == machine.machine
                    && binding.machine_key == signer
                    && machine.registrations.iter().any(|registration| {
                        registration.machine == machine.machine
                            && registration.machine_key == binding.machine_key
                            && registration.sonya_key == binding.sonya_key
                            && registration.enc_key == binding.enc_key
                    })
            })
    })
}

fn valid_evidence(notice: &SharingNotice) -> bool {
    let key = notice.key;
    let bootstrap = &notice.bootstrap;
    let genesis = &bootstrap.descriptor;
    let invitation = &notice.invitation;
    let fingerprint = canonical_json(genesis)
        .ok()
        .map(|bytes| GenesisFingerprint(Sha256::digest(bytes).into()));
    if notice.domain == key.collective_domain
        || genesis.body.domain != key.collective_domain
        || genesis.body.kind != ChainKind::Collective
        || genesis.body.owner_user != bootstrap.owner
        || genesis.body.owner_root != bootstrap.owner_root
        || genesis.signer != bootstrap.owner_root
        || fingerprint != Some(bootstrap.genesis_fingerprint)
        || verify_body(genesis, key.collective_domain).is_err()
        || invitation.signer != bootstrap.owner_root
        || verify_body(invitation, key.collective_domain).is_err()
        || invitation.body.domain != key.collective_domain
        || invitation.body.invitation != key.invitation
        || invitation.body.target_user != key.target_user
        || invitation.body.proof.source_domain != notice.domain
        || invitation.body.proof.certificate.body.user != key.target_user
        || invitation.body.proof.certificate.signer != invitation.body.target_root
        || verify_body(&invitation.body.proof.certificate, notice.domain).is_err()
    {
        return false;
    }
    match (&notice.state, &notice.decision) {
        (SharingNoticeState::Invited, None) => notice.response.is_none(),
        (SharingNoticeState::Responded(response), None) => notice.response == Some(*response),
        (SharingNoticeState::Decided(result), Some(decision)) => {
            decision.signer == bootstrap.owner_root
                && verify_body(decision, key.collective_domain).is_ok()
                && decision.body.domain == key.collective_domain
                && decision.body.invitation == key.invitation
                && decision.body.member == key.target_user
                && decision.body.root == invitation.body.target_root
                && decision.body.proof == invitation.body.proof
                && decision.body.approved == (*result == SharingDecision::Approved)
        }
        _ => false,
    }
}

fn apply(state: &mut CommittedState, signed: &SignedBody<SharingNotice>) -> Result<(), RejectionReason> {
    let notice = &signed.body;
    if !certified(state, notice.key.target_user, signed.signer) {
        return Err(RejectionReason::UnauthorizedSigner);
    }
    let history = state.sharing_notices.get(&notice.key);
    if let Some(previous) = history
        .into_iter()
        .flatten()
        .find(|previous| previous.revision == notice.revision)
    {
        return if previous == notice {
            Ok(())
        } else {
            Err(RejectionReason::ConflictingAction)
        };
    }
    if !valid_evidence(notice) {
        return Err(RejectionReason::InvalidPayload);
    }
    let previous = history.and_then(|records| records.last());
    if previous.map_or(0, |record| record.revision).checked_add(1) != Some(notice.revision) {
        return Err(RejectionReason::StaleRevision);
    }
    if let Some(previous) = previous {
        if previous.invitation != notice.invitation
            || previous.bootstrap != notice.bootstrap
            || matches!(previous.state, SharingNoticeState::Decided(_))
            || matches!(
                (previous.state, notice.state),
                (SharingNoticeState::Responded(_), SharingNoticeState::Invited)
            )
            || (matches!(notice.state, SharingNoticeState::Decided(_)) && previous.response != notice.response)
        {
            return Err(RejectionReason::InvalidPayload);
        }
    } else if notice.state != SharingNoticeState::Invited {
        return Err(RejectionReason::InvalidPayload);
    }
    let history_len = state.sharing_notices.get(&notice.key).map_or(0, Vec::len);
    let signers = state.sharing_notice_signers.entry(notice.key).or_default();
    signers.truncate(history_len);
    signers.resize(history_len, None);
    signers.push(Some(signed.signer));
    state
        .sharing_notices
        .entry(notice.key)
        .or_default()
        .push(notice.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::abci::ChainOwner;
    use super::super::signing::ChainSigner;
    use super::super::transaction::PendingTransaction;
    use super::super::types::{
        AUTHORITY_RULES_VERSION, BlobHash, DomainId, EncKey, GenesisDescriptor, Height, Invitation, InvitationId,
        MachineBound, MachineId, MachineRegistered, MemberIdentityProof, RecordHash, RegisteredTransaction,
        SharingBootstrap, SharingNoticeKey, SharingResponse, UserRegistered,
    };
    use super::*;

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

    #[test]
    fn addressed_receipts_reject_foreign_signers_and_conflicts_but_accept_retries() {
        let personal = DomainId([1; 32]);
        let collective = DomainId([2; 32]);
        let root = ChainSigner::new(iroh::SecretKey::from_bytes(&[3; 32]));
        let owner = ChainSigner::new(iroh::SecretKey::from_bytes(&[4; 32]));
        let machine = ChainSigner::new(iroh::SecretKey::from_bytes(&[5; 32]));
        let foreign = ChainSigner::new(iroh::SecretKey::from_bytes(&[6; 32]));
        let user = UserId([7; 32]);
        let foreign_user = UserId([15; 32]);
        let foreign_machine = MachineId([16; 32]);
        let foreign_key = PublicKey::from(foreign.public_key());
        let owner_user = UserId([8; 32]);
        let machine_id = MachineId([9; 32]);
        let machine_key = PublicKey::from(machine.public_key());
        let root_key = PublicKey::from(root.public_key());
        let owner_key = PublicKey::from(owner.public_key());
        let binding = MachineBound {
            user,
            machine: machine_id,
            machine_key,
            sonya_key: machine_key,
            enc_key: EncKey([10; 32]),
            enc_epoch: 1,
        };
        let mut state = CommittedState::default();
        state.kind = Some(ChainKind::Personal);
        state.owner = Some(ChainOwner { user, root: root_key });
        let transactions = vec![
            PendingTransaction::UserRegistered(signed(
                UserRegistered {
                    user,
                    root_key,
                    enc_key: EncKey([10; 32]),
                    enc_epoch: 1,
                },
                &root,
                personal,
            )),
            PendingTransaction::MachineRegistered(signed(
                MachineRegistered {
                    machine: machine_id,
                    name: String::new(),
                    machine_key,
                    sonya_key: machine_key,
                    enc_key: EncKey([10; 32]),
                },
                &machine,
                personal,
            )),
            PendingTransaction::MachineBound(signed(binding.clone(), &root, personal)),
            PendingTransaction::UserRegistered(signed(
                UserRegistered {
                    user: foreign_user,
                    root_key: foreign_key,
                    enc_key: EncKey([17; 32]),
                    enc_epoch: 1,
                },
                &foreign,
                personal,
            )),
            PendingTransaction::MachineRegistered(signed(
                MachineRegistered {
                    machine: foreign_machine,
                    name: String::new(),
                    machine_key: foreign_key,
                    sonya_key: foreign_key,
                    enc_key: EncKey([17; 32]),
                },
                &foreign,
                personal,
            )),
            PendingTransaction::MachineBound(signed(
                MachineBound {
                    user: foreign_user,
                    machine: foreign_machine,
                    machine_key: foreign_key,
                    sonya_key: foreign_key,
                    enc_key: EncKey([17; 32]),
                    enc_epoch: 1,
                },
                &foreign,
                personal,
            )),
        ];
        state
            .apply_transactions(personal, transactions, Height(1))
            .unwrap();
        let descriptor = signed(
            GenesisDescriptor {
                domain: collective,
                kind: ChainKind::Collective,
                owner_user,
                owner_root: owner_key,
                creator_machine: owner_key,
                initial_roster: vec![(owner_key, 1)],
                rules_version: AUTHORITY_RULES_VERSION,
            },
            &owner,
            collective,
        );
        let fingerprint = GenesisFingerprint(Sha256::digest(canonical_json(&descriptor).unwrap()).into());
        let invitation_id = InvitationId([11; 32]);
        let invitation = signed(
            Invitation {
                domain: collective,
                invitation: invitation_id,
                revision: 1,
                target_user: user,
                target_root: root_key,
                proof: MemberIdentityProof {
                    source_domain: personal,
                    certificate: signed(binding, &root, personal),
                },
            },
            &owner,
            collective,
        );
        let notice = SharingNotice {
            domain: personal,
            key: SharingNoticeKey {
                collective_domain: collective,
                invitation: invitation_id,
                target_user: user,
            },
            revision: 1,
            state: SharingNoticeState::Invited,
            response: None,
            invitation,
            decision: None,
            bootstrap: SharingBootstrap {
                descriptor_ref: BlobHash([12; 32]),
                descriptor,
                genesis_fingerprint: fingerprint,
                expected_snapshot_hash: RecordHash([13; 32]),
                owner: owner_user,
                owner_root: owner_key,
            },
        };
        let tx = |body, signer| PendingTransaction::SharingNotice(Box::new(signed(body, signer, personal)));
        let foreign_tx = tx(notice.clone(), &foreign);
        let foreign_id = foreign_tx.event_id().unwrap();
        state
            .apply_transactions(personal, vec![foreign_tx], Height(2))
            .unwrap();
        assert_eq!(
            state.rejected_events.get(&foreign_id),
            Some(&RejectionReason::UnauthorizedSigner)
        );
        let original = tx(notice.clone(), &machine);
        state
            .apply_transactions(personal, vec![original.clone()], Height(3))
            .unwrap();
        assert_eq!(original.apply(&mut state, Height(4)).unwrap(), ApplyOutcome::Applied);
        let mut conflict = notice.clone();
        conflict.bootstrap.expected_snapshot_hash = RecordHash([14; 32]);
        let conflict_tx = tx(conflict, &machine);
        let conflict_id = conflict_tx.event_id().unwrap();
        state
            .apply_transactions(personal, vec![conflict_tx], Height(5))
            .unwrap();
        assert_eq!(
            state.rejected_events.get(&conflict_id),
            Some(&RejectionReason::ConflictingAction)
        );
        let mut response = notice;
        response.revision = 2;
        response.state = SharingNoticeState::Responded(SharingResponse::Accept);
        response.response = Some(SharingResponse::Accept);
        assert_eq!(
            tx(response.clone(), &machine)
                .apply(&mut state, Height(6))
                .unwrap(),
            ApplyOutcome::Applied
        );
        response.revision = 1;
        assert_eq!(
            tx(response, &machine).apply(&mut state, Height(7)).unwrap(),
            ApplyOutcome::Rejected(RejectionReason::ConflictingAction)
        );
    }
}

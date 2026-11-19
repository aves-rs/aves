//! Verified source-domain authority for collective members.

use super::abci::CommittedState;
use super::transaction::{ApplyOutcome, RejectionReason, verify_body};
use super::types::{MemberAuthority, MemberAuthorityUpdated, MemberStatus, SignedBody, SourceProof, UserId};

/// Resolves the current root and unrevoked certified machines for an active member.
pub(super) fn current_member_authority(state: &CommittedState, member: UserId) -> Option<MemberAuthority> {
    if state
        .owner
        .as_ref()
        .is_some_and(|owner| owner.user == member)
        && !state.members.contains_key(&member)
    {
        let owner = state.owner.as_ref()?;
        let user = state.users.get(&member)?;
        if user.revoked.is_some() {
            return None;
        }
        let root = user
            .root_rotations
            .last()
            .map_or(owner.root, |rotation| rotation.new_root);
        let mut certified_machines = state
            .machines
            .values()
            .flat_map(|machine| {
                machine
                    .bindings
                    .iter()
                    .filter(move |binding| {
                        binding.user == member && !machine.unbindings.iter().any(|event| event.user == member)
                    })
                    .cloned()
            })
            .collect::<Vec<_>>();
        certified_machines.sort_by_key(|machine| machine.machine);
        return Some(MemberAuthority {
            root,
            certified_machines,
            proof_revision: 0,
            history: Vec::new(),
        });
    }
    let record = state
        .members
        .get(&member)
        .filter(|record| record.status == MemberStatus::Active)?;
    if let Some(authority) = state.member_authorities.get(&member) {
        return Some(authority.clone());
    }
    let mut machines = vec![record.proof.certificate.body.clone()];
    for ((user, _), cert) in &state.member_machine_certs {
        if *user == member {
            machines.retain(|machine| machine.machine != cert.machine);
            machines.push(cert.proof.certificate.body.clone());
        }
    }
    machines.retain(|machine| {
        !state
            .machines
            .get(&machine.machine)
            .is_some_and(|record| record.unbindings.iter().any(|event| event.user == member))
    });
    machines.sort_by_key(|machine| machine.machine);
    Some(MemberAuthority {
        root: record.root,
        certified_machines: machines,
        proof_revision: 0,
        history: Vec::new(),
    })
}

/// Applies an ordered proof batch only after validating the complete candidate.
/// Unbinding permanently revokes a machine; rotations order themselves through root
/// continuity, and each binding must verify against the root current at that point.
pub(super) fn apply_authority_update(
    state: &mut CommittedState,
    signed: &SignedBody<MemberAuthorityUpdated>,
) -> ApplyOutcome {
    match update(state, signed) {
        Ok(()) => ApplyOutcome::Applied,
        Err(reason) => ApplyOutcome::Rejected(reason),
    }
}

fn update(state: &mut CommittedState, signed: &SignedBody<MemberAuthorityUpdated>) -> Result<(), RejectionReason> {
    let body = &signed.body;
    let member = state
        .members
        .get(&body.member)
        .ok_or(RejectionReason::InvalidPayload)?;
    if member.proof.source_domain != body.source_domain || body.source_domain == body.domain {
        return Err(RejectionReason::InvalidPayload);
    }
    let mut candidate = current_member_authority(state, body.member).ok_or(RejectionReason::InvalidPayload)?;
    if !candidate
        .certified_machines
        .iter()
        .any(|machine| machine.machine_key == signed.signer)
    {
        return Err(RejectionReason::UnauthorizedSigner);
    }
    if body.previous_revision != candidate.proof_revision
        || body.previous_revision.checked_add(1) != Some(body.new_revision)
    {
        return Err(RejectionReason::StaleRevision);
    }
    if body.proofs.is_empty() {
        return Err(RejectionReason::InvalidPayload);
    }
    let mut seen = candidate
        .history
        .iter()
        .flat_map(|record| record.body.proofs.iter())
        .collect::<Vec<_>>();
    for proof in &body.proofs {
        if seen.contains(&proof) {
            return Err(RejectionReason::ConflictingAction);
        }
        match proof {
            SourceProof::MachineBound(cert) => {
                if cert.signer != candidate.root || cert.body.user != body.member || verify_body(cert, body.source_domain).is_err()
                    || *cert == member.proof.certificate
                    || state.member_machine_certs.iter().any(|((user, _), update)| *user == body.member && update.proof.certificate == *cert)
                    || seen.iter().any(|proof| matches!(proof, SourceProof::MachineUnbound(previous) if previous.body.machine == cert.body.machine))
                    || candidate.certified_machines.iter().any(|machine| machine.machine == cert.body.machine)
                { return Err(RejectionReason::InvalidPayload); }
                candidate.certified_machines.push(cert.body.clone());
            }
            SourceProof::MachineUnbound(cert) => {
                if cert.signer != candidate.root
                    || cert.body.user != body.member
                    || verify_body(cert, body.source_domain).is_err()
                    || !candidate
                        .certified_machines
                        .iter()
                        .any(|machine| machine.machine == cert.body.machine)
                {
                    return Err(RejectionReason::InvalidPayload);
                }
                candidate
                    .certified_machines
                    .retain(|machine| machine.machine != cert.body.machine);
            }
            SourceProof::UserRootRotated(rotation) => {
                if rotation.signer != candidate.root
                    || rotation.body.user != body.member
                    || rotation.body.previous_root != candidate.root
                    || rotation.body.new_root == candidate.root
                    || verify_body(rotation, body.source_domain).is_err()
                {
                    return Err(RejectionReason::InvalidPayload);
                }
                candidate.root = rotation.body.new_root;
            }
        }
        seen.push(proof);
    }
    candidate
        .certified_machines
        .sort_by_key(|machine| machine.machine);
    candidate.proof_revision = body.new_revision;
    candidate.history.push(signed.clone());
    state.member_authorities.insert(body.member, candidate);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::signing::ChainSigner;
    use crate::coordination::transaction::PendingTransaction;
    use crate::coordination::types::{
        DomainId, EncKey, Height, MachineBound, MachineId, MachineUnbound, MemberIdentityProof, MemberRecord,
        PublicKey, RegisteredTransaction, UserRootRotated,
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

    #[test]
    fn relay_rotation_unbinding_and_order_are_verified_atomically() {
        let domain = DomainId([1; 32]);
        let source = DomainId([2; 32]);
        let user = UserId([3; 32]);
        let root = ChainSigner::new(iroh::SecretKey::from_bytes(&[4; 32]));
        let next = ChainSigner::new(iroh::SecretKey::from_bytes(&[5; 32]));
        let third = ChainSigner::new(iroh::SecretKey::from_bytes(&[10; 32]));
        let machine = ChainSigner::new(iroh::SecretKey::from_bytes(&[6; 32]));
        let other = ChainSigner::new(iroh::SecretKey::from_bytes(&[7; 32]));
        let root_key = PublicKey::from(root.public_key());
        let next_key = PublicKey::from(next.public_key());
        let third_key = PublicKey::from(third.public_key());
        let machine_key = PublicKey::from(machine.public_key());
        let machine_id = MachineId([8; 32]);
        let proof = MemberIdentityProof {
            source_domain: source,
            certificate: signed(
                MachineBound {
                    user,
                    machine: machine_id,
                    machine_key,
                    sonya_key: machine_key,
                    enc_key: EncKey([9; 32]),
                    enc_epoch: 1,
                },
                &root,
                source,
            ),
        };
        let mut state = CommittedState::default();
        state.members.insert(
            user,
            MemberRecord {
                member: user,
                status: MemberStatus::Active,
                revision: 0,
                root: root_key,
                proof,
            },
        );
        let rotation = SourceProof::UserRootRotated(signed(
            UserRootRotated {
                user,
                previous_root: root_key,
                new_root: next_key,
                effective_at: 1,
            },
            &root,
            source,
        ));
        let unbound = SourceProof::MachineUnbound(signed(
            MachineUnbound {
                user,
                machine: machine_id,
                effective_at: 2,
                compromised_from: None,
            },
            &third,
            source,
        ));
        let second_rotation = SourceProof::UserRootRotated(signed(
            UserRootRotated {
                user,
                previous_root: next_key,
                new_root: third_key,
                effective_at: 2,
            },
            &next,
            source,
        ));
        let mut body = MemberAuthorityUpdated {
            domain,
            member: user,
            source_domain: source,
            previous_revision: 0,
            new_revision: 1,
            proofs: vec![rotation.clone(), second_rotation, unbound.clone()],
        };
        let bad = signed(body.clone(), &other, domain);
        assert_eq!(
            apply_authority_update(&mut state, &bad),
            ApplyOutcome::Rejected(RejectionReason::UnauthorizedSigner)
        );
        body.proofs.reverse();
        let wrong_order = signed(body.clone(), &machine, domain);
        assert_eq!(
            apply_authority_update(&mut state, &wrong_order),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
        assert_eq!(current_member_authority(&state, user).unwrap().root, root_key);
        body.proofs.reverse();
        let update = signed(body, &machine, domain);
        let tx = PendingTransaction::MemberAuthorityUpdated(update);
        state
            .apply_transactions(domain, vec![tx], Height(1))
            .unwrap();
        let authority = current_member_authority(&state, user).unwrap();
        assert_eq!(authority.root, third_key);
        assert_eq!(authority.proof_revision, 1);
        assert!(authority.certified_machines.is_empty());
        let repeat = MemberAuthorityUpdated {
            domain,
            member: user,
            source_domain: source,
            previous_revision: 1,
            new_revision: 2,
            proofs: vec![rotation],
        };
        assert_eq!(
            apply_authority_update(&mut state, &signed(repeat, &machine, domain)),
            ApplyOutcome::Rejected(RejectionReason::UnauthorizedSigner)
        );
    }
}

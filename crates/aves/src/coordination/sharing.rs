//! Deterministic sharing and verification-report application.

use sha2::{Digest, Sha256};

use super::abci::CommittedState;
use super::authority::current_member_authority;
use super::transaction::{ApplyOutcome, PendingTransaction, RejectionReason, chain_owner_authorized, verify_body};
use super::types::{
    ChainKind, EnvelopeRef, Height, MachineId, MemberStatus, PublicKey, PublishedRecordRef, RecordAttestation,
    RecordHash, ScopedPublicationKey, ShareScope, UserId,
};

fn member_root(state: &CommittedState, user: UserId, signer: PublicKey) -> bool {
    if state.owner.as_ref().is_some_and(|owner| owner.user == user) {
        return chain_owner_authorized(state, signer);
    }
    current_member_authority(state, user).is_some_and(|authority| authority.root == signer)
}

fn member_exists(state: &CommittedState, user: UserId) -> bool {
    state.owner.as_ref().is_some_and(|owner| owner.user == user)
        || state
            .members
            .get(&user)
            .is_some_and(|member| member.status == MemberStatus::Active)
}

fn machine_key(state: &CommittedState, user: UserId, machine: MachineId, revision: u64) -> Option<PublicKey> {
    if !member_exists(state, user) {
        return None;
    }
    if let Some(authority) = current_member_authority(state, user) {
        if authority.proof_revision != revision {
            return None;
        }
        return authority
            .certified_machines
            .iter()
            .find(|cert| cert.machine == machine)
            .map(|cert| cert.machine_key);
    }
    if !state.owner.as_ref().is_some_and(|owner| owner.user == user) {
        return None;
    }
    if revision != 0 {
        return None;
    }
    let record = state.machines.get(&machine)?;
    let user_record = state.users.get(&user)?;
    if user_record.revoked.is_some() || record.unbindings.iter().any(|event| event.user == user) {
        return None;
    }
    let binding = record
        .bindings
        .iter()
        .rev()
        .find(|binding| binding.user == user)?;
    record
        .registrations
        .iter()
        .any(|registration| {
            registration.machine_key == binding.machine_key
                && registration.sonya_key == binding.sonya_key
                && registration.enc_key == binding.enc_key
        })
        .then_some(binding.machine_key)
}

fn publisher_authorized(state: &CommittedState, user: UserId, machine: MachineId, signer: PublicKey) -> bool {
    let revision = current_member_authority(state, user).map_or(0, |authority| authority.proof_revision);
    machine_key(state, user, machine, revision) == Some(signer)
}

fn covered(
    state: &CommittedState,
    key: ScopedPublicationKey,
    recipient: UserId,
    machine: MachineId,
    revision: u64,
) -> bool {
    if machine_key(state, recipient, machine, revision).is_none() {
        return false;
    }
    state.share_grants.iter().rev().any(|grant| {
        grant.domain == key.domain
            && grant.grantor == key.origin_owner_user
            && grant.recipient == recipient
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

/// Resolves a uniquely certified recipient and its current grant.
pub(super) fn covered_ref(
    state: &CommittedState,
    key: ScopedPublicationKey,
    publisher: MachineId,
    envelope: &EnvelopeRef,
) -> bool {
    let owner = state.owner.as_ref().map(|owner| owner.user);
    let users = state
        .members
        .keys()
        .copied()
        .chain(owner.filter(|user| !state.members.contains_key(user)));
    let mut matching = users.filter(|user| {
        let revision = current_member_authority(state, *user).map_or(0, |authority| authority.proof_revision);
        machine_key(state, *user, envelope.target_machine, revision).is_some()
    });
    let Some(recipient) = matching.next() else {
        return false;
    };
    if matching.next().is_some() {
        return false;
    }
    if recipient == key.origin_owner_user && envelope.target_machine == publisher {
        return machine_key(state, recipient, publisher, envelope.certificate_revision).is_some();
    }
    covered(
        state,
        key,
        recipient,
        envelope.target_machine,
        envelope.certificate_revision,
    )
}

/// Computes the digest of the ordered, JSON-encoded publication descriptors.
pub(super) fn records_digest(records: &[PublishedRecordRef]) -> Option<RecordHash> {
    serde_json::to_vec(records)
        .ok()
        .map(|bytes| RecordHash(Sha256::digest(bytes).into()))
}

/// Verifies the source-signed evidence without making a trust claim about its signer.
pub(super) fn valid_attestation(record: &PublishedRecordRef) -> bool {
    match &record.attestation {
        RecordAttestation::Unattested => true,
        RecordAttestation::Reported { report, source_proof } => {
            let body = &report.body;
            let certificate = &source_proof.body;
            body.parquet_root == record.root
                && body.flow_id == record.source_flow_id
                && body.execution_id == record.source_execution_id
                && body.user == certificate.user
                && body.reporting_machine == certificate.machine
                && report.signer == certificate.machine_key
                && verify_body(source_proof, body.domain).is_ok()
                && verify_body(report, body.domain).is_ok()
        }
    }
}

pub(super) fn apply_sharing(
    tx: &PendingTransaction,
    state: &mut CommittedState,
    height: Height,
) -> Option<ApplyOutcome> {
    let result = match tx {
        PendingTransaction::ShareGrantChanged(signed) => {
            let body = &signed.body;
            if !member_root(state, body.grantor, signed.signer) {
                Err(RejectionReason::UnauthorizedSigner)
            } else if !member_exists(state, body.recipient) {
                Err(RejectionReason::InvalidPayload)
            } else {
                let previous = state
                    .share_grants
                    .iter()
                    .filter(|grant| {
                        grant.domain == body.domain
                            && grant.grantor == body.grantor
                            && grant.recipient == body.recipient
                            && grant.scope == body.scope
                    })
                    .max_by_key(|grant| grant.revision);
                if let Some(existing) = state.share_grants.iter().find(|grant| {
                    grant.domain == body.domain
                        && grant.grantor == body.grantor
                        && grant.recipient == body.recipient
                        && grant.scope == body.scope
                        && grant.revision == body.revision
                }) {
                    if existing == body {
                        Ok(())
                    } else {
                        Err(RejectionReason::ConflictingAction)
                    }
                } else if body.previous_revision != previous.map_or(0, |grant| grant.revision)
                    || body.previous_revision.checked_add(1) != Some(body.revision)
                {
                    Err(RejectionReason::StaleRevision)
                } else {
                    state.share_grants.push(body.clone());
                    Ok(())
                }
            }
        }
        PendingTransaction::StoryPublished(signed) => {
            let body = &signed.body;
            if !publisher_authorized(state, body.key.origin_owner_user, body.publisher, signed.signer) {
                Err(RejectionReason::UnauthorizedSigner)
            } else if let Some(existing) = state
                .publications
                .iter()
                .find(|record| record.publication.key == body.key)
            {
                if existing.publication == *body {
                    Ok(())
                } else {
                    Err(RejectionReason::ConflictingAction)
                }
            } else if records_digest(&body.records) != Some(body.digest)
                || body.records.iter().any(|record| {
                    !valid_attestation(record)
                        || record
                            .envelopes
                            .iter()
                            .any(|envelope| !covered_ref(state, body.key, body.publisher, envelope))
                })
            {
                Err(RejectionReason::InvalidPayload)
            } else {
                state.publications.push(super::types::PublicationRecord {
                    publication: body.clone(),
                    height,
                });
                Ok(())
            }
        }
        PendingTransaction::EnvelopeAddition(signed) => {
            let body = &signed.body;
            if !publisher_authorized(state, body.key.origin_owner_user, body.publisher, signed.signer) {
                Err(RejectionReason::UnauthorizedSigner)
            } else if !state.publications.iter().any(|record| {
                record.publication.key == body.key
                    && record
                        .publication
                        .records
                        .iter()
                        .any(|item| item.root == body.envelope.record_root)
            }) || !covered(
                state,
                body.key,
                body.envelope.recipient,
                body.envelope.target_machine,
                body.envelope.certificate_revision,
            ) {
                Err(RejectionReason::InvalidPayload)
            } else if let Some(existing) = state.envelope_additions.iter().find(|entry| {
                entry.key == body.key
                    && entry.envelope.record_root == body.envelope.record_root
                    && entry.envelope.target_machine == body.envelope.target_machine
            }) {
                if existing == body {
                    Ok(())
                } else {
                    Err(RejectionReason::ConflictingAction)
                }
            } else if state.publications.iter().any(|record| {
                record.publication.key == body.key
                    && record.publication.records.iter().any(|item| {
                        item.root == body.envelope.record_root
                            && item
                                .envelopes
                                .iter()
                                .any(|entry| entry.target_machine == body.envelope.target_machine)
                    })
            }) {
                Err(RejectionReason::ConflictingAction)
            } else {
                state.envelope_additions.push(body.clone());
                Ok(())
            }
        }
        PendingTransaction::VerificationReport(signed) => {
            let body = &signed.body;
            if state.kind != Some(ChainKind::Personal)
                || !state
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.user == body.user)
                || !publisher_authorized(state, body.user, body.reporting_machine, signed.signer)
            {
                Err(RejectionReason::UnauthorizedSigner)
            } else if let Some(existing) = state
                .verification_reports
                .iter()
                .find(|report| report.execution_id == body.execution_id && report.revision == body.revision)
            {
                if existing == body {
                    Ok(())
                } else {
                    Err(RejectionReason::ConflictingAction)
                }
            } else if state
                .verification_reports
                .iter()
                .filter(|report| report.execution_id == body.execution_id)
                .map(|report| report.revision)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                != Some(body.revision)
            {
                Err(RejectionReason::StaleRevision)
            } else {
                state.verification_reports.push(body.clone());
                Ok(())
            }
        }
        _ => return None,
    };
    Some(match result {
        Ok(()) => ApplyOutcome::Applied,
        Err(reason) => ApplyOutcome::Rejected(reason),
    })
}

#[cfg(test)]
mod tests {
    use super::super::signing::ChainSigner;
    use super::super::types::{
        BlobHash, CoordinationExecutionId, CoordinationFlowId, DomainId, EncKey, EnvelopeAddition, MachineBound,
        MemberIdentityProof, MemberRecord, PublicationEnvelopeRef, PublicationRecord, RegisteredTransaction,
        ShareGrantChanged, SignedBody, StoryPublished, VerificationReport,
    };
    use super::*;
    use crate::flow::{FlowExecutionId, FlowId};
    use uuid::Uuid;

    fn record_digest(records: &[PublishedRecordRef]) -> RecordHash {
        records_digest(records).unwrap()
    }

    fn addition(story: &StoryPublished, recipient: UserId) -> EnvelopeAddition {
        let record = &story.records[0];
        let envelope = &record.envelopes[0];
        EnvelopeAddition {
            key: story.key,
            publisher: story.publisher,
            envelope: PublicationEnvelopeRef {
                record_root: record.root,
                recipient,
                target_machine: envelope.target_machine,
                certificate_revision: envelope.certificate_revision,
                envelope_ref: envelope.envelope_reference.clone(),
                envelope_digest: envelope.envelope_digest,
            },
        }
    }

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

    fn fixture() -> (
        CommittedState,
        ChainSigner,
        ChainSigner,
        ShareGrantChanged,
        StoryPublished,
    ) {
        let domain = DomainId([1; 32]);
        let root = ChainSigner::new(iroh::SecretKey::from_bytes(&[2; 32]));
        let machine = ChainSigner::new(iroh::SecretKey::from_bytes(&[3; 32]));
        let owner = UserId([4; 32]);
        let target = UserId([5; 32]);
        let mut state = CommittedState::default();
        for (user, machine_id) in [(owner, MachineId([6; 32])), (target, MachineId([7; 32]))] {
            let source = DomainId([8; 32]);
            let key = PublicKey::from(machine.public_key());
            let proof = MemberIdentityProof {
                source_domain: source,
                certificate: signed(
                    MachineBound {
                        user,
                        machine: machine_id,
                        machine_key: key,
                        sonya_key: key,
                        enc_key: EncKey([9; 32]),
                        enc_epoch: 1,
                    },
                    &root,
                    source,
                ),
            };
            state.members.insert(
                user,
                MemberRecord {
                    member: user,
                    status: MemberStatus::Active,
                    revision: 0,
                    root: PublicKey::from(root.public_key()),
                    proof,
                },
            );
        }
        let key = ScopedPublicationKey {
            domain,
            origin_owner_user: owner,
            source_project_id: [10; 16],
            source_ticket_id: [11; 16],
            operation_id: [12; 16],
        };
        let grant = ShareGrantChanged {
            domain,
            grantor: owner,
            recipient: target,
            scope: ShareScope::Ticket(key.source_ticket_id),
            revision: 1,
            previous_revision: 0,
            active: true,
        };
        let envelope = EnvelopeRef {
            target_machine: MachineId([7; 32]),
            certificate_revision: 0,
            envelope_reference: "opaque".into(),
            envelope_digest: RecordHash([14; 32]),
        };
        let records = vec![PublishedRecordRef {
            source_flow_id: CoordinationFlowId(FlowId(Uuid::from_u128(1))),
            source_execution_id: CoordinationExecutionId(FlowExecutionId(Uuid::from_u128(2))),
            stage_number: 1,
            root: BlobHash([13; 32]),
            format_version: 1,
            attestation: RecordAttestation::Unattested,
            envelopes: vec![envelope],
        }];
        let digest = record_digest(&records);
        let story = StoryPublished {
            key,
            publisher: MachineId([6; 32]),
            records,
            digest,
        };
        (state, root, machine, grant, story)
    }

    fn apply(state: &mut CommittedState, tx: PendingTransaction) -> ApplyOutcome {
        let event = tx.event_id().unwrap();
        state
            .apply_transactions(DomainId([1; 32]), vec![tx], Height(1))
            .unwrap();
        state
            .rejected_events
            .get(&event)
            .copied()
            .map_or(ApplyOutcome::Applied, ApplyOutcome::Rejected)
    }

    #[test]
    fn producer_envelope_needs_no_grant_but_other_recipients_do() {
        let (mut state, _, machine, _, mut story) = fixture();
        let domain = story.key.domain;
        story.records[0].envelopes[0].target_machine = story.publisher;
        story.digest = record_digest(&story.records);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Applied
        );
        let (mut state, _, machine, _, mut other) = fixture();
        other.records[0].envelopes[0].target_machine = MachineId([7; 32]);
        other.digest = record_digest(&other.records);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(other, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
    }

    #[test]
    fn revoked_before_publication_is_rejected() {
        let (mut state, root, machine, grant, story) = fixture();
        let domain = grant.domain;
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant.clone(), &root, domain))
            ),
            ApplyOutcome::Applied
        );
        let revoke = ShareGrantChanged {
            revision: 2,
            previous_revision: 1,
            active: false,
            ..grant
        };
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(revoke, &root, domain))
            ),
            ApplyOutcome::Applied
        );
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
    }

    #[test]
    fn foreign_member_cannot_overwrite_grant() {
        let (mut state, root, _, grant, _) = fixture();
        let domain = grant.domain;
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant.clone(), &root, domain))
            ),
            ApplyOutcome::Applied
        );
        let foreign = ChainSigner::new(iroh::SecretKey::from_bytes(&[19; 32]));
        state.members.get_mut(&grant.recipient).unwrap().root = PublicKey::from(foreign.public_key());
        let changed = ShareGrantChanged {
            revision: 2,
            previous_revision: 1,
            active: false,
            ..grant
        };
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(changed, &foreign, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::UnauthorizedSigner)
        );
    }

    #[test]
    fn differing_digest_conflicts() {
        let (mut state, root, machine, grant, mut story) = fixture();
        let domain = grant.domain;
        apply(
            &mut state,
            PendingTransaction::ShareGrantChanged(signed(grant, &root, domain)),
        );
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Applied
        );
        story.digest = RecordHash([20; 32]);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::ConflictingAction)
        );
    }

    #[test]
    fn ordered_record_descriptors_are_hashed_and_retained() {
        let (mut state, root, machine, grant, mut story) = fixture();
        let domain = grant.domain;
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant, &root, domain))
            ),
            ApplyOutcome::Applied
        );
        let mut second = story.records[0].clone();
        second.stage_number = 2;
        second.root = BlobHash([21; 32]);
        story.records.push(second);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
        story.digest = record_digest(&story.records);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Applied
        );
        assert_eq!(state.publications[0].publication.records, story.records);
        let mut reversed = story.records.clone();
        reversed.reverse();
        assert_ne!(record_digest(&reversed), story.digest);
    }

    #[test]
    fn reports_require_matching_source_signature_and_record() {
        let (mut state, root, machine, grant, mut story) = fixture();
        let domain = grant.domain;
        let source = DomainId([8; 32]);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant, &root, domain))
            ),
            ApplyOutcome::Applied
        );
        let record = &story.records[0];
        let proof = state
            .members
            .get(&story.key.origin_owner_user)
            .unwrap()
            .proof
            .certificate
            .clone();
        let report = signed(
            VerificationReport {
                domain: source,
                user: story.key.origin_owner_user,
                work_id: "work".into(),
                flow_id: record.source_flow_id,
                execution_id: record.source_execution_id,
                revision: 1,
                work_state_id: "commit".into(),
                parquet_root: record.root,
                reporting_machine: story.publisher,
                agent_correlation: None,
                node_correlation: None,
                check_ref: None,
                environment_ref: None,
                policy_ref: None,
            },
            &machine,
            source,
        );
        story.records[0].attestation = RecordAttestation::Reported {
            report: Box::new(report.clone()),
            source_proof: Box::new(proof.clone()),
        };
        story.digest = record_digest(&story.records);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Applied
        );
        for case in 0..3 {
            let mut invalid = story.clone();
            invalid.key.operation_id = [case + 30; 16];
            if let RecordAttestation::Reported { report, source_proof } = &mut invalid.records[0].attestation {
                match case {
                    0 => report.sig[0] ^= 1,
                    1 => source_proof.sig[0] ^= 1,
                    _ => {
                        let mut body = report.body.clone();
                        body.parquet_root = BlobHash([31; 32]);
                        **report = signed(body, &machine, source);
                    }
                }
            }
            invalid.digest = record_digest(&invalid.records);
            assert_eq!(
                apply(
                    &mut state,
                    PendingTransaction::StoryPublished(signed(invalid, &machine, domain))
                ),
                ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
            );
        }
    }

    #[test]
    fn shared_machine_identity_is_not_resolved_to_a_granted_member() {
        let (mut state, root, machine, grant, story) = fixture();
        let domain = grant.domain;
        let duplicate_user = UserId([22; 32]);
        let mut duplicate = state.members.get(&grant.recipient).unwrap().clone();
        duplicate.member = duplicate_user;
        state.members.insert(duplicate_user, duplicate);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant, &root, domain))
            ),
            ApplyOutcome::Applied
        );
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
    }

    #[test]
    fn addition_without_grant_is_rejected() {
        let (mut state, _, machine, grant, story) = fixture();
        let domain = story.key.domain;
        state.publications.push(PublicationRecord {
            publication: story.clone(),
            height: Height(1),
        });
        let addition = addition(&story, grant.recipient);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::EnvelopeAddition(signed(addition, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
    }

    #[test]
    fn machine_checks_use_current_authority_revision_and_removal() {
        let (mut state, root, machine, grant, mut story) = fixture();
        let domain = grant.domain;
        let recipient = grant.recipient;
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::ShareGrantChanged(signed(grant, &root, domain))
            ),
            ApplyOutcome::Applied
        );
        let mut authority = current_member_authority(&state, recipient).unwrap();
        authority.proof_revision = 1;
        state
            .member_authorities
            .insert(recipient, authority.clone());
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
        story.records[0].envelopes[0].certificate_revision = 1;
        story.digest = record_digest(&story.records);
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::StoryPublished(signed(story.clone(), &machine, domain))
            ),
            ApplyOutcome::Applied
        );
        authority.certified_machines.clear();
        authority.proof_revision = 2;
        state.member_authorities.insert(recipient, authority);
        let mut addition = addition(&story, recipient);
        addition.envelope.certificate_revision = 2;
        assert_eq!(
            apply(
                &mut state,
                PendingTransaction::EnvelopeAddition(signed(addition, &machine, domain))
            ),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
    }
}

use arc_malachitebft_core_types::{
    CommitCertificate, ExtendedCommitCertificate, NilOrVal, Round, Value as _, VoteExtensions,
};
use arc_malachitebft_signing::Signer;

use super::super::signing::{ChainSigner, CoordinationSigner};
use super::super::types::{
    ConsensusVote, ConsensusVoteKind, EncKey, RegisteredTransaction, SignedBody, UserRegistered,
};
use super::*;

async fn application(path: &std::path::Path) -> CoordinationApplication {
    let store = CoordinationStore::open(path).await.unwrap();
    let recovery = store.load().await.unwrap();
    CoordinationApplication::with_store(store, recovery).unwrap()
}

#[tokio::test]
async fn genesis_snapshot_adopts_restarts_and_is_idempotent() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut source = application(source_dir.path()).await;
    let key = iroh::SecretKey::from_bytes(&[42; 32]);
    let roster = vec![(PublicKey(*key.public().as_bytes()), 7)];
    let chain = source.create_genesis(roster.clone()).await.unwrap();
    let snapshot = source.export_snapshot(None).unwrap();
    assert_eq!(snapshot.domain, chain.domain);
    assert_eq!(snapshot.height, Height(0));
    assert!(snapshot.decided.is_empty());
    let mut target = application(target_dir.path()).await;
    assert_eq!(
        target
            .adopt_snapshot(snapshot.clone(), snapshot.hash)
            .await
            .unwrap(),
        chain
    );
    assert_eq!(
        target
            .adopt_snapshot(snapshot.clone(), snapshot.hash)
            .await
            .unwrap(),
        chain
    );
    drop(target);
    let restored = application(target_dir.path()).await;
    assert_eq!(restored.export_snapshot(None).unwrap(), snapshot);
    assert_eq!(restored.state.genesis_validators, Some(roster));
}

#[tokio::test]
async fn corrupt_or_wrong_anchor_does_not_initialize_host() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut source = application(source_dir.path()).await;
    let key = iroh::SecretKey::from_bytes(&[43; 32]);
    source
        .create_genesis(vec![(PublicKey(*key.public().as_bytes()), 1)])
        .await
        .unwrap();
    let snapshot = source.export_snapshot(None).unwrap();
    let mut target = application(target_dir.path()).await;
    assert!(
        target
            .adopt_snapshot(snapshot.clone(), RecordHash([1; 32]))
            .await
            .is_err()
    );
    let mut corrupt = snapshot.clone();
    corrupt.height = Height(1);
    assert!(target.adopt_snapshot(corrupt, snapshot.hash).await.is_err());
    assert!(target.state.domain.is_none());
    target
        .adopt_snapshot(snapshot.clone(), snapshot.hash)
        .await
        .unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let mut other = application(other_dir.path()).await;
    other
        .create_genesis(vec![(PublicKey(*key.public().as_bytes()), 1)])
        .await
        .unwrap();
    let different = other.export_snapshot(None).unwrap();
    assert!(matches!(
        target
            .adopt_snapshot(different.clone(), different.hash)
            .await,
        Err(CoordinationError::SnapshotHostNotEmpty)
    ));
    assert_eq!(target.export_snapshot(None).unwrap(), snapshot);
}

#[tokio::test]
async fn decided_snapshot_preserves_history_across_restart() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut source = application(source_dir.path()).await;
    let key = iroh::SecretKey::from_bytes(&[44; 32]);
    let address = PublicKey(*key.public().as_bytes());
    let chain = source.create_genesis(vec![(address, 3)]).await.unwrap();
    let value = ConsensusValue("[]".to_owned());
    let vote = ConsensusVote {
        height: Height(1),
        round: 0,
        value: NilOrVal::Val(value.id()),
        kind: ConsensusVoteKind::Precommit,
        address,
        extension: None,
    };
    let signed = CoordinationSigner::new(key, chain.domain)
        .sign_vote(vote)
        .await
        .unwrap();
    let certificate = CommitCertificate::new(Height(1), Round::new(0), value.id(), vec![signed]);
    let extended =
        ExtendedCommitCertificate::from_commit_certificate_and_extensions(certificate, VoteExtensions::default());
    let raw = RawDecidedValue::new(serde_json::to_vec(&value).unwrap().into(), extended);
    source.state.committed.height = Height(1);
    source.state.committed_values.insert(1, raw);
    let snapshot = source.export_snapshot(None).unwrap();
    let mut target = application(target_dir.path()).await;
    target
        .adopt_snapshot(snapshot.clone(), snapshot.hash)
        .await
        .unwrap();
    drop(target);
    let restored = application(target_dir.path()).await;
    assert_eq!(restored.export_snapshot(None).unwrap(), snapshot);
    let mut missing = snapshot.clone();
    missing.decided.clear();
    missing.hash = CoordinationApplication::snapshot_hash(&missing).unwrap();
    let empty_dir = tempfile::tempdir().unwrap();
    let mut empty = application(empty_dir.path()).await;
    assert!(
        empty
            .adopt_snapshot(missing.clone(), missing.hash)
            .await
            .is_err()
    );
    assert!(empty.state.domain.is_none());
}

#[tokio::test]
async fn snapshot_identity_ignores_valid_certificate_variation() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut source = application(source_dir.path()).await;
    let keys: Vec<_> = (50..54)
        .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]))
        .collect();
    let roster = keys
        .iter()
        .map(|key| (PublicKey(*key.public().as_bytes()), 1))
        .collect();
    let domain = source.create_genesis(roster).await.unwrap().domain;
    let value = ConsensusValue("[]".to_owned());
    let mut votes = Vec::new();
    for key in &keys {
        let address = PublicKey(*key.public().as_bytes());
        let vote = ConsensusVote {
            height: Height(1),
            round: 0,
            value: NilOrVal::Val(value.id()),
            kind: ConsensusVoteKind::Precommit,
            address,
            extension: None,
        };
        votes.push(
            CoordinationSigner::new(key.clone(), domain)
                .sign_vote(vote)
                .await
                .unwrap(),
        );
    }
    let raw = |votes| {
        let certificate = CommitCertificate::new(Height(1), Round::new(0), value.id(), votes);
        let extended =
            ExtendedCommitCertificate::from_commit_certificate_and_extensions(certificate, VoteExtensions::default());
        RawDecidedValue::new(serde_json::to_vec(&value).unwrap().into(), extended)
    };
    source.state.committed.height = Height(1);
    source
        .state
        .committed_values
        .insert(1, raw(votes[..3].to_vec()));
    let first = source.export_snapshot(None).unwrap();
    source.state.committed_values.insert(1, raw(votes));
    let second = source.export_snapshot(None).unwrap();
    assert_ne!(first.decided[0].certificate_json, second.decided[0].certificate_json);
    assert_eq!(first.hash, second.hash);

    let second_dir = tempfile::tempdir().unwrap();
    let mut second_target = application(second_dir.path()).await;
    second_target
        .adopt_snapshot(second.clone(), first.hash)
        .await
        .unwrap();
    let mut target = application(target_dir.path()).await;
    target
        .adopt_snapshot(first.clone(), first.hash)
        .await
        .unwrap();
    let signer = ChainSigner::new(keys[0].clone());
    let address = PublicKey::from(signer.public_key());
    let body = UserRegistered {
        user: UserId([55; 32]),
        root_key: address,
        enc_key: EncKey([56; 32]),
        enc_epoch: 1,
    };
    let mut preimage = UserRegistered::PURPOSE.as_bytes().to_vec();
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).unwrap();
    target
        .submit(PendingTransaction::UserRegistered(SignedBody {
            body,
            signer: address,
            sig: signer.sign(&preimage).0,
        }))
        .await
        .unwrap();
    let before = target.export_snapshot(None).unwrap();
    let pending = target.state.pending.len();
    target
        .adopt_snapshot(second.clone(), first.hash)
        .await
        .unwrap();
    assert_eq!(target.state.pending.len(), pending);
    assert_eq!(target.export_snapshot(None).unwrap(), before);

    let mut forged = second.clone();
    forged.decided[0].certificate_json = "{}".to_owned();
    assert!(target.adopt_snapshot(forged, first.hash).await.is_err());
    let mut altered = second.clone();
    altered.decided[0].value_bytes.push(0);
    assert!(target.adopt_snapshot(altered, first.hash).await.is_err());
    let mut altered = second;
    altered.application_json.push(' ');
    assert!(target.adopt_snapshot(altered, first.hash).await.is_err());
    assert_eq!(target.state.pending.len(), pending);
    assert_eq!(target.export_snapshot(None).unwrap(), before);
}

#[tokio::test]
async fn retired_export_preserves_active_and_retained_transient_rows() {
    let directory = tempfile::tempdir().unwrap();
    let mut app = application(directory.path()).await;
    let key = iroh::SecretKey::from_bytes(&[45; 32]);
    let signer = ChainSigner::new(key);
    let address = PublicKey::from(signer.public_key());
    let predecessor = app.create_genesis(vec![(address, 1)]).await.unwrap().domain;
    let body = UserRegistered {
        user: UserId([2; 32]),
        root_key: address,
        enc_key: EncKey([3; 32]),
        enc_epoch: 1,
    };
    let mut preimage = UserRegistered::PURPOSE.as_bytes().to_vec();
    preimage.extend_from_slice(predecessor.as_bytes());
    serde_json::to_writer(&mut preimage, &body).unwrap();
    app.submit(PendingTransaction::UserRegistered(SignedBody {
        body,
        signer: address,
        sig: signer.sign(&preimage).0,
    }))
    .await
    .unwrap();
    let value = ConsensusValue("[]".to_owned());
    let value_id = serde_json::to_vec(&value.id()).unwrap();
    app.state.proposals.insert(
        (1, value_id),
        ProposalRecovery {
            application_bytes: serde_json::to_vec(&value).unwrap(),
            metadata: None,
            parts: Vec::new(),
        },
    );
    let expected = app.export_snapshot(None).unwrap();
    let previous = CoordinationApplication::recovery_state(&app.state).unwrap();
    assert_eq!(previous.pending.len(), 1);
    assert_eq!(previous.proposals.len(), 1);
    let successor = DomainId::from_bytes([46; 32]);
    let mut next = ApplicationState::default();
    next.domain = Some(successor);
    next.genesis_validators = Some(vec![(address, 1)]);
    let next_recovery = CoordinationApplication::recovery_state(&next).unwrap();
    app.store
        .save_successor(predecessor, &previous, &next_recovery)
        .await
        .unwrap();
    app.state = next;
    let active_before = app.export_snapshot(None).unwrap();
    let retired = app.export_chain_snapshot(Some(predecessor)).await.unwrap();
    assert_eq!(retired, expected);
    assert_eq!(app.export_snapshot(None).unwrap(), active_before);
    assert_eq!(app.store.load_retired(predecessor).await.unwrap().unwrap(), previous);
    let json: ApplicationRecovery = serde_json::from_str(&retired.application_json).unwrap();
    assert_eq!(json.restream_nonce, 0);
    assert!(retired.decided.is_empty());
    assert!(
        app.export_chain_snapshot(Some(DomainId::from_bytes([99; 32])))
            .await
            .is_err()
    );
}

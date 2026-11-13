use super::*;
use serde::Serialize;

use super::super::signing::ChainSigner;
use super::super::types::{EncKey, RegisteredTransaction, UserRegistered, ValidatorSet};

fn signed<T: Serialize + RegisteredTransaction>(body: T, signer: &ChainSigner, domain: DomainId) -> SignedBody<T> {
    let mut preimage = T::PURPOSE.as_bytes().to_vec();
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).unwrap();
    SignedBody {
        body,
        signer: signer.public_key().into(),
        sig: signer.sign(&preimage).0,
    }
}

async fn open(path: &std::path::Path) -> CoordinationApplication {
    let store = CoordinationStore::open(path).await.unwrap();
    let recovery = store.load().await.unwrap();
    CoordinationApplication::with_store(store, recovery).unwrap()
}

#[tokio::test]
async fn recovery_rejects_invalid_requests_and_preserves_retired_pending_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let signer = ChainSigner::new(iroh::SecretKey::from_bytes(&[13; 32]));
    let other = ChainSigner::new(iroh::SecretKey::from_bytes(&[14; 32]));
    let key: PublicKey = signer.public_key().into();
    let mut app = open(directory.path()).await;
    let predecessor = app.create_genesis(vec![(key, 1)]).await.unwrap().domain;
    let registration = UserRegistered {
        user: UserId([3; 32]),
        root_key: key,
        enc_key: EncKey([4; 32]),
        enc_epoch: 1,
    };
    PendingTransaction::UserRegistered(signed(registration.clone(), &signer, predecessor))
        .apply(&mut app.state.committed, Height(0))
        .unwrap();
    app.save_state(&app.state).await.unwrap();
    let pending = PendingTransaction::UserRegistered(signed(registration, &signer, predecessor));
    app.submit(pending).await.unwrap();
    let snapshot = app.export_snapshot(None).unwrap();
    let body = Regenesis {
        predecessor,
        successor: DomainId::from_bytes([9; 32]),
        predecessor_height: snapshot.height,
        snapshot_hash: snapshot.hash,
        validators: ValidatorSet(vec![(key, 1)]),
    };
    let mut wrong_hash = body.clone();
    wrong_hash.snapshot_hash = RecordHash([8; 32]);
    assert!(
        app.begin_successor(signed(wrong_hash, &signer, predecessor))
            .await
            .is_err()
    );
    assert!(
        app.begin_successor(signed(body.clone(), &other, predecessor))
            .await
            .is_err()
    );
    let mut wrong_validator = body.clone();
    wrong_validator.validators = ValidatorSet(vec![(other.public_key().into(), 1)]);
    assert!(
        app.begin_successor(signed(wrong_validator, &signer, predecessor))
            .await
            .is_err()
    );
    assert_eq!(app.export_snapshot(None).unwrap(), snapshot);
    assert_eq!(app.state.pending.len(), 1);
    let record = signed(body.clone(), &signer, predecessor);
    let event = app.begin_successor(record.clone()).await.unwrap();
    assert_eq!(app.state.committed.height, Height(0));
    assert!(app.state.pending.is_empty());
    assert_eq!(
        app.store
            .load_retired(predecessor)
            .await
            .unwrap()
            .unwrap()
            .pending
            .len(),
        1
    );
    drop(app);
    let mut app = open(directory.path()).await;
    assert_eq!(app.state.domain, Some(body.successor));
    assert_eq!(app.export_chain_snapshot(Some(predecessor)).await.unwrap(), snapshot);
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let (reply, ready) = tokio::sync::oneshot::channel();
    app.handle_app_msg(arc_malachitebft_app_channel::AppMsg::ConsensusReady { reply }, &network)
        .await
        .unwrap();
    assert_eq!(ready.await.unwrap().0, Height(1));
    assert_eq!(app.begin_successor(record).await.unwrap(), event);
    let mut conflict = body;
    conflict.snapshot_hash = RecordHash([7; 32]);
    assert!(
        app.begin_successor(signed(conflict, &signer, predecessor))
            .await
            .is_err()
    );
}

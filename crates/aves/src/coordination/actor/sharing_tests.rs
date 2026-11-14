use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets, protocol::Router};
use serde::Serialize;

use crate::auth::{ApprovedIssuer, AuthAdmissionRegistry};
use crate::machine::{MachinePeerServices, PeerConnectivity};

use super::super::network::encode_transaction;
use super::super::signing::ChainSigner;
use super::super::transaction::PendingTransaction;
use super::super::types::{
    DomainId, EncKey, Height, MachineId, PublicKey, RegisteredTransaction, SchemaActivated, SignedBody, UserId,
    UserRegistered,
};
use super::Actor;

fn signed<T: Serialize + RegisteredTransaction>(body: T, signer: &ChainSigner, domain: DomainId) -> SignedBody<T> {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(T::PURPOSE.as_bytes());
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).unwrap();
    SignedBody {
        body,
        signer: PublicKey::from(signer.public_key()),
        sig: signer.sign(&preimage).0,
    }
}

async fn actor(path: &std::path::Path) -> Actor {
    let (_, requests) = tokio::sync::mpsc::channel(4);
    let (broadcasts, _) = tokio::sync::mpsc::channel(4);
    Actor::new(requests, path.to_path_buf(), None, None, broadcasts).await
}

#[tokio::test]
async fn offline_admission_is_durable_deduplicated_and_checks_domain() {
    let directory = tempfile::tempdir().unwrap();
    let signer = ChainSigner::new(iroh::SecretKey::from_bytes(&[42; 32]));
    let key = PublicKey::from(signer.public_key());
    let mut host = actor(directory.path()).await;
    let chain = host.abci.create_genesis(vec![(key, 3)]).await.unwrap();
    let body = UserRegistered {
        user: UserId([43; 32]),
        root_key: key,
        enc_key: EncKey([44; 32]),
        enc_epoch: 1,
    };
    let transaction = PendingTransaction::UserRegistered(signed(body.clone(), &signer, chain.domain));
    host.submit(transaction.clone()).await.unwrap();
    host.submit(transaction).await.unwrap();
    assert_eq!(host.abci.state.pending.len(), 1);
    let wrong = PendingTransaction::UserRegistered(signed(body, &signer, DomainId::from_bytes([99; 32])));
    assert!(host.submit(wrong).await.is_err());
    drop(host);
    let restored = actor(directory.path()).await;
    assert_eq!(restored.abci.state.pending.len(), 1);
}

struct NativeFixture {
    services: MachinePeerServices,
    router: Router,
    _root: tempfile::TempDir,
}

impl NativeFixture {
    async fn new(seed: u8, lookup: &MemoryLookup, limit: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .address_lookup(lookup.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .bind()
            .await
            .unwrap();
        lookup.add_endpoint_info(endpoint.addr());
        let gossip = iroh_gossip::net::Gossip::builder()
            .max_message_size(limit)
            .spawn(endpoint.clone());
        let blobs = iroh_blobs::store::fs::FsStore::load(root.path().join("blobs"))
            .await
            .unwrap();
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone().into(), gossip.clone())
            .await
            .unwrap();
        let services = MachinePeerServices::new_with_gossip(
            blobs,
            docs.api().clone(),
            endpoint.clone(),
            Some(gossip.clone()),
            PeerConnectivity::new(true),
        );
        let router = Router::builder(endpoint)
            .accept(iroh_gossip::net::GOSSIP_ALPN, gossip)
            .accept(iroh_docs::net::ALPN, docs)
            .spawn();
        Self {
            services,
            router,
            _root: root,
        }
    }
}

fn admission(local: u8, remote: u8) -> AuthAdmissionRegistry {
    let registry = AuthAdmissionRegistry::new(ApprovedIssuer {
        issuer: iroh::SecretKey::from_bytes(&[local; 32]).public().into(),
    });
    registry
        .upsert_remote(ApprovedIssuer {
            issuer: iroh::SecretKey::from_bytes(&[remote; 32]).public().into(),
        })
        .unwrap();
    registry
}

async fn native_actor(path: &std::path::Path, fixture: &NativeFixture, registry: AuthAdmissionRegistry) -> Actor {
    let (_, requests) = tokio::sync::mpsc::channel(4);
    let (broadcasts, _) = tokio::sync::mpsc::channel(4);
    Actor::new(
        requests,
        path.to_path_buf(),
        Some(std::sync::Arc::new(fixture.services.clone())),
        Some(registry),
        broadcasts,
    )
    .await
}

fn registration(seed: u8, signer: &ChainSigner, domain: DomainId) -> PendingTransaction {
    let key = PublicKey::from(signer.public_key());
    PendingTransaction::UserRegistered(signed(
        UserRegistered {
            user: UserId([seed; 32]),
            root_key: key,
            enc_key: EncKey([seed; 32]),
            enc_epoch: 1,
        },
        signer,
        domain,
    ))
}

async fn pump(left: &mut Actor, right: &mut Actor) {
    let left_events = &mut left.running.as_mut().unwrap().transport.events;
    let right_events = &mut right.running.as_mut().unwrap().transport.events;
    let (is_left, event) = tokio::select! {
        event = left_events.recv() => (true, event),
        event = right_events.recv() => (false, event),
    };
    if let Some(event) = event {
        if is_left {
            left.handle_application_event(Some(event)).await
        } else {
            right.handle_application_event(Some(event)).await
        }
    }
}

#[tokio::test]
async fn admitted_neighbors_share_replay_and_reject_invalid_and_revoked() {
    let lookup = MemoryLookup::new();
    let native_a = NativeFixture::new(51, &lookup, 64 * 1024 * 1024).await;
    let native_b = NativeFixture::new(52, &lookup, 64 * 1024 * 1024).await;
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a_admission = admission(51, 52);
    let b_admission = admission(52, 51);
    let mut a = native_actor(dir_a.path(), &native_a, a_admission.clone()).await;
    let mut b = native_actor(dir_b.path(), &native_b, b_admission.clone()).await;
    let signer_a = ChainSigner::new(iroh::SecretKey::from_bytes(&[51; 32]));
    let signer_b = ChainSigner::new(iroh::SecretKey::from_bytes(&[52; 32]));
    let key_a = PublicKey::from(signer_a.public_key());
    let key_b = PublicKey::from(signer_b.public_key());
    let third = PublicKey(*iroh::SecretKey::from_bytes(&[53; 32]).public().as_bytes());
    let roster = vec![(key_a, 1), (key_b, 1), (third, 5)];
    let domain = a.abci.create_genesis(roster.clone()).await.unwrap().domain;
    // The snapshot lane supplies this adoption in production; this fixture shares the same genesis.
    b.abci.state.domain = Some(domain);
    b.abci.state.genesis_validators = Some(roster);
    b.abci.save_state(&b.abci.state).await.unwrap();
    let offline = registration(61, &signer_b, domain);
    b.submit(offline.clone()).await.unwrap();
    drop(b);
    let mut b = native_actor(dir_b.path(), &native_b, b_admission.clone()).await;
    a.start_server().await.unwrap();
    b.start_server().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while a.abci.state.pending.len() != 1 {
            pump(&mut a, &mut b).await;
        }
    })
    .await
    .expect("restarted offline admission must reach the other validator");
    assert_eq!(b.abci.state.pending.len(), 1);
    let next = registration(62, &signer_b, domain);
    b.submit(next.clone()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while a.abci.state.pending.len() != 2 {
            pump(&mut a, &mut b).await;
        }
    })
    .await
    .expect("connected validator admission must propagate");
    a.handle_application_event(Some(super::ApplicationEvent::Transaction(
        iroh::SecretKey::from_bytes(&[52; 32]).public(),
        Box::new(next),
    )))
    .await;
    assert_eq!(a.abci.state.pending.len(), 2);
    b.stop_server().await.unwrap();
    let disconnected = registration(66, &signer_a, domain);
    a.submit(disconnected).await.unwrap();
    b.start_server().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while b.abci.state.pending.len() != 3 {
            pump(&mut a, &mut b).await;
        }
    })
    .await
    .expect("pending admission must be replayed when a validator reconnects");
    let wrong = registration(63, &signer_b, DomainId::from_bytes([8; 32]));
    let mut tampered = registration(64, &signer_b, domain);
    if let PendingTransaction::UserRegistered(record) = &mut tampered {
        record.body.enc_epoch += 1;
    }
    let tx = b.running.as_ref().unwrap().transport.transactions.clone();
    for value in [wrong, tampered] {
        tx.send(encode_transaction(&value, 64 * 1024 * 1024).unwrap())
            .await
            .unwrap();
    }
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            pump(&mut a, &mut b).await;
        }
    })
    .await;
    assert_eq!(a.abci.state.pending.len(), 3);
    b_admission
        .remove_remote(&iroh::SecretKey::from_bytes(&[51; 32]).public().into())
        .unwrap();
    let revoked = registration(65, &signer_a, domain);
    a.running
        .as_ref()
        .unwrap()
        .transport
        .transactions
        .send(encode_transaction(&revoked, 64 * 1024 * 1024).unwrap())
        .await
        .unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            pump(&mut a, &mut b).await;
        }
    })
    .await;
    assert_eq!(b.abci.state.pending.len(), 3);
    a.stop_server().await.unwrap();
    b.stop_server().await.unwrap();
    native_a.router.shutdown().await.unwrap();
    native_b.router.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_transaction_is_rejected_before_persistence() {
    let lookup = MemoryLookup::new();
    let native = NativeFixture::new(71, &lookup, 512).await;
    let dir = tempfile::tempdir().unwrap();
    let mut host = native_actor(dir.path(), &native, admission(71, 72)).await;
    let signer = ChainSigner::new(iroh::SecretKey::from_bytes(&[71; 32]));
    let domain = host
        .abci
        .create_genesis(vec![(PublicKey::from(signer.public_key()), 1)])
        .await
        .unwrap()
        .domain;
    let tx = PendingTransaction::SchemaActivated(signed(
        SchemaActivated {
            machine: MachineId([73; 32]),
            schema_version: 1,
            platform_minimum: 1,
            active_from: Height(1),
            schema: format!("\"{}\"", "x".repeat(1024)),
        },
        &signer,
        domain,
    ));
    assert!(encode_transaction(&tx, 512).is_err());
    assert!(host.submit(tx).await.is_err());
    assert!(host.abci.state.pending.is_empty());
    drop(host);
    let restored = native_actor(dir.path(), &native, admission(71, 72)).await;
    assert!(restored.abci.state.pending.is_empty());
    native.router.shutdown().await.unwrap();
}

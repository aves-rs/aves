use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{Hash, api::Store, store::fs::FsStore};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::auth::{
    ApprovedIssuer, AuthAdmissionRegistry, AuthFirstContactHandler, Jwt, MachineIssuerId, PairingCoordinator,
};
use crate::bus::BusRpcContext;
use crate::config::{ConfigDomain, ConfigError, ConfigRequest, ConfigRequestPayload, ConfigResponse};
use crate::utils::test_support::MockBus;

use super::super::messages::{MachineBroadcast, MachineRequest, MachineRequestPayload, MachineResponse};
use super::super::peering::{IrohSubstrate, MachinePeerServices, PeerConnectivity};
use super::super::types::{
    ContentEntry, ContentHash, ContentPeer, ContentRoot, EncryptedContent, MachineBootOptions, MachineError,
    MachineSettings, OnlineState, hex_key,
};
use super::*;

const DEFAULT_NAME: &str = "build-host";

fn issuer() -> MachineIssuerId {
    MachineIssuerId::from(iroh::SecretKey::generate().public())
}

fn registry(local: &MachineIssuerId) -> AuthAdmissionRegistry {
    AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() })
}

#[test]
fn blank_names_are_rejected_and_names_are_trimmed() {
    assert!(matches!(validated_name("   "), Err(MachineError::InvalidName)));
    assert_eq!(validated_name("  build box  ").ok().as_deref(), Some("build box"));
}

#[test]
fn peers_list_only_admitted_remote_machines() {
    let local = issuer();
    let remote = issuer();
    let registry = registry(&local);
    registry
        .upsert_remote(ApprovedIssuer { issuer: remote.clone() })
        .expect("remote admitted");

    let availability = peers(&registry, &local);
    assert_eq!(
        availability.admitted_remote_machines,
        vec![hex_key(remote.verification_key())]
    );
    assert_eq!(availability.online, OnlineState::Unavailable);
}

#[derive(Default)]
struct ConfigDouble {
    stored: Mutex<Option<Value>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
    budget: Mutex<Option<usize>>,
}

impl ConfigDouble {
    fn limit_writes(&self, accepted: usize) {
        *self.budget.lock().expect("config double lock") = Some(accepted);
    }

    fn admit_write(&self) -> bool {
        let mut budget = self.budget.lock().expect("config double lock");
        match budget.as_mut() {
            None => true,
            Some(0) => false,
            Some(remaining) => {
                *remaining -= 1;
                true
            }
        }
    }

    fn stored(&self) -> Option<Value> {
        self.stored.lock().expect("config double lock").clone()
    }

    fn set_stored(&self, value: Value) {
        *self.stored.lock().expect("config double lock") = Some(value);
    }

    fn settings(&self) -> MachineSettings {
        serde_json::from_value(self.stored().unwrap_or_else(|| json!({}))).expect("stored machine section decodes")
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }
}

async fn serve_config(mut bus: MockBus, config: Arc<ConfigDouble>) {
    while let Some(envelope) = bus.recv_request().await {
        assert_eq!(
            envelope.test_target(),
            TypeId::of::<ConfigDomain>(),
            "machine must address Config"
        );
        let request = envelope
            .test_downcast::<ConfigRequest>()
            .unwrap_or_else(|_| panic!("config double received a non-config request"));
        let result = match request.payload {
            ConfigRequestPayload::GetConfigSection { section } => {
                assert_eq!(section, SECTION);
                config.reads.fetch_add(1, Ordering::SeqCst);
                Ok(ConfigResponse::ConfigSection {
                    value: config.stored().unwrap_or_else(|| json!({})),
                })
            }
            ConfigRequestPayload::SaveConfigSection { section, value } => {
                assert_eq!(section, SECTION);
                if !config.admit_write() {
                    Err(ConfigError::InvalidSection(section))
                } else {
                    config.set_stored(value);
                    config.writes.fetch_add(1, Ordering::SeqCst);
                    Ok(ConfigResponse::ConfigSectionSaved { section })
                }
            }
            _ => panic!("config double received an operation outside the machine section"),
        };
        let _ = request.reply_to.send(result);
    }
}

struct Harness {
    local: MachineIssuerId,
    config: Arc<ConfigDouble>,
    registry: AuthAdmissionRegistry,
    context: BusRpcContext,
    broadcasts: Mutex<Vec<mpsc::Receiver<MachineBroadcast>>>,
    _config_task: tokio::task::JoinHandle<()>,
}

impl Harness {
    fn new() -> Self {
        let local = issuer();
        let config = Arc::new(ConfigDouble::default());
        let (sender, bus) = MockBus::new(16);
        let config_task = tokio::spawn(serve_config(bus, config.clone()));
        Self {
            registry: registry(&local),
            context: BusRpcContext::authenticated(sender, Jwt::from_opaque("test-jwt".to_owned())),
            local,
            config,
            broadcasts: Mutex::new(Vec::new()),
            _config_task: config_task,
        }
    }

    fn actor(&self) -> Actor {
        self.actor_with_unavailable(None)
    }

    fn actor_with_unavailable(&self, unavailable: Option<String>) -> Actor {
        let (_sender, requests) = mpsc::channel::<MachineRequest>(1);
        let (broadcasts, receiver) = mpsc::channel(16);
        self.broadcasts
            .lock()
            .expect("broadcast lock")
            .push(receiver);
        Actor::new(
            MachineBootOptions {
                machine: self.local.clone(),
                name: DEFAULT_NAME.to_owned(),
            },
            self.context.clone(),
            unavailable,
            self.registry.clone(),
            None,
            requests,
            broadcasts,
        )
    }
}

#[tokio::test]
async fn rename_persists_the_trimmed_name_across_a_fresh_actor() {
    let harness = Harness::new();
    let mut first = harness.actor();
    let renamed = first
        .handle_message(MachineRequestPayload::RenameMachine {
            name: "  build box  ".to_owned(),
        })
        .await
        .expect("rename");
    assert!(matches!(renamed, MachineResponse::MachineRenamed { ref name } if name.as_str() == "build box"));
    assert_eq!(harness.config.writes(), 1);
    assert_eq!(harness.config.settings().name.as_deref(), Some("build box"));

    let mut restarted = harness.actor();
    let inventory = restarted
        .handle_message(MachineRequestPayload::ReadMachineInventory {})
        .await
        .expect("inventory");
    let MachineResponse::MachineInventory { inventory } = inventory else {
        panic!("expected inventory response")
    };
    assert_eq!(inventory.local.name, "build box");
}

#[tokio::test]
async fn blank_rename_is_rejected_before_any_write() {
    let harness = Harness::new();
    let mut actor = harness.actor();
    let error = actor
        .handle_message(MachineRequestPayload::RenameMachine { name: "   ".to_owned() })
        .await
        .expect_err("blank name refused");
    assert!(matches!(error, MachineError::InvalidName));
    assert_eq!(harness.config.writes(), 0);
    assert_eq!(harness.config.stored(), None);
}

#[tokio::test]
async fn refused_save_leaves_the_in_memory_name_unchanged() {
    let harness = Harness::new();
    let mut actor = harness.actor();
    harness.config.limit_writes(0);
    let error = actor
        .handle_message(MachineRequestPayload::RenameMachine {
            name: "build box".to_owned(),
        })
        .await
        .expect_err("refused save surfaces");
    assert!(matches!(error, MachineError::Config(_)));
    assert_eq!(harness.config.writes(), 0);
    let inventory = actor
        .handle_message(MachineRequestPayload::ReadMachineInventory {})
        .await
        .expect("inventory");
    let MachineResponse::MachineInventory { inventory } = inventory else {
        panic!("expected inventory response")
    };
    assert_eq!(
        inventory.local.name, DEFAULT_NAME,
        "memory must not run ahead of the saved value"
    );
}

#[tokio::test]
async fn status_reports_inventory_and_admitted_peers() {
    let harness = Harness::new();
    let mut actor = harness.actor();
    let remote = issuer();
    harness
        .registry
        .upsert_remote(ApprovedIssuer { issuer: remote.clone() })
        .expect("remote admitted");
    let response = actor
        .handle_message(MachineRequestPayload::ReadMachineStatus {})
        .await
        .expect("status");
    let MachineResponse::MachineStatus { status } = response else {
        panic!("expected status response")
    };
    assert_eq!(status.transport_identity, harness.local);
    assert_eq!(
        status.inventory.local.machine_key,
        hex_key(harness.local.verification_key())
    );
    assert_eq!(
        status.peers.admitted_remote_machines,
        vec![hex_key(remote.verification_key())]
    );
    assert_eq!(status.peers.online, OnlineState::Unavailable);
}

#[tokio::test]
async fn lifecycle_does_not_require_config_and_missing_peer_service_fails_closed() {
    let harness = Harness::new();
    let mut actor = harness.actor_with_unavailable(Some("config registration refused".to_owned()));
    for payload in [MachineRequestPayload::StartServer, MachineRequestPayload::StopServer] {
        assert!(matches!(
            actor.handle_message(payload).await,
            Err(MachineError::PeerServiceUnavailable)
        ));
    }
    assert_eq!(harness.config.reads(), 0);
    assert_eq!(harness.config.writes(), 0);
    assert!(matches!(
        harness.broadcasts.lock().expect("broadcast lock")[0].try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn peer_server_transitions_broadcast_once_and_repeated_requests_succeed() {
    let harness = Harness::new();
    let root = tempfile::tempdir().expect("temporary peer store");
    let secret = iroh::SecretKey::from_bytes(&[9; 32]);
    let local = MachineIssuerId::from(secret.public());
    let admission = registry(&local);
    let pairing = Arc::new(Mutex::new(
        PairingCoordinator::load(
            &root.path().join("peers.json"),
            admission.clone(),
            local.clone(),
            uuid::Uuid::new_v4(),
        )
        .expect("pairing coordinator"),
    ));
    let substrate = IrohSubstrate::prepare(root.path(), &secret, PeerConnectivity::new(false))
        .await
        .expect("prepare peer substrate");
    let services = substrate.peer_services();
    let running = substrate.start(None, AuthFirstContactHandler::new(pairing.clone()), pairing);
    let (_sender, requests) = mpsc::channel(1);
    let (broadcasts, mut receiver) = mpsc::channel(16);
    let mut actor = Actor::new(
        MachineBootOptions {
            machine: local,
            name: DEFAULT_NAME.to_owned(),
        },
        harness.context.clone(),
        None,
        admission,
        Some(services.clone()),
        requests,
        broadcasts,
    );

    for (payload, started, broadcast) in [
        (MachineRequestPayload::StartServer, true, true),
        (MachineRequestPayload::StartServer, true, false),
        (MachineRequestPayload::StopServer, false, true),
        (MachineRequestPayload::StopServer, false, false),
        (MachineRequestPayload::StartServer, true, true),
    ] {
        let response = actor
            .handle_message(payload)
            .await
            .expect("server transition");
        assert!(matches!(response, MachineResponse::ServerStarted) == started);
        assert!(matches!(response, MachineResponse::ServerStopped) != started);
        if broadcast {
            let event = receiver.try_recv().expect("transition broadcast");
            assert!(matches!(event, MachineBroadcast::ServerStarted) == started);
            assert!(matches!(event, MachineBroadcast::ServerStopped) != started);
        }
        assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert!(!services.endpoint().await.is_closed());
    }
    assert_eq!(harness.config.reads(), 0);
    assert_eq!(harness.config.writes(), 0);
    drop(actor);
    running
        .shutdown_router()
        .await
        .expect("shutdown peer substrate");
}

#[tokio::test]
async fn peer_server_controller_failure_does_not_broadcast_success() {
    let harness = Harness::new();
    let native = NativeFixture::new(10, false).await;
    let mut actor = native.actor(&harness, 10);
    for payload in [MachineRequestPayload::StartServer, MachineRequestPayload::StopServer] {
        assert!(matches!(
            actor.handle_message(payload).await,
            Err(MachineError::BusControl(_))
        ));
        assert!(matches!(
            harness.broadcasts.lock().expect("broadcast lock")[0].try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
    drop(actor);
    native.shutdown().await;
}

#[tokio::test]
async fn unavailable_configuration_fails_every_request_closed_without_side_effects() {
    let harness = Harness::new();
    let mut actor = harness.actor_with_unavailable(Some("config registration refused".to_owned()));
    let payloads = vec![
        MachineRequestPayload::ReadMachineInventory {},
        MachineRequestPayload::ReadMachineStatus {},
        MachineRequestPayload::RenameMachine {
            name: "build box".to_owned(),
        },
        MachineRequestPayload::ImportContent {
            tag: "test".to_owned(),
            bytes: vec![1],
        },
        MachineRequestPayload::UnpinContent { tag: "test".to_owned() },
        MachineRequestPayload::EncryptContent {
            execution_id: "execution".to_owned(),
            format_version: 1,
            plaintext: vec![1],
            recipients: vec![],
        },
    ];
    for payload in payloads {
        let error = actor
            .handle_message(payload)
            .await
            .expect_err("an unavailable Machine must refuse every request");
        assert!(
            matches!(&error, MachineError::Unavailable { cause } if cause.as_str() == "config registration refused"),
            "unexpected error: {error}"
        );
    }
    assert_eq!(
        harness.config.reads(),
        0,
        "no section may be read while Config is unavailable"
    );
    assert_eq!(
        harness.config.writes(),
        0,
        "no section may be written while Config is unavailable"
    );
    assert_eq!(
        harness.registry.current_issuers().len(),
        1,
        "no peer may be admitted while Config is unavailable"
    );
}

#[tokio::test]
async fn undecodable_section_fails_closed_before_any_write() {
    let harness = Harness::new();
    let corrupt = json!({"name": 42});
    harness.config.set_stored(corrupt.clone());
    let mut actor = harness.actor();
    let rename = actor
        .handle_message(MachineRequestPayload::RenameMachine {
            name: "build box".to_owned(),
        })
        .await
        .expect_err("corrupt section refuses rename");
    assert!(matches!(rename, MachineError::Section { .. }));
    let read = actor
        .handle_message(MachineRequestPayload::ReadMachineInventory {})
        .await
        .expect_err("corrupt section refuses reads");
    assert!(matches!(read, MachineError::Section { .. }));
    assert_eq!(harness.config.writes(), 0);
    assert_eq!(
        harness.config.stored(),
        Some(corrupt),
        "the corrupt section must not be overwritten"
    );
}

struct NativeFixture {
    services: MachinePeerServices,
    router: Router,
    _root: tempfile::TempDir,
}

impl NativeFixture {
    async fn new(seed: u8, enabled: bool) -> Self {
        let root = tempfile::tempdir().expect("temporary store");
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .address_lookup(MemoryLookup::new())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("local bind address")
            .bind()
            .await
            .expect("local endpoint");
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let blobs = FsStore::load(root.path().join("blobs"))
            .await
            .expect("blob store");
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone().into(), gossip.clone())
            .await
            .expect("memory docs");
        let services = MachinePeerServices::new_with_gossip(
            blobs,
            docs.api().clone(),
            endpoint.clone(),
            Some(gossip.clone()),
            PeerConnectivity::new(enabled),
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

    fn actor(&self, harness: &Harness, seed: u8) -> Actor {
        let local = MachineIssuerId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public());
        let (_sender, requests) = mpsc::channel(1);
        let (broadcasts, receiver) = mpsc::channel(16);
        harness
            .broadcasts
            .lock()
            .expect("broadcast lock")
            .push(receiver);
        Actor::new(
            MachineBootOptions {
                machine: local.clone(),
                name: DEFAULT_NAME.to_owned(),
            },
            harness.context.clone(),
            None,
            registry(&local),
            Some(self.services.clone()),
            requests,
            broadcasts,
        )
    }

    async fn shutdown(self) {
        self.router.shutdown().await.expect("router shutdown");
        self.services
            .blobs
            .shutdown()
            .await
            .expect("store shutdown");
    }
}

async fn encrypted(actor: &mut Actor, recipients: Vec<MachineIssuerId>) -> EncryptedContent {
    let response = actor
        .handle_message(MachineRequestPayload::EncryptContent {
            execution_id: "execution-1".to_owned(),
            format_version: 1,
            plaintext: b"private plaintext".to_vec(),
            recipients,
        })
        .await
        .expect("encrypt");
    let MachineResponse::ContentEncrypted { content } = response else {
        panic!("expected encrypted content")
    };
    content
}

async fn decrypted(actor: &mut Actor, content: EncryptedContent) -> Result<Vec<u8>, MachineError> {
    match actor
        .handle_message(MachineRequestPayload::DecryptContent { content })
        .await?
    {
        MachineResponse::ContentDecrypted { plaintext } => Ok(plaintext),
        other => panic!("expected plaintext response, got {other:?}"),
    }
}

#[tokio::test]
async fn raw_and_collection_content_round_trip_and_pins_are_owned() {
    let harness = Harness::new();
    let native = NativeFixture::new(1, false).await;
    let mut actor = native.actor(&harness, 1);
    let bytes = b"raw content".to_vec();
    let response = actor
        .handle_message(MachineRequestPayload::ImportContent {
            tag: "raw".into(),
            bytes: bytes.clone(),
        })
        .await
        .expect("import raw");
    let MachineResponse::ContentImported {
        root: ContentRoot::Raw(hash),
    } = response
    else {
        panic!("expected raw root")
    };
    assert_eq!(hash, Hash::new(&bytes));
    let response = actor
        .handle_message(MachineRequestPayload::ReadContent {
            hash: ContentHash(hash),
        })
        .await
        .expect("read raw");
    assert!(matches!(response, MachineResponse::ContentBytes { bytes: actual } if actual == bytes));
    let entries = vec![ContentEntry {
        name: "file.txt".into(),
        hash,
    }];
    let response = actor
        .handle_message(MachineRequestPayload::ImportContentCollection {
            tag: "collection".into(),
            entries: entries.clone(),
        })
        .await
        .expect("import collection");
    let MachineResponse::ContentImported {
        root: ContentRoot::Collection(collection),
    } = response
    else {
        panic!("expected collection root")
    };
    let response = actor
        .handle_message(MachineRequestPayload::ReadContentCollection {
            hash: ContentHash(collection),
        })
        .await
        .expect("read collection");
    assert!(matches!(response, MachineResponse::ContentCollection { entries: actual } if actual == entries));
    let store: Store = native.services.blobs.clone().into();
    let tag = store
        .tags()
        .get("content/collection")
        .await
        .expect("read tag")
        .expect("collection pinned");
    assert!(tag.format.is_hash_seq());
    actor
        .handle_message(MachineRequestPayload::PinContent {
            tag: "second".into(),
            root: ContentRoot::Raw(hash),
        })
        .await
        .expect("pin");
    assert!(
        store
            .tags()
            .get("content/second")
            .await
            .expect("read tag")
            .is_some()
    );
    actor
        .handle_message(MachineRequestPayload::UnpinContent { tag: "second".into() })
        .await
        .expect("unpin");
    assert!(
        store
            .tags()
            .get("content/second")
            .await
            .expect("read tag")
            .is_none()
    );
    assert!(
        store
            .tags()
            .get("content/raw")
            .await
            .expect("read tag")
            .is_some()
    );
    let error = actor
        .handle_message(MachineRequestPayload::ImportContent {
            tag: "../bad".into(),
            bytes: vec![],
        })
        .await
        .expect_err("malformed tag");
    assert!(matches!(error, MachineError::InvalidContentTag { .. }));
    drop(store);
    drop(actor);
    native.shutdown().await;
}

#[tokio::test]
async fn peer_fetch_denies_unadmitted_removed_self_and_disabled_before_network() {
    let harness = Harness::new();
    let native = NativeFixture::new(2, false).await;
    let mut actor = native.actor(&harness, 2);
    let local = iroh::SecretKey::from_bytes(&[2; 32]).public();
    let remote = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let remote_id = MachineIssuerId::from(remote);
    let fetch = |peer| MachineRequestPayload::FetchContentFromPeer {
        peer: ContentPeer(peer),
        tag: "fetch".into(),
        root: ContentRoot::Raw(Hash::new(b"not fetched")),
    };
    for peer in [local, remote] {
        assert!(matches!(
            actor.handle_message(fetch(peer)).await,
            Err(MachineError::ContentPeerRefused { .. })
        ));
    }
    actor
        .registry
        .upsert_remote(ApprovedIssuer {
            issuer: remote_id.clone(),
        })
        .expect("admit remote");
    assert!(matches!(
        actor.handle_message(fetch(remote)).await,
        Err(MachineError::ContentConnectivityDisabled)
    ));
    actor
        .registry
        .remove_remote(&remote_id)
        .expect("remove remote");
    assert!(matches!(
        actor.handle_message(fetch(remote)).await,
        Err(MachineError::ContentPeerRefused { .. })
    ));
    drop(actor);
    native.shutdown().await;
}

#[tokio::test]
async fn machine_encryption_wraps_producer_and_adds_recipients_without_rewriting() {
    let harness = Harness::new();
    let producer = NativeFixture::new(4, false).await;
    let recipient = NativeFixture::new(5, false).await;
    let stranger = NativeFixture::new(6, false).await;
    let mut producer_actor = producer.actor(&harness, 4);
    let mut recipient_actor = recipient.actor(&harness, 5);
    let mut stranger_actor = stranger.actor(&harness, 6);
    let producer_id = MachineIssuerId::from(iroh::SecretKey::from_bytes(&[4; 32]).public());
    let recipient_id = MachineIssuerId::from(iroh::SecretKey::from_bytes(&[5; 32]).public());
    let content = encrypted(&mut producer_actor, vec![]).await;
    assert_eq!(content.envelopes.len(), 1);
    assert_eq!(content.envelopes[0].recipient, producer_id);
    assert_eq!(
        decrypted(&mut producer_actor, content.clone())
            .await
            .expect("offline producer decrypt"),
        b"private plaintext"
    );
    assert!(matches!(
        decrypted(&mut stranger_actor, content.clone()).await,
        Err(MachineError::ContentNoLocalRecipient)
    ));
    let explicit = encrypted(&mut producer_actor, vec![recipient_id.clone()]).await;
    assert_eq!(
        decrypted(&mut producer_actor, explicit.clone())
            .await
            .expect("producer decrypt"),
        b"private plaintext"
    );
    assert_eq!(
        decrypted(&mut recipient_actor, explicit)
            .await
            .expect("recipient decrypt"),
        b"private plaintext"
    );
    let response = producer_actor
        .handle_message(MachineRequestPayload::AddContentRecipient {
            content: content.clone(),
            recipient: recipient_id.clone(),
        })
        .await
        .expect("add recipient");
    let MachineResponse::ContentEncrypted { content: added } = response else {
        panic!("expected encrypted content")
    };
    assert_eq!(added.ciphertext, content.ciphertext);
    assert_eq!(Hash::new(&added.ciphertext), Hash::new(&content.ciphertext));
    assert_eq!(
        &added.envelopes[..content.envelopes.len()],
        content.envelopes.as_slice()
    );
    assert_eq!(
        decrypted(&mut recipient_actor, added.clone())
            .await
            .expect("added recipient decrypt"),
        b"private plaintext"
    );
    let response = producer_actor
        .handle_message(MachineRequestPayload::AddContentRecipient {
            content: added.clone(),
            recipient: recipient_id.clone(),
        })
        .await
        .expect("duplicate recipient");
    assert!(matches!(response, MachineResponse::ContentEncrypted { content } if content == added));
    let response = producer_actor
        .handle_message(MachineRequestPayload::ReadContentRecipients { content: added.clone() })
        .await
        .expect("read recipients");
    assert!(
        matches!(response, MachineResponse::ContentRecipients { recipients } if recipients == vec![producer_id, recipient_id])
    );
    let json = serde_json::to_value(&added).expect("encode bundle");
    assert_eq!(
        serde_json::from_value::<EncryptedContent>(json.clone()).expect("decode bundle"),
        added
    );
    let serialized = json.to_string();
    assert!(!serialized.contains("private plaintext"));
    assert!(!serialized.contains("secret_key"));
    assert!(!serialized.contains("content_key"));
    assert_eq!(json["execution_id"], "execution-1");
    assert_eq!(json["format_version"], 1);
    assert!(json["envelopes"].is_array());
    let object = json.as_object().expect("bundle object");
    assert_eq!(object.len(), 4);
    for envelope in json["envelopes"].as_array().expect("envelopes") {
        let object = envelope.as_object().expect("envelope object");
        assert_eq!(object.len(), 6);
        for key in [
            "signer",
            "recipient",
            "ciphertext_hash",
            "encapsulated_key",
            "wrapped_key",
            "signature",
        ] {
            assert!(object.contains_key(key), "missing public metadata: {key}");
        }
    }
    drop((producer_actor, recipient_actor, stranger_actor));
    producer.shutdown().await;
    recipient.shutdown().await;
    stranger.shutdown().await;
}

#[tokio::test]
async fn tampered_encrypted_content_fails_closed() {
    let harness = Harness::new();
    let native = NativeFixture::new(7, false).await;
    let mut actor = native.actor(&harness, 7);
    let content = encrypted(&mut actor, vec![]).await;
    let mut ciphertext = content.clone();
    let last = ciphertext.ciphertext.len() - 1;
    ciphertext.ciphertext[last] ^= 1;
    let mut signature = content.clone();
    signature.envelopes[0].signature[0] ^= 1;
    let mut recipient = content.clone();
    recipient.envelopes[0].recipient = MachineIssuerId::from(iroh::SecretKey::from_bytes(&[8; 32]).public());
    let mut execution = content.clone();
    execution.execution_id.push_str("-changed");
    let mut version = content;
    version.format_version += 1;
    for tampered in [ciphertext, signature, recipient, execution, version] {
        assert!(
            decrypted(&mut actor, tampered).await.is_err(),
            "tampering must never yield plaintext"
        );
    }
    drop(actor);
    native.shutdown().await;
}

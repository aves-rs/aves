use arc_malachitebft_app_channel::AppMsg;
use arc_malachitebft_app_channel::app::types::PeerId;
use arc_malachitebft_app_channel::app::types::core::{CommitCertificate, Round, Validity, VoteExtensions};
use arc_malachitebft_codec::{Codec, HasEncodedLen};
use arc_malachitebft_core_consensus::{MisbehaviorEvidence, SignedConsensusMsg};
use arc_malachitebft_core_types::{
    ExtendedCommitCertificate, ExtendedCommitSignature, NilOrVal, PolkaCertificate, PolkaSignature, SignedMessage,
    ValidatorSet as _, Value as _,
};
use arc_malachitebft_engine::{
    host::{Next, ProposedValue},
    network::Status as NetworkStatus,
    util::streaming::{StreamContent, StreamId, StreamMessage},
};
use arc_malachitebft_signing::Signer;
use arc_malachitebft_sync::{RawDecidedValue, Request, Response, Status as SyncStatus, ValueRequest, ValueResponse};
use bytes::Bytes;
use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets, protocol::Router};
use serde::Serialize;

use crate::auth::{ApprovedIssuer, AuthAdmissionRegistry};
use crate::machine::{MachinePeerServices, PeerConnectivity};

use super::super::messages::{CoordinationRequestPayload, CoordinationResponse};
use super::super::network::NetworkAttachment;
use super::super::serialization::{ChainPayloadError, CoordinationJsonCodec};
use super::super::signing::{ChainSignature, ChainSigner, CoordinationSigner};
use super::super::types::{
    ConsensusProposal, ConsensusProposalPart, ConsensusValue, ConsensusVote, ConsensusVoteKind, CoordinationContext,
    CoordinationError, DomainId, EncKey, Genesis, Height, PublicKey, Regenesis, RegisteredTransaction, SignedBody,
    UserId, UserRegistered, ValidatorSet,
};
use super::Actor;

const DOMAIN: DomainId = DomainId::from_bytes([7; 32]);

fn signer(seed: u8) -> ChainSigner {
    ChainSigner::new(iroh::SecretKey::from_bytes(&[seed; 32]))
}

async fn certificate(seed: u8, domain: DomainId, value: &ConsensusValue) -> CommitCertificate<CoordinationContext> {
    let key = iroh::SecretKey::from_bytes(&[seed; 32]);
    let address = PublicKey(*key.public().as_bytes());
    let value_id = value.id();
    let vote = ConsensusVote {
        height: Height(1),
        round: 0,
        value: NilOrVal::Val(value_id.clone()),
        kind: ConsensusVoteKind::Precommit,
        address,
        extension: None,
    };
    let signed_vote = Signer::sign_vote(&CoordinationSigner::new(key, domain), vote)
        .await
        .expect("sign precommit");
    CommitCertificate::new(Height(1), Round::new(0), value_id, vec![signed_vote])
}

fn signed<T: Serialize + RegisteredTransaction>(body: T, signer: &ChainSigner, domain: DomainId) -> SignedBody<T> {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(T::PURPOSE.as_bytes());
    preimage.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut preimage, &body).expect("encode signed body");
    SignedBody {
        body,
        signer: PublicKey::from(signer.public_key()),
        sig: signer.sign(&preimage).0,
    }
}

fn registration(user: u8, root_key: PublicKey) -> UserRegistered {
    UserRegistered {
        user: UserId([user; 32]),
        root_key,
        enc_key: EncKey([9; 32]),
        enc_epoch: 1,
    }
}

fn codec_value() -> ConsensusValue {
    use super::super::transaction::PendingTransaction;

    let owner = signer(35);
    let record = signed(registration(36, PublicKey::from(owner.public_key())), &owner, DOMAIN);
    ConsensusValue(serde_json::to_string(&[PendingTransaction::UserRegistered(record)]).expect("encode transaction"))
}

async fn new_actor(config_dir: &std::path::Path) -> Actor {
    let (_sender, receiver) = tokio::sync::mpsc::channel(4);
    let (broadcasts, _broadcast_rx) = tokio::sync::mpsc::channel(4);
    Actor::new(receiver, config_dir.to_path_buf(), None, None, broadcasts).await
}

fn peer_id(signer: &ChainSigner) -> PeerId {
    let mut bytes = vec![0x00, 0x24, 0x08, 0x01, 0x12, 0x20];
    bytes.extend_from_slice(&signer.public_key().0);
    PeerId::from_bytes(&bytes).expect("valid inline Ed25519 peer id")
}

struct NativeFixture {
    services: MachinePeerServices,
    router: Router,
    _root: tempfile::TempDir,
}

#[tokio::test]
async fn idle_proposal_does_not_block_history_callbacks() {
    use arc_malachitebft_engine::host::HostMsg;
    let directory = tempfile::tempdir().unwrap();
    let mut actor = new_actor(directory.path()).await;
    let owner = signer(70);
    let address = PublicKey::from(owner.public_key());
    let chain = actor.abci.create_genesis(vec![(address, 1)]).await.unwrap();
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let (reply, mut proposed) = tokio::sync::oneshot::channel();
    actor
        .dispatch_consensus_message(
            HostMsg::GetValue {
                height: Height(1),
                round: Round::new(0),
                timeout: std::time::Duration::from_secs(60),
                reply_to: reply.into(),
            },
            &network,
        )
        .await
        .unwrap();
    assert!(matches!(
        proposed.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let (reply, history) = tokio::sync::oneshot::channel();
    actor
        .dispatch_consensus_message(HostMsg::GetHistoryMinHeight { reply_to: reply.into() }, &network)
        .await
        .unwrap();
    assert_eq!(history.await.unwrap(), Height(1));
    let (reply, values) = tokio::sync::oneshot::channel();
    actor
        .dispatch_consensus_message(
            HostMsg::GetDecidedValues {
                range: Height(1)..=Height(1),
                reply_to: reply.into(),
            },
            &network,
        )
        .await
        .unwrap();
    assert!(values.await.unwrap().is_empty());
    actor
        .handle_message(CoordinationRequestPayload::RegisterUser {
            record: signed(registration(70, address), &owner, chain.domain),
        })
        .await
        .unwrap();
    let value = tokio::time::timeout(std::time::Duration::from_secs(1), proposed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.height, Height(1));
    assert!(!value.value.0.is_empty());
}

impl NativeFixture {
    async fn new(seed: u8, lookup: &MemoryLookup) -> Self {
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
            .max_message_size(64 * 1024 * 1024)
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

    fn admission(seed: u8, peers: &[u8]) -> AuthAdmissionRegistry {
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer {
            issuer: iroh::SecretKey::from_bytes(&[seed; 32]).public().into(),
        });
        for peer in peers {
            registry
                .upsert_remote(ApprovedIssuer {
                    issuer: iroh::SecretKey::from_bytes(&[*peer; 32]).public().into(),
                })
                .unwrap();
        }
        registry
    }
}

#[tokio::test]
async fn transport_loss_preserves_reconnect_intent() {
    let directory = tempfile::tempdir().unwrap();
    let mut actor = new_actor(directory.path()).await;
    actor.server_requested = true;

    actor.handle_application_event(None).await;

    assert!(actor.server_requested);
    assert!(actor.running.is_none());
}

#[tokio::test]
async fn explicit_stop_cancels_pending_reconnect_and_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let mut actor = new_actor(directory.path()).await;
    actor.server_requested = true;
    actor.restart_after_recovery = true;

    let response = actor
        .handle_message(CoordinationRequestPayload::StopServer)
        .await
        .unwrap();

    assert!(matches!(response, CoordinationResponse::ServerStopped));
    assert!(!actor.server_requested);
    assert!(!actor.restart_after_recovery);
    actor.reconnect().await;
    assert!(actor.running.is_none());
}

struct NetworkCollector;

#[async_trait::async_trait]
impl ractor::Actor for NetworkCollector {
    type Msg = arc_malachitebft_engine::network::NetworkEvent<CoordinationContext>;
    type State = tokio::sync::mpsc::Sender<Self::Msg>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _: ractor::ActorRef<Self::Msg>,
        sender: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(sender)
    }
    async fn handle(
        &self,
        _: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        sender: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        sender.send(message).await.map_err(Into::into)
    }
}

#[tokio::test]
async fn native_network_replays_bound_listening_address_to_each_subscriber_after_reattachment() {
    use arc_malachitebft_engine::network::{Msg, NetworkEvent};
    use ractor::Actor as _;

    let native = NativeFixture::new(92, &MemoryLookup::new()).await;
    let sockets = native.services.native.read().await.endpoint.bound_sockets();
    assert!(
        sockets
            .iter()
            .any(|socket| socket.ip() == std::net::Ipv4Addr::LOCALHOST)
    );
    let expected = sockets
        .iter()
        .filter_map(|socket| {
            if let std::net::SocketAddr::V6(address) = socket
                && address.scope_id() != 0
            {
                return None;
            }
            assert_ne!(socket.port(), 0);
            let family = if socket.is_ipv4() { "ip4" } else { "ip6" };
            Some(format!("/{family}/{}/udp/{}/quic-v1", socket.ip(), socket.port()))
        })
        .collect::<std::collections::BTreeSet<_>>();

    for _ in 0..2 {
        let network = NetworkAttachment::start(&native.services, NativeFixture::admission(92, &[]), DOMAIN)
            .await
            .unwrap();
        for _ in 0..2 {
            let (events, mut receiver) = tokio::sync::mpsc::channel(expected.len());
            let (observer, task) = NetworkCollector::spawn(None, NetworkCollector, events)
                .await
                .unwrap();
            network
                .actor
                .cast(Msg::Subscribe(Box::new(observer.clone())))
                .unwrap();
            let mut remaining = expected.clone();
            for _ in 0..expected.len() {
                let event = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
                    .await
                    .expect("subscriber must receive native listening address")
                    .expect("subscriber must remain open");
                let NetworkEvent::Listening(address) = event else {
                    panic!("unexpected network event: {event:?}");
                };
                assert!(
                    remaining.remove(&address.to_string()),
                    "unexpected or duplicate listening address"
                );
            }
            assert!(remaining.is_empty());
            observer.stop(None);
            task.await.unwrap();
        }
        network.stop().await.unwrap();
    }
    native.router.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_network_carries_targeted_sync_requests_and_responses() {
    use arc_malachitebft_engine::network::{Msg, NetworkEvent};
    use ractor::Actor as _;
    let lookup = MemoryLookup::new();
    let left = NativeFixture::new(73, &lookup).await;
    let right = NativeFixture::new(74, &lookup).await;
    let left_network = NetworkAttachment::start(&left.services, NativeFixture::admission(73, &[74]), DOMAIN)
        .await
        .unwrap();
    let right_network = NetworkAttachment::start(&right.services, NativeFixture::admission(74, &[73]), DOMAIN)
        .await
        .unwrap();
    let (left_events, mut left_receiver) = tokio::sync::mpsc::channel(32);
    let (right_events, mut right_receiver) = tokio::sync::mpsc::channel(32);
    let (left_observer, left_task) = NetworkCollector::spawn(None, NetworkCollector, left_events)
        .await
        .unwrap();
    let (right_observer, right_task) = NetworkCollector::spawn(None, NetworkCollector, right_events)
        .await
        .unwrap();
    left_network
        .actor
        .cast(Msg::Subscribe(Box::new(left_observer.clone())))
        .unwrap();
    right_network
        .actor
        .cast(Msg::Subscribe(Box::new(right_observer.clone())))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !matches!(left_receiver.recv().await, Some(NetworkEvent::PeerSubscribed(..))) {}
        while !matches!(right_receiver.recv().await, Some(NetworkEvent::PeerSubscribed(..))) {}
    })
    .await
    .expect("shared gossip neighbors must reach both engine subscribers");
    let request = Request::ValueRequest(ValueRequest::new(Height(1)..=Height(1)));
    let (reply, assigned) = tokio::sync::oneshot::channel();
    left_network
        .actor
        .cast(Msg::OutgoingRequest(
            peer_id(&signer(74)),
            request.clone(),
            reply.into(),
        ))
        .unwrap();
    let outbound_id = assigned.await.unwrap();
    let inbound_id = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(NetworkEvent::SyncRequest(id, peer, received)) = right_receiver.recv().await {
                assert_eq!(peer, peer_id(&signer(73)));
                assert_eq!(received, request);
                break id;
            }
        }
    })
    .await
    .expect("request must arrive over the shared router");
    let response = Response::ValueResponse(ValueResponse::new(Height(1), vec![]));
    right_network
        .actor
        .cast(Msg::OutgoingResponse(inbound_id, response.clone()))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(NetworkEvent::SyncResponse(id, peer, received)) = left_receiver.recv().await {
                assert_eq!(id, outbound_id);
                assert_eq!(peer, peer_id(&signer(74)));
                assert_eq!(received, Some(response.clone()));
                break;
            }
        }
    })
    .await
    .expect("response must match the original outbound request");
    left_network.stop().await.unwrap();
    right_network.stop().await.unwrap();
    left_observer.stop(None);
    right_observer.stop(None);
    left_task.await.unwrap();
    right_task.await.unwrap();
    left.router.shutdown().await.unwrap();
    right.router.shutdown().await.unwrap();
}

fn proposal_parts(proposal: &ConsensusProposal, stream_id: StreamId) -> [StreamMessage<ConsensusProposalPart>; 3] {
    let proposal_bytes = serde_json::to_vec(proposal).expect("encode proposal");
    [
        StreamMessage::new(stream_id.clone(), 0, StreamContent::Data(ConsensusProposalPart::First)),
        StreamMessage::new(
            stream_id.clone(),
            1,
            StreamContent::Data(ConsensusProposalPart::Data(proposal_bytes)),
        ),
        StreamMessage::new(stream_id, 2, StreamContent::Data(ConsensusProposalPart::Last)),
    ]
}

async fn receive_proposal_parts(
    actor: &mut Actor,
    proposal: &ConsensusProposal,
    stream_id: StreamId,
    peer: PeerId,
) -> Option<arc_malachitebft_app_channel::app::types::ProposedValue<super::super::types::CoordinationContext>> {
    let parts = proposal_parts(proposal, stream_id);
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let mut proposed = None;
    for (index, part) in parts.into_iter().enumerate() {
        let (reply, response) = tokio::sync::oneshot::channel();
        actor
            .abci
            .handle_app_msg(
                AppMsg::ReceivedProposalPart {
                    from: peer,
                    published_by: Some(peer),
                    part,
                    reply,
                },
                &network,
            )
            .await
            .expect("handle proposal part");
        let result = response.await.expect("proposal callback reply");
        if index < 2 {
            assert!(result.is_none(), "proposal is incomplete until its Last part");
        } else {
            proposed = result;
        }
    }
    proposed
}

fn codec_round_trip<T>(value: &T) -> T
where
    CoordinationJsonCodec: Codec<T, Error = ChainPayloadError>,
{
    let codec = CoordinationJsonCodec::new();
    let encoded = codec.encode(value).expect("encode JSON");
    codec.decode(encoded).expect("decode JSON")
}

#[tokio::test]
async fn codec_round_trips_signed_consensus_and_rejects_vote_extensions() {
    let secret = iroh::SecretKey::from_bytes(&[30; 32]);
    let address = PublicKey(*secret.public().as_bytes());
    let signer = CoordinationSigner::new(secret, DOMAIN);
    let value = codec_value();
    let vote = ConsensusVote {
        height: Height(9),
        round: 3,
        value: NilOrVal::Val(value.id()),
        kind: ConsensusVoteKind::Prevote,
        address,
        extension: None,
    };
    let signed_vote = Signer::sign_vote(&signer, vote).await.expect("sign vote");
    let message = SignedConsensusMsg::Vote(signed_vote.clone());
    match codec_round_trip(&message) {
        SignedConsensusMsg::Vote(decoded) => {
            assert_eq!(decoded.message, signed_vote.message);
            assert!(decoded.message.extension.is_none());
            assert_eq!(decoded.signature, signed_vote.signature);
            assert_eq!(decoded.message.address, address);
            assert_eq!(decoded.message.round, 3);
        }
        other => panic!("unexpected consensus message: {other:?}"),
    }
    let proposal = ConsensusProposal {
        height: Height(9),
        round: 3,
        value: value.clone(),
        pol_round: 1,
        proposer: address,
    };
    let signed_proposal = Signer::sign_proposal(&signer, proposal)
        .await
        .expect("sign proposal");
    match codec_round_trip(&SignedConsensusMsg::Proposal(signed_proposal.clone())) {
        SignedConsensusMsg::Proposal(decoded) => {
            assert_eq!(decoded.message, signed_proposal.message);
            assert_eq!(decoded.message.value, value);
            assert_eq!(decoded.message.proposer, address);
            assert_eq!(decoded.message.pol_round, 1);
            assert_eq!(decoded.signature, signed_proposal.signature);
        }
        other => panic!("unexpected consensus message: {other:?}"),
    }
    let codec = CoordinationJsonCodec::new();
    let mut extended = signed_vote.clone();
    extended.message.extension = Some(SignedMessage::new(vec![1, 2, 3], ChainSignature([8; 64])));
    assert!(matches!(
        codec.encode(&SignedConsensusMsg::Vote(extended)),
        Err(ChainPayloadError::VoteExtensionUnsupported)
    ));
    let encoded = codec.encode(&message).expect("encode vote");
    let mut json: serde_json::Value = serde_json::from_slice(&encoded).expect("vote JSON");
    json["Vote"]["message"]["extension"] = serde_json::json!({"message": [1, 2, 3]});
    let bytes = Bytes::from(serde_json::to_vec(&json).expect("encode extended vote"));
    assert!(matches!(
        <CoordinationJsonCodec as Codec<SignedConsensusMsg<CoordinationContext>>>::decode(&codec, bytes),
        Err(ChainPayloadError::VoteExtensionUnsupported)
    ));
}

#[test]
fn codec_round_trips_proposed_value_and_polka_certificate() {
    let key = PublicKey::from(signer(31).public_key());
    let value = codec_value();
    let proposed = ProposedValue::<CoordinationContext> {
        height: Height(12),
        round: Round::new(4).into(),
        valid_round: Round::new(2).into(),
        proposer: key,
        value: value.clone(),
        validity: Validity::Valid,
    };
    let decoded = codec_round_trip(&proposed);
    assert_eq!(decoded.height, Height(12));
    assert_eq!(decoded.round, proposed.round);
    assert_eq!(decoded.valid_round, proposed.valid_round);
    assert_eq!(decoded.proposer, key);
    assert_eq!(decoded.value, value);
    assert_eq!(decoded.validity, Validity::Valid);
    let polka = PolkaCertificate::<CoordinationContext> {
        height: Height(12),
        round: Round::new(2),
        value_id: value.id(),
        polka_signatures: vec![PolkaSignature::new(key, ChainSignature([31; 64]))],
    };
    let decoded = codec_round_trip(&polka);
    assert_eq!(decoded.height, polka.height);
    assert_eq!(decoded.round, polka.round);
    assert_eq!(decoded.value_id, polka.value_id);
    assert_eq!(decoded.polka_signatures, polka.polka_signatures);
}

#[test]
fn codec_round_trips_sync_status_request_and_response_with_extensions() {
    let owner = signer(32);
    let key = PublicKey::from(owner.public_key());
    let peer = peer_id(&owner);
    let sync_status = SyncStatus::<CoordinationContext> {
        peer_id: peer,
        tip_height: Height(18),
        history_min_height: Height(4),
    };
    assert_eq!(codec_round_trip(&sync_status), sync_status);
    let network_status = NetworkStatus::<CoordinationContext> {
        tip_height: Height(18),
        history_min_height: Height(4),
    };
    let recovered = codec_round_trip(&network_status);
    assert_eq!(recovered.tip_height, Height(18));
    assert_eq!(recovered.history_min_height, Height(4));
    let request = Request::<CoordinationContext>::ValueRequest(ValueRequest::new(Height(4)..=Height(7)));
    assert_eq!(codec_round_trip(&request), request);

    let value = codec_value();
    let payload = Bytes::from(serde_json::to_vec(&value).expect("encode application value"));
    let extension = SignedMessage::new(vec![4, 5, 6], ChainSignature([33; 64]));
    let certificate = ExtendedCommitCertificate::<CoordinationContext> {
        height: Height(4),
        round: Round::new(2),
        value_id: value.id(),
        commit_signatures: vec![ExtendedCommitSignature::new(
            key,
            ChainSignature([34; 64]),
            Some(extension.clone()),
        )],
    };
    let response = Response::<CoordinationContext>::ValueResponse(ValueResponse::new(
        Height(4),
        vec![RawDecidedValue::new(payload.clone(), certificate.clone())],
    ));
    let codec = CoordinationJsonCodec::new();
    let encoded = codec.encode(&response).expect("encode sync response");
    assert_eq!(codec.encoded_len(&response).expect("encoded length"), encoded.len());
    let decoded: Response<CoordinationContext> = codec.decode(encoded).expect("decode sync response");
    let Response::ValueResponse(decoded) = decoded;
    assert_eq!(decoded.start_height, Height(4));
    assert_eq!(decoded.values.len(), 1);
    assert_eq!(decoded.values[0].value_bytes, payload);
    let recovered = &decoded.values[0].certificate;
    assert_eq!(recovered.height, certificate.height);
    assert_eq!(recovered.round, certificate.round);
    assert_eq!(recovered.value_id, certificate.value_id);
    assert_eq!(recovered.commit_signatures.len(), 1);
    assert_eq!(recovered.commit_signatures[0].address, key);
    assert_eq!(recovered.commit_signatures[0].signature, ChainSignature([34; 64]));
    let recovered_extension = recovered.commit_signatures[0]
        .extension
        .as_ref()
        .expect("preserved extension");
    assert_eq!(recovered_extension.message, extension.message);
    assert_eq!(recovered_extension.signature, extension.signature);
}

#[test]
fn codec_rejects_malformed_truncated_and_trailing_json() {
    fn reject<T>()
    where
        CoordinationJsonCodec: Codec<T, Error = ChainPayloadError>,
    {
        let codec = CoordinationJsonCodec::new();
        for input in ["{", "not-json", "{} {}"] {
            assert!(
                <CoordinationJsonCodec as Codec<T>>::decode(&codec, Bytes::copy_from_slice(input.as_bytes())).is_err()
            );
        }
    }
    reject::<SignedConsensusMsg<CoordinationContext>>();
    reject::<ProposedValue<CoordinationContext>>();
    reject::<PolkaCertificate<CoordinationContext>>();
    reject::<SyncStatus<CoordinationContext>>();
    reject::<Request<CoordinationContext>>();
    reject::<Response<CoordinationContext>>();
}

#[tokio::test]
async fn genesis_rejects_invalid_and_duplicate_requests() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    let first = PublicKey::from(signer(20).public_key());
    let second = PublicKey::from(signer(21).public_key());
    for validators in [vec![], vec![(first, 1), (first, 2)]] {
        assert!(matches!(
            actor
                .handle_message(CoordinationRequestPayload::Genesis {
                    genesis: Genesis { validators }
                })
                .await,
            Err(CoordinationError::InvalidValidatorSet)
        ));
    }
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::Genesis {
                genesis: Genesis {
                    validators: vec![(first, 0)]
                },
            })
            .await,
        Err(CoordinationError::ZeroVotingPower)
    ));
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::GetChain)
            .await,
        Err(CoordinationError::NotImplemented)
    ));
    let chain = match actor
        .handle_message(CoordinationRequestPayload::Genesis {
            genesis: Genesis {
                validators: vec![(first, 2), (second, 5)],
            },
        })
        .await
        .expect("create genesis")
    {
        CoordinationResponse::Chain { record } => record,
        other => panic!("unexpected genesis response: {other:?}"),
    };
    assert_eq!(chain.height, Height(0));
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let (reply, ready) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(AppMsg::ConsensusReady { reply }, &network)
        .await
        .expect("read roster");
    let (height, params) = ready.await.expect("ready response");
    assert_eq!(height, Height(1));
    assert_eq!(
        params
            .validator_set
            .get_by_address(&first)
            .expect("first validator")
            .voting_power,
        2
    );
    assert_eq!(
        params
            .validator_set
            .get_by_address(&second)
            .expect("second validator")
            .voting_power,
        5
    );
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::Genesis {
                genesis: Genesis {
                    validators: vec![(second, 1)]
                },
            })
            .await,
        Err(CoordinationError::NotImplemented)
    ));
    assert!(
        matches!(actor.handle_message(CoordinationRequestPayload::GetChain).await,
        Ok(CoordinationResponse::Chain { record }) if record == chain)
    );
}

#[tokio::test]
async fn genesis_admission_and_decision_survive_reconstruction() {
    use super::super::transaction::PendingTransaction;

    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    let owner = signer(22);
    let key = PublicKey::from(owner.public_key());
    let chain = match actor
        .handle_message(CoordinationRequestPayload::Genesis {
            genesis: Genesis {
                validators: vec![(key, 3)],
            },
        })
        .await
        .expect("create genesis")
    {
        CoordinationResponse::Chain { record } => record,
        other => panic!("unexpected genesis response: {other:?}"),
    };
    let record = signed(registration(23, key), &owner, chain.domain);
    let mut invalid_signature = record.clone();
    invalid_signature.sig[0] ^= 1;
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::RegisterUser {
                record: invalid_signature
            })
            .await,
        Err(CoordinationError::SignatureInvalid)
    ));
    PendingTransaction::UserRegistered(signed(registration(22, key), &owner, chain.domain))
        .apply(&mut actor.abci.state.committed, Height(0))
        .expect("commit successor authority");
    actor
        .abci
        .save_state(&actor.abci.state)
        .await
        .expect("persist authority");
    let snapshot = actor
        .abci
        .export_snapshot(None)
        .expect("export predecessor snapshot");
    let invalid_roster = signed(
        Regenesis {
            predecessor: chain.domain,
            successor: DomainId::from_bytes([24; 32]),
            predecessor_height: snapshot.height,
            snapshot_hash: snapshot.hash,
            validators: ValidatorSet(vec![(key, 0)]),
        },
        &owner,
        chain.domain,
    );
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::Regenesis { record: invalid_roster })
            .await,
        Err(CoordinationError::ZeroVotingPower)
    ));
    assert!(actor.abci.pending_transactions().is_empty());
    let pending_id = match actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record: record.clone() })
        .await
        .expect("admit user")
    {
        CoordinationResponse::TransactionSubmitted { event_id } => event_id,
        other => panic!("unexpected admission response: {other:?}"),
    };
    drop(actor);
    let mut actor = new_actor(dir.path()).await;
    assert!(
        matches!(actor.handle_message(CoordinationRequestPayload::GetChain).await,
        Ok(CoordinationResponse::Chain { record }) if record == chain)
    );
    let pending = actor.abci.pending_transactions();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, pending_id);
    assert!(matches!(&pending[0].1, PendingTransaction::UserRegistered(recovered) if recovered == &record));
    let user = record.body.user;
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::GetUser { user })
            .await,
        Ok(CoordinationResponse::User { record: None })
    ));
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let (reply, ready) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(AppMsg::ConsensusReady { reply }, &network)
        .await
        .expect("read recovered roster");
    let (height, params) = ready.await.expect("ready response");
    assert_eq!(height, Height(1));
    assert_eq!(
        params
            .validator_set
            .get_by_address(&key)
            .expect("recovered validator")
            .voting_power,
        3
    );
    let (reply, proposed) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(
            AppMsg::GetValue {
                height: Height(1),
                round: Round::new(0),
                timeout: std::time::Duration::ZERO,
                reply,
            },
            &network,
        )
        .await
        .expect("propose recovered admission");
    let value = proposed.await.expect("proposed value").value;
    let certificate = certificate(22, chain.domain, &value).await;
    let (reply, decided) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(
            AppMsg::Decided {
                certificate: certificate.clone(),
                extensions: VoteExtensions::default(),
                reply,
            },
            &network,
        )
        .await
        .expect("decide recovered admission");
    decided.await.expect("decided acknowledgement");
    drop(actor);
    let mut actor = new_actor(dir.path()).await;
    assert!(
        matches!(actor.handle_message(CoordinationRequestPayload::GetChain).await,
        Ok(CoordinationResponse::Chain { record }) if record.domain == chain.domain && record.height == Height(1))
    );
    assert!(
        matches!(actor.handle_message(CoordinationRequestPayload::GetUser { user }).await,
        Ok(CoordinationResponse::User { record: Some(committed) }) if committed.registered == record.body)
    );
    assert!(actor.abci.pending_transactions().is_empty());
    let (reply, ready) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(AppMsg::ConsensusReady { reply }, &network)
        .await
        .expect("read roster after decision");
    let (height, params) = ready.await.expect("ready response");
    assert_eq!(height, Height(2));
    assert_eq!(
        params
            .validator_set
            .get_by_address(&key)
            .expect("recovered validator")
            .voting_power,
        3
    );
    let (reply, history) = tokio::sync::oneshot::channel();
    let range = Height(1)..=Height(1);
    actor
        .abci
        .handle_app_msg(AppMsg::GetDecidedValues { range, reply }, &network)
        .await
        .expect("read recovered decided value");
    let history = history.await.expect("decided history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].certificate.value_id, certificate.value_id);
    assert_eq!(
        history[0].value_bytes.as_ref(),
        serde_json::to_vec(&value).expect("encode value").as_slice()
    );
}

#[tokio::test]
async fn chain_operations_fail_closed_until_the_host_activates_its_domain() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    let owner = signer(1);
    let record = signed(registration(2, PublicKey::from(owner.public_key())), &owner, DOMAIN);

    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::RegisterUser { record })
            .await,
        Err(CoordinationError::NotImplemented)
    ));
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::GetChain)
            .await,
        Err(CoordinationError::NotImplemented)
    ));

    actor
        .abci
        .activate_chain(DOMAIN)
        .expect("activate real domain");
    assert!(matches!(
        actor.handle_message(CoordinationRequestPayload::GetChain).await,
        Ok(CoordinationResponse::Chain { record }) if record.domain == DOMAIN && record.height == Height(0)
    ));
}

#[tokio::test]
async fn admission_returns_only_pending_and_becomes_visible_after_commit() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    actor.abci.activate_chain(DOMAIN).expect("activate domain");
    let owner = signer(3);
    let user = UserId([4; 32]);
    let signed_record = signed(registration(4, PublicKey::from(owner.public_key())), &owner, DOMAIN);

    let response = actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record: signed_record })
        .await
        .expect("admit signed registration");
    let event_id = match response {
        CoordinationResponse::TransactionSubmitted { event_id } => event_id,
        other => panic!("unexpected admission response: {other:?}"),
    };
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::GetUser { user })
            .await,
        Ok(CoordinationResponse::User { record: None })
    ));

    actor
        .abci
        .apply_committed(event_id, Height(1))
        .expect("apply decided record");
    assert!(matches!(
        actor.handle_message(CoordinationRequestPayload::GetUser { user }).await,
        Ok(CoordinationResponse::User { record: Some(record) })
            if record.user == user && record.registered.enc_epoch == 1
    ));
    assert_eq!(actor.abci.get_chain().expect("chain").height, Height(1));
}

#[tokio::test]
async fn invalid_signatures_and_unauthorized_registrations_are_rejected() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    actor.abci.activate_chain(DOMAIN).expect("activate domain");
    let owner = signer(5);
    let other = signer(6);
    let body = registration(7, PublicKey::from(owner.public_key()));

    let mut wrong_signature = signed(body.clone(), &owner, DOMAIN);
    wrong_signature.sig = other.sign(b"not the registered body").0;
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::RegisterUser {
                record: wrong_signature
            })
            .await,
        Err(CoordinationError::SignatureInvalid)
    ));

    let unauthorized = signed(body, &other, DOMAIN);
    let response = actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record: unauthorized })
        .await
        .expect("signature proves key ownership, commit enforces root authorization");
    let event_id = match response {
        CoordinationResponse::TransactionSubmitted { event_id } => event_id,
        other => panic!("unexpected admission response: {other:?}"),
    };
    assert!(matches!(
        actor.abci.apply_committed(event_id, Height(1)),
        Err(CoordinationError::SignatureInvalid)
    ));
    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::GetUser { user: UserId([7; 32]) })
            .await,
        Ok(CoordinationResponse::User { record: None })
    ));
}

#[tokio::test]
async fn duplicate_admission_is_idempotent() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    actor.abci.activate_chain(DOMAIN).expect("activate domain");
    let owner = signer(8);
    let record = signed(registration(9, PublicKey::from(owner.public_key())), &owner, DOMAIN);
    let first = actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record: record.clone() })
        .await
        .expect("admit");
    let second = actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record })
        .await
        .expect("idempotently admit duplicate");
    let event_id = |response| match response {
        CoordinationResponse::TransactionSubmitted { event_id } => event_id,
        other => panic!("unexpected admission response: {other:?}"),
    };
    assert_eq!(event_id(first), event_id(second));
    assert_eq!(actor.abci.pending_transactions().len(), 1);

    assert!(matches!(
        actor
            .handle_message(CoordinationRequestPayload::StopServer)
            .await,
        Ok(CoordinationResponse::ServerStopped)
    ));
    assert!(actor.running.is_none());
}

#[tokio::test]
#[should_panic(expected = "Machine domain must publish MachinePeerService before Coordination starts")]
async fn starting_without_machine_peer_services_panics() {
    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    let _ = actor
        .handle_message(CoordinationRequestPayload::StartServer)
        .await;
}

#[tokio::test]
async fn app_channel_callbacks_propose_decide_and_finalize_signed_records() {
    use super::super::transaction::PendingTransaction;

    let dir = tempfile::tempdir().expect("temporary configuration directory");
    let mut actor = new_actor(dir.path()).await;
    let (network, _receiver) = tokio::sync::mpsc::channel(4);
    let owner = signer(10);
    let domain = match actor
        .handle_message(CoordinationRequestPayload::Genesis {
            genesis: Genesis {
                validators: vec![(PublicKey::from(owner.public_key()), 1)],
            },
        })
        .await
        .expect("create genesis")
    {
        CoordinationResponse::Chain { record } => record.domain,
        other => panic!("unexpected genesis response: {other:?}"),
    };
    let key = PublicKey::from(owner.public_key());
    PendingTransaction::UserRegistered(signed(registration(10, key), &owner, domain))
        .apply(&mut actor.abci.state.committed, Height(0))
        .expect("commit successor authority");
    actor
        .abci
        .save_state(&actor.abci.state)
        .await
        .expect("persist authority");
    let snapshot = actor
        .abci
        .export_snapshot(None)
        .expect("export predecessor snapshot");
    let successor = DomainId::from_bytes([12; 32]);
    let genesis = signed(
        Regenesis {
            predecessor: domain,
            successor,
            predecessor_height: snapshot.height,
            snapshot_hash: snapshot.hash,
            validators: ValidatorSet(vec![(key, 1)]),
        },
        &owner,
        domain,
    );
    actor
        .handle_message(CoordinationRequestPayload::Regenesis { record: genesis })
        .await
        .expect("admit successor-chain roster");
    let domain = successor;
    let user = UserId([11; 32]);
    let user_record = signed(registration(11, key), &owner, domain);
    actor
        .handle_message(CoordinationRequestPayload::RegisterUser { record: user_record })
        .await
        .expect("admit user registration");

    let (reply, proposed_value) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(
            AppMsg::GetValue {
                height: Height(1),
                round: Round::new(0),
                timeout: std::time::Duration::ZERO,
                reply,
            },
            &network,
        )
        .await
        .expect("handle GetValue");
    let local_value = proposed_value.await.expect("locally proposed value");
    let proposal = ConsensusProposal {
        height: Height(1),
        round: 0,
        value: local_value.value.clone(),
        pol_round: -1,
        proposer: PublicKey::from(owner.public_key()),
    };
    let peer = peer_id(&owner);
    let accepted = receive_proposal_parts(&mut actor, &proposal, StreamId::new(vec![1, 2, 3].into()), peer)
        .await
        .expect("complete proposal accepted");
    assert_eq!(accepted.validity, Validity::Valid);
    assert_eq!(accepted.value, local_value.value);

    let invalid_signed_body = signed(
        registration(14, PublicKey::from(owner.public_key())),
        &owner,
        DomainId::from_bytes([15; 32]),
    );
    let invalid_transactions = serde_json::to_string(&[PendingTransaction::UserRegistered(invalid_signed_body)])
        .expect("encode typed but invalid transaction");
    let invalid_value = ConsensusValue(invalid_transactions);
    let invalid_proposal = ConsensusProposal {
        height: Height(1),
        round: 0,
        value: invalid_value,
        pol_round: -1,
        proposer: PublicKey::from(owner.public_key()),
    };
    let rejected = receive_proposal_parts(&mut actor, &invalid_proposal, StreamId::new(vec![4, 5, 6].into()), peer)
        .await
        .expect("malformed signed value receives a verdict");
    assert_eq!(rejected.validity, Validity::Invalid);

    let certificate = certificate(10, domain, &accepted.value).await;
    let (reply, decided) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(
            AppMsg::Decided {
                certificate: certificate.clone(),
                extensions: VoteExtensions::default(),
                reply,
            },
            &network,
        )
        .await
        .expect("handle Decided");
    decided.await.expect("decided acknowledgement");
    assert!(matches!(
        actor.handle_message(CoordinationRequestPayload::GetUser { user }).await,
        Ok(CoordinationResponse::User { record: Some(record) }) if record.user == user
    ));

    drop(actor);
    let mut actor = new_actor(dir.path()).await;
    assert!(matches!(
        actor.handle_message(CoordinationRequestPayload::GetUser { user }).await,
        Ok(CoordinationResponse::User { record: Some(record) }) if record.user == user
    ));
    assert_eq!(actor.abci.get_chain().expect("recovered chain").height, Height(1));
    let (reply, history) = tokio::sync::oneshot::channel();
    let range = Height(1)..=Height(1);
    actor
        .abci
        .handle_app_msg(AppMsg::GetDecidedValues { range, reply }, &network)
        .await
        .expect("read recovered history");
    let history = history.await.expect("history response");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].certificate.value_id, certificate.value_id);

    let (reply, _finalized) = tokio::sync::oneshot::channel();
    assert!(matches!(
        actor
            .abci
            .handle_app_msg(
                AppMsg::Finalized {
                    certificate: CommitCertificate {
                        height: Height(2),
                        ..certificate.clone()
                    },
                    extensions: VoteExtensions::default(),
                    evidence: MisbehaviorEvidence {
                        proposals: Default::default(),
                        votes: Default::default(),
                    },
                    reply,
                },
                &network
            )
            .await,
        Err(CoordinationError::NotImplemented)
    ));

    let (reply, finalized) = tokio::sync::oneshot::channel();
    actor
        .abci
        .handle_app_msg(
            AppMsg::Finalized {
                certificate,
                extensions: VoteExtensions::default(),
                evidence: MisbehaviorEvidence {
                    proposals: Default::default(),
                    votes: Default::default(),
                },
                reply,
            },
            &network,
        )
        .await
        .expect("handle Finalized");
    assert!(matches!(
        finalized.await.expect("finalized next-height response"),
        Next::Start(Height(2), _)
    ));
}

//! Malachite transport over the shared iroh Gossip substrate.

use std::collections::HashMap;

use arc_malachitebft_app_channel::NetworkMsg as ApplicationNetworkMsg;
use arc_malachitebft_core_consensus::SignedConsensusMsg;
use arc_malachitebft_engine::{
    network::{Msg as MalachiteNetworkMsg, NetworkEvent, PersistentPeerError, Subscriber},
    util::streaming::{StreamContent, StreamId, StreamMessage},
};
use arc_malachitebft_network::{Channel, Multiaddr, PeerId};
use arc_malachitebft_sync::{InboundRequestId, OutboundFailureReason, OutboundRequestId};
use futures_util::StreamExt;
use iroh_gossip::{
    api::{ApiError as GossipApiError, Event as GossipEvent, GossipTopic},
    proto::state::TopicId,
};
use ractor::{Actor as RactorActor, ActorProcessingErr, ActorRef};
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

use crate::auth::AuthAdmissionRegistry;
use crate::machine::MachinePeerService;

use super::serialization::{
    ChainPayloadError, ChainWireMessage, DirectedMessage, NodeKey, decode_network_event, encode_network_message,
    malachite_peer_id,
};
use super::transaction::PendingTransaction;
use super::types::{ConsensusProposalPart, CoordinationContext as ConsensusContext, DomainId};

pub(super) enum ApplicationEvent {
    NeighborUp(iroh::EndpointId),
    Transaction(iroh::EndpointId, Box<PendingTransaction>),
}

/// Encodes the application envelope and checks the iroh 0.101 neighbor frame size.
pub(super) fn encode_transaction(
    transaction: &PendingTransaction,
    max_message_size: usize,
) -> Result<Vec<u8>, ChainNetworkError> {
    let payload =
        serde_json::to_vec(&ChainWireMessage::Transaction(transaction.clone())).map_err(ChainPayloadError::from)?;
    // The 0.101 stream header carries the topic; the frame holds tags, hash, length and scope.
    let mut length = payload.len();
    let mut varint_bytes = 1usize;
    while length >= 128 {
        length >>= 7;
        varint_bytes += 1;
    }
    let frame_size = payload.len().saturating_add(35 + varint_bytes);
    if frame_size >= max_message_size {
        return Err(ChainNetworkError::MessageTooLarge);
    }
    Ok(payload)
}

struct NetworkMailbox;

#[async_trait::async_trait]
impl RactorActor for NetworkMailbox {
    type Msg = MalachiteNetworkMsg<ConsensusContext>;
    type State = mpsc::Sender<Self::Msg>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        sender: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(sender)
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        sender: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        sender
            .send(message)
            .await
            .map_err(|_| ChainNetworkError::CommandQueueClosed.into())
    }
}

/// Direct transport seam over the Gossip instance owned by Machine.
pub(crate) struct CoordinationNetwork {
    topic: GossipTopic,
    endpoint: iroh::Endpoint,
    admission: AuthAdmissionRegistry,
    subscribers: Vec<Box<dyn Subscriber<NetworkEvent<ConsensusContext>>>>,
    local: NodeKey,
    max_message_size: usize,
    outbound: HashMap<OutboundRequestId, PeerId>,
    inbound: HashMap<InboundRequestId, (PeerId, String)>,
}

impl CoordinationNetwork {
    /// Joins the caller-supplied chain topic using only currently admitted peer identities.
    pub(crate) async fn join(
        services: &dyn MachinePeerService,
        admission: AuthAdmissionRegistry,
        topic_id: TopicId,
    ) -> Result<Self, ChainNetworkError> {
        let (endpoint, gossip) = services.transport().await;
        let gossip = gossip.ok_or(ChainNetworkError::GossipUnavailable)?;
        let peers = admission
            .current_issuers()
            .into_iter()
            .map(|issuer| issuer.issuer.public_key())
            .filter(|peer| *peer != endpoint.id())
            .collect();

        let local = *endpoint.id().as_bytes();
        let max_message_size = gossip.max_message_size();
        let topic = gossip.subscribe(topic_id, peers).await?;
        Ok(Self {
            topic,
            endpoint,
            admission,
            subscribers: Vec::new(),
            local,
            max_message_size,
            outbound: HashMap::new(),
            inbound: HashMap::new(),
        })
    }

    /// Handles engine input and delivers supported publications over shared Gossip.
    pub(crate) async fn handle_engine_message(
        &mut self,
        message: MalachiteNetworkMsg<ConsensusContext>,
    ) -> Result<(), ChainNetworkError> {
        match message {
            MalachiteNetworkMsg::Subscribe(subscriber) => {
                if !self.endpoint.is_closed() {
                    for socket in self.endpoint.bound_sockets() {
                        // Multiaddr cannot encode an IPv6 socket's scope identifier.
                        if let std::net::SocketAddr::V6(address) = socket
                            && address.scope_id() != 0
                        {
                            continue;
                        }
                        let family = if socket.is_ipv4() { "ip4" } else { "ip6" };
                        let address: Multiaddr = format!("/{family}/{}/udp/{}/quic-v1", socket.ip(), socket.port())
                            .parse()
                            .expect("bound IP socket must be a valid multiaddress");
                        subscriber.send(NetworkEvent::Listening(address));
                    }
                }
                for peer in self.topic.neighbors() {
                    if self.require_admitted_peer(peer).is_ok() {
                        let peer = malachite_peer_id(*peer.as_bytes())?;
                        subscriber.send(NetworkEvent::PeerConnected(peer));
                        subscriber.send(NetworkEvent::PeerSubscribed(peer, Channel::Sync));
                    }
                }
                self.subscribers.push(subscriber);
                Ok(())
            }
            message @ MalachiteNetworkMsg::PublishConsensusMsg(SignedConsensusMsg::Proposal(_)) => {
                let payload = encode_network_message(&message)?;
                self.publish(payload).await?;
                let MalachiteNetworkMsg::PublishConsensusMsg(SignedConsensusMsg::Proposal(proposal)) = message else {
                    unreachable!()
                };
                let parts = [
                    StreamContent::Data(ConsensusProposalPart::First),
                    StreamContent::Data(ConsensusProposalPart::Data(
                        serde_json::to_vec(&proposal.message).map_err(ChainPayloadError::from)?,
                    )),
                    StreamContent::Data(ConsensusProposalPart::Last),
                    StreamContent::Fin,
                ];
                let stream_id = StreamId::new(uuid::Uuid::new_v4().as_bytes().to_vec().into());
                for (sequence, content) in parts.into_iter().enumerate() {
                    let payload = encode_network_message(&MalachiteNetworkMsg::PublishProposalPart(
                        StreamMessage::new(stream_id.clone(), sequence as u64, content),
                    ))?;
                    self.publish(payload).await?;
                }
                Ok(())
            }
            message @ (MalachiteNetworkMsg::PublishConsensusMsg(_)
            | MalachiteNetworkMsg::PublishLivenessMsg(_)
            | MalachiteNetworkMsg::PublishProposalPart(_)
            | MalachiteNetworkMsg::BroadcastStatus(_)) => {
                let payload = encode_network_message(&message)?;
                self.publish(payload).await
            }
            MalachiteNetworkMsg::OutgoingRequest(peer, request, reply) => {
                let id = OutboundRequestId::new(uuid::Uuid::new_v4());
                if reply.send(id.clone()).is_err() {
                    return Ok(());
                }
                let message = ChainWireMessage::SyncRequest {
                    request_id: id.to_string(),
                    request,
                };
                match self.send_to(peer, message).await {
                    Ok(()) => {
                        self.outbound.insert(id, peer);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "coordination sync request send failed");
                        self.publish_event(NetworkEvent::SyncRequestFailed(id, peer, OutboundFailureReason::Io));
                    }
                }
                Ok(())
            }
            MalachiteNetworkMsg::OutgoingResponse(id, response) => {
                if let Some((peer, remote_id)) = self.inbound.remove(&id)
                    && let Err(error) = self
                        .send_to(
                            peer,
                            ChainWireMessage::SyncResponse {
                                request_id: remote_id,
                                response: Some(response),
                            },
                        )
                        .await
                {
                    tracing::warn!(%error, "coordination sync response send failed");
                    self.publish_event(NetworkEvent::SyncInboundRequestFailed(id, peer));
                }
                Ok(())
            }
            MalachiteNetworkMsg::CancelRequest(id) => {
                self.outbound.remove(&id);
                Ok(())
            }
            MalachiteNetworkMsg::CancelInboundRequest(id) => {
                self.inbound.remove(&id);
                Ok(())
            }
            MalachiteNetworkMsg::DumpState(reply) => {
                let _ = reply.send(None);
                Ok(())
            }
            MalachiteNetworkMsg::UpdatePersistentPeers(_, reply) => {
                let _ = reply.send(Err(PersistentPeerError::InternalError(
                    "Coordination does not own peer membership".to_owned(),
                )));
                Ok(())
            }
            MalachiteNetworkMsg::UpdateValidatorSet(_) | MalachiteNetworkMsg::ValidatorProofVerified { .. } => Ok(()),
            MalachiteNetworkMsg::NewEvent(_) => Err(ChainPayloadError::UnsupportedMessage(
                "NewEvent belongs to Malachite's bundled libp2p transport",
            )
            .into()),
        }
    }

    fn publish_event(&self, event: NetworkEvent<ConsensusContext>) {
        for subscriber in &self.subscribers {
            subscriber.send(event.clone());
        }
    }

    async fn publish(&mut self, payload: Vec<u8>) -> Result<(), ChainNetworkError> {
        self.require_admitted_neighbors()?;
        if payload.len() >= self.max_message_size {
            return Err(ChainNetworkError::MessageTooLarge);
        }
        self.topic.broadcast_neighbors(payload.into()).await?;
        Ok(())
    }

    async fn send_to(&mut self, peer: PeerId, message: ChainWireMessage) -> Result<(), ChainNetworkError> {
        let recipient = self
            .topic
            .neighbors()
            .find(|key| malachite_peer_id(*key.as_bytes()).ok() == Some(peer))
            .ok_or(ChainNetworkError::PeerNotAdmitted)?;
        self.require_admitted_peer(recipient)?;
        let payload = serde_json::to_vec(&DirectedMessage {
            recipient: *recipient.as_bytes(),
            message,
        })
        .map_err(ChainPayloadError::from)?;
        if payload.len() >= self.max_message_size {
            return Err(ChainNetworkError::MessageTooLarge);
        }
        self.topic.broadcast_neighbors(payload.into()).await?;
        Ok(())
    }

    /// Returns the next event from an admitted direct iroh neighbor.
    async fn next_event(
        &mut self,
        application: &mpsc::Sender<ApplicationEvent>,
    ) -> Result<NetworkEvent<ConsensusContext>, ChainNetworkError> {
        loop {
            let event = self
                .topic
                .next()
                .await
                .ok_or(ChainNetworkError::TopicClosed)??;
            let event = match event {
                GossipEvent::NeighborUp(peer) => {
                    self.require_admitted_peer(peer)?;
                    application
                        .try_send(ApplicationEvent::NeighborUp(peer))
                        .map_err(|_| ChainNetworkError::ApplicationQueueUnavailable)?;
                    let peer = malachite_peer_id(*peer.as_bytes())?;
                    self.publish_event(NetworkEvent::PeerConnected(peer));
                    NetworkEvent::PeerSubscribed(peer, Channel::Sync)
                }
                GossipEvent::NeighborDown(peer) => {
                    let peer = malachite_peer_id(*peer.as_bytes())?;
                    self.outbound.retain(|_, owner| *owner != peer);
                    self.inbound.retain(|_, (owner, _)| *owner != peer);
                    NetworkEvent::PeerDisconnected(peer)
                }
                GossipEvent::Received(message) => {
                    if !message.scope.is_direct() {
                        return Err(ChainNetworkError::RelayedMessage);
                    }
                    self.require_admitted_peer(message.delivered_from)?;
                    let from = malachite_peer_id(*message.delivered_from.as_bytes())?;
                    let value: serde_json::Value =
                        serde_json::from_slice(&message.content).map_err(ChainPayloadError::from)?;
                    if value.get("kind").and_then(serde_json::Value::as_str) == Some("transaction") {
                        let wire: ChainWireMessage = serde_json::from_value(value).map_err(ChainPayloadError::from)?;
                        if let ChainWireMessage::Transaction(transaction) = wire {
                            application
                                .try_send(ApplicationEvent::Transaction(
                                    message.delivered_from,
                                    Box::new(transaction),
                                ))
                                .map_err(|_| ChainNetworkError::ApplicationQueueUnavailable)?;
                        }
                        continue;
                    }
                    if value.get("recipient").is_some() {
                        let directed: DirectedMessage =
                            serde_json::from_value(value).map_err(ChainPayloadError::from)?;
                        if directed.recipient != self.local {
                            continue;
                        }
                        match directed.message {
                            ChainWireMessage::SyncRequest { request_id, request } => {
                                let id = InboundRequestId::new(uuid::Uuid::new_v4());
                                self.inbound.insert(id.clone(), (from, request_id));
                                NetworkEvent::SyncRequest(id, from, request)
                            }
                            ChainWireMessage::SyncResponse { request_id, response } => {
                                let id = OutboundRequestId::new(request_id);
                                if self.outbound.get(&id) != Some(&from) {
                                    continue;
                                }
                                self.outbound.remove(&id);
                                NetworkEvent::SyncResponse(id, from, response)
                            }
                            _ => return Err(ChainNetworkError::PeerControlUnsupported),
                        }
                    } else {
                        let event = decode_network_event(*message.delivered_from.as_bytes(), &message.content)?;
                        if matches!(event, NetworkEvent::SyncRequest(..) | NetworkEvent::SyncResponse(..)) {
                            continue;
                        }
                        event
                    }
                }
                GossipEvent::Lagged => return Err(ChainNetworkError::TopicLagged),
            };
            self.publish_event(event.clone());
            return Ok(event);
        }
    }

    fn require_admitted_neighbors(&self) -> Result<(), ChainNetworkError> {
        for peer in self.topic.neighbors() {
            self.require_admitted_peer(peer)?;
        }
        Ok(())
    }

    fn require_admitted_peer(&self, peer: iroh::EndpointId) -> Result<(), ChainNetworkError> {
        if self
            .admission
            .current_issuers()
            .iter()
            .any(|issuer| issuer.issuer.public_key() == peer)
        {
            Ok(())
        } else {
            Err(ChainNetworkError::PeerNotAdmitted)
        }
    }
}

/// The engine's network actor and the topic task share this one owned lifetime.
pub(super) struct NetworkAttachment {
    pub(super) actor: arc_malachitebft_engine::network::NetworkRef<ConsensusContext>,
    pub(super) sender: mpsc::Sender<ApplicationNetworkMsg<ConsensusContext>>,
    pub(super) transactions: mpsc::Sender<Vec<u8>>,
    pub(super) events: mpsc::Receiver<ApplicationEvent>,
    pub(super) max_message_size: usize,
    actor_task: JoinHandle<()>,
    transport_task: JoinHandle<()>,
}

impl NetworkAttachment {
    /// Connects the Malachite actor contract to a topic on the shared iroh router.
    pub(super) async fn start(
        services: &dyn MachinePeerService,
        admission: AuthAdmissionRegistry,
        domain: DomainId,
    ) -> Result<Self, ChainNetworkError> {
        let mut network =
            CoordinationNetwork::join(services, admission, TopicId::from_bytes(*domain.as_bytes())).await?;
        let (commands, mut incoming) = mpsc::channel(128);
        let (sender, mut publications) = mpsc::channel::<ApplicationNetworkMsg<ConsensusContext>>(128);
        let (transactions, mut pending) = mpsc::channel::<Vec<u8>>(128);
        let (application, events) = mpsc::channel(128);
        let max_message_size = network.max_message_size;
        let (actor, actor_task) = RactorActor::spawn(None, NetworkMailbox, commands).await?;
        let owner = actor.clone();
        let transport_task = tokio::spawn(async move {
            let closed = network.endpoint.closed();
            tokio::pin!(closed);
            loop {
                let result = tokio::select! {
                    () = &mut closed => break,
                    command = incoming.recv() => match command {
                        Some(command) => network.handle_engine_message(command).await,
                        None => break,
                    },
                    publication = publications.recv() => match publication {
                        Some(publication) => network.handle_engine_message(publication.into()).await,
                        None => break,
                    },
                    transaction = pending.recv() => match transaction {
                        Some(payload) => {
                            match network.require_admitted_neighbors() {
                                Ok(()) => network.topic.broadcast_neighbors(payload.into()).await.map_err(Into::into),
                                Err(error) => Err(error),
                            }
                        }
                        None => break,
                    },
                    event = network.next_event(&application) => match event {
                        Err(ChainNetworkError::TopicClosed | ChainNetworkError::TopicLagged) => break,
                        result => result.map(|_| ()),
                    },
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "coordination network message refused");
                }
            }
            owner.stop(None);
        });
        Ok(Self {
            actor,
            sender,
            transactions,
            events,
            max_message_size,
            actor_task,
            transport_task,
        })
    }

    /// Detaches the chain topic and awaits its actor without stopping shared iroh.
    pub(super) async fn stop(self) -> Result<(), JoinError> {
        self.actor.stop(None);
        self.transport_task.abort();
        let transport = self.transport_task.await;
        let actor = self.actor_task.await;
        if let Err(error) = transport
            && !error.is_cancelled()
        {
            return Err(error);
        }
        actor
    }
}

/// Failure to use the shared Machine-owned iroh Gossip transport.
#[derive(Debug, thiserror::Error)]
pub enum ChainNetworkError {
    /// The engine command channel has closed.
    #[error("coordination network command queue is closed")]
    CommandQueueClosed,
    /// The actor event queue is full or closed; the event was not delivered.
    #[error("coordination application event queue is full or closed")]
    ApplicationQueueUnavailable,
    /// The Machine native-services instance has no live Gossip handle.
    #[error("the shared iroh Gossip handle is unavailable")]
    GossipUnavailable,
    /// iroh Gossip failed to join or exchange a message.
    #[error("iroh Gossip failed: {0}")]
    Gossip(#[from] GossipApiError),
    /// A direct neighbor is not currently admitted by Auth.
    #[error("an iroh Gossip neighbor is not currently admitted")]
    PeerNotAdmitted,
    /// The serialized packet exceeds the shared Gossip transport's message bound.
    #[error("coordination packet exceeds the iroh message size limit")]
    MessageTooLarge,
    /// The engine-facing network actor could not start.
    #[error("coordination network actor could not start: {0}")]
    Spawn(#[from] ractor::SpawnErr),
    /// Gossip delivered a message from a non-direct origin.
    #[error("relayed Gossip messages are not accepted as authenticated Malachite peers")]
    RelayedMessage,
    /// The subscribed topic ended unexpectedly.
    #[error("the iroh Gossip topic stream closed")]
    TopicClosed,
    /// The receiver lagged and missed network messages.
    #[error("the iroh Gossip receiver lagged and missed messages")]
    TopicLagged,
    /// Malachite asked Coordination to manage its persistent peer set.
    #[error("Coordination does not own Malachite persistent peer control")]
    PeerControlUnsupported,
    /// Malachite asked Coordination to manage validator-proof state.
    #[error("Coordination does not own Malachite validator-proof control")]
    ValidatorProofControlUnsupported,
    /// JSON encoding/decoding failed.
    #[error(transparent)]
    Payload(#[from] ChainPayloadError),
}

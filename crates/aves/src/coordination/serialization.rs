//! Serde support for the coordination domain's Malachite network messages.

use arc_malachitebft_codec::{Codec, HasEncodedLen};
use arc_malachitebft_core_consensus::{LivenessMsg, SignedConsensusMsg};
use arc_malachitebft_core_types::{
    Context, ExtendedCommitCertificate, ExtendedCommitSignature, NilOrVal, PolkaCertificate, PolkaSignature, Round,
    RoundCertificate, RoundCertificateType, RoundSignature, SignedExtension, SignedMessage, SignedProposal, SignedVote,
    ValidatorProof, Validity, VoteType,
};
use arc_malachitebft_engine::{
    consensus::state_dump::types::Round as EngineRound,
    host::ProposedValue,
    network::{Msg as MalachiteNetworkMsg, NetworkEvent, Status as NetworkStatus},
    util::streaming::{StreamContent, StreamId, StreamMessage},
};
use arc_malachitebft_network::PeerId;
use arc_malachitebft_sync::{
    InboundRequestId, OutboundRequestId, RawDecidedValue, Request, Response, Status as SyncStatus, ValueRequest,
    ValueResponse,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::signing::ChainSignature;
use super::transaction::PendingTransaction;
use super::types::{
    ConsensusProposal, ConsensusProposalPart, ConsensusValueId, ConsensusVote, CoordinationContext as ConsensusContext,
};

/// Key bytes of one peer: its ed25519 node key, which is also its iroh `NodeId`.
pub type NodeKey = [u8; 32];

/// JSON codec shared by the coordination WAL and Malachite's default sync actor.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CoordinationJsonCodec;

impl CoordinationJsonCodec {
    /// Creates the stateless JSON codec for the concrete coordination context.
    pub(crate) const fn new() -> Self {
        Self
    }
}

pub(super) fn encode_network_message(
    message: &MalachiteNetworkMsg<ConsensusContext>,
) -> Result<Vec<u8>, ChainPayloadError> {
    let message = match message {
        MalachiteNetworkMsg::PublishConsensusMsg(message) => {
            if matches!(message, SignedConsensusMsg::Vote(vote) if vote.message.extension.is_some()) {
                return Err(ChainPayloadError::VoteExtensionUnsupported);
            }
            ChainWireMessage::Consensus(message.clone())
        }
        MalachiteNetworkMsg::PublishLivenessMsg(message) => ChainWireMessage::Liveness(message.clone()),
        MalachiteNetworkMsg::PublishProposalPart(message) => ChainWireMessage::ProposalPart(message.clone()),
        MalachiteNetworkMsg::BroadcastStatus(status) => ChainWireMessage::Status(status.clone()),
        MalachiteNetworkMsg::OutgoingResponse(_, _) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "OutgoingResponse requires the transport's inbound-request peer correlation",
            ));
        }
        MalachiteNetworkMsg::Subscribe(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "Subscribe is a local subscriber control",
            ));
        }
        MalachiteNetworkMsg::OutgoingRequest(_, _, _) => {
            return Err(ChainPayloadError::RequestIdRequired);
        }
        MalachiteNetworkMsg::CancelRequest(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "CancelRequest requires the live request stream",
            ));
        }
        MalachiteNetworkMsg::CancelInboundRequest(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "CancelInboundRequest requires the live request stream",
            ));
        }
        MalachiteNetworkMsg::DumpState(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "DumpState is a local engine query",
            ));
        }
        MalachiteNetworkMsg::UpdatePersistentPeers(_, _) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "UpdatePersistentPeers is local network control",
            ));
        }
        MalachiteNetworkMsg::UpdateValidatorSet(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "UpdateValidatorSet is local engine control",
            ));
        }
        MalachiteNetworkMsg::ValidatorProofVerified { .. } => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "ValidatorProofVerified is a local verification result",
            ));
        }
        MalachiteNetworkMsg::NewEvent(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "NewEvent is a local transport event",
            ));
        }
    };
    serde_json::to_vec(&message).map_err(ChainPayloadError::from)
}

pub(super) fn decode_network_event(
    peer: NodeKey,
    payload: &[u8],
) -> Result<NetworkEvent<ConsensusContext>, ChainPayloadError> {
    let peer_id = malachite_peer_id(peer)?;
    let value: serde_json::Value = serde_json::from_slice(payload)?;
    if wire_vote_has_extension(&value) {
        return Err(ChainPayloadError::VoteExtensionUnsupported);
    }
    match value.get("kind").and_then(serde_json::Value::as_str) {
        Some(
            "consensus" | "liveness" | "proposal_part" | "validator_proof" | "status" | "sync_request"
            | "sync_response",
        ) => {}
        Some(kind) => return Err(ChainPayloadError::UnsupportedWireKind(kind.to_owned())),
        None => return Err(ChainPayloadError::UnsupportedWireKind("missing kind".to_owned())),
    }
    let message: ChainWireMessage = serde_json::from_value(value)?;
    Ok(match message {
        ChainWireMessage::Consensus(SignedConsensusMsg::Vote(vote)) => {
            if vote.message.extension.is_some() {
                return Err(ChainPayloadError::VoteExtensionUnsupported);
            }
            NetworkEvent::Vote(peer_id, vote)
        }
        ChainWireMessage::Consensus(SignedConsensusMsg::Proposal(proposal)) => {
            NetworkEvent::Proposal(peer_id, proposal)
        }
        ChainWireMessage::Liveness(LivenessMsg::Vote(vote)) => {
            if vote.message.extension.is_some() {
                return Err(ChainPayloadError::VoteExtensionUnsupported);
            }
            NetworkEvent::Vote(peer_id, vote)
        }
        ChainWireMessage::Liveness(LivenessMsg::PolkaCertificate(certificate)) => {
            NetworkEvent::PolkaCertificate(peer_id, certificate)
        }
        ChainWireMessage::Liveness(LivenessMsg::SkipRoundCertificate(certificate)) => {
            NetworkEvent::RoundCertificate(peer_id, certificate)
        }
        ChainWireMessage::ProposalPart(message) => NetworkEvent::ProposalPart(peer_id, Some(peer_id), message),
        ChainWireMessage::ValidatorProof(proof) => NetworkEvent::ValidatorProofReceived { peer_id, proof },
        ChainWireMessage::Status(status) => NetworkEvent::Status(peer_id, status),
        ChainWireMessage::SyncRequest {
            request_id, request, ..
        } => NetworkEvent::SyncRequest(InboundRequestId::new(request_id), peer_id, request),
        ChainWireMessage::SyncResponse {
            request_id, response, ..
        } => NetworkEvent::SyncResponse(OutboundRequestId::new(request_id), peer_id, response),
        ChainWireMessage::Transaction(_) => {
            return Err(ChainPayloadError::UnsupportedMessage(
                "transaction is application traffic",
            ));
        }
    })
}

pub(super) fn malachite_peer_id(peer: NodeKey) -> Result<PeerId, ChainPayloadError> {
    let mut bytes = Vec::with_capacity(38);
    bytes.extend_from_slice(&[0x00, 0x24, 0x08, 0x01, 0x12, 0x20]);
    bytes.extend_from_slice(&peer);
    PeerId::from_bytes(&bytes).map_err(|_| ChainPayloadError::PeerId)
}

/// Deserializes an authenticated peer payload into its Malachite network event.
pub(super) fn decode_chain_network_event(
    peer: NodeKey,
    payload: &[u8],
) -> Result<NetworkEvent<ConsensusContext>, ChainPayloadError> {
    decode_network_event(peer, payload)
}

/// Serializes a sync response with the inbound request identity it answers.
pub(super) fn serialize_sync_response(
    request_id: &InboundRequestId,
    response: &Response<ConsensusContext>,
) -> Result<Vec<u8>, ChainPayloadError> {
    serde_json::to_vec(&ChainWireMessage::SyncResponse {
        request_id: request_id.to_string(),
        response: Some(response.clone()),
    })
    .map_err(ChainPayloadError::from)
}

#[derive(Serialize, Deserialize)]
pub(super) struct DirectedMessage {
    pub(super) recipient: NodeKey,
    pub(super) message: ChainWireMessage,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSignature")]
struct ChainSignatureDef(#[serde(with = "serde_big_array::BigArray")] [u8; 64]);

#[derive(Serialize, Deserialize)]
#[serde(remote = "SignedMessage<ConsensusContext, ConsensusVote>")]
struct SignedVoteDef {
    message: ConsensusVote,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "SignedMessage<ConsensusContext, ConsensusProposal>")]
struct SignedProposalDef {
    message: ConsensusProposal,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "EngineRound")]
enum EngineRoundDef {
    Nil,
    Some(u32),
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Validity")]
enum ValidityDef {
    Valid,
    Invalid,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ProposedValue<ConsensusContext>")]
struct ProposedValueDef {
    height: <ConsensusContext as Context>::Height,
    #[serde(with = "EngineRoundDef")]
    round: EngineRound,
    #[serde(with = "EngineRoundDef")]
    valid_round: EngineRound,
    proposer: <ConsensusContext as Context>::Address,
    value: <ConsensusContext as Context>::Value,
    #[serde(with = "ValidityDef")]
    validity: Validity,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "PolkaSignature<ConsensusContext>")]
struct PolkaSignatureDef {
    address: <ConsensusContext as Context>::Address,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize)]
struct PolkaSignatureSerialized(#[serde(with = "PolkaSignatureDef")] PolkaSignature<ConsensusContext>);

#[derive(Deserialize)]
struct PolkaSignatureOwned(#[serde(with = "PolkaSignatureDef")] PolkaSignature<ConsensusContext>);

mod polka_signatures {
    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };

    use super::{ConsensusContext, PolkaSignature, PolkaSignatureOwned, PolkaSignatureSerialized};

    pub fn serialize<S>(values: &Vec<PolkaSignature<ConsensusContext>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            sequence.serialize_element(&PolkaSignatureSerialized(value.clone()))?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<PolkaSignature<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SignaturesVisitor;

        impl<'de> Visitor<'de> for SignaturesVisitor {
            type Value = Vec<PolkaSignature<ConsensusContext>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence of Malachite polka signatures")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut signatures = Vec::new();
                while let Some(signature) = sequence.next_element::<PolkaSignatureOwned>()? {
                    signatures.push(signature.0);
                }
                Ok(signatures)
            }
        }

        deserializer.deserialize_seq(SignaturesVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "PolkaCertificate<ConsensusContext>")]
struct PolkaCertificateDef {
    height: <ConsensusContext as Context>::Height,
    round: Round,
    value_id: ConsensusValueId,
    #[serde(with = "polka_signatures")]
    polka_signatures: Vec<PolkaSignature<ConsensusContext>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "RoundSignature<ConsensusContext>")]
struct RoundSignatureDef {
    vote_type: VoteType,
    value_id: NilOrVal<ConsensusValueId>,
    address: <ConsensusContext as Context>::Address,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize)]
struct RoundSignatureSerialized(#[serde(with = "RoundSignatureDef")] RoundSignature<ConsensusContext>);

#[derive(Deserialize)]
struct RoundSignatureOwned(#[serde(with = "RoundSignatureDef")] RoundSignature<ConsensusContext>);

mod round_signatures {
    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };

    use super::{ConsensusContext, RoundSignature, RoundSignatureOwned, RoundSignatureSerialized};

    pub fn serialize<S>(values: &Vec<RoundSignature<ConsensusContext>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            sequence.serialize_element(&RoundSignatureSerialized(value.clone()))?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<RoundSignature<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SignaturesVisitor;

        impl<'de> Visitor<'de> for SignaturesVisitor {
            type Value = Vec<RoundSignature<ConsensusContext>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence of Malachite round signatures")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut signatures = Vec::new();
                while let Some(signature) = sequence.next_element::<RoundSignatureOwned>()? {
                    signatures.push(signature.0);
                }
                Ok(signatures)
            }
        }

        deserializer.deserialize_seq(SignaturesVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "RoundCertificate<ConsensusContext>")]
struct RoundCertificateDef {
    height: <ConsensusContext as Context>::Height,
    round: Round,
    cert_type: RoundCertificateType,
    #[serde(with = "round_signatures")]
    round_signatures: Vec<RoundSignature<ConsensusContext>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "LivenessMsg<ConsensusContext>")]
enum LivenessMsgDef {
    Vote(#[serde(with = "SignedVoteDef")] SignedVote<ConsensusContext>),
    PolkaCertificate(#[serde(with = "PolkaCertificateDef")] PolkaCertificate<ConsensusContext>),
    SkipRoundCertificate(#[serde(with = "RoundCertificateDef")] RoundCertificate<ConsensusContext>),
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ValidatorProof<ConsensusContext>")]
struct ValidatorProofDef {
    public_key: Vec<u8>,
    peer_id: Vec<u8>,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "SignedConsensusMsg<ConsensusContext>")]
enum SignedConsensusMsgDef {
    Vote(#[serde(with = "SignedVoteDef")] SignedVote<ConsensusContext>),
    Proposal(#[serde(with = "SignedProposalDef")] SignedProposal<ConsensusContext>),
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "StreamId")]
struct StreamIdDef(#[serde(getter = "stream_id_bytes")] Vec<u8>);

fn stream_id_bytes(stream_id: &StreamId) -> Vec<u8> {
    stream_id.to_bytes().to_vec()
}

impl From<StreamIdDef> for StreamId {
    fn from(stream_id: StreamIdDef) -> Self {
        Self::new(stream_id.0.into())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "StreamContent<ConsensusProposalPart>")]
enum StreamContentDef {
    Data(ConsensusProposalPart),
    Fin,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "StreamMessage<ConsensusProposalPart>")]
struct StreamMessageDef {
    #[serde(with = "StreamIdDef")]
    stream_id: StreamId,
    sequence: u64,
    #[serde(with = "StreamContentDef")]
    content: StreamContent<ConsensusProposalPart>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "NetworkStatus<ConsensusContext>")]
struct NetworkStatusDef {
    tip_height: <ConsensusContext as Context>::Height,
    history_min_height: <ConsensusContext as Context>::Height,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "SyncStatus<ConsensusContext>")]
struct SyncStatusDef {
    #[serde(with = "peer_id")]
    peer_id: PeerId,
    tip_height: <ConsensusContext as Context>::Height,
    history_min_height: <ConsensusContext as Context>::Height,
}

mod peer_id {
    use arc_malachitebft_network::PeerId;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(peer: &PeerId, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&peer.to_base58())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PeerId, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        encoded.parse().map_err(D::Error::custom)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ValueRequest<ConsensusContext>")]
struct ValueRequestDef {
    range: std::ops::RangeInclusive<<ConsensusContext as Context>::Height>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Request<ConsensusContext>")]
enum RequestDef {
    ValueRequest(#[serde(with = "ValueRequestDef")] ValueRequest<ConsensusContext>),
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "SignedMessage<ConsensusContext, Vec<u8>>")]
struct SignedExtensionDef {
    message: Vec<u8>,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
}

#[derive(Serialize)]
struct SignedExtensionSerialized(#[serde(with = "SignedExtensionDef")] SignedExtension<ConsensusContext>);

#[derive(Deserialize)]
struct SignedExtensionOwned(#[serde(with = "SignedExtensionDef")] SignedExtension<ConsensusContext>);

mod optional_signed_extensions {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{ConsensusContext, SignedExtension, SignedExtensionOwned, SignedExtensionSerialized};

    pub fn serialize<S>(value: &Option<SignedExtension<ConsensusContext>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(extension) => serializer.serialize_some(&SignedExtensionSerialized(extension.clone())),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SignedExtension<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<SignedExtensionOwned>::deserialize(deserializer).map(|extension| extension.map(|value| value.0))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ExtendedCommitSignature<ConsensusContext>")]
struct ExtendedCommitSignatureDef {
    address: <ConsensusContext as Context>::Address,
    #[serde(with = "ChainSignatureDef")]
    signature: ChainSignature,
    #[serde(with = "optional_signed_extensions")]
    extension: Option<SignedExtension<ConsensusContext>>,
}

#[derive(Serialize)]
struct ExtendedCommitSignatureSerialized(
    #[serde(with = "ExtendedCommitSignatureDef")] ExtendedCommitSignature<ConsensusContext>,
);

#[derive(Deserialize)]
struct ExtendedCommitSignatureOwned(
    #[serde(with = "ExtendedCommitSignatureDef")] ExtendedCommitSignature<ConsensusContext>,
);

mod extended_commit_signatures {
    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };

    use super::{
        ConsensusContext, ExtendedCommitSignature, ExtendedCommitSignatureOwned, ExtendedCommitSignatureSerialized,
    };

    pub fn serialize<S>(
        values: &Vec<ExtendedCommitSignature<ConsensusContext>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            sequence.serialize_element(&ExtendedCommitSignatureSerialized(value.clone()))?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<ExtendedCommitSignature<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SignaturesVisitor;

        impl<'de> Visitor<'de> for SignaturesVisitor {
            type Value = Vec<ExtendedCommitSignature<ConsensusContext>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence of Malachite extended commit signatures")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut signatures = Vec::new();
                while let Some(signature) = sequence.next_element::<ExtendedCommitSignatureOwned>()? {
                    signatures.push(signature.0);
                }
                Ok(signatures)
            }
        }

        deserializer.deserialize_seq(SignaturesVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ExtendedCommitCertificate<ConsensusContext>")]
struct ExtendedCommitCertificateDef {
    height: <ConsensusContext as Context>::Height,
    round: Round,
    value_id: ConsensusValueId,
    #[serde(with = "extended_commit_signatures")]
    commit_signatures: Vec<ExtendedCommitSignature<ConsensusContext>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "RawDecidedValue<ConsensusContext>")]
struct RawDecidedValueDef {
    #[serde(getter = "raw_value_bytes")]
    value_bytes: Vec<u8>,
    #[serde(with = "ExtendedCommitCertificateDef")]
    certificate: ExtendedCommitCertificate<ConsensusContext>,
}

fn raw_value_bytes(value: &RawDecidedValue<ConsensusContext>) -> Vec<u8> {
    value.value_bytes.as_ref().to_vec()
}

impl From<RawDecidedValueDef> for RawDecidedValue<ConsensusContext> {
    fn from(value: RawDecidedValueDef) -> Self {
        Self::new(value.value_bytes.into(), value.certificate)
    }
}

#[derive(Serialize)]
struct RawDecidedValueSerialized(#[serde(with = "RawDecidedValueDef")] RawDecidedValue<ConsensusContext>);

#[derive(Deserialize)]
struct RawDecidedValueOwned(#[serde(with = "RawDecidedValueDef")] RawDecidedValue<ConsensusContext>);

mod raw_decided_values {
    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };

    use super::{ConsensusContext, RawDecidedValue, RawDecidedValueOwned, RawDecidedValueSerialized};

    pub fn serialize<S>(values: &Vec<RawDecidedValue<ConsensusContext>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            sequence.serialize_element(&RawDecidedValueSerialized(value.clone()))?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<RawDecidedValue<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValuesVisitor;

        impl<'de> Visitor<'de> for ValuesVisitor {
            type Value = Vec<RawDecidedValue<ConsensusContext>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence of Malachite decided values")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<RawDecidedValueOwned>()? {
                    values.push(value.0);
                }
                Ok(values)
            }
        }

        deserializer.deserialize_seq(ValuesVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ValueResponse<ConsensusContext>")]
struct ValueResponseDef {
    start_height: <ConsensusContext as Context>::Height,
    #[serde(with = "raw_decided_values")]
    values: Vec<RawDecidedValue<ConsensusContext>>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Response<ConsensusContext>")]
enum ResponseDef {
    ValueResponse(#[serde(with = "ValueResponseDef")] ValueResponse<ConsensusContext>),
}

#[derive(Serialize)]
struct ResponseSerialized(#[serde(with = "ResponseDef")] Response<ConsensusContext>);

#[derive(Deserialize)]
struct ResponseOwned(#[serde(with = "ResponseDef")] Response<ConsensusContext>);

mod optional_responses {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{ConsensusContext, Response, ResponseOwned, ResponseSerialized};

    pub fn serialize<S>(value: &Option<Response<ConsensusContext>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(response) => serializer.serialize_some(&ResponseSerialized(response.clone())),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Response<ConsensusContext>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<ResponseOwned>::deserialize(deserializer).map(|response| response.map(|value| value.0))
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub(super) enum ChainWireMessage {
    Transaction(PendingTransaction),
    Consensus(#[serde(with = "SignedConsensusMsgDef")] SignedConsensusMsg<ConsensusContext>),
    Liveness(#[serde(with = "LivenessMsgDef")] LivenessMsg<ConsensusContext>),
    ProposalPart(#[serde(with = "StreamMessageDef")] StreamMessage<ConsensusProposalPart>),
    ValidatorProof(#[serde(with = "ValidatorProofDef")] ValidatorProof<ConsensusContext>),
    Status(#[serde(with = "NetworkStatusDef")] NetworkStatus<ConsensusContext>),
    SyncRequest {
        request_id: String,
        #[serde(with = "RequestDef")]
        request: Request<ConsensusContext>,
    },
    SyncResponse {
        request_id: String,
        #[serde(with = "optional_responses")]
        response: Option<Response<ConsensusContext>>,
    },
}

#[derive(Deserialize)]
struct ConsensusOwned(#[serde(with = "SignedConsensusMsgDef")] SignedConsensusMsg<ConsensusContext>);

#[derive(Deserialize)]
struct LivenessOwned(#[serde(with = "LivenessMsgDef")] LivenessMsg<ConsensusContext>);

#[derive(Deserialize)]
struct ProposalPartOwned(#[serde(with = "StreamMessageDef")] StreamMessage<ConsensusProposalPart>);

#[derive(Deserialize)]
struct ValidatorProofOwned(#[serde(with = "ValidatorProofDef")] ValidatorProof<ConsensusContext>);

#[derive(Deserialize)]
struct NetworkStatusOwned(#[serde(with = "NetworkStatusDef")] NetworkStatus<ConsensusContext>);

// Serde's adjacently tagged enum deserializer needs wrappers for remote-derived newtypes.
#[derive(Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
enum ChainWireMessageOwned {
    Transaction(PendingTransaction),
    Consensus(ConsensusOwned),
    Liveness(LivenessOwned),
    ProposalPart(ProposalPartOwned),
    ValidatorProof(ValidatorProofOwned),
    Status(NetworkStatusOwned),
    SyncRequest {
        request_id: String,
        #[serde(with = "RequestDef")]
        request: Request<ConsensusContext>,
    },
    SyncResponse {
        request_id: String,
        #[serde(with = "optional_responses")]
        response: Option<Response<ConsensusContext>>,
    },
}

impl<'de> Deserialize<'de> for ChainWireMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match ChainWireMessageOwned::deserialize(deserializer)? {
            ChainWireMessageOwned::Transaction(value) => Self::Transaction(value),
            ChainWireMessageOwned::Consensus(value) => Self::Consensus(value.0),
            ChainWireMessageOwned::Liveness(value) => Self::Liveness(value.0),
            ChainWireMessageOwned::ProposalPart(value) => Self::ProposalPart(value.0),
            ChainWireMessageOwned::ValidatorProof(value) => Self::ValidatorProof(value.0),
            ChainWireMessageOwned::Status(value) => Self::Status(value.0),
            ChainWireMessageOwned::SyncRequest { request_id, request } => Self::SyncRequest { request_id, request },
            ChainWireMessageOwned::SyncResponse { request_id, response } => Self::SyncResponse { request_id, response },
        })
    }
}

impl Codec<SignedConsensusMsg<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<SignedConsensusMsg<ConsensusContext>, Self::Error> {
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if signed_vote_has_extension(&value) {
            return Err(ChainPayloadError::VoteExtensionUnsupported);
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let message = SignedConsensusMsgDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(message)
    }

    fn encode(&self, message: &SignedConsensusMsg<ConsensusContext>) -> Result<Bytes, Self::Error> {
        if matches!(message, SignedConsensusMsg::Vote(vote) if vote.message.extension.is_some()) {
            return Err(ChainPayloadError::VoteExtensionUnsupported);
        }
        encode_remote_json(|serializer| SignedConsensusMsgDef::serialize(message, serializer))
    }
}

impl Codec<ProposedValue<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<ProposedValue<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let value = ProposedValueDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(value)
    }

    fn encode(&self, value: &ProposedValue<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| ProposedValueDef::serialize(value, serializer))
    }
}

impl Codec<PolkaCertificate<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<PolkaCertificate<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let certificate = PolkaCertificateDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(certificate)
    }

    fn encode(&self, certificate: &PolkaCertificate<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| PolkaCertificateDef::serialize(certificate, serializer))
    }
}

impl Codec<NetworkStatus<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<NetworkStatus<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let status = NetworkStatusDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(status)
    }

    fn encode(&self, status: &NetworkStatus<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| NetworkStatusDef::serialize(status, serializer))
    }
}

impl Codec<SyncStatus<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<SyncStatus<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let status = SyncStatusDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(status)
    }

    fn encode(&self, status: &SyncStatus<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| SyncStatusDef::serialize(status, serializer))
    }
}

impl Codec<Request<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<Request<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = RequestDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(request)
    }

    fn encode(&self, request: &Request<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| RequestDef::serialize(request, serializer))
    }
}

impl Codec<Response<ConsensusContext>> for CoordinationJsonCodec {
    type Error = ChainPayloadError;

    fn decode(&self, bytes: Bytes) -> Result<Response<ConsensusContext>, Self::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let response = ResponseDef::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(response)
    }

    fn encode(&self, response: &Response<ConsensusContext>) -> Result<Bytes, Self::Error> {
        encode_remote_json(|serializer| ResponseDef::serialize(response, serializer))
    }
}

impl HasEncodedLen<Response<ConsensusContext>> for CoordinationJsonCodec {
    fn encoded_len(&self, response: &Response<ConsensusContext>) -> Result<usize, Self::Error> {
        <Self as Codec<Response<ConsensusContext>>>::encode(self, response).map(|bytes| bytes.len())
    }
}

fn signed_vote_has_extension(value: &serde_json::Value) -> bool {
    value
        .get("Vote")
        .and_then(|vote| vote.get("message"))
        .is_some_and(|message| message.get("extension").is_some())
}

fn wire_vote_has_extension(value: &serde_json::Value) -> bool {
    matches!(
        value.get("kind").and_then(serde_json::Value::as_str),
        Some("consensus" | "liveness")
    ) && value.get("message").is_some_and(signed_vote_has_extension)
}

fn encode_remote_json(
    serialize: impl FnOnce(&mut serde_json::Serializer<Vec<u8>>) -> Result<(), serde_json::Error>,
) -> Result<Bytes, ChainPayloadError> {
    let mut serializer = serde_json::Serializer::new(Vec::new());
    serialize(&mut serializer)?;
    Ok(Bytes::from(serializer.into_inner()))
}

/// JSON encoding/decoding errors for this incremental transport surface.
#[derive(Debug, thiserror::Error)]
pub enum ChainPayloadError {
    /// JSON or remote-derived message serialization failed.
    #[error("coordination JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Vote extensions are not supported by `ConsensusVote`'s serde contract.
    #[error("consensus vote extensions are not supported by the application JSON representation")]
    VoteExtensionUnsupported,
    /// The Malachite message variant is local control, not a wire publication.
    #[error("unsupported Malachite network message variant: {0}")]
    UnsupportedMessage(&'static str),
    /// Outgoing sync requests need the ID assigned by Malachite's transport actor.
    #[error("outgoing sync request must be serialized with its assigned Malachite request ID")]
    RequestIdRequired,
    /// The JSON envelope names an unknown wire message kind.
    #[error("unsupported coordination wire kind: {0}")]
    UnsupportedWireKind(String),
    /// The authenticated iroh identity cannot be represented as a Malachite PeerId.
    #[error("invalid authenticated peer identity for Malachite")]
    PeerId,
}

#[cfg(test)]
mod tests {
    use arc_malachitebft_core_types::{NilOrVal, SignedMessage};

    use super::*;
    use crate::coordination::types::{ConsensusVoteKind, Height, PublicKey};

    #[test]
    fn adjacent_wire_variants_round_trip_through_remote_adapters() {
        let vote = SignedMessage::new(
            ConsensusVote {
                height: Height(1),
                round: 0,
                value: NilOrVal::Nil,
                kind: ConsensusVoteKind::Prevote,
                address: PublicKey([1; 32]),
                extension: None,
            },
            ChainSignature([2; 64]),
        );
        let messages = [
            ChainWireMessage::Consensus(SignedConsensusMsg::Vote(vote.clone())),
            ChainWireMessage::Liveness(LivenessMsg::Vote(vote)),
            ChainWireMessage::ProposalPart(StreamMessage::new(
                StreamId::new(vec![1, 2].into()),
                0,
                StreamContent::Data(ConsensusProposalPart::First),
            )),
            ChainWireMessage::ValidatorProof(ValidatorProof {
                public_key: vec![1; 32],
                peer_id: vec![2; 38],
                signature: ChainSignature([3; 64]),
            }),
            ChainWireMessage::Status(NetworkStatus {
                tip_height: Height(5),
                history_min_height: Height(1),
            }),
        ];
        for message in messages {
            let expected = serde_json::to_value(message).unwrap();
            let decoded: ChainWireMessage = serde_json::from_value(expected.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
        }
    }

    #[test]
    fn sync_status_peer_id_uses_malachite_human_readable_form() {
        let peer = malachite_peer_id([4; 32]).unwrap();
        let status = SyncStatus::<ConsensusContext> {
            peer_id: peer,
            tip_height: Height(5),
            history_min_height: Height(1),
        };
        let codec = CoordinationJsonCodec::new();
        let encoded = codec.encode(&status).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["peer_id"], peer.to_base58());
        let decoded: SyncStatus<ConsensusContext> = codec.decode(encoded).unwrap();
        assert_eq!(decoded, status);
    }
}

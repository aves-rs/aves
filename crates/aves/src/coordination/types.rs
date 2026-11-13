use arc_malachitebft_app_channel::app::config::{ConsensusConfig, NodeConfig as MalachiteNodeConfig, ValueSyncConfig};
use arc_malachitebft_core_types::ValidatorSet as _;
use serde::{Deserialize, Serialize};
use specta::Type;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;

use crate::auth::AvesAuthError;
use crate::bus::DispatchContextError;
use crate::flow::{FlowExecutionId as AvesFlowExecutionId, FlowId as AvesFlowId};
use crate::runtime::{GeneratedCallError, ResponseProjectionError};
use crate::utils::database::DatabaseError;
use crate::utils::errors::SharedInfraError;

use super::signing::{ChainPublicKey, ChainSignature, ChainSigningScheme, KeyEncodingError};

/// Stable 32-byte identifier for one coordination chain.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Type, schemars::JsonSchema,
)]
pub struct DomainId(pub [u8; 32]);

impl DomainId {
    /// Wraps the chain identifier bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the chain identifier bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Committed block height on a coordination chain.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Type,
    schemars::JsonSchema,
)]
pub struct Height(pub u64);

impl arc_malachitebft_core_types::Height for Height {
    const ZERO: Self = Self(0);
    const INITIAL: Self = Self(1);

    fn increment_by(&self, n: u64) -> Self {
        Self(self.0 + n)
    }

    fn decrement_by(&self, n: u64) -> Option<Self> {
        self.0.checked_sub(n).map(Self)
    }

    fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Display for Height {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// An address and signing key share the same validated Ed25519 public-key bytes.
impl Display for PublicKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}

impl arc_malachitebft_core_types::Address for PublicKey {}

/// Malachite value identity, represented as the canonical JSON source text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConsensusValueId(pub String);

impl Display for ConsensusValueId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Consensus value payload retained as JSON text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ConsensusValue(pub String);

impl<'de> Deserialize<'de> for ConsensusValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let transactions_json = String::deserialize(deserializer)?;
        serde_json::from_str::<Vec<serde_json::Value>>(&transactions_json).map_err(serde::de::Error::custom)?;
        Ok(Self(transactions_json))
    }
}

impl arc_malachitebft_core_types::Value for ConsensusValue {
    type Id = ConsensusValueId;

    fn id(&self) -> Self::Id {
        ConsensusValueId(self.0.clone())
    }
}

/// A validator entry adapted to Malachite's typed validator contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusValidator {
    pub address: PublicKey,
    pub public_key: ChainPublicKey,
    pub voting_power: u64,
}

impl Serialize for ChainPublicKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ChainPublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let key = <[u8; 32]>::try_from(bytes.as_slice()).map_err(serde::de::Error::custom)?;
        ChainSigningScheme::decode_public_key(&key).map_err(serde::de::Error::custom)
    }
}

impl arc_malachitebft_core_types::Validator<CoordinationContext> for ConsensusValidator {
    fn address(&self) -> &PublicKey {
        &self.address
    }
    fn public_key(&self) -> &ChainPublicKey {
        &self.public_key
    }
    fn voting_power(&self) -> u64 {
        self.voting_power
    }
}

/// Validators in Malachite's deterministic power-descending, address-ascending order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConsensusValidatorSet(Vec<ConsensusValidator>);

impl ConsensusValidatorSet {
    /// Builds a validator set, retaining voting powers and applying Malachite's canonical order.
    pub fn try_new(mut validators: Vec<ConsensusValidator>) -> Result<Self, super::CoordinationError> {
        validators.sort_by(|left, right| {
            right
                .voting_power
                .cmp(&left.voting_power)
                .then_with(|| left.address.cmp(&right.address))
        });
        Self::validate(&validators)?;
        Ok(Self(validators))
    }

    fn validate(validators: &[ConsensusValidator]) -> Result<(), super::CoordinationError> {
        if validators.is_empty() {
            return Err(super::CoordinationError::InvalidValidatorSet);
        }

        let mut total_voting_power = 0_u64;
        let mut addresses = HashSet::with_capacity(validators.len());
        for (index, validator) in validators.iter().enumerate() {
            if validator.voting_power == 0 {
                return Err(super::CoordinationError::ZeroVotingPower);
            }
            if validator.address.0 != validator.public_key.0
                || !addresses.insert(validator.address)
                || ChainSigningScheme::decode_public_key(&validator.address.0).is_err()
            {
                return Err(super::CoordinationError::InvalidValidatorSet);
            }
            if index > 0 {
                let previous = &validators[index - 1];
                if previous.voting_power < validator.voting_power
                    || (previous.voting_power == validator.voting_power && previous.address >= validator.address)
                {
                    return Err(super::CoordinationError::InvalidValidatorSet);
                }
            }
            total_voting_power = total_voting_power
                .checked_add(validator.voting_power)
                .ok_or(super::CoordinationError::InvalidValidatorSet)?;
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ConsensusValidatorSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let validators = Vec::<ConsensusValidator>::deserialize(deserializer)?;
        Self::validate(&validators).map_err(serde::de::Error::custom)?;
        Ok(Self(validators))
    }
}

impl arc_malachitebft_core_types::ValidatorSet<CoordinationContext> for ConsensusValidatorSet {
    fn count(&self) -> usize {
        self.0.len()
    }

    fn total_voting_power(&self) -> u64 {
        self.0.iter().map(|validator| validator.voting_power).sum()
    }

    fn get_by_address(&self, address: &PublicKey) -> Option<&ConsensusValidator> {
        self.0
            .iter()
            .find(|validator| &validator.address == address)
    }

    fn get_by_index(&self, index: usize) -> Option<&ConsensusValidator> {
        self.0.get(index)
    }
}

/// A proposal carrying a JSON value and its proposer address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusProposal {
    pub height: Height,
    pub round: i64,
    pub value: ConsensusValue,
    pub pol_round: i64,
    pub proposer: PublicKey,
}

impl arc_malachitebft_core_types::Proposal<CoordinationContext> for ConsensusProposal {
    fn height(&self) -> Height {
        self.height
    }
    fn round(&self) -> arc_malachitebft_core_types::Round {
        self.round.into()
    }
    fn value(&self) -> &ConsensusValue {
        &self.value
    }
    fn take_value(self) -> ConsensusValue {
        self.value
    }
    fn pol_round(&self) -> arc_malachitebft_core_types::Round {
        self.pol_round.into()
    }
    fn validator_address(&self) -> &PublicKey {
        &self.proposer
    }
}

/// Proposal-stream markers; the payload itself is transported by Malachite codecs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusProposalPart {
    First,
    Data(Vec<u8>),
    Last,
}

impl arc_malachitebft_core_types::ProposalPart<CoordinationContext> for ConsensusProposalPart {
    fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }
    fn is_last(&self) -> bool {
        matches!(self, Self::Last)
    }
}

/// JSON-friendly vote kind mapped to Malachite's vote type at the trait boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusVoteKind {
    Prevote,
    Precommit,
}

/// A typed Malachite vote. Equality intentionally excludes its optional extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusVote {
    pub height: Height,
    pub round: i64,
    #[serde(with = "consensus_value_id_serde")]
    pub value: arc_malachitebft_core_types::NilOrVal<ConsensusValueId>,
    pub kind: ConsensusVoteKind,
    pub address: PublicKey,
    #[serde(skip)]
    pub extension: Option<arc_malachitebft_core_types::SignedExtension<CoordinationContext>>,
}

mod consensus_value_id_serde {
    use super::ConsensusValueId;
    use arc_malachitebft_core_types::NilOrVal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &NilOrVal<ConsensusValueId>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            NilOrVal::Nil => Option::<&ConsensusValueId>::None.serialize(serializer),
            NilOrVal::Val(value) => Some(value).serialize(serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NilOrVal<ConsensusValueId>, D::Error> {
        Ok(Option::<ConsensusValueId>::deserialize(deserializer)?.map_or(NilOrVal::Nil, NilOrVal::Val))
    }
}

impl PartialEq for ConsensusVote {
    fn eq(&self, other: &Self) -> bool {
        self.height == other.height
            && self.round == other.round
            && self.value == other.value
            && self.kind == other.kind
            && self.address == other.address
    }
}
impl Eq for ConsensusVote {}

impl arc_malachitebft_core_types::Vote<CoordinationContext> for ConsensusVote {
    fn height(&self) -> Height {
        self.height
    }
    fn round(&self) -> arc_malachitebft_core_types::Round {
        self.round.into()
    }
    fn value(&self) -> &arc_malachitebft_core_types::NilOrVal<ConsensusValueId> {
        &self.value
    }
    fn take_value(self) -> arc_malachitebft_core_types::NilOrVal<ConsensusValueId> {
        self.value
    }
    fn vote_type(&self) -> arc_malachitebft_core_types::VoteType {
        match self.kind {
            ConsensusVoteKind::Prevote => arc_malachitebft_core_types::VoteType::Prevote,
            ConsensusVoteKind::Precommit => arc_malachitebft_core_types::VoteType::Precommit,
        }
    }
    fn validator_address(&self) -> &PublicKey {
        &self.address
    }
    fn extension(&self) -> Option<&arc_malachitebft_core_types::SignedExtension<CoordinationContext>> {
        self.extension.as_ref()
    }
    fn take_extension(&mut self) -> Option<arc_malachitebft_core_types::SignedExtension<CoordinationContext>> {
        self.extension.take()
    }
    fn extend(mut self, extension: arc_malachitebft_core_types::SignedExtension<CoordinationContext>) -> Self {
        self.extension = Some(extension);
        self
    }
}

/// Stateless context binding the concrete coordination consensus types.
#[derive(Debug, Clone, Copy, Default)]
pub struct CoordinationContext;

impl arc_malachitebft_core_types::Context for CoordinationContext {
    type Address = PublicKey;
    type Height = Height;
    type ProposalPart = ConsensusProposalPart;
    type Proposal = ConsensusProposal;
    type Validator = ConsensusValidator;
    type ValidatorSet = ConsensusValidatorSet;
    type Timeouts = arc_malachitebft_core_types::LinearTimeouts;
    type Value = ConsensusValue;
    type Vote = ConsensusVote;
    type Extension = Vec<u8>;
    type SigningScheme = ChainSigningScheme;

    fn select_proposer<'a>(
        &self,
        validators: &'a Self::ValidatorSet,
        height: Height,
        round: arc_malachitebft_core_types::Round,
    ) -> &'a Self::Validator {
        assert!(validators.count() > 0, "validator set must not be empty");
        assert!(
            round != arc_malachitebft_core_types::Round::Nil,
            "round must not be nil"
        );
        let round = u128::from(round.as_u32().expect("round was asserted to be defined"));
        let count = validators.count() as u128;
        let index = ((u128::from(height.0) - 1 + round) % count) as usize;
        validators
            .get_by_index(index)
            .expect("proposer index is within the validator set")
    }

    fn new_proposal(
        &self,
        height: Height,
        round: arc_malachitebft_core_types::Round,
        value: ConsensusValue,
        pol_round: arc_malachitebft_core_types::Round,
        address: PublicKey,
    ) -> ConsensusProposal {
        ConsensusProposal {
            height,
            round: round.as_i64(),
            value,
            pol_round: pol_round.as_i64(),
            proposer: address,
        }
    }

    fn new_prevote(
        &self,
        height: Height,
        round: arc_malachitebft_core_types::Round,
        value: arc_malachitebft_core_types::NilOrVal<ConsensusValueId>,
        address: PublicKey,
    ) -> ConsensusVote {
        ConsensusVote {
            height,
            round: round.as_i64(),
            value,
            kind: ConsensusVoteKind::Prevote,
            address,
            extension: None,
        }
    }

    fn new_precommit(
        &self,
        height: Height,
        round: arc_malachitebft_core_types::Round,
        value: arc_malachitebft_core_types::NilOrVal<ConsensusValueId>,
        address: PublicKey,
    ) -> ConsensusVote {
        ConsensusVote {
            height,
            round: round.as_i64(),
            value,
            kind: ConsensusVoteKind::Precommit,
            address,
            extension: None,
        }
    }
}

/// Malachite node tuning and filesystem locations for one coordination engine.
pub(super) struct CoordinationNodeConfig {
    consensus: ConsensusConfig,
    value_sync: ValueSyncConfig,
}

impl CoordinationNodeConfig {
    /// Uses Malachite's consensus and value-sync settings without another transport.
    pub(super) fn new() -> Self {
        Self {
            consensus: ConsensusConfig::default(),
            value_sync: ValueSyncConfig::default(),
        }
    }
}

impl MalachiteNodeConfig for CoordinationNodeConfig {
    fn moniker(&self) -> &str {
        "aves-coordination"
    }
    fn consensus(&self) -> &ConsensusConfig {
        &self.consensus
    }
    fn consensus_mut(&mut self) -> &mut ConsensusConfig {
        &mut self.consensus
    }
    fn value_sync(&self) -> &ValueSyncConfig {
        &self.value_sync
    }
    fn value_sync_mut(&mut self) -> &mut ValueSyncConfig {
        &mut self.value_sync
    }
}

/// Implements Malachite's typed key conversion contract using the byte signer.
impl arc_malachitebft_core_types::SigningScheme for ChainSigningScheme {
    type DecodingError = KeyEncodingError;
    type Signature = ChainSignature;
    type PublicKey = ChainPublicKey;
    type PrivateKey = super::signing::ChainPrivateKey;

    fn decode_signature(bytes: &[u8]) -> Result<Self::Signature, Self::DecodingError> {
        ChainSigningScheme::decode_signature(bytes)
    }
    fn encode_signature(signature: &Self::Signature) -> Vec<u8> {
        ChainSigningScheme::encode_signature(signature)
    }
    fn decode_public_key(bytes: &[u8]) -> Result<Self::PublicKey, Self::DecodingError> {
        ChainSigningScheme::decode_public_key(bytes)
    }
    fn encode_public_key(key: &Self::PublicKey) -> Vec<u8> {
        ChainSigningScheme::encode_public_key(key)
    }
}

/// Current committed projection of one user identity on a coordination chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserRecord {
    /// Stable identity selected when the user registered.
    pub user: UserId,
    /// Initial registration event.
    pub registered: UserRegistered,
    /// Committed encryption-key rotations in chain order.
    pub encryption_rotations: Vec<UserEncRotated>,
    /// Committed root rotations in chain order.
    pub root_rotations: Vec<UserRootRotated>,
    /// Successor identities linked from this user, in chain order.
    pub successors: Vec<UserSuccessorLinked>,
    /// Revocation event, if the identity was revoked.
    pub revoked: Option<UserRevoked>,
}

/// Current committed projection of one machine identity on a coordination chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineRecord {
    /// Machine identity.
    pub machine: MachineId,
    /// Registration events in chain order.
    pub registrations: Vec<MachineRegistered>,
    /// Binding events in chain order.
    pub bindings: Vec<MachineBound>,
    /// Unbinding events in chain order.
    pub unbindings: Vec<MachineUnbound>,
}

/// One execution's creation and optional terminal event, all from committed records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ExecutionRecord {
    /// Execution attempt identity.
    pub execution_id: CoordinationExecutionId,
    /// Flow that was executed.
    pub flow_id: CoordinationFlowId,
    /// Machine that performed the execution.
    pub machine: MachineId,
    /// Landing event, when the execution landed.
    pub landed: Option<ExecutionLanded>,
    /// Cancellation event, when the execution was cancelled.
    pub cancelled: Option<ExecutionCancelled>,
}

/// Chain identity and committed tip returned by `get_chain`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ChainRecord {
    /// Chain domain identifier.
    pub domain: DomainId,
    /// Latest committed height.
    pub height: Height,
}

/// One retained decided value and its encoded Malachite certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct SnapshotDecided {
    /// Committed height of this value.
    pub height: Height,
    /// Serialized application value.
    pub value_bytes: Vec<u8>,
    /// Existing network-encoded certificate representation.
    pub certificate_json: String,
}

/// A caller-trusted committed projection and its complete decided history.
///
/// The hash is an integrity identifier, not a certificate over the projection. Callers
/// must obtain the expected hash through an independent trusted channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct CommittedSnapshot {
    /// Established chain identity.
    pub domain: DomainId,
    /// Committed tip.
    pub height: Height,
    /// Serialized application recovery projection, without transient state.
    pub application_json: String,
    /// Complete decided history, ordered from height one through the tip.
    pub decided: Vec<SnapshotDecided>,
    /// SHA-256 identity of the canonical committed projection and ordered height/value bytes.
    /// Certificate proof representation is excluded; proofs are verified separately.
    pub hash: RecordHash,
}

/// A record body together with the identity and Ed25519 signature of its signer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct SignedBody<T> {
    /// The signed record contents.
    pub body: T,
    /// Ed25519 public key bytes of the signer.
    pub signer: PublicKey,
    /// Ed25519 signature over the purpose, chain identifier, and canonical body.
    #[serde(with = "serde_big_array::BigArray")]
    #[schemars(with = "Vec<u8>", length(min = 64, max = 64))]
    pub sig: [u8; 64],
}

/// Purpose label used when signing and verifying an on-chain record body.
pub trait RegisteredTransaction {
    /// Stable, versioned signing purpose for this record type.
    const PURPOSE: &'static str;
}

/// Validator identities and voting power committed by a regenesis record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct ValidatorSet(pub Vec<(PublicKey, u64)>);

/// Initial validator roster for a new, empty coordination chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct Genesis {
    /// Caller-selected validator public keys and their voting powers.
    pub validators: Vec<(PublicKey, u64)>,
}

/// Stable identity returned when a signed transaction is admitted to a chain host.
///
/// The identifier names the exact signed bytes; it is not a chain-state projection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct CoordinationEventId([u8; 32]);

impl CoordinationEventId {
    /// Wraps the digest bytes returned by the chain host.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes used by the bus response.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Fixed-size identifier used by an application coordination record.
macro_rules! byte_id {
    ($name:ident, $length:literal) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            Type,
            schemars::JsonSchema
        )]
        pub struct $name(pub [u8; $length]);
    };
}

byte_id!(UserId, 32);
byte_id!(PublicKey, 32);
byte_id!(MachineId, 32);
byte_id!(EncKey, 32);
byte_id!(BlobHash, 32);
byte_id!(RecordHash, 32);

/// Typed Flow identity encoded as its canonical 16 UUID bytes in coordination bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct CoordinationFlowId(pub AvesFlowId);

/// Typed Flow execution identity encoded as its canonical 16 UUID bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct CoordinationExecutionId(pub AvesFlowExecutionId);

/// Root key registration, signed by the root key it registers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserRegistered {
    pub user: UserId,
    pub root_key: PublicKey,
    pub enc_key: EncKey,
    pub enc_epoch: u32,
}

impl RegisteredTransaction for UserRegistered {
    const PURPOSE: &'static str = "aves/coordination/record/user-registered/v1";
}

/// Encryption-key rotation signed by the user's root key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserEncRotated {
    pub user: UserId,
    pub enc_key: EncKey,
    pub enc_epoch: u32,
    pub previous_epoch: u32,
}

impl RegisteredTransaction for UserEncRotated {
    const PURPOSE: &'static str = "aves/coordination/record/user-enc-rotated/v1";
}

/// Same-user root rotation signed by the previous root key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserRootRotated {
    pub user: UserId,
    pub previous_root: PublicKey,
    pub new_root: PublicKey,
    pub effective_at: u64,
}

impl RegisteredTransaction for UserRootRotated {
    const PURPOSE: &'static str = "aves/coordination/record/user-root-rotated/v1";
}

/// Successor user linkage signed by the predecessor's root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserSuccessorLinked {
    pub predecessor: UserId,
    pub successor: UserId,
    pub predecessor_root: PublicKey,
    pub successor_root: PublicKey,
    pub epoch: u32,
}

impl RegisteredTransaction for UserSuccessorLinked {
    const PURPOSE: &'static str = "aves/coordination/record/user-successor-linked/v1";
}

/// User revocation signed by the root key being revoked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct UserRevoked {
    pub user: UserId,
    pub root_key: PublicKey,
    pub effective_at: u64,
}

impl RegisteredTransaction for UserRevoked {
    const PURPOSE: &'static str = "aves/coordination/record/user-revoked/v1";
}

/// Machine registration, signed by its own key to prove possession.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineRegistered {
    pub machine: MachineId,
    pub name: String,
    pub machine_key: PublicKey,
    pub sonya_key: PublicKey,
    pub enc_key: EncKey,
}

impl RegisteredTransaction for MachineRegistered {
    const PURPOSE: &'static str = "aves/coordination/record/machine-registered/v1";
}

/// Root certification of a registered machine's keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineBound {
    pub user: UserId,
    pub machine: MachineId,
    pub machine_key: PublicKey,
    pub sonya_key: PublicKey,
    pub enc_key: EncKey,
    pub enc_epoch: u32,
}

impl RegisteredTransaction for MachineBound {
    const PURPOSE: &'static str = "aves/coordination/record/machine-bound/v1";
}

/// Root removal of a machine's certificates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineUnbound {
    pub user: UserId,
    pub machine: MachineId,
    pub effective_at: u64,
    pub compromised_from: Option<u64>,
}

impl RegisteredTransaction for MachineUnbound {
    const PURPOSE: &'static str = "aves/coordination/record/machine-unbound/v1";
}

/// Creation of one Flow execution record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ExecutionCreated {
    pub execution_id: CoordinationExecutionId,
    pub flow_id: CoordinationFlowId,
    pub machine: MachineId,
}

impl RegisteredTransaction for ExecutionCreated {
    const PURPOSE: &'static str = "aves/coordination/record/execution-created/v1";
}

/// Acceptance of an execution's immutable encrypted record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ExecutionLanded {
    pub execution_id: CoordinationExecutionId,
    pub blob_uri: String,
    pub blob_hash: BlobHash,
    pub decoding_schema_version: u32,
}

impl RegisteredTransaction for ExecutionLanded {
    const PURPOSE: &'static str = "aves/coordination/record/execution-landed/v1";
}

/// Cancellation referencing the retained execution record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ExecutionCancelled {
    pub execution_id: CoordinationExecutionId,
    pub record_hash: RecordHash,
    pub discarded_at: u64,
    pub expires_at: u64,
}

impl RegisteredTransaction for ExecutionCancelled {
    const PURPOSE: &'static str = "aves/coordination/record/execution-cancelled/v1";
}

/// Successor chain created by a host snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct Regenesis {
    pub predecessor: DomainId,
    pub successor: DomainId,
    pub predecessor_height: Height,
    pub snapshot_hash: RecordHash,
    pub validators: ValidatorSet,
}

impl RegisteredTransaction for Regenesis {
    const PURPOSE: &'static str = "aves/coordination/record/regenesis/v1";
}

/// Project schema activation on the chain timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct SchemaActivated {
    pub machine: MachineId,
    pub schema_version: u32,
    pub platform_minimum: u32,
    pub active_from: Height,
    pub schema: String,
}

impl RegisteredTransaction for SchemaActivated {
    const PURPOSE: &'static str = "aves/coordination/record/schema-activated/v1";
}

/// Typed failures produced by coordination foundations.
#[derive(Debug, Error, Type)]
pub enum CoordinationError {
    /// The snapshot is corrupt, inconsistent, or does not match the trust anchor.
    #[error("invalid coordination snapshot")]
    InvalidSnapshot,
    /// An initialized host cannot be replaced by a different snapshot.
    #[error("coordination host is not empty")]
    SnapshotHostNotEmpty,
    /// Bus authentication rejected the request.
    #[error(transparent)]
    #[specta(skip)]
    Auth(#[from] AvesAuthError),
    /// The caller did not supply an available bus dispatch context.
    #[error(transparent)]
    #[specta(skip)]
    DispatchContext(#[from] DispatchContextError),
    /// Sending or receiving a bus request failed.
    #[error(transparent)]
    #[specta(skip)]
    Transport(#[from] SharedInfraError),
    /// A generated bus operation received the wrong response variant.
    #[error(transparent)]
    #[specta(skip)]
    ResponseProjection(#[from] ResponseProjectionError),
    /// Serializing or deserializing a bus request or response failed.
    #[error(transparent)]
    #[specta(skip)]
    Json(#[from] serde_json::Error),
    /// A validator set was empty, duplicated or inconsistent, unsorted on the wire, or overflowed its total power.
    #[error("invalid validator set")]
    InvalidValidatorSet,
    /// A validator entry had zero voting power.
    #[error("validator voting power must be positive")]
    ZeroVotingPower,
    /// A signature did not verify under the expected key, purpose and domain.
    #[error("signature verification failed")]
    SignatureInvalid,
    /// JSON serialization or deserialization of a typed body failed.
    #[error("JSON serialization failed")]
    Encoding,
    /// A frozen JSON payload was not valid JSON.
    #[error("frozen JSON payload is invalid")]
    InvalidJson,
    /// Coordination persistence failed; the underlying database error is retained as the source.
    #[error("coordination persistence failed: {0}")]
    #[specta(skip)]
    Storage(#[source] turso::Error),
    /// A shared Aves database lifecycle or migration operation failed.
    #[error("coordination database operation failed: {0}")]
    #[specta(skip)]
    Database(#[source] DatabaseError),
    /// A pending transaction index could not be represented by the database sequence type.
    #[error("pending transaction sequence exceeds the database range")]
    PendingSequenceOverflow,
    /// The native chain transport failed.
    #[error("coordination network failed: {0}")]
    #[specta(skip)]
    Network(#[from] super::network::ChainNetworkError),
    /// Malachite could not construct its engine.
    #[error("coordination engine failed: {0}")]
    #[specta(skip)]
    Engine(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// An owned engine or transport task failed while stopping.
    #[error("coordination task failed: {0}")]
    #[specta(skip)]
    Task(#[from] tokio::task::JoinError),
    /// The requested coordination operation is not implemented yet.
    #[error("coordination operation not implemented")]
    NotImplemented,
}

impl From<GeneratedCallError<CoordinationError>> for CoordinationError {
    fn from(error: GeneratedCallError<CoordinationError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => error.into(),
            GeneratedCallError::InputClosed => SharedInfraError::ChannelSend {
                target_domain: "coordination".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "bus input closed".to_owned(),
            }
            .into(),
            GeneratedCallError::ReplyDropped => SharedInfraError::ChannelRecv {
                target_domain: "coordination".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "reply dropped".to_owned(),
            }
            .into(),
            GeneratedCallError::UnexpectedResponse { expected, actual } => {
                ResponseProjectionError { expected, actual }.into()
            }
        }
    }
}

impl rspc::Error for CoordinationError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(self.to_string(), Some(self)))
    }
}

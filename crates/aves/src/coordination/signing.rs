//! Byte-oriented Ed25519 signing primitives for coordination.

use arc_malachitebft_core_types::{
    PublicKey as MalachitePublicKey, Signature as MalachiteSignature, SignedMessage, ValidatorProof, VoteExtensionScope,
};
use arc_malachitebft_signing::{Error as SigningError, Signer, VerificationResult, Verifier};
use async_trait::async_trait;
use iroh::{PublicKey, SecretKey, Signature};
use serde::Serialize;

use super::types::{
    ConsensusProposal, ConsensusVote, CoordinationContext, DomainId, PublicKey as CoordinationPublicKey,
};

pub type ChainPrivateKey = SecretKey;

type VoteScope = VoteExtensionScope<CoordinationContext>;
type VoteSignature = SignedMessage<CoordinationContext, ConsensusVote>;
type ProposalSignature = SignedMessage<CoordinationContext, ConsensusProposal>;
type Vote = ConsensusVote;
type Proposal = ConsensusProposal;
type ConsensusSig = MalachiteSignature<CoordinationContext>;
type ConsensusKey = MalachitePublicKey<CoordinationContext>;
type ConsensusProof = ValidatorProof<CoordinationContext>;
type ExtensionSig = SignedMessage<CoordinationContext, Vec<u8>>;
type VerifyResult = Result<VerificationResult, SigningError>;

const VOTE_PURPOSE: &[u8] = b"aves/coordination/consensus/vote/v1";
const PROPOSAL_PURPOSE: &[u8] = b"aves/coordination/consensus/proposal/v1";

/// Ed25519 public key bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainPublicKey(pub [u8; 32]);

impl TryFrom<CoordinationPublicKey> for ChainPublicKey {
    type Error = KeyEncodingError;

    fn try_from(public_key: CoordinationPublicKey) -> Result<Self, Self::Error> {
        ChainSigningScheme::decode_public_key(&public_key.0)
    }
}

impl From<ChainPublicKey> for CoordinationPublicKey {
    fn from(public_key: ChainPublicKey) -> Self {
        Self(public_key.0)
    }
}

/// Raw Ed25519 signature bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChainSignature(pub [u8; 64]);

/// Key or signature bytes have an invalid length or key encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid Ed25519 key or signature encoding")]
pub struct KeyEncodingError;

/// Ed25519 key and signature byte conversion helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainSigningScheme;

impl ChainSigningScheme {
    /// Decodes an exact-length Ed25519 signature.
    pub fn decode_signature(bytes: &[u8]) -> Result<ChainSignature, KeyEncodingError> {
        <[u8; 64]>::try_from(bytes)
            .map(ChainSignature)
            .map_err(|_| KeyEncodingError)
    }

    /// Encodes a signature as its 64 raw bytes.
    pub fn encode_signature(signature: &ChainSignature) -> Vec<u8> {
        signature.0.to_vec()
    }

    /// Decodes an exact-length, valid Ed25519 public key.
    pub fn decode_public_key(bytes: &[u8]) -> Result<ChainPublicKey, KeyEncodingError> {
        let key = <[u8; 32]>::try_from(bytes).map_err(|_| KeyEncodingError)?;
        PublicKey::from_bytes(&key).map_err(|_| KeyEncodingError)?;
        Ok(ChainPublicKey(key))
    }

    /// Encodes a public key as its 32 raw bytes.
    pub fn encode_public_key(public_key: &ChainPublicKey) -> Vec<u8> {
        public_key.0.to_vec()
    }
}

/// Signs caller-supplied bytes; Malachite adapters encode typed messages separately.
#[derive(Debug, Clone)]
pub struct ChainSigner {
    key: ChainPrivateKey,
}

impl ChainSigner {
    /// Creates a signer for the given private key.
    pub fn new(key: ChainPrivateKey) -> Self {
        Self { key }
    }

    /// Returns the signer's public key.
    pub fn public_key(&self) -> ChainPublicKey {
        ChainPublicKey(*self.key.public().as_bytes())
    }

    /// Signs the supplied bytes exactly as provided.
    pub fn sign(&self, message: &[u8]) -> ChainSignature {
        ChainSignature(self.key.sign(message).to_bytes())
    }
}

/// Verifies caller-supplied bytes without interpreting or encoding them.
#[derive(Debug, Clone, Copy)]
pub struct ChainVerifier;

impl ChainVerifier {
    /// Returns whether the signature is valid for the exact supplied message bytes.
    pub fn verify(&self, public_key: &ChainPublicKey, message: &[u8], signature: &ChainSignature) -> bool {
        PublicKey::from_bytes(&public_key.0).is_ok_and(|key| {
            key.verify(message, &Signature::from_bytes(&signature.0))
                .is_ok()
        })
    }
}

/// Malachite signer bound to one coordination chain.
#[derive(Debug, Clone)]
pub(super) struct CoordinationSigner {
    signer: ChainSigner,
    domain: DomainId,
}

impl CoordinationSigner {
    /// Creates a signer whose consensus preimages are bound to `domain`.
    pub(super) fn new(key: ChainPrivateKey, domain: DomainId) -> Self {
        Self {
            signer: ChainSigner::new(key),
            domain,
        }
    }
}

/// Malachite verifier bound to one coordination chain.
#[derive(Debug, Clone, Copy)]
pub(super) struct CoordinationVerifier(DomainId);

impl CoordinationVerifier {
    /// Creates a verifier bound to `domain`.
    pub(super) const fn new(domain: DomainId) -> Self {
        Self(domain)
    }
}

#[async_trait]
impl Signer<CoordinationContext> for CoordinationSigner {
    async fn sign_vote(&self, vote: ConsensusVote) -> Result<VoteSignature, SigningError> {
        let signature = self
            .signer
            .sign(&preimage(VOTE_PURPOSE, self.domain, &vote)?);
        Ok(SignedMessage::new(vote, signature))
    }

    async fn sign_proposal(&self, proposal: ConsensusProposal) -> Result<ProposalSignature, SigningError> {
        let signature = self
            .signer
            .sign(&preimage(PROPOSAL_PURPOSE, self.domain, &proposal)?);
        Ok(SignedMessage::new(proposal, signature))
    }

    async fn sign_vote_extension(&self, _: VoteScope, _: Vec<u8>) -> Result<ExtensionSig, SigningError> {
        Err(SigningError::new())
    }

    async fn sign_validator_proof(
        &self,
        public_key: Vec<u8>,
        peer_id: Vec<u8>,
    ) -> Result<ConsensusProof, SigningError> {
        let decoded = ChainSigningScheme::decode_public_key(&public_key).map_err(SigningError::from_source)?;
        if decoded != self.signer.public_key() {
            return Err(SigningError::new());
        }
        let bytes = ConsensusProof::signing_bytes(&public_key, &peer_id);
        let signature = self.signer.sign(&bytes);
        Ok(ValidatorProof::new(public_key, peer_id, signature))
    }
}

#[async_trait]
impl Verifier<CoordinationContext> for CoordinationVerifier {
    async fn verify_signed_vote(
        &self,
        vote: &Vote,
        signature: &ConsensusSig,
        public_key: &ConsensusKey,
    ) -> VerifyResult {
        verify_message(VOTE_PURPOSE, self.0, vote, signature, public_key)
    }

    async fn verify_signed_proposal(
        &self,
        proposal: &Proposal,
        signature: &ConsensusSig,
        public_key: &ConsensusKey,
    ) -> VerifyResult {
        verify_message(PROPOSAL_PURPOSE, self.0, proposal, signature, public_key)
    }

    async fn verify_signed_vote_extension(
        &self,
        _: &VoteScope,
        _: &Vec<u8>,
        _: &ConsensusSig,
        _: &ConsensusKey,
    ) -> VerifyResult {
        Err(SigningError::new())
    }

    async fn verify_validator_proof(&self, proof: &ConsensusProof) -> VerifyResult {
        let Ok(public_key) = proof.decoded_public_key() else {
            return Ok(VerificationResult::Invalid);
        };
        let bytes = ConsensusProof::signing_bytes(&proof.public_key, &proof.peer_id);
        let valid = ChainVerifier.verify(&public_key, &bytes, &proof.signature);
        Ok(VerificationResult::from_bool(valid))
    }
}

fn preimage<T: Serialize>(purpose: &[u8], domain: DomainId, message: &T) -> Result<Vec<u8>, SigningError> {
    let mut bytes = purpose.to_vec();
    bytes.extend_from_slice(domain.as_bytes());
    serde_json::to_writer(&mut bytes, message).map_err(SigningError::from_source)?;
    Ok(bytes)
}

fn verify_message<T: Serialize>(
    purpose: &[u8],
    domain: DomainId,
    message: &T,
    signature: &ChainSignature,
    public_key: &ChainPublicKey,
) -> VerifyResult {
    let valid = ChainVerifier.verify(public_key, &preimage(purpose, domain, message)?, signature);
    Ok(VerificationResult::from_bool(valid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::types::{ConsensusValue, ConsensusVoteKind, Height};
    use arc_malachitebft_core_types::NilOrVal;

    const DOMAIN: DomainId = DomainId::from_bytes([31; 32]);

    fn make_signer(seed: u8) -> CoordinationSigner {
        CoordinationSigner::new(SecretKey::from_bytes(&[seed; 32]), DOMAIN)
    }

    fn vote(address: CoordinationPublicKey) -> ConsensusVote {
        ConsensusVote {
            height: Height(4),
            round: 2,
            value: NilOrVal::Nil,
            kind: ConsensusVoteKind::Precommit,
            address,
            extension: None,
        }
    }

    fn proposal(address: CoordinationPublicKey) -> ConsensusProposal {
        ConsensusProposal {
            height: Height(4),
            round: 2,
            value: ConsensusValue("[{\"entry\":1}]".to_owned()),
            pol_round: 1,
            proposer: address,
        }
    }

    #[tokio::test]
    async fn vote_signing_binds_message_domain_key_and_signature() {
        let signer = make_signer(41);
        let address = signer.signer.public_key().into();
        let signed = Signer::sign_vote(&signer, vote(address)).await.unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        let key = signer.signer.public_key();

        assert!(
            verifier
                .verify_signed_vote(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_vote = signed.message.clone();
        changed_vote.round += 1;
        assert!(
            verifier
                .verify_signed_vote(&changed_vote, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            CoordinationVerifier::new(DomainId::from_bytes([32; 32]))
                .verify_signed_vote(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );

        let other_key = make_signer(42).signer.public_key();
        assert!(
            verifier
                .verify_signed_vote(&signed.message, &signed.signature, &other_key)
                .await
                .unwrap()
                .is_invalid()
        );
        let mut changed_signature = signed.signature;
        changed_signature.0[0] ^= 1;
        assert!(
            verifier
                .verify_signed_vote(&signed.message, &changed_signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
    }

    #[tokio::test]
    async fn proposal_signing_binds_message_domain_and_key() {
        let signer = make_signer(51);
        let address = signer.signer.public_key().into();
        let signed = Signer::sign_proposal(&signer, proposal(address))
            .await
            .unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        let key = signer.signer.public_key();

        assert!(
            verifier
                .verify_signed_proposal(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_proposal = signed.message.clone();
        changed_proposal.round += 1;
        assert!(
            verifier
                .verify_signed_proposal(&changed_proposal, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            CoordinationVerifier::new(DomainId::from_bytes([32; 32]))
                .verify_signed_proposal(&signed.message, &signed.signature, &key)
                .await
                .unwrap()
                .is_invalid()
        );
        let other_key = make_signer(52).signer.public_key();
        assert!(
            verifier
                .verify_signed_proposal(&signed.message, &signed.signature, &other_key)
                .await
                .unwrap()
                .is_invalid()
        );
    }

    #[tokio::test]
    async fn validator_proof_binds_key_and_peer_identity() {
        let signer = make_signer(61);
        let public_key = ChainSigningScheme::encode_public_key(&signer.signer.public_key());
        let proof = Signer::sign_validator_proof(&signer, public_key.clone(), b"peer-a".to_vec())
            .await
            .unwrap();
        let verifier = CoordinationVerifier::new(DOMAIN);
        assert!(
            verifier
                .verify_validator_proof(&proof)
                .await
                .unwrap()
                .is_valid()
        );

        let mut changed_peer = proof.clone();
        changed_peer.peer_id = b"peer-b".to_vec();
        assert!(
            verifier
                .verify_validator_proof(&changed_peer)
                .await
                .unwrap()
                .is_invalid()
        );

        let other_key = ChainSigningScheme::encode_public_key(&make_signer(62).signer.public_key());
        let mut changed_key = proof.clone();
        changed_key.public_key = other_key.clone();
        assert!(
            verifier
                .verify_validator_proof(&changed_key)
                .await
                .unwrap()
                .is_invalid()
        );
        assert!(
            Signer::sign_validator_proof(&signer, other_key, b"peer-a".to_vec())
                .await
                .is_err()
        );
    }
}

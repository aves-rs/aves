//! Machine-key content encryption and signed recipient wraps.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use ed25519_dalek::{SigningKey, VerifyingKey};
use hpke::{
    Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable, aead::ChaCha20Poly1305 as HpkeAead,
    kdf::HkdfSha256, kem::X25519HkdfSha256, single_shot_open, single_shot_seal,
};
use iroh::{SecretKey, Signature};
use iroh_blobs::Hash;
use serde::Serialize;
use zeroize::Zeroizing;

use crate::auth::MachineIssuerId;

use super::actor::Actor;
use super::types::{ContentEnvelope, ContentHash, EncryptedContent, MachineError};

type Kem = X25519HkdfSha256;
type RecipientPublicKey = <Kem as KemTrait>::PublicKey;
type RecipientPrivateKey = <Kem as KemTrait>::PrivateKey;
type EncappedKey = <Kem as KemTrait>::EncappedKey;

const CONTENT_PURPOSE: &str = "aves/machine/content/v1";
const WRAP_PURPOSE: &str = "aves/machine/content-wrap/v1";
const SIGN_PURPOSE: &str = "aves/machine/content-signature/v1";
const HEADER_LEN: usize = 16;

impl Actor {
    /// Encrypts once and always wraps the key for the producing endpoint.
    pub(super) async fn encrypt_content(
        &self,
        execution_id: String,
        format_version: u32,
        plaintext: Vec<u8>,
        recipients: Vec<MachineIssuerId>,
    ) -> Result<EncryptedContent, MachineError> {
        let signer = self.content_machine_key().await?;
        let plaintext = Zeroizing::new(plaintext);
        let mut key = Zeroizing::new([0u8; 32]);
        getrandom::fill(key.as_mut()).map_err(|_| MachineError::ContentAuthentication)?;
        let mut nonce = [0u8; 12];
        getrandom::fill(&mut nonce).map_err(|_| MachineError::ContentAuthentication)?;
        let aad = json(&(CONTENT_PURPOSE, &execution_id, format_version))?;
        let cipher_key: &Key = (&*key).into();
        let cipher = ChaCha20Poly1305::new(cipher_key);
        let encrypted = cipher
            .encrypt(
                (&nonce).into(),
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| MachineError::ContentAuthentication)?;
        let mut ciphertext = format_version.to_be_bytes().to_vec();
        ciphertext.extend_from_slice(&nonce);
        ciphertext.extend_from_slice(&encrypted);
        let mut content = EncryptedContent {
            execution_id,
            format_version,
            ciphertext,
            envelopes: Vec::new(),
        };
        let producer = MachineIssuerId::from(signer.public());
        for recipient in std::iter::once(producer).chain(recipients) {
            if !content
                .envelopes
                .iter()
                .any(|entry| entry.recipient == recipient)
            {
                content
                    .envelopes
                    .push(wrap_key(&content, &signer, recipient, &key)?);
            }
        }
        Ok(content)
    }

    /// Verifies the envelopes and opens the local wrap before authenticating content.
    pub(super) async fn decrypt_content(&self, content: EncryptedContent) -> Result<Vec<u8>, MachineError> {
        let signer = self.content_machine_key().await?;
        validate_content(&content)?;
        let key = open_key(&content, &signer)?;
        decrypt_bytes(&content, &key)
    }

    /// Adds one wrap without changing ciphertext or existing recipient envelopes.
    pub(super) async fn add_content_recipient(
        &self,
        mut content: EncryptedContent,
        recipient: MachineIssuerId,
    ) -> Result<EncryptedContent, MachineError> {
        let signer = self.content_machine_key().await?;
        validate_content(&content)?;
        let key = open_key(&content, &signer)?;
        // Confirm the recovered key actually authenticates this ciphertext before sharing it.
        let _plaintext = Zeroizing::new(decrypt_bytes(&content, &key)?);
        if !content
            .envelopes
            .iter()
            .any(|entry| entry.recipient == recipient)
        {
            content
                .envelopes
                .push(wrap_key(&content, &signer, recipient, &key)?);
        }
        Ok(content)
    }

    /// Returns only identities from envelopes whose metadata and signatures validate.
    pub(super) async fn read_content_recipients(
        &self,
        content: EncryptedContent,
    ) -> Result<Vec<MachineIssuerId>, MachineError> {
        self.peer_services
            .as_ref()
            .ok_or(MachineError::ContentUnavailable)?;
        validate_content(&content)?;
        let mut recipients = Vec::new();
        for envelope in content.envelopes {
            if !recipients.contains(&envelope.recipient) {
                recipients.push(envelope.recipient);
            }
        }
        Ok(recipients)
    }

    async fn content_machine_key(&self) -> Result<SecretKey, MachineError> {
        let services = self
            .peer_services
            .as_ref()
            .ok_or(MachineError::ContentUnavailable)?;
        Ok(services.native.read().await.endpoint.secret_key().clone())
    }
}

fn json(value: &impl Serialize) -> Result<Vec<u8>, MachineError> {
    serde_json::to_vec(value).map_err(|source| MachineError::ContentCodec {
        source: Box::new(source),
    })
}

fn wrap_info(
    content: &EncryptedContent,
    recipient: &MachineIssuerId,
    hash: ContentHash,
) -> Result<Vec<u8>, MachineError> {
    json(&(
        WRAP_PURPOSE,
        &content.execution_id,
        content.format_version,
        hash,
        recipient,
    ))
}

fn signing_bytes(content: &EncryptedContent, envelope: &ContentEnvelope) -> Result<Vec<u8>, MachineError> {
    json(&(
        SIGN_PURPOSE,
        &content.execution_id,
        content.format_version,
        &envelope.signer,
        &envelope.recipient,
        envelope.ciphertext_hash,
        envelope.encapsulated_key,
        &envelope.wrapped_key,
    ))
}

fn wrap_key(
    content: &EncryptedContent,
    signer: &SecretKey,
    recipient: MachineIssuerId,
    key: &[u8; 32],
) -> Result<ContentEnvelope, MachineError> {
    let verifying =
        VerifyingKey::from_bytes(&recipient.verification_key()).map_err(|_| MachineError::ContentInvalidKey)?;
    if verifying.is_weak() {
        return Err(MachineError::ContentInvalidKey);
    }
    let public = RecipientPublicKey::from_bytes(&verifying.to_montgomery().to_bytes())
        .map_err(|_| MachineError::ContentInvalidKey)?;
    let hash = ContentHash(Hash::new(&content.ciphertext));
    let info = wrap_info(content, &recipient, hash)?;
    let (encapped, wrapped_key) =
        single_shot_seal::<HpkeAead, HkdfSha256, Kem>(&OpModeS::Base, &public, &info, key, &[])
            .map_err(|_| MachineError::ContentHpke)?;
    let mut encapsulated_key = [0u8; 32];
    encapped.write_exact(&mut encapsulated_key);
    let mut envelope = ContentEnvelope {
        signer: MachineIssuerId::from(signer.public()),
        recipient,
        ciphertext_hash: hash,
        encapsulated_key,
        wrapped_key,
        signature: [0u8; 64],
    };
    envelope.signature = signer.sign(&signing_bytes(content, &envelope)?).to_bytes();
    Ok(envelope)
}

fn validate_content(content: &EncryptedContent) -> Result<(), MachineError> {
    if content.ciphertext.len() < HEADER_LEN + 16 || content.ciphertext[..4] != content.format_version.to_be_bytes() {
        return Err(MachineError::ContentAuthentication);
    }
    let hash = Hash::new(&content.ciphertext);
    for envelope in &content.envelopes {
        if envelope.ciphertext_hash.0 != hash {
            return Err(MachineError::ContentInvalidHash);
        }
        let recipient = VerifyingKey::from_bytes(&envelope.recipient.verification_key())
            .map_err(|_| MachineError::ContentInvalidKey)?;
        if recipient.is_weak() || envelope.wrapped_key.len() != 48 {
            return Err(MachineError::ContentInvalidKey);
        }
        let signature = Signature::from_bytes(&envelope.signature);
        envelope
            .signer
            .public_key()
            .verify(&signing_bytes(content, envelope)?, &signature)
            .map_err(|_| MachineError::ContentInvalidSignature)?;
    }
    Ok(())
}

fn open_key(content: &EncryptedContent, signer: &SecretKey) -> Result<Zeroizing<[u8; 32]>, MachineError> {
    let local = MachineIssuerId::from(signer.public());
    let envelope = content
        .envelopes
        .iter()
        .find(|entry| entry.recipient == local)
        .ok_or(MachineError::ContentNoLocalRecipient)?;
    let seed = Zeroizing::new(signer.to_bytes());
    let signing = SigningKey::from_bytes(&seed);
    let scalar = Zeroizing::new(signing.to_scalar_bytes());
    let private = RecipientPrivateKey::from_bytes(scalar.as_ref()).map_err(|_| MachineError::ContentInvalidKey)?;
    let encapped = EncappedKey::from_bytes(&envelope.encapsulated_key).map_err(|_| MachineError::ContentInvalidKey)?;
    let info = wrap_info(content, &envelope.recipient, envelope.ciphertext_hash)?;
    let opened = Zeroizing::new(
        single_shot_open::<HpkeAead, HkdfSha256, Kem>(
            &OpModeR::Base,
            &private,
            &encapped,
            &info,
            &envelope.wrapped_key,
            &[],
        )
        .map_err(|_| MachineError::ContentHpke)?,
    );
    if opened.len() != 32 {
        return Err(MachineError::ContentInvalidKey);
    }
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&opened);
    Ok(key)
}

fn decrypt_bytes(content: &EncryptedContent, key: &[u8; 32]) -> Result<Vec<u8>, MachineError> {
    let aad = json(&(CONTENT_PURPOSE, &content.execution_id, content.format_version))?;
    let cipher_key: &Key = key.into();
    let cipher = ChaCha20Poly1305::new(cipher_key);
    let nonce: &[u8; 12] = content.ciphertext[4..HEADER_LEN]
        .try_into()
        .map_err(|_| MachineError::ContentAuthentication)?;
    let nonce: &Nonce = nonce.into();
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: &content.ciphertext[HEADER_LEN..],
                aad: &aad,
            },
        )
        .map_err(|_| MachineError::ContentAuthentication)
}

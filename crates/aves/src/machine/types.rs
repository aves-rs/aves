use iroh::PublicKey;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use specta::Type;
use thiserror::Error;

use crate::auth::{AvesAuthError, MachineIssuerId};
use crate::bus::{BusControlError, DispatchContextError};
use crate::config::ConfigError;
use crate::runtime::GeneratedCallError;
use crate::utils::errors::SharedInfraError;

const DEFAULT_MACHINE_NAME: &str = "this machine";

/// A native blob hash, serialized using iroh's validated representation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct ContentHash(
    #[specta(type = String)]
    #[schemars(with = "String")]
    pub Hash,
);

/// A native peer identity, serialized using iroh's validated representation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct ContentPeer(
    #[specta(type = String)]
    #[schemars(with = "String")]
    pub PublicKey,
);

/// A native content hash, distinguished by the blob format it identifies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub enum ContentRoot {
    /// A single raw blob.
    Raw(
        #[specta(type = String)]
        #[schemars(with = "String")]
        Hash,
    ),
    /// A collection in native HashSeq format.
    Collection(
        #[specta(type = String)]
        #[schemars(with = "String")]
        Hash,
    ),
}

impl ContentRoot {
    /// Returns the underlying native hash.
    pub fn hash(self) -> Hash {
        match self {
            Self::Raw(hash) | Self::Collection(hash) => hash,
        }
    }
}

/// A named blob within a collection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ContentEntry {
    /// Collection entry name.
    pub name: String,
    /// Native blob hash.
    #[specta(type = String)]
    #[schemars(with = "String")]
    pub hash: Hash,
}

/// Ciphertext and signed machine-recipient envelopes bound to an execution and format.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct EncryptedContent {
    /// Execution identity authenticated by content and envelope operations.
    pub execution_id: String,
    /// Plaintext record format version.
    pub format_version: u32,
    /// Encoded authenticated ciphertext.
    pub ciphertext: Vec<u8>,
    /// Signed wraps for machine recipients, including the producing machine.
    pub envelopes: Vec<ContentEnvelope>,
}

/// Public metadata and wrapped key for one machine recipient.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct ContentEnvelope {
    /// Machine that signed the envelope.
    pub signer: MachineIssuerId,
    /// Machine addressed by the envelope.
    pub recipient: MachineIssuerId,
    /// Native hash of the ciphertext protected by this envelope.
    pub ciphertext_hash: ContentHash,
    /// HPKE encapsulated public key.
    pub encapsulated_key: [u8; 32],
    /// HPKE-wrapped content key.
    pub wrapped_key: Vec<u8>,
    /// Signature binding the execution, version, ciphertext and recipient.
    #[serde(with = "serde_big_array::BigArray")]
    #[schemars(with = "Vec<u8>", length(min = 64, max = 64))]
    pub signature: [u8; 64],
}

/// Construction-only inputs for the Machine domain, supplied by the runtime after Auth.
#[derive(Clone, Debug)]
pub struct MachineBootOptions {
    pub machine: MachineIssuerId,
    pub name: String,
}

impl MachineBootOptions {
    /// Names the machine after the OS hostname, falling back to a fixed default when it is empty.
    pub fn new(machine: MachineIssuerId) -> Self {
        let name = gethostname::gethostname()
            .to_string_lossy()
            .trim()
            .to_owned();
        let name = if name.is_empty() {
            DEFAULT_MACHINE_NAME.to_owned()
        } else {
            name
        };
        Self { machine, name }
    }
}

/// The secret-free inventory record of this machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineRecord {
    /// Lowercase hex of the machine's Ed25519 public identity.
    pub machine_key: String,
    pub name: String,
}

/// The local machine inventory returned by public reads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineInventory {
    pub local: MachineRecord,
}

/// The secret-free status of this machine: inventory, transport identity and peers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct MachineStatus {
    pub inventory: MachineInventory,
    /// The iroh endpoint identity this machine speaks with; it is the same key as its machine identity.
    pub transport_identity: MachineIssuerId,
    pub peers: PeerAvailability,
}

/// Whether a peer is currently reachable. Only `Unavailable` is reported: Auth's admission registry records trust, not liveness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnlineState {
    Unavailable,
}

/// Peers this machine admits through Auth's single admission registry, with no liveness claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct PeerAvailability {
    /// Lowercase hex of each admitted remote machine key, excluding this machine.
    pub admitted_remote_machines: Vec<String>,
    pub online: OnlineState,
}

/// The Config section `machine`, containing this machine's rename.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MachineSettings {
    /// The user's rename; absent until renamed so the hostname default applies.
    pub name: Option<String>,
}

/// Encodes a 32-byte public identity as lowercase hex without `format!`.
pub(super) fn hex_key(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(char::from(HEX[usize::from(byte >> 4)]));
        text.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    text
}

/// Machine domain failures.
#[derive(Debug, Error, Serialize, Deserialize, Type)]
pub enum MachineError {
    /// An RPC dispatch context was rejected before reaching Machine.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    DispatchContext(#[from] DispatchContextError),
    /// A root-signed Auth operation failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    Auth(#[from] AvesAuthError),
    /// A Config operation failed while reading or saving the Machine section.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    Config(#[from] ConfigError),
    /// Peer server lifecycle control failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    BusControl(#[from] BusControlError),
    /// The peer server capability was not supplied.
    #[error("machine peer service is unavailable")]
    PeerServiceUnavailable,
    /// Machine configuration could not be registered at startup, so Machine cannot serve requests.
    #[error("machine configuration is unavailable: {cause}")]
    Unavailable { cause: String },
    /// The stored Machine section could not be decoded.
    #[error("machine configuration section is invalid: {cause}")]
    Section { cause: String },
    /// A machine name must contain visible characters.
    #[error("machine name cannot be empty")]
    InvalidName,
    /// Content storage is not available on this machine.
    #[error("content service is unavailable")]
    ContentUnavailable,
    /// The content tag is not valid.
    #[error("invalid content tag `{tag}`")]
    InvalidContentTag { tag: String },
    /// The native blob store rejected an operation.
    #[error("content blob store operation failed: {source}")]
    #[serde(skip)]
    #[specta(skip)]
    ContentBlob {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A collection could not be decoded or is malformed.
    #[error("content collection `{hash}` is invalid: {source}")]
    #[serde(skip)]
    #[specta(skip)]
    InvalidContentCollection {
        hash: Hash,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Peer connectivity is disabled or unavailable.
    #[error("peer connectivity is disabled")]
    ContentConnectivityDisabled,
    /// The requested peer is not authorized for content access.
    #[error("peer `{peer}` is not authorized for content access")]
    ContentPeerRefused { peer: String },
    /// An authorized peer could not be reached.
    #[error("failed to reach peer `{peer}`: {source}")]
    #[serde(skip)]
    #[specta(skip)]
    ContentPeerUnreachable {
        peer: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Transfer or verification of a blob failed.
    #[error("content `{hash}` failed verification: {source}")]
    #[serde(skip)]
    #[specta(skip)]
    ContentIntegrity {
        hash: Hash,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Encrypted content or an envelope has an invalid encoding.
    #[error("invalid encrypted content encoding: {source}")]
    #[serde(skip)]
    #[specta(skip)]
    ContentCodec {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Content authentication or decryption failed.
    #[error("content authentication failed")]
    ContentAuthentication,
    /// HPKE wrapping or unwrapping failed.
    #[error("content key wrapping failed")]
    ContentHpke,
    /// An envelope signature failed verification.
    #[error("content envelope signature is invalid")]
    ContentInvalidSignature,
    /// An envelope identifies a different ciphertext.
    #[error("content envelope ciphertext hash does not match")]
    ContentInvalidHash,
    /// A machine public key cannot be used for encryption.
    #[error("content recipient machine key is invalid")]
    ContentInvalidKey,
    /// The content has no envelope for this machine.
    #[error("content has no envelope for the local machine")]
    ContentNoLocalRecipient,
    /// The generated actor request could not be delivered.
    #[error("machine channel send failed for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// The generated actor reply could not be received.
    #[error("machine channel receive failed for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// A generated request exceeded its reply timeout.
    #[error("machine reply timed out for {target_domain}.{operation} after {timeout}")]
    OneshotReplyTimeout {
        target_domain: String,
        operation: String,
        timeout: String,
    },
    /// The actor dropped a generated reply channel.
    #[error("machine reply was dropped for {target_domain}.{operation}: {cause}")]
    OneshotReplyMissing {
        target_domain: String,
        operation: String,
        cause: String,
    },
}

impl From<serde_json::Error> for MachineError {
    fn from(source: serde_json::Error) -> Self {
        Self::ContentCodec {
            source: Box::new(source),
        }
    }
}

impl From<SharedInfraError> for MachineError {
    fn from(error: SharedInfraError) -> Self {
        match error {
            SharedInfraError::ChannelSend {
                target_domain,
                operation,
                cause,
            } => Self::ChannelSend {
                target_domain,
                operation,
                cause,
            },
            SharedInfraError::ChannelRecv {
                target_domain,
                operation,
                cause,
            } => Self::ChannelRecv {
                target_domain,
                operation,
                cause,
            },
            SharedInfraError::OneshotReplyTimeout {
                target_domain,
                operation,
                timeout,
            } => Self::OneshotReplyTimeout {
                target_domain,
                operation,
                timeout,
            },
            SharedInfraError::OneshotReplyMissing {
                target_domain,
                operation,
                cause,
            } => Self::OneshotReplyMissing {
                target_domain,
                operation,
                cause,
            },
        }
    }
}

impl From<GeneratedCallError<MachineError>> for MachineError {
    fn from(error: GeneratedCallError<MachineError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => error.into(),
            GeneratedCallError::InputClosed => Self::ChannelSend {
                target_domain: "machine".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "bus input closed".to_owned(),
            },
            GeneratedCallError::ReplyDropped => Self::ChannelRecv {
                target_domain: "machine".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "reply dropped".to_owned(),
            },
            GeneratedCallError::UnexpectedResponse { expected, actual } => Self::OneshotReplyMissing {
                target_domain: "machine".to_owned(),
                operation: "dispatch".to_owned(),
                cause: format!("expected {expected}, received {actual}"),
            },
        }
    }
}

impl rspc::Error for MachineError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(self.to_string(), Some(self)))
    }
}

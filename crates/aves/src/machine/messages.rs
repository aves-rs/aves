use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::auth::MachineIssuerId;
use crate::bus::BusRpcContext;

use super::domain::MachineDomain;
use super::types::{
    ContentEntry, ContentHash, ContentPeer, ContentRoot, EncryptedContent, MachineError, MachineInventory,
    MachineStatus,
};

/// Machine request routed from the generated handler to the actor.
pub struct MachineRequest {
    pub payload: MachineRequestPayload,
    pub reply_to: oneshot::Sender<Result<MachineResponse, MachineError>>,
}

/// Public machine, content, and encryption operations.
#[aves::bus_requests(marker = MachineDomain, context = BusRpcContext, root_error = MachineError, domain_error = MachineError)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MachineRequestPayload {
    /// Starts the machine peer server.
    #[operation(name = "startServer", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartServer,

    /// Stops the machine peer server.
    #[operation(name = "stopServer", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopServer,

    /// Returns the local machine inventory without any key material.
    #[operation(name = "readMachineInventory", kind = "query", response = MachineInventoryOutput, response_variant = "MachineInventory")]
    ReadMachineInventory {},

    /// Returns the inventory, transport identity, and peer availability.
    #[operation(name = "readMachineStatus", kind = "query", response = MachineStatusOutput, response_variant = "MachineStatus")]
    ReadMachineStatus {},

    /// Renames this machine; the name is recorded only in memory until persistence lands.
    #[operation(name = "renameMachine", kind = "mutation", response = MachineRenamedOutput, response_variant = "MachineRenamed")]
    RenameMachine { name: String },

    /// Imports and pins a raw blob.
    #[operation(name = "importContent", kind = "mutation", response = ContentImportedOutput, response_variant = "ContentImported")]
    ImportContent { tag: String, bytes: Vec<u8> },

    /// Imports and pins a collection of named blob hashes.
    #[operation(name = "importContentCollection", kind = "mutation", response = ContentImportedOutput, response_variant = "ContentImported")]
    ImportContentCollection { tag: String, entries: Vec<ContentEntry> },

    /// Reads a locally available raw blob.
    #[operation(name = "readContent", kind = "query", response = ContentBytesOutput, response_variant = "ContentBytes")]
    ReadContent { hash: ContentHash },

    /// Reads a locally available collection.
    #[operation(name = "readContentCollection", kind = "query", response = ContentCollectionOutput, response_variant = "ContentCollection")]
    ReadContentCollection { hash: ContentHash },

    /// Pins a content root under a tag.
    #[operation(name = "pinContent", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    PinContent { tag: String, root: ContentRoot },

    /// Removes a content tag.
    #[operation(name = "unpinContent", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    UnpinContent { tag: String },

    /// Fetches and pins content from an authorized peer.
    #[operation(name = "fetchContentFromPeer", kind = "mutation", response = ContentImportedOutput, response_variant = "ContentImported")]
    FetchContentFromPeer {
        peer: ContentPeer,
        tag: String,
        root: ContentRoot,
    },

    /// Encrypts content for the producer and the requested machine recipients.
    #[operation(name = "encryptContent", kind = "mutation", response = ContentEncryptedOutput, response_variant = "ContentEncrypted")]
    EncryptContent {
        execution_id: String,
        format_version: u32,
        plaintext: Vec<u8>,
        recipients: Vec<MachineIssuerId>,
    },

    /// Decrypts content addressed to this machine after verification.
    #[operation(name = "decryptContent", kind = "mutation", response = ContentDecryptedOutput, response_variant = "ContentDecrypted")]
    DecryptContent { content: EncryptedContent },

    /// Adds a machine recipient without replacing ciphertext or existing envelopes.
    #[operation(name = "addContentRecipient", kind = "mutation", response = ContentEncryptedOutput, response_variant = "ContentEncrypted")]
    AddContentRecipient {
        content: EncryptedContent,
        recipient: MachineIssuerId,
    },

    /// Lists the machine recipients of encrypted content after verification.
    #[operation(name = "readContentRecipients", kind = "query", response = ContentRecipientsOutput, response_variant = "ContentRecipients")]
    ReadContentRecipients { content: EncryptedContent },
}

/// Responses produced by Machine requests.
#[aves::bus_responses]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MachineResponse {
    /// The machine peer server started.
    ServerStarted,
    /// The machine peer server stopped.
    ServerStopped,
    /// The local machine inventory.
    MachineInventory { inventory: MachineInventory },
    /// The machine's new name.
    MachineRenamed { name: String },
    /// The machine's status and peers.
    MachineStatus { status: MachineStatus },
    /// The imported or fetched content root.
    ContentImported { root: ContentRoot },
    /// Bytes of a locally stored blob.
    ContentBytes { bytes: Vec<u8> },
    /// Named hashes in a locally stored collection.
    ContentCollection { entries: Vec<ContentEntry> },
    /// Encrypted content and its recipient envelopes.
    ContentEncrypted { content: EncryptedContent },
    /// Authenticated plaintext.
    ContentDecrypted { plaintext: Vec<u8> },
    /// Machine identities addressed by the envelopes.
    ContentRecipients { recipients: Vec<MachineIssuerId> },
    /// The requested operation completed.
    Success,
}

/// Notifications of persisted Machine changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MachineBroadcast {
    /// The machine peer server started.
    ServerStarted,
    /// The machine peer server stopped.
    ServerStopped,
    /// The local machine's name has changed.
    MachineRenamed { name: String },
}

//! Machine inventory, content storage and machine-key encryption.

mod actor;
mod content;
mod domain;
mod encryption;
mod handle;
mod peering;

pub mod messages;
pub mod types;

pub use domain::MachineDomain;
pub use messages::{MachineBroadcast, MachineRequestPayload, MachineResponse};
pub use peering::{
    IrohSubstrate, MachineNativeServices, MachinePeerService, MachinePeerServices, PeerConnectivity,
    RunningIrohSubstrate, call_remote_operation_with,
};
pub use types::{
    ContentEntry, ContentEnvelope, ContentHash, ContentPeer, ContentRoot, EncryptedContent, MachineBootOptions,
    MachineError, MachineInventory, MachineRecord, MachineStatus, OnlineState, PeerAvailability,
};

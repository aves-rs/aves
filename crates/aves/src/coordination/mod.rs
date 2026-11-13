//! Application-neutral chain coordination and host integration for Malachite 0.8.2.
//!
//! Aves owns typed coordination messages, committed record projections, and the ABCI host
//! interface. It knows no Yach transaction enum and imports no Corv or Yach code.

mod abci;
mod actor;
mod chain;
mod domain;
mod handle;
mod network;
mod persistence;
mod serialization;
mod signing;
mod transaction;
mod types;

pub mod messages;

pub use domain::CoordinationDomain;
pub use messages::{CoordinationBroadcast, CoordinationRequestPayload, CoordinationResponse};
pub use types::{
    BlobHash, ChainRecord, CommittedSnapshot, CoordinationError, CoordinationEventId, CoordinationExecutionId,
    CoordinationFlowId, DomainId, EncKey, ExecutionCancelled, ExecutionCreated, ExecutionLanded, ExecutionRecord,
    Height, MachineBound, MachineId, MachineRecord, MachineRegistered, MachineUnbound, PublicKey, RecordHash,
    Regenesis, SchemaActivated, SignedBody, SnapshotDecided, UserEncRotated, UserId, UserRecord, UserRegistered,
    UserRevoked, UserRootRotated, UserSuccessorLinked, ValidatorSet,
};

//! Application-neutral chain coordination and host integration for Malachite 0.8.2.
//!
//! Aves owns typed coordination messages, committed record projections, and the ABCI host
//! interface. It knows no Yach transaction enum and imports no Corv or Yach code.

mod abci;
mod actor;
mod authority;
mod chain;
mod domain;
mod handle;
mod membership;
mod network;
mod notices;
mod persistence;
mod roster;
mod serialization;
mod sharing;
mod signing;
mod transaction;
mod types;

pub mod messages;

pub use domain::CoordinationDomain;
pub use domain::CoordinationSigningService;
pub use messages::{CoordinationBroadcast, CoordinationRequestPayload, CoordinationResponse};
pub use signing::MachineRecord as SignableMachineRecord;
pub use types::{
    AUTHORITY_RULES_VERSION, Acceptance, BlobHash, ChainHalted, ChainKind, ChainPolicyChanged, ChainPolicyRecord,
    ChainRecord, CommittedSnapshot, CoordinationError, CoordinationEventId, CoordinationExecutionId,
    CoordinationFlowId, DomainId, EffectiveValidator, EncKey, EnvelopeAddition, EnvelopeRef, ExecutionCancelled,
    ExecutionCreated, ExecutionLanded, ExecutionRecord, GenesisDescriptor, GenesisFingerprint, Height, Invitation,
    InvitationId, InvitationRecord, MachineBound, MachineId, MachineRecord, MachineRegistered, MachineUnbound,
    MemberAuthority, MemberAuthorityUpdated, MemberIdentityProof, MemberMachineCertUpdate,
    MemberMachineSelfRegistration, MemberPowerChanged, MemberPowerRecord, MemberRecord, MemberRemoved, MemberStatus,
    MemberValidatorSplitChanged, MemberValidatorSplitRecord, MembershipDecision, OwnerRegistration, PublicKey,
    PublicationEnvelopeRef, PublicationRecord, PublishedRecordRef, RecordAttestation, RecordHash, Regenesis,
    RegisteredTransaction, SchemaActivated, ScopedPublicationKey, ShareGrantChanged, ShareScope, SharingBootstrap,
    SharingDecision, SharingNotice, SharingNoticeKey, SharingNoticeRecord, SharingNoticeState, SharingResponse,
    SignedBody, SnapshotDecided, SourceProof, StoryPublished, UserEncRotated, UserId, UserRecord, UserRegistered,
    UserRevoked, UserRootRotated, UserSuccessorLinked, ValidatorSet, ValidatorSplitPart, VerificationReport,
};

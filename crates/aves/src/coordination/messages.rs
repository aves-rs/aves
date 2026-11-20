use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::bus::BusRpcContext;

use super::domain::CoordinationDomain;
use super::signing::MachineRecord as SignableMachineRecord;
use super::types::{
    Acceptance, ChainHalted, ChainPolicyChanged, ChainPolicyRecord, ChainRecord, CommittedSnapshot, CoordinationError,
    CoordinationEventId, CoordinationExecutionId, DomainId, EffectiveValidator, EnvelopeAddition, ExecutionCancelled,
    ExecutionCreated, ExecutionLanded, ExecutionRecord, GenesisDescriptor, Height, Invitation, InvitationRecord,
    MachineBound, MachineId, MachineRecord, MachineRegistered, MachineUnbound, MemberAuthority, MemberAuthorityUpdated,
    MemberMachineCertUpdate, MemberPowerChanged, MemberPowerRecord, MemberRecord, MemberRemoved,
    MemberValidatorSplitChanged, MemberValidatorSplitRecord, MembershipDecision, OwnerRegistration, PublicKey,
    PublicationRecord, RecordHash, Regenesis, SchemaActivated, ScopedPublicationKey, ShareGrantChanged, ShareScope,
    SharingNotice, SharingNoticeKey, SharingNoticeRecord, SignedBody, StoryPublished, UserEncRotated, UserId,
    UserRecord, UserRegistered, UserRevoked, UserRootRotated, UserSuccessorLinked, VerificationReport,
};

/// Request routed from the generated coordination handler to the chain host owner.
pub struct CoordinationRequest {
    pub payload: CoordinationRequestPayload,
    pub reply_to: oneshot::Sender<Result<CoordinationResponse, CoordinationError>>,
}

/// Typed operations over coordination records and committed chain projections.
#[aves::bus_requests(
    marker = CoordinationDomain,
    context = BusRpcContext,
    root_error = CoordinationError,
    domain_error = CoordinationError
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinationRequestPayload {
    /// Reads a chain's effective roster at the requested height, or its next height.
    #[operation(name = "getEffectiveRoster", kind = "query", response = EffectiveRosterOutput, response_variant = "EffectiveRoster")]
    GetEffectiveRoster { domain: DomainId, height: Option<u64> },
    /// Signs a typed record with the local machine key after authorization checks.
    #[privileged(reason = "Machine-key signing is security authority")]
    #[operation(name = "signMachineRecord", kind = "mutation", response = SignedMachineRecordOutput, response_variant = "SignedMachineRecord")]
    SignMachineRecord {
        domain: DomainId,
        record: SignableMachineRecord,
    },
    /// Submits a root-authorized sharing grant revision.
    #[operation(name = "submitShareGrantChanged", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitShareGrantChanged {
        domain: DomainId,
        record: SignedBody<ShareGrantChanged>,
    },
    /// Submits a machine-signed story publication.
    #[operation(name = "submitStoryPublished", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitStoryPublished {
        domain: DomainId,
        record: SignedBody<StoryPublished>,
    },
    /// Submits an opaque recipient envelope reference.
    #[operation(name = "submitEnvelopeAddition", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitEnvelopeAddition {
        domain: DomainId,
        record: SignedBody<EnvelopeAddition>,
    },
    /// Submits a machine-signed verification report.
    #[operation(name = "submitVerificationReport", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitVerificationReport {
        domain: DomainId,
        record: SignedBody<VerificationReport>,
    },
    /// Lists committed current grants matching optional scope and recipient filters.
    #[operation(name = "listShareGrants", kind = "query", response = ShareGrantsOutput, response_variant = "ShareGrants")]
    ListShareGrants {
        domain: DomainId,
        scope: Option<ShareScope>,
        recipient: Option<UserId>,
    },
    /// Lists committed story publications.
    #[operation(name = "listPublications", kind = "query", response = PublicationsOutput, response_variant = "Publications")]
    ListPublications { domain: DomainId },
    /// Reads one owner-qualified publication.
    #[operation(name = "getPublication", kind = "query", response = PublicationOutput, response_variant = "Publication")]
    GetPublication {
        domain: DomainId,
        key: ScopedPublicationKey,
    },
    /// Lists opaque envelope additions for a publication.
    #[operation(name = "listEnvelopeAdditions", kind = "query", response = EnvelopeAdditionsOutput, response_variant = "EnvelopeAdditions")]
    ListEnvelopeAdditions {
        domain: DomainId,
        key: ScopedPublicationKey,
    },
    /// Lists committed report references.
    #[operation(name = "listVerificationReports", kind = "query", response = VerificationReportsOutput, response_variant = "VerificationReports")]
    ListVerificationReports { domain: DomainId },
    /// Starts every locally hosted chain during runtime startup.
    #[privileged(reason = "coordination runtime lifecycle is owned by Aves")]
    #[operation(name = "startAllServers", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartAllServers,

    /// Stops every locally hosted chain during runtime shutdown.
    #[privileged(reason = "coordination runtime lifecycle is owned by Aves")]
    #[operation(name = "stopAllServers", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopAllServers,

    /// Starts the coordination server and its configured chain hosts.
    #[operation(name = "startServer", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartServer { domain: DomainId },

    /// Stops the coordination server and its running chain hosts.
    #[operation(name = "stopServer", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopServer { domain: DomainId },

    /// Creates a new empty chain with the caller-selected initial validator powers.
    #[operation(name = "genesis", kind = "mutation", response = ChainOutput, response_variant = "Chain")]
    Genesis {
        domain: DomainId,
        genesis: SignedBody<GenesisDescriptor>,
    },

    /// Locally pins and joins a delivered chain; this operation is not remotely published.
    #[operation(name = "joinChain", kind = "mutation", response = ChainOutput, response_variant = "Chain")]
    JoinChain {
        descriptor: SignedBody<GenesisDescriptor>,
        expected_snapshot_hash: Option<RecordHash>,
        participants: Vec<PublicKey>,
    },

    /// Exports committed state for the active chain or a retired predecessor.
    #[operation(name = "exportSnapshot", kind = "query", response = SnapshotOutput, response_variant = "Snapshot")]
    ExportSnapshot { domain: DomainId },

    /// Adopts a snapshot against an independently trusted expected hash.
    #[operation(name = "adoptSnapshot", kind = "mutation", response = ChainOutput, response_variant = "Chain")]
    AdoptSnapshot {
        domain: DomainId,
        snapshot: CommittedSnapshot,
        expected_hash: RecordHash,
    },

    /// Submits the collective owner registration.
    #[operation(name = "registerCollectiveOwner", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RegisterCollectiveOwner {
        domain: DomainId,
        record: SignedBody<OwnerRegistration>,
    },
    /// Submits an invitation.
    #[operation(name = "submitInvitation", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitInvitation {
        domain: DomainId,
        record: SignedBody<Invitation>,
    },
    /// Submits an invitation decision.
    #[operation(name = "submitMembershipDecision", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMembershipDecision {
        domain: DomainId,
        record: SignedBody<MembershipDecision>,
    },
    /// Submits a machine-signed addressed notice to the recipient's personal chain.
    #[operation(name = "submitSharingNotice", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitSharingNotice {
        domain: DomainId,
        record: SignedBody<SharingNotice>,
    },
    /// Lists the latest committed notices for a user.
    #[operation(name = "listSharingNotices", kind = "query", response = SharingNoticesOutput, response_variant = "SharingNotices")]
    ListSharingNotices { domain: DomainId, target_user: UserId },
    /// Reads the latest committed revision of an addressed notice.
    #[operation(name = "getSharingNotice", kind = "query", response = SharingNoticeOutput, response_variant = "SharingNotice")]
    GetSharingNotice { domain: DomainId, key: SharingNoticeKey },
    /// Submits a member removal.
    #[operation(name = "submitMemberRemoved", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMemberRemoved {
        domain: DomainId,
        record: SignedBody<MemberRemoved>,
    },
    /// Submits a member power assignment.
    #[operation(name = "submitMemberPowerChanged", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMemberPowerChanged {
        domain: DomainId,
        record: SignedBody<MemberPowerChanged>,
    },
    /// Submits a member machine split.
    #[operation(name = "submitMemberValidatorSplitChanged", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMemberValidatorSplitChanged {
        domain: DomainId,
        record: SignedBody<MemberValidatorSplitChanged>,
    },
    /// Submits a chain policy change.
    #[operation(name = "submitChainPolicyChanged", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitChainPolicyChanged {
        domain: DomainId,
        record: SignedBody<ChainPolicyChanged>,
    },
    /// Submits an invitation acceptance.
    #[operation(name = "submitAcceptance", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitAcceptance {
        domain: DomainId,
        record: SignedBody<Acceptance>,
    },
    /// Submits a member machine certificate update.
    #[operation(name = "submitMemberMachineCertUpdate", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMemberMachineCertUpdate {
        domain: DomainId,
        record: SignedBody<MemberMachineCertUpdate>,
    },
    /// Submits an authenticated source-authority proof batch.
    #[operation(name = "submitMemberAuthorityUpdated", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    SubmitMemberAuthorityUpdated {
        domain: DomainId,
        record: SignedBody<MemberAuthorityUpdated>,
    },
    /// Reads a member's current authority and its proof history.
    #[operation(name = "getMemberAuthority", kind = "query", response = MemberAuthorityOutput, response_variant = "MemberAuthority")]
    GetMemberAuthority { domain: DomainId, member: UserId },

    /// Lists committed members and their statuses.
    #[operation(name = "listMembers", kind = "query", response = MembersOutput, response_variant = "Members")]
    ListMembers { domain: DomainId },
    /// Lists addressed invitations for local consumers. Remote invitees must use the
    /// Project operation that authenticates and binds the target user to the peer.
    #[operation(name = "listInvitationsForUser", kind = "query", response = InvitationsOutput, response_variant = "Invitations")]
    ListInvitationsForUser { domain: DomainId, target_user: UserId },
    /// Reads a member's committed power and split.
    #[operation(name = "getMemberPower", kind = "query", response = MemberPowerOutput, response_variant = "MemberPower")]
    GetMemberPower { domain: DomainId, member: UserId },
    /// Reads the committed chain policy.
    #[operation(name = "getChainPolicy", kind = "query", response = ChainPolicyOutput, response_variant = "ChainPolicy")]
    GetChainPolicy { domain: DomainId },

    /// Submits a signed user registration record.
    #[operation(name = "registerUser", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RegisterUser {
        domain: DomainId,
        record: SignedBody<UserRegistered>,
    },

    /// Submits a signed user encryption-key rotation record.
    #[operation(name = "rotateUserEncryptionKey", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RotateUserEncryptionKey {
        domain: DomainId,
        record: SignedBody<UserEncRotated>,
    },

    /// Submits a signed user root-key rotation record.
    #[operation(name = "rotateUserRootKey", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RotateUserRootKey {
        domain: DomainId,
        record: SignedBody<UserRootRotated>,
    },

    /// Submits a signed successor-user linkage record.
    #[operation(name = "linkUserSuccessor", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    LinkUserSuccessor {
        domain: DomainId,
        record: SignedBody<UserSuccessorLinked>,
    },

    /// Submits a signed user revocation record.
    #[operation(name = "revokeUser", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RevokeUser {
        domain: DomainId,
        record: SignedBody<UserRevoked>,
    },

    /// Submits a signed machine registration record.
    #[operation(name = "registerMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RegisterMachine {
        domain: DomainId,
        record: SignedBody<MachineRegistered>,
    },

    /// Submits a signed machine-binding record.
    #[operation(name = "bindMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    BindMachine {
        domain: DomainId,
        record: SignedBody<MachineBound>,
    },

    /// Submits a signed machine-unbinding record.
    #[operation(name = "unbindMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    UnbindMachine {
        domain: DomainId,
        record: SignedBody<MachineUnbound>,
    },

    /// Submits a signed execution-creation record.
    #[operation(name = "createExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    CreateExecution {
        domain: DomainId,
        record: SignedBody<ExecutionCreated>,
    },

    /// Submits a signed execution-landing record.
    #[operation(name = "landExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    LandExecution {
        domain: DomainId,
        record: SignedBody<ExecutionLanded>,
    },

    /// Submits a signed execution-cancellation record.
    #[operation(name = "cancelExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    CancelExecution {
        domain: DomainId,
        record: SignedBody<ExecutionCancelled>,
    },

    /// Submits a signed schema-activation record.
    #[operation(name = "activateSchema", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    ActivateSchema {
        domain: DomainId,
        record: SignedBody<SchemaActivated>,
    },

    /// Begins regenesis with its signed chain-transition record.
    #[operation(name = "regenesis", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    Regenesis {
        domain: DomainId,
        record: SignedBody<Regenesis>,
    },

    /// Reads a user's committed record projection.
    #[operation(name = "getUser", kind = "query", response = UserOutput, response_variant = "User")]
    GetUser { domain: DomainId, user: UserId },

    /// Reads a machine's committed record projection.
    #[operation(name = "getMachine", kind = "query", response = MachineOutput, response_variant = "Machine")]
    GetMachine { domain: DomainId, machine: MachineId },

    /// Lists committed machine projections.
    #[operation(name = "listMachines", kind = "query", response = MachinesListOutput, response_variant = "MachinesList")]
    ListMachines { domain: DomainId },

    /// Reads one execution's committed record projection.
    #[operation(name = "getExecution", kind = "query", response = ExecutionOutput, response_variant = "Execution")]
    GetExecution {
        domain: DomainId,
        execution: CoordinationExecutionId,
    },

    /// Lists committed execution projections.
    #[operation(name = "listExecutions", kind = "query", response = ExecutionsListOutput, response_variant = "ExecutionsList")]
    ListExecutions { domain: DomainId },

    /// Reads the schema active at the requested committed height.
    #[operation(name = "getSchema", kind = "query", response = SchemaOutput, response_variant = "Schema")]
    GetSchema { domain: DomainId, at: Height },

    /// Lists committed schema activations in chain order.
    #[operation(name = "getSchemaHistory", kind = "query", response = SchemaHistoryOutput, response_variant = "SchemaHistory")]
    GetSchemaHistory { domain: DomainId },

    /// Reads the chain identifier and committed tip.
    #[operation(name = "getChain", kind = "query", response = ChainOutput, response_variant = "Chain")]
    GetChain { domain: DomainId },

    /// Reads the committed regenesis record, if present.
    #[operation(name = "getRegenesis", kind = "query", response = RegenesisOutput, response_variant = "Regenesis")]
    GetRegenesis { domain: DomainId },

    /// Reads the durable disposition of a submitted event.
    #[operation(name = "getSubmissionStatus", kind = "query", response = SubmissionStatusOutput, response_variant = "SubmissionStatus")]
    GetSubmissionStatus {
        domain: DomainId,
        event_id: CoordinationEventId,
    },
}

/// Durable disposition of an event on one hosted chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, specta::Type, schemars::JsonSchema)]
pub enum SubmissionStatus {
    /// No admission or commitment is recorded.
    Absent,
    /// The event is durably admitted but not decided.
    Pending,
    /// The event was applied at this height.
    Committed { height: Height },
    /// The event was deterministically rejected at this height.
    Rejected {
        reason: SubmissionRejectionReason,
        height: Height,
    },
}

/// Deterministic reason a submitted event was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, specta::Type, schemars::JsonSchema)]
pub enum SubmissionRejectionReason {
    /// The signer lacked authority at application time.
    UnauthorizedSigner,
    /// The payload was invalid at application time.
    InvalidPayload,
    /// The same action revision has a different payload.
    ConflictingAction,
    /// The action does not advance the previous revision.
    StaleRevision,
}

/// Coordination server lifecycle notifications.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinationBroadcast {
    /// The coordination engine has started.
    ServerStarted { domain: DomainId },
    /// The coordination engine and its transport have stopped.
    ServerStopped { domain: DomainId },
    /// A durable commit advanced this chain; consumers must query its state.
    ChainAdvanced { domain: DomainId, height: Height },
}

/// Responses produced by coordination operations.
#[aves::bus_responses]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinationResponse {
    /// Height-specific validators and the chain's current halted state.
    EffectiveRoster {
        height: u64,
        validators: Vec<EffectiveValidator>,
        halted: Option<ChainHalted>,
    },
    /// Immutable signed body bytes and their durable event identity.
    SignedMachineRecord {
        signed_bytes: Vec<u8>,
        event_id: CoordinationEventId,
    },
    /// Current share-grant revisions.
    ShareGrants { records: Vec<ShareGrantChanged> },
    /// Committed publications and their heights.
    Publications { records: Vec<PublicationRecord> },
    /// A committed publication and its height.
    Publication { record: Option<PublicationRecord> },
    /// Opaque envelope additions.
    EnvelopeAdditions { records: Vec<EnvelopeAddition> },
    /// Verification evidence references.
    VerificationReports { records: Vec<VerificationReport> },
    /// Coordination server startup completed.
    ServerStarted,
    /// Coordination server shutdown completed.
    ServerStopped,

    /// The chain host durably admitted the signed record; admission is not commitment.
    TransactionSubmitted { event_id: CoordinationEventId },
    /// Durable admission or decision status.
    SubmissionStatus { status: SubmissionStatus },
    /// Committed member records.
    Members { records: Vec<MemberRecord> },
    /// Verified current authority and proof history for an active member.
    MemberAuthority { authority: Option<MemberAuthority> },
    /// Invitations addressed to the requested user.
    Invitations { records: Vec<InvitationRecord> },
    /// Latest committed addressed notices.
    SharingNotices { records: Vec<SharingNoticeRecord> },
    /// Latest committed addressed notice, if present.
    SharingNotice { record: Option<SharingNoticeRecord> },
    /// Owner-assigned total and member-selected split, if present.
    MemberPower {
        power: Option<MemberPowerRecord>,
        split: Option<MemberValidatorSplitRecord>,
    },
    /// Current committed policy, if changed.
    ChainPolicy { policy: Option<ChainPolicyRecord> },
    /// A user's committed record projection, if registered.
    User { record: Option<UserRecord> },
    /// A machine's committed record projection, if known.
    Machine { record: Option<MachineRecord> },
    /// The committed machine projections.
    MachinesList { records: Vec<MachineRecord> },
    /// An execution's committed record projection, if known.
    Execution { record: Option<ExecutionRecord> },
    /// The committed execution projections.
    ExecutionsList { records: Vec<ExecutionRecord> },
    /// The schema active at the requested height, if one exists.
    Schema { record: Option<SchemaActivated> },
    /// All committed schema activation records in chain order.
    SchemaHistory { records: Vec<SchemaActivated> },
    /// Chain identity and current committed tip.
    Chain { record: ChainRecord },
    /// Committed state and complete decided history.
    Snapshot { snapshot: CommittedSnapshot },
    /// The committed regenesis record, if one exists.
    Regenesis { record: Option<Regenesis> },
}

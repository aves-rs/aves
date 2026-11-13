use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::bus::BusRpcContext;

use super::domain::CoordinationDomain;
use super::types::{
    ChainRecord, CommittedSnapshot, CoordinationError, CoordinationEventId, CoordinationExecutionId, DomainId,
    ExecutionCancelled, ExecutionCreated, ExecutionLanded, ExecutionRecord, Genesis, Height, MachineBound, MachineId,
    MachineRecord, MachineRegistered, MachineUnbound, RecordHash, Regenesis, SchemaActivated, SignedBody,
    UserEncRotated, UserId, UserRecord, UserRegistered, UserRevoked, UserRootRotated, UserSuccessorLinked,
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
    /// Starts the coordination server and its configured chain hosts.
    #[operation(name = "startServer", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartServer,

    /// Stops the coordination server and its running chain hosts.
    #[operation(name = "stopServer", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopServer,

    /// Creates a new empty chain with the caller-selected initial validator powers.
    #[operation(name = "genesis", kind = "mutation", response = ChainOutput, response_variant = "Chain")]
    Genesis { genesis: Genesis },

    /// Exports committed state for the active chain or a retired predecessor.
    #[operation(name = "exportSnapshot", kind = "query", response = SnapshotOutput, response_variant = "Snapshot")]
    ExportSnapshot { domain: Option<DomainId> },

    /// Adopts a snapshot against an independently trusted expected hash.
    #[operation(name = "adoptSnapshot", kind = "mutation", response = ChainOutput, response_variant = "Chain")]
    AdoptSnapshot {
        snapshot: CommittedSnapshot,
        expected_hash: RecordHash,
    },

    /// Submits a signed user registration record.
    #[operation(name = "registerUser", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RegisterUser { record: SignedBody<UserRegistered> },

    /// Submits a signed user encryption-key rotation record.
    #[operation(name = "rotateUserEncryptionKey", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RotateUserEncryptionKey { record: SignedBody<UserEncRotated> },

    /// Submits a signed user root-key rotation record.
    #[operation(name = "rotateUserRootKey", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RotateUserRootKey { record: SignedBody<UserRootRotated> },

    /// Submits a signed successor-user linkage record.
    #[operation(name = "linkUserSuccessor", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    LinkUserSuccessor { record: SignedBody<UserSuccessorLinked> },

    /// Submits a signed user revocation record.
    #[operation(name = "revokeUser", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RevokeUser { record: SignedBody<UserRevoked> },

    /// Submits a signed machine registration record.
    #[operation(name = "registerMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    RegisterMachine { record: SignedBody<MachineRegistered> },

    /// Submits a signed machine-binding record.
    #[operation(name = "bindMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    BindMachine { record: SignedBody<MachineBound> },

    /// Submits a signed machine-unbinding record.
    #[operation(name = "unbindMachine", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    UnbindMachine { record: SignedBody<MachineUnbound> },

    /// Submits a signed execution-creation record.
    #[operation(name = "createExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    CreateExecution { record: SignedBody<ExecutionCreated> },

    /// Submits a signed execution-landing record.
    #[operation(name = "landExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    LandExecution { record: SignedBody<ExecutionLanded> },

    /// Submits a signed execution-cancellation record.
    #[operation(name = "cancelExecution", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    CancelExecution { record: SignedBody<ExecutionCancelled> },

    /// Submits a signed schema-activation record.
    #[operation(name = "activateSchema", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    ActivateSchema { record: SignedBody<SchemaActivated> },

    /// Begins regenesis with its signed chain-transition record.
    #[operation(name = "regenesis", kind = "mutation", response = TransactionSubmittedOutput, response_variant = "TransactionSubmitted")]
    Regenesis { record: SignedBody<Regenesis> },

    /// Reads a user's committed record projection.
    #[operation(name = "getUser", kind = "query", response = UserOutput, response_variant = "User")]
    GetUser { user: UserId },

    /// Reads a machine's committed record projection.
    #[operation(name = "getMachine", kind = "query", response = MachineOutput, response_variant = "Machine")]
    GetMachine { machine: MachineId },

    /// Lists committed machine projections.
    #[operation(name = "listMachines", kind = "query", response = MachinesListOutput, response_variant = "MachinesList")]
    ListMachines,

    /// Reads one execution's committed record projection.
    #[operation(name = "getExecution", kind = "query", response = ExecutionOutput, response_variant = "Execution")]
    GetExecution { execution: CoordinationExecutionId },

    /// Lists committed execution projections.
    #[operation(name = "listExecutions", kind = "query", response = ExecutionsListOutput, response_variant = "ExecutionsList")]
    ListExecutions,

    /// Reads the schema active at the requested committed height.
    #[operation(name = "getSchema", kind = "query", response = SchemaOutput, response_variant = "Schema")]
    GetSchema { at: Height },

    /// Lists committed schema activations in chain order.
    #[operation(name = "getSchemaHistory", kind = "query", response = SchemaHistoryOutput, response_variant = "SchemaHistory")]
    GetSchemaHistory,

    /// Reads the chain identifier and committed tip.
    #[operation(name = "getChain", kind = "query", response = ChainOutput, response_variant = "Chain")]
    GetChain,

    /// Reads the committed regenesis record, if present.
    #[operation(name = "getRegenesis", kind = "query", response = RegenesisOutput, response_variant = "Regenesis")]
    GetRegenesis,
}

/// Coordination server lifecycle notifications.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinationBroadcast {
    /// The coordination engine has started.
    ServerStarted,
    /// The coordination engine and its transport have stopped.
    ServerStopped,
}

/// Responses produced by coordination operations.
#[aves::bus_responses]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinationResponse {
    /// Coordination server startup completed.
    ServerStarted,
    /// Coordination server shutdown completed.
    ServerStopped,

    /// The chain host durably admitted the signed record; admission is not commitment.
    TransactionSubmitted { event_id: CoordinationEventId },
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

//! Typed request and response contracts for Decide.

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::bus::BusRpcContext;
use aves::flow::{LifecycleCorrelation, NodeExecutionId};
use aves::utils::uuid::SpectaUuid;

use super::domain::DecideDomain;
use super::types::{
    BoolAnswer, BoolQuestion, ChoiceAnswer, ChoiceQuestion, DecideContent, DecideCredential, DecideError,
    DecideNodeConfig, DecideNodeTickResult, ScoreAnswer, ScoreQuestion, empty_decide_credential,
};

/// Internal actor request and reply channel.
pub(super) struct DecideRequest {
    pub(super) payload: DecideRequestPayload,
    pub(super) reply_to: oneshot::Sender<Result<DecideResponse, DecideError>>,
}

/// Public decision requests and privileged runtime controls.
#[aves::bus_requests(marker = DecideDomain, context = BusRpcContext, root_error = DecideError, domain_error = DecideError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DecideRequestPayload {
    /// Starts accepting Decide asks and node starts.
    #[operation(name = "startServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StartServer,
    /// Ask a bounded boolean question.
    #[operation(name = "askBool", kind = "mutation", response = BoolOutput, response_variant = "Bool")]
    AskBool {
        provider: String,
        model: String,
        state: DecideContent,
        question: BoolQuestion,
        credential_id: Option<SpectaUuid>,
        /// Optional override; with credentialId it must canonically equal the credential's authorized binding or fail with EndpointUnauthorized.
        endpoint: Option<String>,
        /// Per-attempt timeout in seconds; absent or zero defaults to 300, maximum 3600.
        timeout_secs: Option<u64>,
    },
    /// Ask a bounded choice question.
    #[operation(name = "askChoice", kind = "mutation", response = ChoiceOutput, response_variant = "Choice")]
    AskChoice {
        provider: String,
        model: String,
        state: DecideContent,
        question: ChoiceQuestion,
        credential_id: Option<SpectaUuid>,
        /// Optional override; with credentialId it must canonically equal the credential's authorized binding or fail with EndpointUnauthorized.
        endpoint: Option<String>,
        /// Per-attempt timeout in seconds; absent or zero defaults to 300, maximum 3600.
        timeout_secs: Option<u64>,
    },
    /// Ask a bounded score question.
    #[operation(name = "askScore", kind = "mutation", response = ScoreOutput, response_variant = "Score")]
    AskScore {
        provider: String,
        model: String,
        state: DecideContent,
        question: ScoreQuestion,
        credential_id: Option<SpectaUuid>,
        /// Optional override; with credentialId it must canonically equal the credential's authorized binding or fail with EndpointUnauthorized.
        endpoint: Option<String>,
        /// Per-attempt timeout in seconds; absent or zero defaults to 300, maximum 3600.
        timeout_secs: Option<u64>,
    },
    /// Supply an application-owned credential to the memory-only store.
    #[privileged(reason = "Application-owned decision credential supply")]
    SupplyCredential {
        credential_id: SpectaUuid,
        #[serde(skip, default = "empty_decide_credential")]
        credential: DecideCredential,
    },
    /// Remove a credential from the memory-only store.
    #[privileged(reason = "Application-owned decision credential removal")]
    RemoveCredential { credential_id: SpectaUuid },
    /// Start a Flow node execution.
    #[privileged(reason = "Decide node lifecycle is admitted only from Aves Flow")]
    StartNode {
        correlation: LifecycleCorrelation,
        config: DecideNodeConfig,
    },
    /// Tick a Flow node execution.
    #[privileged(reason = "Decide node lifecycle is admitted only from Aves Flow")]
    TickNode { correlation: LifecycleCorrelation },
    /// Cancel a Flow node execution.
    #[privileged(reason = "Decide node cancellation is admitted only from Aves Flow")]
    CancelNode { node_execution_id: NodeExecutionId },
    /// Stops accepting new work and cancels in-flight asks and node executions; supplied credentials are retained.
    #[operation(name = "stopServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StopServer,
}

/// Results returned by Decide requests.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DecideResponse {
    Bool {
        answer: BoolAnswer,
        model: String,
    },
    Choice {
        answer: ChoiceAnswer,
        model: String,
    },
    Score {
        answer: ScoreAnswer,
        model: String,
    },
    Success,
    NodeStarted {
        correlation: LifecycleCorrelation,
    },
    NodeTicked {
        correlation: LifecycleCorrelation,
        result: DecideNodeTickResult,
    },
}

/// Uninhabited bus registration marker; Decide emits no broadcasts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DecideBroadcast {}

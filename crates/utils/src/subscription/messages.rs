//! Typed request and response contracts for Subscription.

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::bus::BusRpcContext;

use super::domain::SubscriptionDomain;
use super::types::{SavedResetOutcome, SubscriptionError};

/// Internal actor request and reply channel.
pub(super) struct SubscriptionRequest {
    pub(super) payload: SubscriptionRequestPayload,
    pub(super) reply_to: oneshot::Sender<Result<SubscriptionResponse, SubscriptionError>>,
}

/// Subscription lifecycle and allowance requests.
#[aves::bus_requests(marker = SubscriptionDomain, context = BusRpcContext, root_error = SubscriptionError, domain_error = SubscriptionError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionRequestPayload {
    /// Starts provider allowance observation.
    #[operation(name = "startServer", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartServer,
    /// Stops provider allowance observation.
    #[operation(name = "stopServer", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopServer,
    /// Spend one saved reset on the specified observed account.
    #[privileged(reason = "spends a saved provider reset on the user's account")]
    UseSavedReset {
        /// Insula provider identifier.
        provider: String,
        /// Account label from the allowance observation.
        account: String,
        /// Stable caller-supplied identity for this redemption attempt.
        request_id: String,
        /// Provider-defined reset kind, when required.
        kind: Option<String>,
    },
}

/// Results returned by Subscription requests.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionResponse {
    /// Allowance observation is running.
    ServerStarted,
    /// Allowance observation has stopped.
    ServerStopped,
    /// Provider decision for the requested reset.
    SavedResetUsed {
        outcome: SavedResetOutcome,
        decided_at_ms: i64,
    },
}

/// Events broadcast by the Subscription domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionBroadcast {
    /// Allowance observation is running.
    ServerStarted,
    /// Allowance observation has stopped.
    ServerStopped,
}

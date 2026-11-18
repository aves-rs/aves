//! Typed request and response contracts for Subscription.

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::bus::BusRpcContext;

use super::domain::SubscriptionDomain;
use super::types::SubscriptionError;

/// Internal actor request and reply channel.
pub(super) struct SubscriptionRequest {
    pub(super) payload: SubscriptionRequestPayload,
    pub(super) reply_to: oneshot::Sender<Result<SubscriptionResponse, SubscriptionError>>,
}

/// Subscription lifecycle requests.
#[aves::bus_requests(marker = SubscriptionDomain, context = BusRpcContext, root_error = SubscriptionError, domain_error = SubscriptionError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionRequestPayload {
    /// Starts provider allowance observation.
    #[operation(name = "startServer", kind = "mutation", response = ServerStartedOutput, response_variant = "ServerStarted")]
    StartServer,
    /// Stops provider allowance observation.
    #[operation(name = "stopServer", kind = "mutation", response = ServerStoppedOutput, response_variant = "ServerStopped")]
    StopServer,
}

/// Results returned by Subscription requests.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionResponse {
    /// Allowance observation is running.
    ServerStarted,
    /// Allowance observation has stopped.
    ServerStopped,
}

/// Events broadcast by the Subscription domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionBroadcast {
    /// Allowance observation is running.
    ServerStarted,
    /// Allowance observation has stopped.
    ServerStopped,
}

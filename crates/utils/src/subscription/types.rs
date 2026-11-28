//! Subscription domain errors.

use aves::auth::AvesAuthError;
use aves::bus::DispatchContextError;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;
use quota_core::codex_resets::{ConsumeOutcome, SavedResetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use specta::Type;
use thiserror::Error;

/// The provider's decision when asked to spend a saved reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SavedResetOutcome {
    /// A saved reset was spent.
    Reset,
    /// The account had no usage eligible for a reset.
    NothingToReset,
    /// No saved reset was available.
    NoCredit,
    /// This request had already redeemed a reset.
    AlreadyRedeemed,
}

impl From<ConsumeOutcome> for SavedResetOutcome {
    fn from(outcome: ConsumeOutcome) -> Self {
        match outcome {
            ConsumeOutcome::Reset => Self::Reset,
            ConsumeOutcome::NothingToReset => Self::NothingToReset,
            ConsumeOutcome::NoCredit => Self::NoCredit,
            ConsumeOutcome::AlreadyRedeemed => Self::AlreadyRedeemed,
        }
    }
}

/// Serializable errors at the Subscription domain boundary.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum SubscriptionError {
    /// There is no running allowance observer.
    #[error("Subscription observation is not running")]
    NotRunning,
    /// Insula cannot redeem a reset for this provider.
    #[error("Provider does not support saved resets")]
    ResetUnsupportedProvider,
    /// The requested account isn't known to the live observer.
    #[error("No observed account matches the saved-reset request")]
    ResetUnknownAccount,
    /// Insula refused another reset too soon after a prior redemption.
    #[error("Account recently redeemed a saved reset")]
    ResetRecentlyUsed { last_reset_at_ms: i64 },
    /// The provider refused the reset request.
    #[error("Provider refused the saved reset")]
    ResetRefused,
    /// Insula could not obtain a definitive provider response.
    #[error("Saved-reset service is unavailable")]
    ResetUnavailable,
    /// The supplied reset request is not valid for the provider.
    #[error("Invalid saved-reset request")]
    ResetInvalidRequest,
    /// The provider imposed its own cooldown.
    #[error("Provider cooldown blocks the saved reset")]
    ResetCooldown { retry_at_ms: Option<i64> },
    /// The provider returned a bounded refusal result and optional reason code.
    #[error("Provider declined the saved reset")]
    ResetDeclined { result: String, reason: Option<String> },
    /// The bus rejected or could not route a request.
    #[error("Bus error")]
    Bus { message: String },
    /// The domain input channel is closed.
    #[error("Channel send error")]
    ChannelSend,
    /// The reply channel closed before a response arrived.
    #[error("Oneshot reply channel closed")]
    OneshotReply,
    /// A request or response failed to serialize.
    #[error("Serialization failed")]
    Serialization { message: String },
    /// Shared runtime plumbing failed.
    #[error("Infrastructure error")]
    Infrastructure {
        kind: String,
        target_domain: String,
        operation: String,
        message: String,
    },
}

impl SubscriptionError {
    /// Stable low-cardinality RPC code.
    pub fn rpc_code(&self) -> &'static str {
        match self {
            Self::NotRunning => "not_running",
            Self::ResetUnsupportedProvider => "reset_unsupported_provider",
            Self::ResetUnknownAccount => "reset_unknown_account",
            Self::ResetRecentlyUsed { .. } => "reset_recently_used",
            Self::ResetRefused => "reset_refused",
            Self::ResetUnavailable => "reset_unavailable",
            Self::ResetInvalidRequest => "reset_invalid_request",
            Self::ResetCooldown { .. } => "reset_cooldown",
            Self::ResetDeclined { .. } => "reset_declined",
            Self::Bus { .. } => "bus",
            Self::ChannelSend => "channel_send",
            Self::OneshotReply => "oneshot_reply",
            Self::Serialization { .. } => "serialization",
            Self::Infrastructure { .. } => "infrastructure",
        }
    }
}

impl From<SavedResetError> for SubscriptionError {
    fn from(error: SavedResetError) -> Self {
        match error {
            SavedResetError::UnsupportedProvider => Self::ResetUnsupportedProvider,
            SavedResetError::UnknownAccount => Self::ResetUnknownAccount,
            SavedResetError::RecentlyReset(last_reset_at) => Self::ResetRecentlyUsed {
                last_reset_at_ms: last_reset_at.timestamp_millis(),
            },
            SavedResetError::ProviderRefused(_) => Self::ResetRefused,
            SavedResetError::ProviderUnavailable(_) => Self::ResetUnavailable,
            SavedResetError::InvalidRequest(_) => Self::ResetInvalidRequest,
            SavedResetError::ProviderCooldown(retry_at) => Self::ResetCooldown {
                retry_at_ms: retry_at.map(|at| at.timestamp_millis()),
            },
            SavedResetError::ProviderDeclined { result, reason } => Self::ResetDeclined { result, reason },
        }
    }
}

impl From<SharedInfraError> for SubscriptionError {
    fn from(error: SharedInfraError) -> Self {
        let (kind, target_domain, operation, message) = match error {
            SharedInfraError::ChannelSend {
                target_domain,
                operation,
                cause,
            } => ("channel_send", target_domain, operation, cause),
            SharedInfraError::ChannelRecv {
                target_domain,
                operation,
                cause,
            } => ("channel_recv", target_domain, operation, cause),
            SharedInfraError::OneshotReplyTimeout {
                target_domain,
                operation,
                timeout,
            } => ("oneshot_reply_timeout", target_domain, operation, timeout),
            SharedInfraError::OneshotReplyMissing {
                target_domain,
                operation,
                cause,
            } => ("oneshot_reply_missing", target_domain, operation, cause),
        };
        Self::Infrastructure {
            kind: kind.to_owned(),
            target_domain,
            operation,
            message,
        }
    }
}

impl From<GeneratedCallError<SubscriptionError>> for SubscriptionError {
    fn from(error: GeneratedCallError<SubscriptionError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => Self::Bus {
                message: error.to_string(),
            },
            GeneratedCallError::InputClosed => Self::ChannelSend,
            GeneratedCallError::ReplyDropped => Self::OneshotReply,
            GeneratedCallError::UnexpectedResponse { expected, actual } => Self::Bus {
                message: format!("expected {expected}, got {actual}"),
            },
        }
    }
}

impl From<AvesAuthError> for SubscriptionError {
    fn from(error: AvesAuthError) -> Self {
        Self::Bus {
            message: error.to_string(),
        }
    }
}

impl From<DispatchContextError> for SubscriptionError {
    fn from(error: DispatchContextError) -> Self {
        Self::Bus {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for SubscriptionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization {
            message: error.to_string(),
        }
    }
}

impl rspc::Error for SubscriptionError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        let safe_error = serde_json::json!({
            "DomainError": {
                "domain": "subscription",
                "code": self.rpc_code(),
                "message": self.to_string(),
                "details": {},
            }
        });
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(safe_error, None::<Self>))
    }
}

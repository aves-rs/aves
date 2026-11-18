//! Subscription domain errors.

use aves::auth::AvesAuthError;
use aves::bus::DispatchContextError;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use specta::Type;
use thiserror::Error;

/// Serializable errors at the Subscription domain boundary.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum SubscriptionError {
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
            Self::Bus { .. } => "bus",
            Self::ChannelSend => "channel_send",
            Self::OneshotReply => "oneshot_reply",
            Self::Serialization { .. } => "serialization",
            Self::Infrastructure { .. } => "infrastructure",
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

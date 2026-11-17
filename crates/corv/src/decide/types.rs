//! Provider-neutral decision contracts and credential custody rules.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::net::IpAddr;
use std::time::Duration;

use aves::auth::AvesAuthError;
use aves::bus::DispatchContextError;
use aves::flow::NodeExecutionId;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;
use aves::utils::uuid::SpectaUuid;
use reqwest::Url;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use specta::Type;
use thiserror::Error;
use zeroize::Zeroizing;

use super::plugins::{DecideAuth, decide_providers};

/// Structured or textual content supplied with a decision.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Type, JsonSchema)]
#[serde(untagged)]
pub enum DecideContent {
    Text(String),
    Object(BTreeMap<String, serde_json::Value>),
    Array(Vec<serde_json::Value>),
}

/// Serializable errors at the Decide domain boundary.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub enum DecideError {
    #[error("Unknown provider")]
    UnknownProvider { provider: String },
    #[error("Credential unavailable")]
    CredentialUnavailable { credential_id: SpectaUuid },
    /// The requested provider does not own the supplied credential.
    #[error("Provider does not match credential")]
    ProviderCredentialMismatch {
        credential_id: SpectaUuid,
        provider: String,
    },
    #[error("Authentication required")]
    AuthenticationRequired,
    #[error("The supplied endpoint does not match the credential's authorized binding")]
    EndpointUnauthorized,
    #[error("Invalid question")]
    InvalidQuestion { message: String },
    #[error("Provider request can be retried")]
    ProviderRetryable {
        retry_after_seconds: Option<u64>,
        message: String,
    },
    #[error("Provider request failed")]
    ProviderRequestFailed { message: String },
    #[error("Provider response invalid")]
    ProviderResponseInvalid { message: String },
    #[error("Decision capacity reached")]
    Busy,
    #[error("Invalid timeout {value}; maximum is {maximum}")]
    TimeoutInvalid { value: u64, maximum: u64 },
    #[error("Node idempotency conflict")]
    NodeIdempotencyConflict { node_execution_id: NodeExecutionId },
    #[error("Node completion not found")]
    NodeCompletionNotFound { node_execution_id: NodeExecutionId },
    #[error("Node completion dropped")]
    NodeCompletionDropped,
    #[error("Node cancelled")]
    NodeCancelled { node_execution_id: NodeExecutionId },
    #[error("Decide is shutting down")]
    ShuttingDown,
    #[error("Validation failed")]
    Validation { message: String },
    #[error("Bus error")]
    Bus { message: String },
    #[error("Channel send error")]
    ChannelSend,
    #[error("Channel receive error")]
    ChannelRecv,
    #[error("Oneshot reply channel closed")]
    OneshotReply,
    #[error("Infrastructure error")]
    Infrastructure {
        kind: String,
        target_domain: String,
        operation: String,
        message: String,
    },
}

impl DecideError {
    /// Stable low-cardinality RPC code.
    pub fn rpc_code(&self) -> &'static str {
        match self {
            Self::UnknownProvider { .. } => "unknown_provider",
            Self::CredentialUnavailable { .. } => "credential_unavailable",
            Self::ProviderCredentialMismatch { .. } => "provider_credential_mismatch",
            Self::AuthenticationRequired => "authentication_required",
            Self::EndpointUnauthorized => "endpoint_unauthorized",
            Self::InvalidQuestion { .. } => "invalid_question",
            Self::ProviderRetryable { .. } => "provider_retryable",
            Self::ProviderRequestFailed { .. } => "provider_request_failed",
            Self::ProviderResponseInvalid { .. } => "provider_response_invalid",
            Self::Busy => "busy",
            Self::TimeoutInvalid { .. } => "timeout_invalid",
            Self::NodeIdempotencyConflict { .. } => "node_idempotency_conflict",
            Self::NodeCompletionNotFound { .. } => "node_completion_not_found",
            Self::NodeCompletionDropped => "node_completion_dropped",
            Self::NodeCancelled { .. } => "node_cancelled",
            Self::ShuttingDown => "shutting_down",
            Self::Validation { .. } => "validation",
            Self::Bus { .. } => "bus",
            Self::ChannelSend => "channel_send",
            Self::ChannelRecv => "channel_recv",
            Self::OneshotReply => "oneshot_reply",
            Self::Infrastructure { .. } => "infrastructure",
        }
    }
}

impl From<SharedInfraError> for DecideError {
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

impl From<GeneratedCallError<DecideError>> for DecideError {
    fn from(error: GeneratedCallError<DecideError>) -> Self {
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

impl From<AvesAuthError> for DecideError {
    fn from(error: AvesAuthError) -> Self {
        Self::Bus {
            message: error.to_string(),
        }
    }
}

impl From<DispatchContextError> for DecideError {
    fn from(error: DispatchContextError) -> Self {
        Self::Bus {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for DecideError {
    fn from(error: serde_json::Error) -> Self {
        Self::Validation {
            message: error.to_string(),
        }
    }
}

impl rspc::Error for DecideError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        let safe_error = serde_json::json!({
            "DomainError": {
                "domain": "decide",
                "code": self.rpc_code(),
                "message": self.to_string(),
                "details": {},
            }
        });
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(safe_error, None::<Self>))
    }
}

/// A probability question about a true outcome.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BoolQuestion {
    #[serde(default)]
    pub instructions: Option<DecideContent>,
    #[serde(default)]
    pub true_description: Option<DecideContent>,
    #[serde(default)]
    pub false_description: Option<DecideContent>,
}

impl BoolQuestion {
    /// Validate the neutral question contract.
    pub fn validate(&self) -> Result<(), DecideError> {
        Ok(())
    }
}

/// A selection from named criteria.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChoiceQuestion {
    #[serde(default)]
    pub instructions: Option<DecideContent>,
    pub criteria: BTreeMap<String, Option<DecideContent>>,
}

impl ChoiceQuestion {
    /// Validate the count and names of criteria.
    pub fn validate(&self) -> Result<(), DecideError> {
        if !(1..=255).contains(&self.criteria.len()) || self.criteria.keys().any(|name| name.trim().is_empty()) {
            return Err(DecideError::InvalidQuestion {
                message: "Choice requires 1 to 255 non-blank criterion names".into(),
            });
        }
        Ok(())
    }
}

/// Ordered scoring levels, from low to high.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScoreQuestion {
    #[serde(default)]
    pub instructions: Option<DecideContent>,
    pub criteria: Vec<DecideContent>,
}

impl ScoreQuestion {
    /// Validate the number of levels.
    pub fn validate(&self) -> Result<(), DecideError> {
        if !(1..=10).contains(&self.criteria.len()) {
            return Err(DecideError::InvalidQuestion {
                message: "Score requires 1 to 10 levels".into(),
            });
        }
        Ok(())
    }
}

/// One bounded question.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DecideQuestion {
    Bool(BoolQuestion),
    Choice(ChoiceQuestion),
    Score(ScoreQuestion),
}

/// Probability of true, not a thresholded boolean or confidence.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BoolAnswer {
    /// Probability of true in [0, 1].
    pub probability: f64,
}

/// Selection and its distribution; confidence is not calibrated correctness or comparable across providers.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChoiceAnswer {
    pub choice: String,
    /// Distribution concentration, not calibrated correctness or comparable across providers.
    pub confidence: f64,
    pub probabilities: BTreeMap<String, f64>,
}

/// Scoring result and its distribution.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScoreAnswer {
    /// Fractional level index in [0, n−1], never rounded or normalized.
    pub score: f64,
    /// Distribution concentration, not calibrated correctness or comparable across providers.
    pub confidence: f64,
    pub legend: BTreeMap<String, DecideContent>,
    pub probabilities: BTreeMap<String, f64>,
}

/// Answer corresponding to the question kind.
#[derive(Serialize, Deserialize, Clone, Debug, Type, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DecideAnswer {
    Bool(BoolAnswer),
    Choice(ChoiceAnswer),
    Score(ScoreAnswer),
}

/// Answer and the model that actually produced it.
#[derive(Clone, Debug)]
pub struct DecideOutcome {
    pub model: String,
    pub answer: DecideAnswer,
}

/// Secret and its application-authorized endpoint binding; never serialized.
#[derive(Clone)]
pub struct DecideCredential {
    pub provider: String,
    pub api_key: Option<Zeroizing<String>>,
    pub endpoint: Url,
}

impl DecideCredential {
    /// Validate provider authentication and bind the credential to its endpoint.
    pub fn new(provider_id: &str, api_key: Option<Zeroizing<String>>, endpoint: &str) -> Result<Self, DecideError> {
        let provider = decide_providers()
            .find(|provider| provider.id == provider_id)
            .ok_or_else(|| DecideError::UnknownProvider {
                provider: provider_id.to_owned(),
            })?;
        let api_key = api_key.filter(|key| !key.trim().is_empty());
        if provider.credential == DecideAuth::Required && api_key.is_none() {
            return Err(DecideError::AuthenticationRequired);
        }
        let endpoint = validate_keyless_endpoint(endpoint)?;
        if api_key.is_some() {
            validate_credentialed_endpoint(&endpoint)?;
        }
        Ok(Self {
            provider: provider_id.to_owned(),
            api_key,
            endpoint,
        })
    }
}

impl Debug for DecideCredential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("DecideCredential(<redacted>)")
    }
}

/// Serde default for a skipped privileged field; supply validation prevents custody of this empty key.
pub fn empty_decide_credential() -> DecideCredential {
    DecideCredential {
        provider: String::new(),
        api_key: None,
        endpoint: Url::parse("http://127.0.0.1/").expect("static loopback URL is valid"),
    }
}

fn validate_endpoint_parts(url: &Url) -> Result<(), DecideError> {
    if !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DecideError::Validation {
            message: "Endpoint must have a host and no userinfo, query, or fragment".into(),
        });
    }
    Ok(())
}

/// Require TLS except for numeric loopback addresses, and reject URL credentials and suffixes.
pub fn validate_credentialed_endpoint(url: &Url) -> Result<(), DecideError> {
    validate_endpoint_parts(url)?;
    let loopback = url.host().is_some_and(|host| {
        host.to_string()
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(DecideError::Validation {
            message: "Credentialed endpoint requires HTTPS or numeric loopback HTTP".into(),
        });
    }
    Ok(())
}

/// Parse and validate an endpoint that does not carry a credential.
pub fn validate_keyless_endpoint(raw: &str) -> Result<Url, DecideError> {
    let url = Url::parse(raw).map_err(|_| DecideError::Validation {
        message: "Invalid endpoint".into(),
    })?;
    validate_endpoint_parts(&url)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DecideError::Validation {
            message: "Endpoint requires HTTP or HTTPS".into(),
        });
    }
    Ok(url)
}

/// Canonical URL serialization for exact endpoint-binding comparison.
pub fn canonical_endpoint(url: &Url) -> String {
    url.as_str().to_owned()
}

/// Flow configuration for a boolean question.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BoolNodeConfig {
    pub(super) model: String,
    pub(super) credential_id: SpectaUuid,
    #[serde(default)]
    pub(super) timeout_secs: u64,
    pub(super) state: DecideContent,
    pub(super) question: BoolQuestion,
}

/// Flow configuration for a choice question.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ChoiceNodeConfig {
    pub(super) model: String,
    pub(super) credential_id: SpectaUuid,
    #[serde(default)]
    pub(super) timeout_secs: u64,
    pub(super) state: DecideContent,
    pub(super) question: ChoiceQuestion,
}

/// Flow configuration for a score question.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ScoreNodeConfig {
    pub(super) model: String,
    pub(super) credential_id: SpectaUuid,
    #[serde(default)]
    pub(super) timeout_secs: u64,
    pub(super) state: DecideContent,
    pub(super) question: ScoreQuestion,
}

/// Typed Flow configuration envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum DecideNodeConfig {
    Bool(BoolNodeConfig),
    Choice(ChoiceNodeConfig),
    Score(ScoreNodeConfig),
}

/// Immediate Flow tick result.
#[derive(Clone, Debug, Serialize, Deserialize, Type, JsonSchema)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum DecideNodeTickResult {
    Running,
    Succeeded { output: serde_json::Value },
    Failed { error: DecideError },
}

/// Resolved request; the endpoint is always present when a credential is supplied.
#[derive(Clone, Debug)]
pub struct DecideAskRequest {
    pub model: String,
    pub endpoint: Option<Url>,
    pub timeout: Duration,
    pub state: DecideContent,
    pub question: DecideQuestion,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_rejects_scalar_values_other_than_text() {
        for value in [serde_json::json!(1), serde_json::json!(true), serde_json::Value::Null] {
            assert!(serde_json::from_value::<DecideContent>(value).is_err());
        }
        for value in [serde_json::json!("text"), serde_json::json!({}), serde_json::json!([])] {
            assert!(serde_json::from_value::<DecideContent>(value).is_ok());
        }
    }

    #[test]
    fn endpoint_custody_restricts_secrets_and_normalizes_bindings() {
        for raw in [
            "http://localhost/",
            "http://192.0.2.1/",
            "https://user@host/",
            "https://host/?q=1",
            "https://host/#f",
        ] {
            assert!(validate_credentialed_endpoint(&Url::parse(raw).unwrap()).is_err());
        }
        for raw in ["http://127.0.0.1/", "http://[::1]/", "https://example.com/"] {
            assert!(validate_credentialed_endpoint(&Url::parse(raw).unwrap()).is_ok());
        }
        assert!(validate_keyless_endpoint("http://localhost/").is_ok());
        assert!(validate_keyless_endpoint("https://user:pass@host/").is_err());
        assert!(validate_keyless_endpoint("file:///path").is_err());
        assert_eq!(
            canonical_endpoint(&Url::parse("HTTPS://EXAMPLE.COM:443").unwrap()),
            "https://example.com/"
        );
        assert_eq!(
            format!("{:?}", empty_decide_credential()),
            "DecideCredential(<redacted>)"
        );
    }

    #[test]
    fn credential_constructor_enforces_provider_and_key_admission() {
        assert!(matches!(
            DecideCredential::new("unknown", None, "http://localhost"),
            Err(DecideError::UnknownProvider { .. })
        ));
        for key in [None, Some(Zeroizing::new("  ".into()))] {
            assert!(matches!(
                DecideCredential::new("jev", key, "https://example.com"),
                Err(DecideError::AuthenticationRequired)
            ));
        }
        let keyless = DecideCredential::new("ollama", Some(Zeroizing::new("  ".into())), "http://localhost").unwrap();
        assert!(keyless.api_key.is_none());
        assert!(DecideCredential::new("ollama", Some(Zeroizing::new("key".into())), "http://localhost").is_err());
        assert!(DecideCredential::new("ollama", Some(Zeroizing::new("key".into())), "https://example.com").is_ok());
        assert!(matches!(
            DecideCredential::new("", None, "http://127.0.0.1"),
            Err(DecideError::UnknownProvider { .. })
        ));
    }
}

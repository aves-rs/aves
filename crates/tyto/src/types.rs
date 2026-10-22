use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::Error as IoError;
use std::sync::Arc;

use chrono::Utc;
use rspc::{Error as RspcError, ProcedureError, ResolverError};
use schemars::JsonSchema;
use serde::de::Error as SerdeDeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Error as JsonError, Map, Value, json};
use specta::Type;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use uuid::Uuid;
use zeroize::Zeroizing;

use aves::ErrorCode;
use aves::bus::DispatchContextError;
use aves::flow::FlowError;
use aves::runtime::GeneratedCallError;
use aves::utils::{bigint::SpectaBigInt, integers::IntegerCastError, uuid::SpectaUuid};

use super::channels::MessagingSubscription;
use super::database::StoreError;
use super::messages::{ListenerType, MessagingResponse};

/// Single-provider settings projected from one persisted connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema, Default)]
pub(super) struct MessagingConfig {
    /// Optional Telegram listener settings.
    pub(super) telegram: Option<TelegramConfig>,

    /// Optional Slack listener settings.
    pub(super) slack: Option<SlackConfig>,

    /// Optional email listener settings.
    pub(super) email: Option<EmailConfig>,
}

/// Authentication supplied through the trusted host connector boundary only.
/// This type deliberately has no serialization or public schema implementation.
#[derive(Clone)]
pub enum MessagingCredentials {
    /// Telegram Bot API authentication.
    Telegram { token: Zeroizing<String> },

    /// Slack Socket Mode and Web API authentication.
    Slack {
        app_token: Zeroizing<String>,
        bot_token: Zeroizing<String>,
    },

    /// Email mailbox authentication.
    Email {
        username: Zeroizing<String>,
        password: Zeroizing<String>,
    },
}

impl Debug for MessagingCredentials {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("MessagingCredentials(<redacted>)")
    }
}

/// Tyto-owned configuration persisted in the generic Flow trigger record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionTriggerConfig {
    /// UUID referencing the persisted messaging connection.
    pub connection_id: SpectaUuid,

    /// None receives all accessible channels; Some must contain at least one identity.
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_subscribed_channels")]
    pub subscribed_channels: Option<Vec<String>>,

    /// Protocol-native sender identities allowed to fire this trigger; empty means unrestricted.
    #[serde(default)]
    pub allowed_senders: Vec<String>,

    /// Optional handoff of accepted inbound messages to Knowledge.
    #[serde(default)]
    pub persistence: PersistencePolicy,
}

/// Optional Knowledge destination for an accepted message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema, Default)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PersistencePolicy {
    /// Do not persist the message.
    #[default]
    None,

    /// Create a Knowledge note under this explicit vault-relative directory.
    Note {
        /// Vault-relative directory; no implicit directory is selected.
        directory: String,
    },

    /// Store a Knowledge Data object under this explicit namespace.
    Data {
        /// Namespace; no implicit namespace is selected.
        namespace: String,
    },
}

/// Generates a collision-resistant Knowledge destination without using message text.
pub(super) fn persistence_identifier(scope: &str) -> (String, String) {
    let id = Uuid::new_v4();
    let date = Utc::now().format("%Y-%m-%d");
    (format!("{scope}/{id}"), format!("{date}-{id}.md"))
}

/// Provider-specific destination referencing a persisted connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessagingSenderConfig {
    /// Email sender address.
    Email(EmailSenderConfig),

    /// Slack destination channel.
    Slack(SlackSenderConfig),

    /// Telegram destination chat.
    Telegram(TelegramSenderConfig),
}

impl MessagingSenderConfig {
    /// Returns the provider, referenced connection, and borrowed persistence policy.
    pub(super) fn connection_info(&self) -> (ListenerType, SpectaUuid, &PersistencePolicy) {
        match self {
            Self::Email(config) => (ListenerType::Email, config.connection_id, &config.persistence),
            Self::Slack(config) => (ListenerType::Slack, config.connection_id, &config.persistence),
            Self::Telegram(config) => (ListenerType::Telegram, config.connection_id, &config.persistence),
        }
    }

    /// Builds a sender from provider destination fields, defaulting missing strings to empty.
    pub(super) fn from_destination(provider: ListenerType, connection_id: SpectaUuid, destination: &Value) -> Self {
        let field = |name| {
            destination
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        match provider {
            ListenerType::Email => Self::Email(EmailSenderConfig {
                connection_id,
                from: field("from"),
                persistence: PersistencePolicy::None,
            }),
            ListenerType::Slack => Self::Slack(SlackSenderConfig {
                connection_id,
                channel_id: field("channel_id"),
                persistence: PersistencePolicy::None,
            }),
            ListenerType::Telegram => Self::Telegram(TelegramSenderConfig {
                connection_id,
                chat_id: field("chat_id"),
                persistence: PersistencePolicy::None,
            }),
        }
    }
}

/// Email destination fields referencing a persisted messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmailSenderConfig {
    /// UUID referencing the persisted messaging connection.
    pub connection_id: SpectaUuid,

    /// Explicit sender address; never inferred from credential username.
    pub from: String,

    /// Optional persistence destination for accepted outbound messages.
    #[serde(default)]
    pub persistence: PersistencePolicy,
}

/// Slack destination fields referencing a persisted messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlackSenderConfig {
    /// UUID referencing the persisted messaging connection.
    pub connection_id: SpectaUuid,

    /// Destination channel identifier.
    pub channel_id: String,

    /// Optional persistence destination for accepted outbound messages.
    #[serde(default)]
    pub persistence: PersistencePolicy,
}

/// Telegram destination fields referencing a persisted messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TelegramSenderConfig {
    /// UUID referencing the persisted messaging connection.
    pub connection_id: SpectaUuid,

    /// Destination chat identifier.
    pub chat_id: String,

    /// Optional persistence destination for accepted outbound messages.
    #[serde(default)]
    pub persistence: PersistencePolicy,
}

/// Configuration for a Flow node that sends one outbound message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MessageNodeConfig {
    pub(super) connection_id: SpectaUuid,
    pub(super) provider: ListenerType,
    pub(super) destination: Value,
    pub(super) body: String,
    #[serde(default)]
    pub(super) metadata: Value,
}

/// Permission Flow node input with a provider destination and decision deadline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PermissionNodeConfig {
    pub(super) connection_id: SpectaUuid,
    pub(super) provider: ListenerType,
    pub(super) destination: Value,
    pub(super) body: String,
    #[serde(default)]
    pub(super) metadata: Value,
    pub(super) duration_seconds: u64,
}

/// Email-specific normalized-message metadata used to build an SMTP message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub(super) struct EmailSendMetadata {
    /// Primary recipients.
    pub(super) to: Vec<String>,

    /// Carbon-copy recipients.
    pub(super) cc: Vec<String>,

    /// Blind-carbon-copy recipients.
    pub(super) bcc: Vec<String>,

    /// Optional subject line.
    pub(super) subject: Option<String>,
}

/// Slack outbound metadata is empty; destination is part of its sender config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct SlackSendMetadata {}

/// Telegram outbound metadata is empty; destination is part of its sender config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct TelegramSendMetadata {}

/// Provider acceptance details for a successfully accepted message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessagingSendReceipt {
    /// Provider-assigned message identifier.
    ProviderMessageId { message_id: String },

    /// SMTP accepted the message but supplied no provider message identifier.
    SmtpAccepted { response: String },
}

/// Stable category for a typed outbound Send failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MessagingSendErrorCode {
    /// Transient transport failure.
    Transport,

    /// Provider server failure.
    Server,

    /// Provider rate limit.
    RateLimited,

    /// Invalid or expired credentials.
    InvalidCredentials,

    /// Invalid or unavailable destination.
    InvalidDestination,

    /// Permission denied by the provider.
    PermissionDenied,

    /// Invalid sender configuration or metadata.
    Validation,
}

/// Typed outbound Send failures with stable messaging-send error identity.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema, ErrorCode)]
pub enum MessagingSendError {
    /// Transient transport/server failure that may be retried.
    #[error("message send retryable failure: {message}")]
    Retryable {
        /// Stable failure category.
        code: MessagingSendErrorCode,

        /// Safe human-readable failure description.
        message: String,

        /// Optional provider Retry-After delay in seconds.
        retry_after_seconds: Option<u64>,
    },
    /// Invalid credentials, destination, permission, or message validation.
    #[error("message send terminal failure: {message}")]
    Terminal {
        /// Stable failure category.
        code: MessagingSendErrorCode,

        /// Safe human-readable failure description.
        message: String,
    },
}

/// Rejects explicit channel filters that cannot identify any valid channel.
fn deserialize_subscribed_channels<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error> {
    let channels = Option::<Vec<String>>::deserialize(deserializer)?;
    if channels
        .as_ref()
        .is_some_and(|channels| channels.is_empty() || channels.iter().any(|channel| channel.trim().is_empty()))
    {
        return Err(SerdeDeError::custom(
            "An explicit channel selection must contain non-empty channel identities",
        ));
    }
    Ok(channels)
}

impl From<&SubscriptionTriggerConfig> for MessagingSubscription {
    fn from(trigger: &SubscriptionTriggerConfig) -> Self {
        Self {
            subscribed_channels: trigger.subscribed_channels.clone(),
            allowed_senders: trigger.allowed_senders.clone(),
        }
    }
}

/// Optional Telegram listener settings loaded by a named channel configuration.
///
/// Missing settings are accepted during configuration loading and validated only
/// when a Telegram listener is built.
#[derive(Default, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TelegramConfig {
    /// Optional Telegram Bot API base URL override.
    pub api_url: Option<String>,

    /// Sensitive Telegram Bot API token used to authenticate the listener.
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    pub token: Option<Zeroizing<String>>,
}

impl Debug for TelegramConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("TelegramConfig(<authentication redacted>)")
    }
}

/// Optional Slack listener settings loaded by a named channel configuration.
///
/// Missing settings are accepted during configuration loading and validated only
/// when a Slack listener is built.
#[derive(Default, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SlackConfig {
    /// Sensitive Slack app-level token used for Socket Mode authentication.
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    pub app_token: Option<Zeroizing<String>>,

    /// Sensitive Slack bot token used for API authentication.
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    pub bot_token: Option<Zeroizing<String>>,

    /// Whether replies to threaded Slack messages are posted in the same thread.
    pub auto_thread_reply: Option<bool>,

    /// Optional number of hours to retain thread-reply context.
    #[specta(type = Option<SpectaBigInt>)]
    pub thread_ttl_hours: Option<u64>,

    /// Whether Slack should expand links included in thread replies.
    pub unfurl_links: Option<bool>,
}

impl Debug for SlackConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("SlackConfig(<authentication redacted>)")
    }
}

/// Optional email listener settings loaded by a named channel configuration.
///
/// Missing settings are accepted during configuration loading and validated only
/// when an email listener is built.
#[derive(Default, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EmailConfig {
    /// Optional hostname of the IMAP server used to receive messages.
    pub imap_server: Option<String>,

    /// Optional port of the IMAP server.
    pub imap_port: Option<u16>,

    /// Optional hostname of the SMTP server associated with the mailbox.
    pub smtp_server: Option<String>,

    /// Optional port of the SMTP server.
    pub smtp_port: Option<u16>,

    /// Optional mailbox username used for authentication.
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    pub username: Option<Zeroizing<String>>,

    /// Sensitive mailbox password used for authentication.
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    pub password: Option<Zeroizing<String>>,
}

impl Debug for EmailConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("EmailConfig(<authentication redacted>)")
    }
}

/// An admitted inbound update ready for the Flow workstream.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct NormalizedMessage {
    /// The metadata associated with the message.
    pub metadata: Value,

    /// The body of the message.
    pub body: String,
}

/// Provider-specific transport settings stored with a messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConnectionTransport {
    /// Email transport settings.
    Email(EmailConfig),
    /// Slack transport settings.
    Slack(SlackConfig),
    /// Telegram transport settings.
    Telegram(TelegramConfig),
}

impl ConnectionTransport {
    /// Returns the provider represented by these persisted transport settings.
    pub(super) fn provider(&self) -> ListenerType {
        match self {
            Self::Email(_) => ListenerType::Email,
            Self::Slack(_) => ListenerType::Slack,
            Self::Telegram(_) => ListenerType::Telegram,
        }
    }

    /// Projects these settings into the single-provider plugin connection config.
    pub(super) fn messaging_config(&self) -> MessagingConfig {
        match self {
            Self::Email(config) => MessagingConfig {
                email: Some(config.clone()),
                ..Default::default()
            },
            Self::Slack(config) => MessagingConfig {
                slack: Some(config.clone()),
                ..Default::default()
            },
            Self::Telegram(config) => MessagingConfig {
                telegram: Some(config.clone()),
                ..Default::default()
            },
        }
    }
}

/// Whole-second retention duration for persisted messaging history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, JsonSchema)]
pub struct RetentionDuration {
    /// Number of seconds to retain history.
    #[specta(type = SpectaBigInt)]
    pub seconds: u64,
}

impl RetentionDuration {
    /// Creates a strictly positive retention duration.
    pub(super) fn new(seconds: u64) -> Result<Self, MessagingError> {
        if seconds == 0 {
            return Err(MessagingError::InvalidRetention {
                reason: "duration must be positive".to_owned(),
            });
        }
        Ok(Self { seconds })
    }
}

/// Persisted messaging connection and its RPC-facing configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct ConnectionRecord {
    /// Immutable connection UUID.
    pub id: SpectaUuid,
    /// Editable display name.
    pub display_name: String,
    /// Messaging provider.
    pub provider: ListenerType,
    /// Enclave credential UUID.
    pub credential_id: SpectaUuid,
    /// Provider-specific transport settings.
    pub transport: ConnectionTransport,
    /// Whether inbound listening is enabled.
    pub enabled: bool,
    /// Effective history retention duration.
    pub retention: RetentionDuration,
}

/// Input for creating a persisted messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NewConnection {
    pub display_name: String,
    pub provider: ListenerType,
    pub credential_id: SpectaUuid,
    pub transport: ConnectionTransport,
    pub enabled: bool,
    pub retention: RetentionDuration,
}

/// Input for updating a persisted messaging connection.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditConnection {
    pub id: SpectaUuid,
    pub display_name: String,
    pub provider: ListenerType,
    pub credential_id: SpectaUuid,
    pub transport: ConnectionTransport,
    pub retention: RetentionDuration,
}

/// Strictly positive relative duration for a permission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PermissionDuration {
    /// Number of seconds until expiration.
    pub(super) seconds: u64,
}

impl PermissionDuration {
    /// Creates a strictly positive permission duration.
    pub(super) fn new(seconds: u64) -> Result<Self, MessagingError> {
        if seconds == 0 {
            return Err(MessagingError::InvalidPermissionDuration {
                reason: "duration must be positive".to_owned(),
            });
        }
        Ok(Self { seconds })
    }

    /// Converts this relative duration into an absolute UTC expiration timestamp.
    pub(super) fn expires_at(&self, now_seconds: i64) -> Result<i64, MessagingError> {
        let seconds = i64::try_from(self.seconds).map_err(|_| MessagingError::InvalidPermissionDuration {
            reason: "duration exceeds timestamp range".to_owned(),
        })?;
        now_seconds
            .checked_add(seconds)
            .ok_or_else(|| MessagingError::InvalidPermissionDuration {
                reason: "expiration timestamp overflow".to_owned(),
            })
    }
}

/// A normalized provider callback interaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum InteractionAction {
    /// Approve the permission request.
    Approve,
    /// Deny the permission request.
    Deny,
    /// The callback action was not recognized.
    Unknown,
}

impl InteractionAction {
    /// Returns the stable action string used by callback persistence.
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Unknown => "unknown",
        }
    }
}

/// Provider callback data and identity supplied to the actor for arbitration.
#[derive(Debug, Clone)]
pub(super) struct MessagingInteraction {
    /// Permission request UUID embedded in the provider callback.
    pub(super) request_id: SpectaUuid,
    /// Normalized decision action.
    pub(super) action: InteractionAction,
    /// Raw provider callback payload.
    pub(super) raw_payload: String,
}

/// Encodes request UUIDs in callback data within Telegram's 64-byte limit.
pub(super) struct PermissionButtonPayload;

impl PermissionButtonPayload {
    /// Encodes an approve or deny button payload.
    pub(super) fn encode(request_id: SpectaUuid, action: InteractionAction) -> String {
        let prefix = match action {
            InteractionAction::Approve => "ap",
            InteractionAction::Deny => "dn",
            InteractionAction::Unknown => panic!("unknown actions cannot be encoded"),
        };
        format!("{prefix}|{}", request_id.0)
    }

    /// Decodes a callback payload, returning `None` for malformed or unknown data.
    pub(super) fn decode(data: &str) -> Option<(SpectaUuid, InteractionAction)> {
        let (prefix, uuid) = data.split_once('|')?;
        if uuid.contains('|') {
            return None;
        }
        let action = match prefix {
            "ap" => InteractionAction::Approve,
            "dn" => InteractionAction::Deny,
            _ => return None,
        };
        Uuid::parse_str(uuid)
            .ok()
            .map(|id| (SpectaUuid(id), action))
    }
}

/// Errors that can occur in the subscription domain.
///
/// Variants store only cloneable, serializable data so subscription errors can
/// cross RPC boundaries without retaining raw external error types.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema, ErrorCode)]
pub enum MessagingError {
    /// The generated client could not acquire an admitted dispatch context.
    #[error("bus dispatch failed: {message}")]
    BusDispatch { message: String },
    /// Bus authentication rejected a generated operation.
    #[error("bus authentication failed: {message}")]
    BusAuthentication { message: String },
    /// A reply did not contain the generated operation's expected response.
    #[error("unexpected bus response: expected {expected}, received {actual}")]
    UnexpectedBusResponse { expected: String, actual: String },
    /// Flow admission and persistence errors preserve their original typed source.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    #[schemars(skip)]
    Flow(Arc<FlowError>),
    /// The host has not published the required Flow trigger source service.
    #[error("Flow trigger source service is unavailable")]
    FlowServiceUnavailable,
    /// Sending a message over the subscription actor channel failed.
    #[error("channel send error for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },

    /// Shared infrastructure send failure with routing context.
    #[error("channel send error for {target_domain}.{operation}: {cause}")]
    InfrastructureChannelSend {
        /// Domain expected to receive the channel message.
        target_domain: String,
        /// Operation being routed when the send failed.
        operation: String,
        /// Safe human-readable underlying cause.
        cause: String,
    },

    /// Receiving a message from the subscription actor channel failed.
    #[error("channel receive error for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },

    /// Shared infrastructure receive failure with routing context.
    #[error("channel receive error for {target_domain}.{operation}: {cause}")]
    InfrastructureChannelRecv {
        /// Domain expected to produce the channel message.
        target_domain: String,
        /// Operation waiting for the receive when it failed.
        operation: String,
        /// Safe human-readable underlying cause.
        cause: String,
    },

    /// The requested subscription listener does not exist.
    #[error("subscription not found: {id}")]
    SubscriptionNotFound {
        /// Missing subscription listener identifier.
        id: u32,
    },

    /// Serialization or deserialization failed.
    #[error("serialization error: {message}")]
    Serialization {
        /// Human-readable serialization failure.
        message: String,
    },

    /// Filesystem or operating-system IO failed.
    #[error("io error at {path}: {message}")]
    Io {
        /// Path or operation context if known.
        path: String,
        /// Human-readable IO failure.
        message: String,
    },

    /// Waiting for a oneshot reply exceeded the caller's timeout.
    #[error("oneshot reply timed out for {target_domain}.{operation} after {timeout}")]
    OneshotReplyTimeout {
        /// Domain expected to answer the oneshot request.
        target_domain: String,
        /// Operation waiting for the reply.
        operation: String,
        /// Human-readable timeout duration that elapsed.
        timeout: String,
    },

    /// A oneshot reply sender was dropped before a response was sent.
    #[error("oneshot reply channel closed for {target_domain}.{operation}: {cause}")]
    OneshotReplyMissing {
        /// Domain expected to answer the oneshot request.
        target_domain: String,
        /// Operation waiting for the reply.
        operation: String,
        /// Safe human-readable underlying cause.
        cause: String,
    },

    /// Broadcasting a subscription domain event failed.
    #[error("broadcast error for {target_domain}.{operation}: {cause}")]
    Broadcast {
        /// Domain whose broadcast was being sent.
        target_domain: String,
        /// Broadcast operation or event being sent.
        operation: String,
        /// Safe human-readable underlying cause.
        cause: String,
    },

    /// A bus bridge rejected a subscription request.
    #[error("bus error: {message}")]
    BusError {
        /// Human-readable bus bridge failure.
        message: String,
    },

    /// Channel setup or connection failed before a listener could run.
    #[error("Channel connection failed for {listener_type:?}: {message}")]
    ChannelConnectionFailed {
        /// Channel type that failed.
        listener_type: ListenerType,
        /// Human-readable failure summary safe for logs and RPC responses.
        message: String,
    },

    /// Channel procedure parsing failed.
    #[error("Failed to parse channel procedure '{procedure}': {reason}")]
    ChannelParseError {
        /// Raw procedure string; callers must redact before logging.
        procedure: String,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Database operation failure.
    #[error("database error: {message}")]
    Database {
        /// Human-readable database failure.
        message: String,
    },

    /// The durable message database could not be opened or accessed.
    #[error("message database unavailable")]
    DatabaseUnavailable,

    /// Typed outbound Send failure with retry classification.
    #[error(transparent)]
    Send(#[from] MessagingSendError),

    /// The requested connection does not exist.
    #[error("connection not found: {connection_id}")]
    ConnectionNotFound { connection_id: String },
    /// The provider does not support interactive requests.
    #[error("interactive messaging is unsupported for {provider:?}")]
    InteractiveUnsupported { provider: ListenerType },
    /// The permission duration is invalid or cannot form a timestamp.
    #[error("invalid permission duration: {reason}")]
    InvalidPermissionDuration { reason: String },
    /// The retention duration is invalid or cannot form an expiry timestamp.
    #[error("invalid retention duration: {reason}")]
    InvalidRetention { reason: String },
    /// Disabling a connection is blocked by pending permission requests.
    #[error("connection {connection_id} has pending permission requests")]
    DisableRejectedPending {
        connection_id: String,
        pending_request_ids: Vec<String>,
    },
    /// Provider listener configuration is invalid for inbound operation.
    #[error("invalid inbound configuration for {provider:?}: {reason}")]
    InboundConfigurationInvalid { provider: ListenerType, reason: String },
    /// A provider callback payload is invalid.
    #[error("invalid callback payload: {payload}")]
    InvalidCallbackPayload { payload: String },
    /// The permission request does not exist.
    #[error("permission request not found: {request_id}")]
    PermissionRequestNotFound { request_id: String },
}

impl MessagingError {
    /// Canonical identity for an embedding application's root error adapter.
    pub fn rpc_identity(&self) -> (&str, &str, Map<String, Value>) {
        match self {
            Self::Flow(error) => ("flow", error.rpc_code(), error.rpc_details()),
            _ => ("subscription", self.rpc_code(), Map::new()),
        }
    }
}

/// Errors produced by concrete subscription channel listeners.
///
/// The actor retry loop uses these typed variants to preserve retry semantics
/// without depending on protocol-specific external error types.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub(super) enum MessagingSubscriptionError {
    /// Listener configuration was incomplete or invalid at runtime.
    #[error("channel configuration error for {listener_type:?}: {message}")]
    Configuration {
        /// Channel type that rejected its configuration.
        listener_type: ListenerType,
        /// Human-readable configuration failure.
        message: String,
    },

    /// Connecting to the external messaging platform failed.
    #[error("channel connection error for {listener_type:?}: {message}")]
    Connection {
        /// Channel type whose connection failed.
        listener_type: ListenerType,
        /// Human-readable connection failure.
        message: String,
    },

    /// Authentication or authorization with the external platform failed.
    #[error("channel authentication error for {listener_type:?}: {message}")]
    Authentication {
        /// Channel type whose authentication failed.
        listener_type: ListenerType,
        /// Human-readable authentication failure.
        message: String,
    },

    /// External protocol handling failed after a channel connected.
    #[error("channel protocol error for {listener_type:?}: {message}")]
    Protocol {
        /// Channel type whose protocol handling failed.
        listener_type: ListenerType,
        /// Human-readable protocol failure.
        message: String,
    },

    /// A channel operation exceeded its deadline.
    #[error("channel timeout for {listener_type:?}.{operation} after {timeout}")]
    Timeout {
        /// Channel type whose operation timed out.
        listener_type: ListenerType,
        /// Operation that exceeded its deadline.
        operation: String,
        /// Human-readable timeout duration that elapsed.
        timeout: String,
    },

    /// Forwarding a normalized message to the actor failed.
    #[error("channel message send error for {listener_type:?}: {cause}")]
    MessageSend {
        /// Channel type that could not forward a message.
        listener_type: ListenerType,
        /// Safe human-readable underlying cause.
        cause: String,
    },

    /// The channel stream closed unexpectedly and should be retried.
    #[error("channel stream closed for {listener_type:?}: {message}")]
    StreamClosed {
        /// Channel type whose stream closed.
        listener_type: ListenerType,
        /// Human-readable close reason.
        message: String,
    },
}

impl From<IntegerCastError> for MessagingError {
    fn from(error: IntegerCastError) -> Self {
        MessagingError::Database {
            message: error.to_string(),
        }
    }
}

impl From<GeneratedCallError<MessagingError>> for MessagingError {
    fn from(error: GeneratedCallError<MessagingError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => Self::BusAuthentication {
                message: error.to_string(),
            },
            GeneratedCallError::InputClosed => Self::ChannelSend {
                target_domain: "subscription".to_owned(),
                operation: "generated.dispatch".to_owned(),
                cause: "input closed".to_owned(),
            },
            GeneratedCallError::ReplyDropped => Self::OneshotReplyMissing {
                target_domain: "subscription".to_owned(),
                operation: "generated.reply".to_owned(),
                cause: "reply dropped".to_owned(),
            },
            GeneratedCallError::UnexpectedResponse { expected, actual } => {
                Self::UnexpectedBusResponse { expected, actual }
            }
        }
    }
}

impl From<DispatchContextError> for MessagingError {
    fn from(error: DispatchContextError) -> Self {
        Self::BusDispatch {
            message: error.to_string(),
        }
    }
}

impl From<IoError> for MessagingError {
    fn from(e: IoError) -> Self {
        MessagingError::Io {
            path: String::new(),
            message: e.to_string(),
        }
    }
}

impl From<JsonError> for MessagingError {
    fn from(e: JsonError) -> Self {
        MessagingError::Serialization { message: e.to_string() }
    }
}

impl From<SendError<MessagingResponse>> for MessagingError {
    fn from(_: SendError<MessagingResponse>) -> Self {
        Self::ChannelSend {
            target_domain: "subscription".to_owned(),
            operation: "broadcast".to_owned(),
            cause: "receiver closed".to_owned(),
        }
    }
}

impl From<FlowError> for MessagingError {
    fn from(error: FlowError) -> Self {
        Self::Flow(Arc::new(error))
    }
}

impl From<StoreError> for MessagingError {
    fn from(_: StoreError) -> Self {
        Self::DatabaseUnavailable
    }
}

impl RspcError for MessagingError {
    fn into_procedure_error(self) -> ProcedureError {
        let (domain, code, details) = self.rpc_identity();
        let safe_error = json!({ "DomainError": {
            "domain": domain, "code": code, "message": self.to_string(), "details": details,
        }});
        ProcedureError::Resolver(ResolverError::new(safe_error, None::<Self>))
    }
}

use std::fmt::{Display, Formatter, Result as FmtResult};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use tokio::sync::oneshot;
use zeroize::Zeroizing;

use aves::bus::BusRpcContext;
use aves::utils::uuid::SpectaUuid;

use super::domain::MessagingDomain;
use super::types::{
    ConnectionRecord, EditConnection, MessagingCredentials, MessagingError, MessagingSendReceipt,
    MessagingSenderConfig, NewConnection, NormalizedMessage,
};

/// Internal actor envelope for a generated messaging request and its reply channel.
pub(super) struct MessagingRequest {
    pub(super) payload: MessagingRequestPayload,
    pub(super) reply_to: oneshot::Sender<Result<MessagingResponse, MessagingError>>,
}

#[aves::bus_requests(marker = MessagingDomain, context = BusRpcContext, root_error = MessagingError, domain_error = MessagingError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
/// RPC requests for managing subscription listeners and their channel configurations.
pub enum MessagingRequestPayload {
    /// Starts all persisted subscription listeners.
    #[operation(name = "startServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StartServer,

    /// Supply one native messaging credential to Tyto's memory-only store.
    #[privileged(reason = "Enclave-owned messaging credential supply")]
    SupplyCredential {
        credential_id: SpectaUuid,
        #[serde(skip, default = "empty_credentials")]
        credentials: MessagingCredentials,
    },

    /// Remove one credential from Tyto's memory-only store.
    #[privileged(reason = "Enclave-owned messaging credential removal")]
    RemoveCredential { credential_id: SpectaUuid },

    /// Lists all persisted messaging connections.
    #[operation(name = "listConnections", kind = "query", response = ConnectionsOutput, response_variant = "Connections")]
    ListConnections,

    /// Creates a persisted messaging connection.
    #[operation(name = "createConnection", kind = "mutation", response = ConnectionOutput, response_variant = "Connection")]
    CreateConnection { input: NewConnection },

    /// Updates a persisted messaging connection.
    #[operation(name = "updateConnection", kind = "mutation", response = ConnectionOutput, response_variant = "Connection")]
    UpdateConnection { id: SpectaUuid, input: EditConnection },

    /// Deletes a persisted messaging connection.
    #[operation(name = "deleteConnection", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    DeleteConnection { connection_id: SpectaUuid },

    /// Enables or disables a persisted messaging connection.
    #[operation(name = "setConnectionEnabled", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    SetConnectionEnabled { connection_id: SpectaUuid, enabled: bool },

    /// Lists all persisted subscription listeners.
    #[operation(name = "list", kind = "query", response = SubscriptionsOutput, response_variant = "Subscriptions")]
    List,

    /// Sends one normalized message through the configured provider.
    #[operation(name = "send", kind = "mutation", response = SendOutput, response_variant = "Send")]
    Send {
        sender: MessagingSenderConfig,
        message: NormalizedMessage,
    },

    /// Returns the JSON Schema for one provider's outbound metadata.
    #[operation(name = "sendMetadataSchema", kind = "query", response = SendMetadataSchemaOutput, response_variant = "SendMetadataSchema")]
    SendMetadataSchema { listener_type: ListenerType },

    /// Stops every active subscription listener without deleting persisted subscriptions.
    #[operation(name = "stopServer", kind = "mutation", response = SuccessOutput, response_variant = "Success")]
    StopServer,
}

#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
/// RPC results returned by subscription listener and channel-configuration operations.
pub enum MessagingResponse {
    /// Persisted messaging connections returned by `subscription.listConnections`.
    Connections { connections: Vec<ConnectionRecord> },

    /// A persisted messaging connection returned by a create or update operation.
    Connection { connection: ConnectionRecord },

    /// Typed subscription rows returned by `subscription.list`.
    Subscriptions {
        /// Currently running listeners admitted by the Flow owner.
        subscriptions: Vec<SubscriptionRow>,
    },

    /// Delivers an inbound channel message for observability and flow triggering.
    ChannelMessage {
        /// Normalized text content received from the channel.
        body: String,
        /// Channel-specific structured details preserved with the message.
        metadata: Value,
    },

    /// Acknowledges an admitted Flow execution, not its terminal completion.
    SubscriptionTrigger,

    /// Confirms a lifecycle operation completed successfully.
    Success,

    /// Provider acceptance receipt for a public Send operation.
    Send { receipt: MessagingSendReceipt },

    /// JSON Schema for the selected provider's outbound message metadata.
    SendMetadataSchema { schema: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Events broadcast to root consumers for flow triggering and message observation.
pub enum MessagingBroadcast {
    /// Signals that an already admitted subscription event triggered its flow.
    Trigger,
    /// Carries an inbound message to consumers that observe channel traffic.
    ChannelMessage { body: String, metadata: Value },
}

/// A typed row returned by `subscription.list`, representing one active subscription.
#[derive(Debug, Clone, Serialize, Deserialize, Type, JsonSchema)]
pub struct SubscriptionRow {
    /// Unique subscription ID assigned at subscribe time.
    pub id: SpectaUuid,
    /// The channel type this subscription listens on.
    pub listener_type: ListenerType,
    /// None receives all channels available through the authenticated connector.
    pub subscribed_channels: Option<Vec<String>>,
    /// The flow that owns this subscription.
    pub flow_id: SpectaUuid,
}

/// Identifies the inbound channel protocol a subscription listener uses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type, JsonSchema, PartialEq, Eq, Hash)]
pub enum ListenerType {
    /// Telegram Bot API listener.
    Telegram,
    /// Slack Socket Mode listener.
    Slack,
    /// Email IMAP listener.
    Email,
}

impl Display for ListenerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(match self {
            ListenerType::Telegram => "Telegram",
            ListenerType::Slack => "Slack",
            ListenerType::Email => "Email",
        })
    }
}

impl ListenerType {
    /// Decodes persisted capitalized provider labels; callers own error policy.
    pub(super) fn from_storage(value: &str) -> Option<Self> {
        match value {
            "Email" => Some(Self::Email),
            "Slack" => Some(Self::Slack),
            "Telegram" => Some(Self::Telegram),
            _ => None,
        }
    }
}

/// Zeroizing placeholder used only when deserializing the deliberately skipped credential.
fn empty_credentials() -> MessagingCredentials {
    MessagingCredentials::Telegram {
        token: Zeroizing::new(String::new()),
    }
}

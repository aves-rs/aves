mod email;
mod slack;
mod telegram;

use std::sync::Arc;

use async_trait::async_trait;
use schemars::Schema;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use aves::utils::uuid::SpectaUuid;

use crate::messages::ListenerType;
use crate::types::{
    MessagingConfig, MessagingCredentials, MessagingError, MessagingInteraction, MessagingSendReceipt,
    MessagingSenderConfig, MessagingSubscriptionError, NormalizedMessage,
};

pub(super) const CHANNEL_MESSAGE_BUFFER_SIZE: usize = 100;

pub(super) type MessagingPluginHandle = Arc<Mutex<Box<dyn MessagingPlugin>>>;

/// A raw provider delivery captured at ingress plus its optional normalized projection.
#[derive(Debug, Clone)]
pub(super) struct InboundEvent {
    /// The payload exactly as supplied by the provider channel crate, captured BEFORE normalization.
    pub(super) raw_payload: Vec<u8>,
    /// The normalized message when normalization is supported and succeeds; None for raw-only events.
    pub(super) normalized: Option<NormalizedMessage>,
}

/// Target and access policy for one subscription, independent of Flow and authentication.
#[derive(Clone)]
pub(super) struct MessagingSubscription {
    /// None admits every delivered channel; an empty explicit selection is invalid.
    pub(super) subscribed_channels: Option<Vec<String>>,
    /// Protocol-native sender identities permitted to deliver messages.
    pub(super) allowed_senders: Vec<String>,
}

impl MessagingSubscription {
    pub(super) fn validate(&self, listener_type: ListenerType) -> Result<(), MessagingSubscriptionError> {
        if self
            .subscribed_channels
            .as_ref()
            .is_some_and(|channels| channels.is_empty() || channels.iter().any(|channel| channel.trim().is_empty()))
        {
            return Err(MessagingSubscriptionError::Configuration {
                listener_type,
                message: "An explicit channel selection must contain non-empty channel identities".to_owned(),
            });
        }
        Ok(())
    }
}

/// Owned provider or subscriber work, cancelled by the runtime that registered it.
pub(super) type MessagingTask = JoinHandle<Result<(), MessagingSubscriptionError>>;

/// A stateful messaging implementation owns its authenticated connection and subscriptions.
#[async_trait]
pub(super) trait MessagingPlugin: Send + Sync {
    /// Establish authenticated service access without binding a target or ACL.
    async fn connect(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError>;

    /// Verify remote access using the proposed transport settings and supplied credentials.
    async fn verify_remote(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError>;

    /// Send a normalized message using provider sender configuration and connected state.
    async fn send(
        &mut self,
        sender: &MessagingSenderConfig,
        message: &NormalizedMessage,
    ) -> Result<MessagingSendReceipt, MessagingError>;

    /// Return the JSON Schema for this provider's outbound message metadata.
    fn send_metadata_schema(&self) -> Schema;

    /// Start one unfiltered inflow for the authenticated bot/mailbox. The single enabled inflow
    /// emits inbound events (raw captured before normalization) on `tx` and permission-button
    /// interaction callbacks on `interaction_tx`; interactions bypass `run_filters`.
    async fn listen(
        &mut self,
        tx: mpsc::Sender<InboundEvent>,
        interaction_tx: mpsc::Sender<MessagingInteraction>,
    ) -> Result<MessagingTask, MessagingSubscriptionError>;

    /// Send an interactive Approve/Deny request and return the provider message identifier.
    async fn send_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        request_id: SpectaUuid,
        request_text: &str,
    ) -> Result<String, MessagingError>;

    /// Remove the buttons and show a terminal outcome while preserving the original request text.
    async fn edit_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        provider_message_id: &str,
        request_text: &str,
        outcome_label: &str,
    ) -> Result<(), MessagingError>;

    /// Validate and filter a dedicated input queue into its subscription's output queue.
    async fn run_filters(
        &mut self,
        subscription: MessagingSubscription,
        rx: mpsc::Receiver<NormalizedMessage>,
        tx: mpsc::Sender<NormalizedMessage>,
    ) -> Result<MessagingTask, MessagingSubscriptionError>;
}

/// Selects a messaging implementation; protocol behavior stays in that implementation.
pub(super) fn for_kind(kind: ListenerType) -> Box<dyn MessagingPlugin> {
    match kind {
        ListenerType::Email => Box::new(email::EmailPlugin::default()),
        ListenerType::Slack => Box::new(slack::SlackPlugin::default()),
        ListenerType::Telegram => Box::new(telegram::TelegramPlugin::default()),
    }
}

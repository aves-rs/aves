use std::sync::Arc;

use async_trait::async_trait;
use schemars::{Schema, schema_for};
use teloxide_core::Bot;
use tokio::sync::mpsc;

use aves::utils::uuid::SpectaUuid;

use crate::channels::{InboundEvent, MessagingPlugin, MessagingSubscription, MessagingTask};
use crate::messages::ListenerType;
use crate::types::{
    MessagingConfig, MessagingCredentials, MessagingError, MessagingInteraction, MessagingSendError,
    MessagingSendErrorCode, MessagingSendReceipt, MessagingSenderConfig, MessagingSubscriptionError, NormalizedMessage,
    TelegramSendMetadata,
};

use super::connection::configure;
use super::send::{edit_interactive, send, send_interactive};
use super::subscribe::{listen, run_filters};

/// Stateful Telegram messaging plugin.
#[derive(Default)]
pub(crate) struct TelegramPlugin {
    connection: Option<Arc<Bot>>,
}

#[async_trait]
impl MessagingPlugin for TelegramPlugin {
    async fn connect(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError> {
        self.connection = None;
        self.connection = Some(configure(settings, credentials)?);

        Ok(())
    }

    async fn listen(
        &mut self,
        tx: mpsc::Sender<InboundEvent>,
        interaction_tx: mpsc::Sender<MessagingInteraction>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        let bot = self
            .connection
            .as_ref()
            .cloned()
            .ok_or_else(|| MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Telegram,
                message: "Telegram plugin is not connected".to_owned(),
            })?;
        listen(bot, tx, interaction_tx).await
    }

    async fn run_filters(
        &mut self,
        subscription: MessagingSubscription,
        rx: mpsc::Receiver<NormalizedMessage>,
        tx: mpsc::Sender<NormalizedMessage>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        run_filters(subscription, rx, tx)
    }

    async fn send(
        &mut self,
        sender: &MessagingSenderConfig,
        message: &NormalizedMessage,
    ) -> Result<MessagingSendReceipt, MessagingError> {
        match sender {
            MessagingSenderConfig::Telegram(sender) => {
                let bot = self.connection.as_deref().ok_or_else(not_connected)?;
                Ok(send(bot, sender, message).await?)
            }
            _ => Err(unsupported_send()),
        }
    }

    async fn send_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        request_id: SpectaUuid,
        request_text: &str,
    ) -> Result<String, MessagingError> {
        match sender {
            MessagingSenderConfig::Telegram(sender) => {
                let bot = self.connection.as_deref().ok_or_else(not_connected)?;
                Ok(send_interactive(bot, sender, request_id, request_text).await?)
            }
            _ => Err(unsupported_send()),
        }
    }

    async fn edit_interactive(
        &mut self,
        sender: &MessagingSenderConfig,
        provider_message_id: &str,
        request_text: &str,
        outcome_label: &str,
    ) -> Result<(), MessagingError> {
        match sender {
            MessagingSenderConfig::Telegram(sender) => {
                let bot = self.connection.as_deref().ok_or_else(not_connected)?;
                Ok(edit_interactive(bot, sender, provider_message_id, request_text, outcome_label).await?)
            }
            _ => Err(unsupported_send()),
        }
    }

    fn send_metadata_schema(&self) -> Schema {
        schema_for!(TelegramSendMetadata)
    }
}

fn not_connected() -> MessagingError {
    MessagingSendError::Terminal {
        code: MessagingSendErrorCode::InvalidCredentials,
        message: "Telegram plugin is not connected".to_owned(),
    }
    .into()
}

fn unsupported_send() -> MessagingError {
    MessagingSendError::Terminal {
        code: MessagingSendErrorCode::Validation,
        message: "Telegram does not accept sender configurations from other channels".to_string(),
    }
    .into()
}

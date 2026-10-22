use std::sync::Arc;
use std::time::Duration;

use teloxide_core::Bot;
use teloxide_core::payloads::GetUpdatesSetters;
use teloxide_core::prelude::Requester;
use teloxide_core::types::{AllowedUpdate, CallbackQuery, Update, UpdateKind};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::channels::{InboundEvent, MessagingSubscription, MessagingTask};
use crate::messages::ListenerType;
use crate::types::{MessagingInteraction, MessagingSubscriptionError, NormalizedMessage};

use super::filter::TelegramFilter;
use super::helpers::{REQUEST_DEADLINE_SECS, callback_interaction, normalize_update};

/// Telegram long-poll wait, in seconds.
const LONG_POLL_TIMEOUT_SECS: u32 = 30;

/// Maximum number of updates requested per poll.
const POLL_LIMIT: u8 = 100;

/// Outer deadline for a long-poll request.
const GET_UPDATES_DEADLINE_SECS: u64 = (LONG_POLL_TIMEOUT_SECS as u64) * 2;

/// Poll Telegram and forward supported updates until either receiver closes.
pub(super) async fn listen(
    bot: Arc<Bot>,
    tx: mpsc::Sender<InboundEvent>,
    interaction_tx: mpsc::Sender<MessagingInteraction>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    Ok(tokio::spawn(async move {
        let mut offset = 0;
        loop {
            let updates = tokio::select! {
                _ = tx.closed() => return Ok(()),
                _ = interaction_tx.closed() => return Ok(()),
                result = timeout(Duration::from_secs(GET_UPDATES_DEADLINE_SECS), bot.get_updates().offset(offset).timeout(LONG_POLL_TIMEOUT_SECS).limit(POLL_LIMIT).allowed_updates([AllowedUpdate::Message, AllowedUpdate::ChannelPost, AllowedUpdate::CallbackQuery])) => {
                    result.map_err(|_| MessagingSubscriptionError::Timeout { listener_type: ListenerType::Telegram, operation: "getUpdates".to_owned(), timeout: format!("{GET_UPDATES_DEADLINE_SECS}s") })?.map_err(|source| MessagingSubscriptionError::Protocol { listener_type: ListenerType::Telegram, message: format!("Telegram getUpdates error: {source}") })?
                }
            };
            for update in updates {
                offset = next_offset(&update);
                match &update.kind {
                    UpdateKind::Message(_) | UpdateKind::ChannelPost(_) => {
                        let event = project_inbound_update(&update)?;
                        if tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                    UpdateKind::CallbackQuery(callback) => {
                        timeout(
                            Duration::from_secs(REQUEST_DEADLINE_SECS),
                            bot.answer_callback_query(callback.id.clone()),
                        )
                        .await
                        .map_err(|_| MessagingSubscriptionError::Timeout {
                            listener_type: ListenerType::Telegram,
                            operation: "answerCallbackQuery".to_owned(),
                            timeout: format!("{}s", REQUEST_DEADLINE_SECS),
                        })?
                        .map_err(|source| MessagingSubscriptionError::Protocol {
                            listener_type: ListenerType::Telegram,
                            message: format!("Telegram callback acknowledgement failed: {source}"),
                        })?;
                        if let Some(interaction) = project_callback(callback)?
                            && interaction_tx.send(interaction).await.is_err()
                        {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
            }
        }
    }))
}

/// Validates and normalizes a subscription before starting its filtering task.
pub(super) fn run_filters(
    mut subscription: MessagingSubscription,
    mut rx: mpsc::Receiver<NormalizedMessage>,
    tx: mpsc::Sender<NormalizedMessage>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    subscription.validate(ListenerType::Telegram)?;
    if let Some(channels) = &mut subscription.subscribed_channels {
        for channel in channels {
            *channel = channel
                .parse::<i64>()
                .map_err(|_| MessagingSubscriptionError::Configuration {
                    listener_type: ListenerType::Telegram,
                    message: "Telegram channel IDs must be integers".to_owned(),
                })?
                .to_string();
        }
    }
    let policy = TelegramFilter::new(subscription.subscribed_channels, subscription.allowed_senders);
    Ok(tokio::spawn(async move {
        while let Some(message) = tokio::select! { _ = tx.closed() => None, message = rx.recv() => message } {
            if let Some(accepted) = policy.apply(message)
                && tx.send(accepted).await.is_err()
            {
                break;
            }
        }
        Ok(())
    }))
}

fn next_offset(update: &Update) -> i32 {
    update.id.0 as i32 + 1
}

fn project_inbound_update(update: &Update) -> Result<InboundEvent, MessagingSubscriptionError> {
    let raw_payload = serde_json::to_string(update).map_err(|error| MessagingSubscriptionError::Protocol {
        listener_type: ListenerType::Telegram,
        message: format!("Telegram update serialization failed: {error}"),
    })?;
    Ok(InboundEvent {
        raw_payload: raw_payload.into_bytes(),
        normalized: normalize_update(update),
    })
}

fn project_callback(callback: &CallbackQuery) -> Result<Option<MessagingInteraction>, MessagingSubscriptionError> {
    let raw_payload = serde_json::to_string(callback).map_err(|error| MessagingSubscriptionError::Protocol {
        listener_type: ListenerType::Telegram,
        message: format!("Telegram callback serialization failed: {error}"),
    })?;
    Ok(callback_interaction(callback, raw_payload))
}

#[cfg(test)]
pub(super) fn test_project_inbound_update(update: &Update) -> Result<(i32, InboundEvent), MessagingSubscriptionError> {
    Ok((next_offset(update), project_inbound_update(update)?))
}

#[cfg(test)]
pub(super) fn test_project_callback(
    callback: &CallbackQuery,
) -> Result<Option<MessagingInteraction>, MessagingSubscriptionError> {
    project_callback(callback)
}

use std::time::Duration;

use teloxide_core::{
    Bot,
    payloads::SendMessageSetters,
    requests::Requester,
    types::{ChatId, MessageId},
};
use tokio::time::timeout;

use aves::utils::uuid::SpectaUuid;

use crate::types::{
    MessagingSendError, MessagingSendErrorCode, MessagingSendReceipt, NormalizedMessage, TelegramSenderConfig,
};

use super::helpers::{
    REQUEST_DEADLINE_SECS, classify_send_error, parse_chat_id, permission_keyboard, validate_send_metadata,
};

/// Send one plain-text message to the configured Telegram chat.
pub(super) async fn send(
    bot: &Bot,
    sender: &TelegramSenderConfig,
    message: &NormalizedMessage,
) -> Result<MessagingSendReceipt, MessagingSendError> {
    validate_send_metadata(&message.metadata)?;

    let chat_id: ChatId = parse_chat_id(&sender.chat_id)?;

    let sent = timeout(
        Duration::from_secs(REQUEST_DEADLINE_SECS),
        bot.send_message(chat_id, message.body.clone()),
    )
    .await
    .map_err(|_| MessagingSendError::Retryable {
        code: MessagingSendErrorCode::Transport,
        message: "Telegram send request timed out".to_owned(),
        retry_after_seconds: None,
    })?
    .map_err(classify_send_error)?;

    Ok(MessagingSendReceipt::ProviderMessageId {
        message_id: sent.id.to_string(),
    })
}

/// Send a permission prompt with its fixed approve/deny keyboard.
pub(super) async fn send_interactive(
    bot: &Bot,
    sender: &TelegramSenderConfig,
    request_id: SpectaUuid,
    request_text: &str,
) -> Result<String, MessagingSendError> {
    let chat_id = parse_chat_id(&sender.chat_id)?;
    let sent = timeout(
        Duration::from_secs(REQUEST_DEADLINE_SECS),
        bot.send_message(chat_id, request_text)
            .reply_markup(permission_keyboard(request_id)),
    )
    .await
    .map_err(|_| MessagingSendError::Retryable {
        code: MessagingSendErrorCode::Transport,
        message: "Telegram interactive send request timed out".to_owned(),
        retry_after_seconds: None,
    })?
    .map_err(classify_send_error)?;
    Ok(sent.id.0.to_string())
}

/// Edit a permission message to include its final outcome.
pub(super) async fn edit_interactive(
    bot: &Bot,
    sender: &TelegramSenderConfig,
    provider_message_id: &str,
    request_text: &str,
    outcome_label: &str,
) -> Result<(), MessagingSendError> {
    let chat_id = parse_chat_id(&sender.chat_id)?;
    let message_id = provider_message_id
        .parse::<i32>()
        .map(MessageId)
        .map_err(|_| MessagingSendError::Terminal {
            code: MessagingSendErrorCode::Validation,
            message: "Telegram message ID must be an integer".to_owned(),
        })?;
    timeout(
        Duration::from_secs(REQUEST_DEADLINE_SECS),
        bot.edit_message_text(chat_id, message_id, finalized_text(request_text, outcome_label)),
    )
    .await
    .map_err(|_| MessagingSendError::Retryable {
        code: MessagingSendErrorCode::Transport,
        message: "Telegram interactive edit request timed out".to_owned(),
        retry_after_seconds: None,
    })?
    .map_err(classify_send_error)?;
    Ok(())
}

fn finalized_text(request_text: &str, outcome_label: &str) -> String {
    format!("{request_text}\n\n{outcome_label}")
}

#[cfg(test)]
pub(super) fn test_finalized_text(request_text: &str, outcome_label: &str) -> String {
    finalized_text(request_text, outcome_label)
}

use serde::Deserialize;
use serde_json::{Value, json};
use teloxide_core::errors::{ApiError, RequestError};
use teloxide_core::types::{
    CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update, UpdateKind,
};

use aves::utils::uuid::SpectaUuid;

use crate::types::{
    InteractionAction, MessagingInteraction, MessagingSendError, MessagingSendErrorCode, NormalizedMessage,
    PermissionButtonPayload, TelegramSendMetadata,
};

pub(super) const REQUEST_DEADLINE_SECS: u64 = 60;

pub(super) fn permission_keyboard(request_id: SpectaUuid) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new([[
        InlineKeyboardButton::callback(
            "Approve",
            PermissionButtonPayload::encode(request_id, InteractionAction::Approve),
        ),
        InlineKeyboardButton::callback(
            "Deny",
            PermissionButtonPayload::encode(request_id, InteractionAction::Deny),
        ),
    ]])
}

pub(super) fn callback_interaction(callback: &CallbackQuery, raw_payload: String) -> Option<MessagingInteraction> {
    let data = callback.data.as_deref()?;
    let (request_id, action) = PermissionButtonPayload::decode(data)?;
    Some(MessagingInteraction {
        request_id,
        action,
        raw_payload,
    })
}

/// Validate provider-neutral metadata against Telegram's empty metadata contract.
pub(super) fn validate_send_metadata(metadata: &Value) -> Result<(), MessagingSendError> {
    TelegramSendMetadata::deserialize(metadata)
        .map(|_| ())
        .map_err(|error| MessagingSendError::Terminal {
            code: MessagingSendErrorCode::Validation,
            message: format!("Telegram metadata must be empty: {error}"),
        })
}

/// Parse a Telegram chat destination as its native numeric chat ID.
pub(super) fn parse_chat_id(raw: &str) -> Result<ChatId, MessagingSendError> {
    raw.parse::<i64>()
        .map(ChatId)
        .map_err(|_| MessagingSendError::Terminal {
            code: MessagingSendErrorCode::InvalidDestination,
            message: "Telegram chat ID must be an integer".to_owned(),
        })
}

/// Classify a teloxide request failure for the Send retry owner.
pub(super) fn classify_send_error(error: RequestError) -> MessagingSendError {
    match error {
        RequestError::RetryAfter(seconds) => MessagingSendError::Retryable {
            code: MessagingSendErrorCode::RateLimited,
            message: "Telegram rate limit exceeded".to_owned(),
            retry_after_seconds: Some(u64::from(seconds.seconds())),
        },
        RequestError::Network(_) | RequestError::Io(_) | RequestError::InvalidJson { .. } => {
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Transport,
                message: "Telegram request transport failed".to_owned(),
                retry_after_seconds: None,
            }
        }
        RequestError::Api(api_error) => classify_api_error(api_error),
        RequestError::MigrateToChatId(_) => MessagingSendError::Terminal {
            code: MessagingSendErrorCode::InvalidDestination,
            message: "Telegram chat has migrated; update the configured chat ID".to_owned(),
        },
    }
}

fn classify_api_error(error: ApiError) -> MessagingSendError {
    let (code, message) = match error {
        ApiError::InvalidToken => (
            MessagingSendErrorCode::InvalidCredentials,
            "Telegram rejected the bot token",
        ),
        ApiError::BotBlocked
        | ApiError::ChatNotFound
        | ApiError::GroupDeactivated
        | ApiError::BotKicked
        | ApiError::BotKickedFromSupergroup
        | ApiError::BotKickedFromChannel
        | ApiError::CantInitiateConversation
        | ApiError::CantTalkWithBots
        | ApiError::NotEnoughRightsToPostMessages => (
            MessagingSendErrorCode::PermissionDenied,
            "Telegram bot cannot send messages to the configured chat",
        ),
        ApiError::MessageTextIsEmpty | ApiError::MessageIsTooLong | ApiError::CantParseEntities(_) => {
            (MessagingSendErrorCode::Validation, "Telegram rejected the message text")
        }
        ApiError::Unknown(_) | ApiError::UnknownHost => {
            return MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Server,
                message: "Telegram reported a temporary API failure".to_owned(),
                retry_after_seconds: None,
            };
        }
        _ => (
            MessagingSendErrorCode::InvalidDestination,
            "Telegram rejected the destination or request",
        ),
    };
    MessagingSendError::Terminal {
        code,
        message: message.to_owned(),
    }
}

/// Normalize a Telegram update into a [`NormalizedMessage`].
pub(super) fn normalize_update(update: &Update) -> Option<NormalizedMessage> {
    let msg = match &update.kind {
        UpdateKind::Message(m) | UpdateKind::ChannelPost(m) => m,
        _ => return None,
    };

    let body = extract_body(msg)?;

    let chat_id = msg.chat.id.0.to_string();
    let sender_id = msg.from.as_ref().map(|user| user.id.0.to_string());

    let metadata = json!({
        "channel": "telegram",
        "chat_id": chat_id,
        "sender_user_id": sender_id,
        "sender_username": msg.from.as_ref().and_then(|user| user.username.as_deref()),
        "message_id": msg.id.0,
    });

    Some(NormalizedMessage { body, metadata })
}

/// Extract the plain-text body from a Telegram [`Message`].
pub(super) fn extract_body(msg: &Message) -> Option<String> {
    msg.text()
        .filter(|text| !text.is_empty())
        .or_else(|| msg.caption())
        .map(str::to_owned)
}

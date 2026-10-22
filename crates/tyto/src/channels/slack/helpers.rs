use serde::Deserialize;
use serde_json::{Value, json};
use slack_morphism::{
    SlackActionId, SlackChannelId, SlackMessageContent, SlackTeamId,
    api::SlackApiChatPostMessageRequest,
    blocks::{
        SlackActionsBlock, SlackBlock, SlackBlockButtonElement, SlackBlockButtonStyle, SlackBlockPlainText,
        SlackBlockPlainTextOnly, SlackSectionBlock,
    },
    errors::{SlackClientError, SlackRateLimitError},
    events::{SlackEventId, SlackInteractionEvent, SlackMessageEvent},
};

use aves::utils::uuid::SpectaUuid;

use crate::types::{
    InteractionAction, MessagingInteraction, MessagingSendError, MessagingSendErrorCode, NormalizedMessage,
    PermissionButtonPayload, SlackSendMetadata,
};

/// Validate a Slack channel ID before processing message metadata.
pub(super) fn validate_channel_id(channel_id: &str) -> Result<(), MessagingSendError> {
    if channel_id.trim().is_empty() {
        return Err(terminal_send_error(
            MessagingSendErrorCode::InvalidDestination,
            "Slack channel id cannot be empty",
        ));
    }
    Ok(())
}

/// Validate Slack metadata and construct the plain-text Web API request.
pub(super) fn post_message_request(
    channel_id: &str,
    message: &NormalizedMessage,
) -> Result<SlackApiChatPostMessageRequest, MessagingSendError> {
    validate_channel_id(channel_id)?;

    SlackSendMetadata::deserialize(&message.metadata).map_err(|error| {
        terminal_send_error(
            MessagingSendErrorCode::Validation,
            format!("Slack message metadata is invalid: {error}"),
        )
    })?;

    Ok(SlackApiChatPostMessageRequest::new(
        channel_id.to_owned().into(),
        SlackMessageContent::new().with_text(message.body.clone()),
    ))
}

/// Build the fixed section-and-actions permission prompt.
pub(super) fn interactive_blocks(request_id: SpectaUuid, request_text: &str) -> [SlackBlock; 2] {
    let section = SlackSectionBlock::new().with_text(SlackBlockPlainText::new(request_text.to_owned()).into());
    let approve = SlackBlockButtonElement::new(SlackBlockPlainTextOnly::from("Approve"))
        .with_action_id(SlackActionId::new("permission_approve".to_owned()))
        .with_value(PermissionButtonPayload::encode(request_id, InteractionAction::Approve))
        .with_style(SlackBlockButtonStyle::Primary);
    let deny = SlackBlockButtonElement::new(SlackBlockPlainTextOnly::from("Deny"))
        .with_action_id(SlackActionId::new("permission_deny".to_owned()))
        .with_value(PermissionButtonPayload::encode(request_id, InteractionAction::Deny))
        .with_style(SlackBlockButtonStyle::Danger);
    [
        SlackBlock::Section(section),
        SlackBlock::Actions(SlackActionsBlock::new([approve.into(), deny.into()].into())),
    ]
}

pub(super) fn interaction_to_message(event: &SlackInteractionEvent) -> Option<MessagingInteraction> {
    let SlackInteractionEvent::BlockActions(event) = event else {
        return None;
    };
    let value = event
        .actions
        .as_ref()?
        .iter()
        .find_map(|action| action.value.as_deref())?;
    let (request_id, action) = PermissionButtonPayload::decode(value)?;
    Some(MessagingInteraction {
        request_id,
        action,
        raw_payload: serde_json::to_string(event).unwrap_or_default(),
    })
}

/// Convert a Slack SDK error to the stable outbound-send classification.
pub(super) fn classify_send_error(error: SlackClientError) -> MessagingSendError {
    match error {
        SlackClientError::RateLimitError(error) => rate_limit_error(error),
        SlackClientError::HttpError(error) => classify_http_error(error.status_code.as_u16(), error.to_string()),
        SlackClientError::ApiError(error) if error.code == "ratelimited" => {
            retryable_send_error(MessagingSendErrorCode::RateLimited, error.to_string(), None)
        }
        SlackClientError::ApiError(error) => {
            let code = match error.code.as_str() {
                "invalid_auth" | "not_authed" | "token_revoked" | "account_inactive" => {
                    MessagingSendErrorCode::InvalidCredentials
                }
                "channel_not_found" | "is_archived" => MessagingSendErrorCode::InvalidDestination,
                "not_in_channel" | "missing_scope" | "restricted_action" | "missing_permission" => {
                    MessagingSendErrorCode::PermissionDenied
                }
                _ => MessagingSendErrorCode::Validation,
            };
            terminal_send_error(code, error.to_string())
        }
        other => retryable_send_error(MessagingSendErrorCode::Transport, other.to_string(), None),
    }
}

fn classify_http_error(status: u16, message: String) -> MessagingSendError {
    match status {
        500..=599 => retryable_send_error(MessagingSendErrorCode::Server, message, None),
        429 => retryable_send_error(MessagingSendErrorCode::RateLimited, message, None),
        _ => terminal_send_error(MessagingSendErrorCode::Validation, message),
    }
}

fn rate_limit_error(error: SlackRateLimitError) -> MessagingSendError {
    retryable_send_error(
        MessagingSendErrorCode::RateLimited,
        error.to_string(),
        error.retry_after.map(|duration| {
            duration
                .as_secs()
                .saturating_add(u64::from(duration.subsec_nanos() > 0))
        }),
    )
}

fn terminal_send_error(code: MessagingSendErrorCode, message: impl Into<String>) -> MessagingSendError {
    MessagingSendError::Terminal {
        code,
        message: message.into(),
    }
}

fn retryable_send_error(
    code: MessagingSendErrorCode,
    message: impl Into<String>,
    retry_after_seconds: Option<u64>,
) -> MessagingSendError {
    MessagingSendError::Retryable {
        code,
        message: message.into(),
        retry_after_seconds,
    }
}

/// Normalize a Slack message without applying any subscription policy.
pub(super) fn normalize_update(
    msg_event: &SlackMessageEvent,
    channel_id: SlackChannelId,
    team_id: &SlackTeamId,
    event_id: &SlackEventId,
) -> Option<NormalizedMessage> {
    let body = match msg_event
        .content
        .as_ref()
        .and_then(|c| c.text.as_deref())
        .filter(|t| !t.is_empty())
    {
        Some(t) => t.to_string(),
        None => {
            tracing::debug!("Slack message event has no text content; skipping");
            return None;
        }
    };

    let metadata: Value = json!({
        "channel_type": "slack",
        "channel_id": channel_id.as_ref(),
        "user_id": msg_event.sender.user.as_ref().map(|u| u.as_ref()),
        "ts": msg_event.origin.ts.to_string(),
        "thread_ts": msg_event.origin.thread_ts.as_ref().map(|t| t.to_string()),
        "team_id": team_id.as_ref(),
        "event_id": event_id.as_ref(),
    });

    Some(NormalizedMessage { body, metadata })
}

#[cfg(test)]
pub(super) fn test_classify_http_error(status: u16) -> MessagingSendError {
    classify_http_error(status, String::new())
}

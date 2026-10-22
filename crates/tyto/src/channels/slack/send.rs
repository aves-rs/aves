use std::time::Duration;

use slack_morphism::{
    SlackMessageContent, SlackTs,
    api::{SlackApiChatPostMessageRequest, SlackApiChatUpdateRequest},
};
use tokio::time::timeout;

use aves::utils::uuid::SpectaUuid;

use crate::types::{
    MessagingError, MessagingSendError, MessagingSendErrorCode, MessagingSendReceipt, NormalizedMessage,
    SlackSenderConfig,
};

use super::{connection::SlackConnection, helpers};

/// Post a Slack message with the shared send deadline and error policy.
async fn post_message(
    connection: &SlackConnection,
    request: &SlackApiChatPostMessageRequest,
) -> Result<String, MessagingSendError> {
    let session = connection.client.open_session(&connection.bot_token);
    let response = timeout(Duration::from_secs(60), session.chat_post_message(request))
        .await
        .map_err(|_| MessagingSendError::Retryable {
            code: MessagingSendErrorCode::Transport,
            message: "Slack chat.postMessage timed out after 60 seconds".to_owned(),
            retry_after_seconds: None,
        })?
        .map_err(helpers::classify_send_error)?;
    Ok(response.ts.to_string())
}

/// Send one plain-text message to the configured Slack channel.
pub(super) async fn send(
    connection: &SlackConnection,
    sender: &SlackSenderConfig,
    message: &NormalizedMessage,
) -> Result<MessagingSendReceipt, MessagingError> {
    let request = helpers::post_message_request(&sender.channel_id, message)?;
    let message_id = post_message(connection, &request).await?;

    Ok(MessagingSendReceipt::ProviderMessageId { message_id })
}

/// Post a permission prompt and return Slack's message timestamp.
pub(super) async fn send_interactive(
    connection: &SlackConnection,
    sender: &SlackSenderConfig,
    request_id: SpectaUuid,
    request_text: &str,
) -> Result<String, MessagingError> {
    let request = prepare_interactive_send(sender, request_id, request_text)?;
    Ok(post_message(connection, &request).await?)
}

fn prepare_interactive_send(
    sender: &SlackSenderConfig,
    request_id: SpectaUuid,
    request_text: &str,
) -> Result<SlackApiChatPostMessageRequest, MessagingSendError> {
    helpers::validate_channel_id(&sender.channel_id)?;
    let blocks = helpers::interactive_blocks(request_id, request_text);
    let content = SlackMessageContent::new()
        .with_text(request_text.to_owned())
        .with_blocks(blocks.into());
    Ok(SlackApiChatPostMessageRequest::new(
        sender.channel_id.clone().into(),
        content,
    ))
}

/// Replace a permission prompt with its outcome and clear its buttons.
pub(super) async fn edit_interactive(
    connection: &SlackConnection,
    sender: &SlackSenderConfig,
    provider_message_id: &str,
    request_text: &str,
    outcome_label: &str,
) -> Result<(), MessagingError> {
    let request = prepare_interactive_edit(sender, provider_message_id, request_text, outcome_label)?;
    let session = connection.client.open_session(&connection.bot_token);
    timeout(Duration::from_secs(60), session.chat_update(&request))
        .await
        .map_err(|_| MessagingSendError::Retryable {
            code: MessagingSendErrorCode::Transport,
            message: "Slack chat.update timed out after 60 seconds".to_owned(),
            retry_after_seconds: None,
        })?
        .map_err(helpers::classify_send_error)?;
    Ok(())
}

fn prepare_interactive_edit(
    sender: &SlackSenderConfig,
    provider_message_id: &str,
    request_text: &str,
    outcome_label: &str,
) -> Result<SlackApiChatUpdateRequest, MessagingSendError> {
    helpers::validate_channel_id(&sender.channel_id)?;
    let content = SlackMessageContent::new()
        .with_text(format!("{request_text}\n\n{outcome_label}"))
        .with_blocks(Vec::new());
    Ok(SlackApiChatUpdateRequest::new(
        sender.channel_id.clone().into(),
        content,
        SlackTs::new(provider_message_id.to_owned()),
    ))
}

#[cfg(test)]
pub(super) fn test_prepare_interactive_send(
    sender: &SlackSenderConfig,
    request_id: SpectaUuid,
    request_text: &str,
) -> Result<SlackApiChatPostMessageRequest, MessagingSendError> {
    prepare_interactive_send(sender, request_id, request_text)
}

#[cfg(test)]
pub(super) fn test_prepare_interactive_edit(
    sender: &SlackSenderConfig,
    provider_message_id: &str,
    request_text: &str,
    outcome_label: &str,
) -> Result<SlackApiChatUpdateRequest, MessagingSendError> {
    prepare_interactive_edit(sender, provider_message_id, request_text, outcome_label)
}

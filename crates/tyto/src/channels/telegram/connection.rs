use std::sync::Arc;
use std::time::Duration;

use teloxide_core::Bot;
use teloxide_core::requests::Requester;
use tokio::time::timeout;
use url::Url;

use crate::messages::ListenerType;
use crate::types::{MessagingConfig, MessagingCredentials, MessagingError, MessagingSubscriptionError, TelegramConfig};

use super::helpers::REQUEST_DEADLINE_SECS;

/// Builds the Telegram transport from settings and supplied credentials.
pub(super) fn configure(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<Arc<Bot>, MessagingError> {
    let MessagingCredentials::Telegram { token } = credentials else {
        return Err(MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Telegram,
            message: "credential protocol does not match the listener".to_owned(),
        });
    };

    let mut config = settings
        .telegram
        .clone()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Telegram,
            message: "Telegram config block is missing".to_owned(),
        })?;
    config.token = Some(token.clone());

    prepare(&config)
}

fn prepare(config: &TelegramConfig) -> Result<Arc<Bot>, MessagingError> {
    let token = config
        .token
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Telegram,
            message: "Telegram token is missing".to_owned(),
        })?;
    let bot = build_bot(token, config.api_url.as_deref()).map_err(|error| MessagingError::ChannelConnectionFailed {
        listener_type: ListenerType::Telegram,
        message: error.to_string(),
    })?;
    Ok(Arc::new(bot))
}

/// Verify Telegram credentials by calling getMe without retaining connection state.
pub(super) async fn verify_remote(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<(), MessagingError> {
    let bot = configure(settings, credentials)?;
    timeout(Duration::from_secs(REQUEST_DEADLINE_SECS), bot.get_me())
        .await
        .map_err(|_| connection_failure("Telegram request timed out"))?
        .map_err(|error| {
            // RequestError Display can include the tokenized Bot API URL; never expose it.
            let message = match error {
                teloxide_core::errors::RequestError::Api(teloxide_core::errors::ApiError::InvalidToken) => {
                    "Telegram rejected the bot token"
                }
                _ => "Telegram request failed",
            };
            connection_failure(message)
        })?;
    Ok(())
}

fn connection_failure(message: &str) -> MessagingError {
    MessagingError::ChannelConnectionFailed {
        listener_type: ListenerType::Telegram,
        message: message.to_owned(),
    }
}

fn build_bot(token: &str, api_url: Option<&str>) -> Result<Bot, MessagingSubscriptionError> {
    let bot = Bot::new(token);

    match api_url {
        None => Ok(bot),
        Some(raw_url) => {
            let url = Url::parse(raw_url).map_err(|source| MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Telegram,
                message: format!("Telegram invalid api_url: {source}"),
            })?;
            Ok(bot.set_api_url(url))
        }
    }
}

#[cfg(test)]
pub(super) fn test_prepare(config: &TelegramConfig) -> Result<Arc<Bot>, MessagingError> {
    prepare(config)
}

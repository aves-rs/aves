use std::sync::Arc;

use teloxide_core::Bot;
use url::Url;

use crate::messages::ListenerType;
use crate::types::{MessagingConfig, MessagingCredentials, MessagingError, MessagingSubscriptionError, TelegramConfig};

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

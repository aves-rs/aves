use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use slack_morphism::prelude::{
    SlackApiAppsConnectionOpenRequest, SlackApiToken, SlackApiTokenType, SlackApiTokenValue, SlackClient,
    SlackClientHyperConnector, SlackClientHyperHttpsConnector,
};
use tokio::time::timeout;

use crate::messages::ListenerType;
use crate::types::{MessagingConfig, MessagingCredentials, MessagingError, MessagingSubscriptionError, SlackConfig};

/// Shared Slack API client and its app and bot credentials.
pub(super) struct SlackConnection {
    pub(super) client: Arc<SlackClient<SlackClientHyperHttpsConnector>>,
    pub(super) token: SlackApiToken,
    pub(super) bot_token: SlackApiToken,
}

pub(super) fn configure(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<Arc<SlackConnection>, MessagingError> {
    let MessagingCredentials::Slack { app_token, bot_token } = credentials else {
        return Err(MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Slack,
            message: "credential protocol does not match the listener".to_owned(),
        });
    };

    let mut config = settings
        .slack
        .clone()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Slack,
            message: "Slack config block is missing".to_owned(),
        })?;

    config.app_token = Some(app_token.clone());
    config.bot_token = Some(bot_token.clone());

    prepare(&config)
}

/// Apply the Socket Mode listen deadline and add Slack connection context.
pub(super) async fn connect<F, T, E>(future: F) -> Result<T, MessagingSubscriptionError>
where
    F: Future<Output = Result<T, E>>,
    E: Display,
{
    let deadline = Duration::from_secs(60);
    timeout(deadline, future)
        .await
        .map_err(|_| MessagingSubscriptionError::Timeout {
            listener_type: ListenerType::Slack,
            operation: "listen_for".to_owned(),
            timeout: format!("{deadline:?}"),
        })?
        .map_err(|error| slack_connection(format!("Slack Socket Mode listen_for failed: {error}")))
}

/// Verify both Slack credentials through HTTP API calls without starting Socket Mode.
pub(super) async fn verify_remote(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<(), MessagingError> {
    let connection = configure(settings, credentials)?;

    connection
        .client
        .open_session(&connection.bot_token)
        .auth_test()
        .await
        .map_err(|_| connection_failure("Slack rejected the bot token or auth_test failed"))?;

    connection
        .client
        .open_session(&connection.token)
        .apps_connections_open(&SlackApiAppsConnectionOpenRequest {})
        .await
        .map_err(|_| connection_failure("Slack rejected the app token or apps.connections.open failed"))?;

    Ok(())
}

fn connection_failure(message: &str) -> MessagingError {
    MessagingError::ChannelConnectionFailed {
        listener_type: ListenerType::Slack,
        message: message.to_owned(),
    }
}

fn prepare(config: &SlackConfig) -> Result<Arc<SlackConnection>, MessagingError> {
    let app_token = config
        .app_token
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Slack,
            message: "Slack app token is missing".to_owned(),
        })?;

    let bot_token = config
        .bot_token
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Slack,
            message: "Slack bot token is missing".to_owned(),
        })?;

    let client = Arc::new(SlackClient::new(SlackClientHyperConnector::new().map_err(|error| {
        MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Slack,
            message: slack_connection(format!("Slack hyper connector init failed: {error}")).to_string(),
        }
    })?));
    let token =
        SlackApiToken::new(SlackApiTokenValue::new(app_token.to_string())).with_token_type(SlackApiTokenType::App);
    let bot_token =
        SlackApiToken::new(SlackApiTokenValue::new(bot_token.to_string())).with_token_type(SlackApiTokenType::Bot);

    Ok(Arc::new(SlackConnection {
        client,
        token,
        bot_token,
    }))
}

fn slack_connection(message: String) -> MessagingSubscriptionError {
    MessagingSubscriptionError::Connection {
        listener_type: ListenerType::Slack,
        message,
    }
}

#[cfg(test)]
pub(super) fn test_prepare(config: &SlackConfig) -> Result<Arc<SlackConnection>, MessagingError> {
    prepare(config)
}

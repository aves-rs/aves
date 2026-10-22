use std::sync::Arc;

use async_trait::async_trait;
use aves::utils::uuid::SpectaUuid;
use schemars::{Schema, schema_for};
use tokio::sync::{Mutex, mpsc};
use zeroize::Zeroizing;

use crate::channels::{InboundEvent, MessagingPlugin, MessagingSubscription, MessagingTask};
use crate::messages::ListenerType;
use crate::types::{
    EmailConfig, EmailSendMetadata, MessagingConfig, MessagingCredentials, MessagingError, MessagingInteraction,
    MessagingSendError, MessagingSendErrorCode, MessagingSendReceipt, MessagingSenderConfig,
    MessagingSubscriptionError, NormalizedMessage,
};

use super::connection::{ImapConnection, configure, connect};
use super::send::send;
use super::subscribe::{listen, run_filters};

#[derive(Default)]
pub(crate) struct EmailPlugin {
    connection: Option<ImapConnection>,
    settings: Option<EmailConfig>,
    credentials: Option<(Zeroizing<String>, Zeroizing<String>)>,
}

#[async_trait]
impl MessagingPlugin for EmailPlugin {
    async fn connect(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError> {
        self.connection = None;
        self.credentials = None;

        if let (Some(mut config), MessagingCredentials::Email { username, password }) =
            (settings.email.clone(), credentials)
            && config.imap_server.is_none()
            && config.smtp_server.is_some()
        {
            config.username = Some(username.clone());
            config.password = Some(password.clone());
            self.credentials = Some((username.clone(), password.clone()));
            self.settings = Some(config);
            return Ok(());
        }

        let config = configure(settings, credentials)?;
        let session = connect(&config)
            .await
            .map_err(|error| MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Email,
                message: error.to_string(),
            })?;

        self.connection = Some(Arc::new(Mutex::new(Some(session))));
        self.settings = Some(config);
        if let MessagingCredentials::Email { username, password } = credentials {
            self.credentials = Some((username.clone(), password.clone()));
        }
        Ok(())
    }

    async fn listen(
        &mut self,
        tx: mpsc::Sender<InboundEvent>,
        _interaction_tx: mpsc::Sender<MessagingInteraction>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        let settings = self
            .settings
            .as_ref()
            .ok_or_else(|| MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Email,
                message: "Email plugin is not connected".to_owned(),
            })?;
        if settings.imap_server.is_none() {
            return Err(MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Email,
                message: "IMAP server is required for inbound email listening".to_owned(),
            });
        }

        let connection = self.connection.as_ref().cloned();
        let connection = match connection {
            Some(connection) if connection.lock().await.is_some() => connection,
            _ => {
                let session = connect(settings).await?;
                let connection = Arc::new(Mutex::new(Some(session)));
                self.connection = Some(connection.clone());
                connection
            }
        };
        listen(connection, tx).await
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
            MessagingSenderConfig::Email(sender) => {
                let Some(settings) = self.settings.as_ref() else {
                    return Err(MessagingSendError::Terminal {
                        code: MessagingSendErrorCode::InvalidCredentials,
                        message: "Email settings are not connected".to_owned(),
                    }
                    .into());
                };
                let Some((username, password)) = self.credentials.as_ref() else {
                    return Err(MessagingSendError::Terminal {
                        code: MessagingSendErrorCode::InvalidCredentials,
                        message: "Email credentials are not connected".to_owned(),
                    }
                    .into());
                };
                Ok(send(sender, settings, (username, password), message).await?)
            }
            _ => Err(unsupported_send()),
        }
    }

    async fn send_interactive(
        &mut self,
        _sender: &MessagingSenderConfig,
        _request_id: SpectaUuid,
        _request_text: &str,
    ) -> Result<String, MessagingError> {
        Err(MessagingError::InteractiveUnsupported {
            provider: ListenerType::Email,
        })
    }

    async fn edit_interactive(
        &mut self,
        _sender: &MessagingSenderConfig,
        _provider_message_id: &str,
        _request_text: &str,
        _outcome_label: &str,
    ) -> Result<(), MessagingError> {
        Err(MessagingError::InteractiveUnsupported {
            provider: ListenerType::Email,
        })
    }

    fn send_metadata_schema(&self) -> Schema {
        schema_for!(EmailSendMetadata)
    }
}

fn unsupported_send() -> MessagingError {
    MessagingSendError::Terminal {
        code: MessagingSendErrorCode::Validation,
        message: "Email provider cannot send messages from other channels".to_string(),
    }
    .into()
}

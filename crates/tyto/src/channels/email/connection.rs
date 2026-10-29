use std::sync::Arc;
use std::time::Duration;

use async_imap::{Client, Session};
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_native_tls::{TlsConnector, TlsStream};

use crate::messages::ListenerType;
use crate::types::{EmailConfig, MessagingConfig, MessagingCredentials, MessagingError, MessagingSubscriptionError};

use super::send::build_smtp_transport;

pub(super) type ImapConnection = Arc<Mutex<Option<ImapSession>>>;
pub(super) type ImapSession = Session<TlsStream<TcpStream>>;

/// Validates email settings and applies the supplied credentials.
pub(super) fn configure(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<EmailConfig, MessagingError> {
    let MessagingCredentials::Email { username, password } = credentials else {
        return Err(MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "credential protocol does not match the listener".to_owned(),
        });
    };

    let mut config = settings
        .email
        .clone()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "Email config block is missing".to_owned(),
        })?;

    config
        .imap_server
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "IMAP server is missing".to_owned(),
        })?;
    config.username = Some(username.clone());
    config.password = Some(password.clone());

    prepare(&config)?;
    Ok(config)
}

/// Opens an authenticated IMAP session using the configured transport.
pub(super) async fn connect(config: &EmailConfig) -> Result<ImapSession, MessagingSubscriptionError> {
    let deadline = Duration::from_secs(30);

    let server = config
        .imap_server
        .as_deref()
        .expect("IMAP server validated at construction");

    let port = config.imap_port.unwrap_or(993);

    let username = config
        .username
        .as_deref()
        .expect("username validated at construction");

    let password = config
        .password
        .as_deref()
        .expect("password validated at construction");

    let tcp = timeout(deadline, TcpStream::connect((server, port)))
        .await
        .map_err(|_| email_timeout("tcp_connect", deadline))?
        .map_err(|error| email_connection(format!("IMAP TCP connect failed: {error}")))?;
    let connector = TlsConnector::from(
        NativeTlsConnector::new().map_err(|error| email_connection(format!("TLS connector build failed: {error}")))?,
    );

    let tls = timeout(deadline, connector.connect(server, tcp))
        .await
        .map_err(|_| email_timeout("tls_handshake", deadline))?
        .map_err(|error| email_connection(format!("IMAP TLS handshake failed: {error}")))?;

    let mut client = Client::new(tls);
    timeout(deadline, client.read_response())
        .await
        .map_err(|_| email_timeout("imap_greeting", deadline))?
        .map_err(|error| email_connection(format!("IMAP greeting read failed: {error}")))?
        .ok_or_else(|| MessagingSubscriptionError::StreamClosed {
            listener_type: ListenerType::Email,
            message: "IMAP server closed connection before greeting".to_owned(),
        })?;

    let session = timeout(deadline, client.login(username.as_str(), password.as_str()))
        .await
        .map_err(|_| email_timeout("imap_login", deadline))?
        .map_err(|(error, _)| MessagingSubscriptionError::Authentication {
            listener_type: ListenerType::Email,
            message: format!("IMAP login failed: {error}"),
        })?;

    Ok(session)
}

/// Authenticates every configured remote email transport without selecting a mailbox or sending mail.
pub(super) async fn verify_remote(
    settings: &MessagingConfig,
    credentials: &MessagingCredentials,
) -> Result<(), MessagingError> {
    let MessagingCredentials::Email { username, password } = credentials else {
        return Err(connection_failed("credential protocol does not match the listener"));
    };
    let mut config = settings
        .email
        .clone()
        .ok_or_else(|| connection_failed("Email config block is missing"))?;
    config.username = Some(username.clone());
    config.password = Some(password.clone());
    if config.imap_server.is_none() && config.smtp_server.is_none() {
        return Err(connection_failed("Email requires an IMAP or SMTP server"));
    }

    if config.imap_server.is_some() {
        prepare(&config)?;
        let session = connect(&config).await.map_err(|error| match error {
            MessagingSubscriptionError::Authentication { .. } => connection_failed("IMAP authentication rejected"),
            _ => connection_failed("IMAP connectivity probe failed"),
        })?;
        drop(session);
    }
    if config.smtp_server.is_some() {
        let transport = build_smtp_transport(&config, (username, password))
            .map_err(|_| connection_failed("SMTP transport configuration is invalid"))?;
        match timeout(Duration::from_secs(30), transport.test_connection()).await {
            Err(_) => return Err(connection_failed("SMTP connection probe timed out")),
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return Err(connection_failed("SMTP server rejected connection probe")),
            Ok(Err(error)) if error.is_response() && error.is_permanent() => {
                return Err(connection_failed("SMTP authentication rejected"));
            }
            Ok(Err(_)) => return Err(connection_failed("SMTP connectivity probe failed")),
        }
    }
    Ok(())
}

fn connection_failed(message: &str) -> MessagingError {
    MessagingError::ChannelConnectionFailed {
        listener_type: ListenerType::Email,
        message: message.to_owned(),
    }
}

/// Validates the required IMAP server and credentials without taking ownership.
fn prepare(config: &EmailConfig) -> Result<(), MessagingError> {
    config
        .imap_server
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "IMAP server is missing".to_owned(),
        })?;

    config
        .username
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "Email username is missing".to_owned(),
        })?;

    config
        .password
        .as_ref()
        .ok_or_else(|| MessagingError::ChannelConnectionFailed {
            listener_type: ListenerType::Email,
            message: "Email password is missing".to_owned(),
        })?;

    Ok(())
}

/// Builds an email protocol error for sibling provider modules.
pub(super) fn email_protocol(message: String) -> MessagingSubscriptionError {
    MessagingSubscriptionError::Protocol {
        listener_type: ListenerType::Email,
        message,
    }
}

fn email_connection(message: String) -> MessagingSubscriptionError {
    MessagingSubscriptionError::Connection {
        listener_type: ListenerType::Email,
        message,
    }
}

/// Builds an email timeout error with the stage's deadline.
pub(super) fn email_timeout(operation: &str, timeout: Duration) -> MessagingSubscriptionError {
    MessagingSubscriptionError::Timeout {
        listener_type: ListenerType::Email,
        operation: operation.to_owned(),
        timeout: format!("{timeout:?}"),
    }
}

#[cfg(test)]
pub(super) fn test_prepare(config: &EmailConfig) -> Result<(), MessagingError> {
    prepare(config)
}

#[cfg(test)]
mod verification_tests {
    use super::*;

    #[tokio::test]
    async fn verify_remote_rejects_wrong_credential_without_network() {
        let error = verify_remote(
            &MessagingConfig {
                email: Some(EmailConfig {
                    smtp_server: Some("127.0.0.1".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &MessagingCredentials::Telegram {
                token: "secret-token".to_owned().into(),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, MessagingError::ChannelConnectionFailed { message, .. } if message == "credential protocol does not match the listener")
        );
        assert!(!error.to_string().contains("secret-token"));
    }

    #[tokio::test]
    async fn verify_remote_rejects_no_configured_server_without_network() {
        let credentials = MessagingCredentials::Email {
            username: "user@example.com".to_owned().into(),
            password: "secret-password".to_owned().into(),
        };
        let error = verify_remote(
            &MessagingConfig {
                email: Some(EmailConfig::default()),
                ..Default::default()
            },
            &credentials,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, MessagingError::ChannelConnectionFailed { .. }));
        assert!(!error.to_string().contains("secret-password"));
    }
}

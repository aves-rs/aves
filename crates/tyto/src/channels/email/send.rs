use std::time::Duration;

use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::{AsyncSmtpTransport, Error as SmtpError};
use lettre::{AsyncTransport, Message, Tokio1Executor};
use tokio::time::timeout;
use zeroize::Zeroizing;

use crate::types::{
    EmailConfig, EmailSendMetadata, EmailSenderConfig, MessagingSendError, MessagingSendErrorCode,
    MessagingSendReceipt, NormalizedMessage,
};

use super::helpers::{deserialize_metadata, parse_mailbox};

const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends one email through the configured SMTP transport.
pub(super) async fn send(
    sender: &EmailSenderConfig,
    settings: &EmailConfig,
    credentials: (&Zeroizing<String>, &Zeroizing<String>),
    message: &NormalizedMessage,
) -> Result<MessagingSendReceipt, MessagingSendError> {
    let email = build_email(sender, message)?;

    let transport = build_smtp_transport(settings, credentials)
        .map_err(|error| terminal(MessagingSendErrorCode::Validation, &error))?;

    let response = timeout(SEND_TIMEOUT, transport.send(email))
        .await
        .map_err(|_| MessagingSendError::Retryable {
            code: MessagingSendErrorCode::Transport,
            message: "SMTP send timed out".to_owned(),
            retry_after_seconds: None,
        })?
        .map_err(classify_error)?;

    Ok(MessagingSendReceipt::SmtpAccepted {
        response: format!(
            "{} {}",
            response.code(),
            response.message().collect::<Vec<_>>().join(" ")
        ),
    })
}

pub(super) fn build_smtp_transport(
    settings: &EmailConfig,
    credentials: (&Zeroizing<String>, &Zeroizing<String>),
) -> Result<AsyncSmtpTransport<Tokio1Executor>, String> {
    let host = settings
        .smtp_server
        .as_deref()
        .ok_or_else(|| "SMTP server is missing".to_owned())?;
    let port = settings.smtp_port.unwrap_or(587);
    let credentials = Credentials::new(credentials.0.to_string(), credentials.1.to_string());
    let transport = if port == 465 {
        AsyncSmtpTransport::<Tokio1Executor>::relay(host)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
    }
    .map_err(|error| error.to_string())?
    .port(port)
    .credentials(credentials)
    .timeout(Some(SEND_TIMEOUT))
    .build();
    Ok(transport)
}

fn build_email(sender: &EmailSenderConfig, message: &NormalizedMessage) -> Result<Message, MessagingSendError> {
    let metadata: EmailSendMetadata = deserialize_metadata(&message.metadata)?;
    if metadata.to.is_empty() && metadata.cc.is_empty() && metadata.bcc.is_empty() {
        return Err(terminal(
            MessagingSendErrorCode::Validation,
            "at least one email recipient is required",
        ));
    }

    let mut builder = Message::builder().from(parse_mailbox(&sender.from)?);

    for address in &metadata.to {
        builder = builder.to(parse_mailbox(address)?);
    }
    for address in &metadata.cc {
        builder = builder.cc(parse_mailbox(address)?);
    }
    for address in &metadata.bcc {
        builder = builder.bcc(parse_mailbox(address)?);
    }
    if let Some(subject) = metadata.subject {
        builder = builder.subject(subject);
    }
    builder
        .header(ContentType::TEXT_PLAIN)
        .body(message.body.clone())
        .map_err(|error| terminal(MessagingSendErrorCode::Validation, &error.to_string()))
}

/// Maps lettre response and transport failures to the public send categories.
fn classify_error(error: SmtpError) -> MessagingSendError {
    let message = error.to_string();
    let response_code = error.status().map(u16::from);
    if error.is_transient() {
        return MessagingSendError::Retryable {
            code: transient_code(response_code),
            message,
            retry_after_seconds: None,
        };
    }
    if error.is_timeout() || error.is_transport_shutdown() || (!error.is_response() && !error.is_permanent()) {
        return MessagingSendError::Retryable {
            code: MessagingSendErrorCode::Transport,
            message,
            retry_after_seconds: None,
        };
    }
    MessagingSendError::Terminal {
        code: permanent_code(response_code),
        message,
    }
}

fn transient_code(response_code: Option<u16>) -> MessagingSendErrorCode {
    if response_code.is_some_and(|code| matches!(code, 421 | 450 | 451 | 452)) {
        MessagingSendErrorCode::RateLimited
    } else {
        MessagingSendErrorCode::Server
    }
}

fn permanent_code(response_code: Option<u16>) -> MessagingSendErrorCode {
    match response_code {
        Some(530 | 535) => MessagingSendErrorCode::InvalidCredentials,
        Some(550 | 551 | 553) => MessagingSendErrorCode::InvalidDestination,
        Some(554) => MessagingSendErrorCode::PermissionDenied,
        _ => MessagingSendErrorCode::Server,
    }
}

fn terminal(code: MessagingSendErrorCode, message: &str) -> MessagingSendError {
    MessagingSendError::Terminal {
        code,
        message: message.to_owned(),
    }
}

#[cfg(test)]
pub(super) fn test_build_email(
    sender: &EmailSenderConfig,
    message: &NormalizedMessage,
) -> Result<Message, MessagingSendError> {
    build_email(sender, message)
}

#[cfg(test)]
pub(super) fn test_transient_code(response_code: Option<u16>) -> MessagingSendErrorCode {
    transient_code(response_code)
}

#[cfg(test)]
pub(super) fn test_permanent_code(response_code: Option<u16>) -> MessagingSendErrorCode {
    permanent_code(response_code)
}

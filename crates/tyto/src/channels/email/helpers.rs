use lettre::message::Mailbox;
use mail_parser::MessageParser;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::messages::ListenerType;
use crate::types::{MessagingSendError, MessagingSendErrorCode, NormalizedMessage};

/// Projects a parsed email into the shared message representation.
pub(super) fn normalize_update(raw: &[u8]) -> Option<NormalizedMessage> {
    let parser = MessageParser::default();
    let message = parser.parse(raw)?;

    let body = message
        .body_text(0)
        .map(|s| s.into_owned())
        .unwrap_or_default();

    let subject = message.subject().map(|s| s.to_string()).unwrap_or_default();

    let from = message.from().and_then(|addresses| addresses.first());
    let from_name = from.and_then(|address| address.name()).map(str::to_owned);
    let from_address = from
        .and_then(|address| address.address())
        .map(str::to_owned);

    let message_id = message.message_id().map(|s| s.to_string());
    let date = message.date().map(|d| d.to_rfc3339());

    if body.is_empty() && subject.is_empty() {
        return None;
    }

    let metadata = json!({
        "channel_type": ListenerType::Email,
        "subject": subject,
        "from_name": from_name,
        "from_address": from_address,
        "message_id": message_id,
        "date": date,
    });

    Some(NormalizedMessage { body, metadata })
}

/// Deserializes outbound metadata without cloning its JSON value.
pub(super) fn deserialize_metadata<T: DeserializeOwned>(metadata: &Value) -> Result<T, MessagingSendError> {
    T::deserialize(metadata).map_err(|error| MessagingSendError::Terminal {
        code: MessagingSendErrorCode::Validation,
        message: format!("invalid outbound email metadata: {error}"),
    })
}

/// Validates an email address using lettre's mailbox parser.
pub(super) fn parse_mailbox(address: &str) -> Result<Mailbox, MessagingSendError> {
    address
        .parse()
        .map_err(|error| MessagingSendError::Terminal {
            code: MessagingSendErrorCode::InvalidDestination,
            message: format!("invalid email address: {error}"),
        })
}

pub(super) fn sender_in_list(message: &NormalizedMessage, list: &[String]) -> bool {
    if list.is_empty() {
        return true;
    }

    let sender = message
        .metadata
        .get("from_address")
        .and_then(|value| value.as_str())
        .filter(|sender| !sender.is_empty());

    sender.is_some_and(|sender| list.iter().any(|entry| entry.eq_ignore_ascii_case(sender)))
}

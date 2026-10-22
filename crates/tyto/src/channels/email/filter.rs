use serde_json::Value;

use crate::types::NormalizedMessage;

use super::helpers::sender_in_list;

/// Owns email-specific sender and mailbox admission policy.
pub(super) struct EmailFilter {
    pub(super) allowed_senders: Vec<String>,
    pub(super) subscribed_channels: Option<Vec<String>>,
}

impl EmailFilter {
    pub(super) fn new(allowed_senders: Vec<String>) -> Self {
        Self {
            allowed_senders,
            subscribed_channels: None,
        }
    }

    pub(super) fn apply(&self, message: NormalizedMessage) -> Option<NormalizedMessage> {
        let allowed = sender_in_list(&message, &self.allowed_senders)
            && self.subscribed_channels.as_ref().is_none_or(|channels| {
                message
                    .metadata
                    .get("channel_id")
                    .and_then(Value::as_str)
                    .is_some_and(|channel| {
                        channels.iter().any(|selected| {
                            selected == channel
                                || (selected.eq_ignore_ascii_case("INBOX") && channel.eq_ignore_ascii_case("INBOX"))
                        })
                    })
            });
        if allowed { Some(message) } else { None }
    }
}

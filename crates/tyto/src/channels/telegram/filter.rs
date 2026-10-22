use serde_json::Value;

use crate::types::NormalizedMessage;

/// Applies Telegram chat selection and sender ACLs to normalized messages.
pub(super) struct TelegramFilter {
    pub(super) subscribed_channels: Option<Vec<String>>,
    pub(super) allowed_senders: Vec<String>,
}

impl TelegramFilter {
    pub(super) fn new(subscribed_channels: Option<Vec<String>>, allowed_senders: Vec<String>) -> Self {
        Self {
            subscribed_channels,
            allowed_senders,
        }
    }

    pub(super) fn apply(&self, message: NormalizedMessage) -> Option<NormalizedMessage> {
        let metadata = &message.metadata;
        let channel = metadata.get("chat_id").and_then(Value::as_str);
        let sender_id = metadata.get("sender_user_id").and_then(Value::as_str);
        let username = metadata.get("sender_username").and_then(Value::as_str);
        if !self
            .subscribed_channels
            .as_ref()
            .is_none_or(|channels| channel.is_some_and(|channel| channels.iter().any(|selected| selected == channel)))
            || (!self.allowed_senders.is_empty()
                && !self.allowed_senders.iter().any(|allowed| {
                    sender_id == Some(allowed.as_str())
                        || username.is_some_and(|username| {
                            allowed
                                .trim_start_matches('@')
                                .eq_ignore_ascii_case(username)
                        })
                }))
        {
            return None;
        }
        Some(message)
    }
}

use serde_json::Value;

use crate::types::NormalizedMessage;

pub(super) struct SlackFilter {
    pub(super) subscribed_channels: Option<Vec<String>>,
    pub(super) allowed_senders: Vec<String>,
}

impl SlackFilter {
    pub(super) fn new(subscribed_channels: Option<Vec<String>>) -> Self {
        Self {
            subscribed_channels,
            allowed_senders: Vec::new(),
        }
    }

    pub(super) fn with_senders(mut self, allowed_senders: Vec<String>) -> Self {
        self.allowed_senders = allowed_senders;
        self
    }

    pub(super) fn accepts(&self, channel: &str) -> bool {
        self.subscribed_channels
            .as_ref()
            .is_none_or(|channels| channels.iter().any(|selected| selected == channel))
    }

    /// Checks message metadata against this subscription's channel and sender policy.
    pub(super) fn accepts_message(&self, message: &NormalizedMessage) -> bool {
        let Some(channel) = message.metadata.get("channel_id").and_then(Value::as_str) else {
            return false;
        };
        let sender = message.metadata.get("user_id").and_then(Value::as_str);
        self.accepts(channel)
            && (self.allowed_senders.is_empty()
                || sender.is_some_and(|sender| self.allowed_senders.iter().any(|allowed| allowed == sender)))
    }
}

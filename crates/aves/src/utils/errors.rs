use thiserror::Error;

/// Infrastructure failures shared by Aves and embedding application domains.
///
/// Domains convert this non-wire error into their own outward error types.
#[derive(Debug, Clone, Error)]
pub enum SharedInfraError {
    /// Sending a request or reply over an actor/channel boundary failed.
    #[error("channel send error for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// Receiving a request or reply over an actor/channel boundary failed.
    #[error("channel receive error for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// Waiting for a oneshot reply exceeded the caller's timeout.
    #[error("oneshot reply timed out for {target_domain}.{operation} after {timeout}")]
    OneshotReplyTimeout {
        target_domain: String,
        operation: String,
        timeout: String,
    },
    /// The oneshot reply sender was dropped before a response was sent.
    #[error("oneshot reply channel closed for {target_domain}.{operation}: {cause}")]
    OneshotReplyMissing {
        target_domain: String,
        operation: String,
        cause: String,
    },
}

#[cfg(test)]
mod tests {
    use super::SharedInfraError;

    #[test]
    fn variants_display_exact_context_and_cause() {
        let cases = [
            (
                SharedInfraError::ChannelSend {
                    target_domain: "orders".into(),
                    operation: "place".into(),
                    cause: "receiver dropped".into(),
                },
                "channel send error for orders.place: receiver dropped",
            ),
            (
                SharedInfraError::ChannelRecv {
                    target_domain: "history".into(),
                    operation: "load".into(),
                    cause: "channel closed".into(),
                },
                "channel receive error for history.load: channel closed",
            ),
            (
                SharedInfraError::OneshotReplyTimeout {
                    target_domain: "market".into(),
                    operation: "snapshot".into(),
                    timeout: "250ms".into(),
                },
                "oneshot reply timed out for market.snapshot after 250ms",
            ),
            (
                SharedInfraError::OneshotReplyMissing {
                    target_domain: "watch".into(),
                    operation: "refresh".into(),
                    cause: "reply sender dropped".into(),
                },
                "oneshot reply channel closed for watch.refresh: reply sender dropped",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }
}

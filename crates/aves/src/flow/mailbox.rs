use std::collections::VecDeque;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;

use super::blackboard::BoundOutputs;
use super::messages::LifecycleCorrelation;
use super::types::NodeExecutionId;

/// Terminal state reported by a node owner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub(super) enum TerminalStatus {
    /// The node completed successfully.
    Success,
    /// The node completed unsuccessfully.
    Failure,
}

/// A terminal owner result, correlated to exactly one node execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub(super) struct TerminalResult {
    /// Durable execution correlation.
    pub(super) correlation: LifecycleCorrelation,
    /// Owner outcome.
    pub(super) status: TerminalStatus,
    /// Named values eligible for the configured output bindings.
    pub(super) outputs: BoundOutputs,
    /// Owner-neutral terminal detail retained for inspection.
    pub(super) state: Value,
    /// Typed owner failure text, when the terminal result failed.
    pub(super) error: Option<String>,
}

/// Liveness notification from Flow's token manager.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub(super) struct TokenExpired {
    /// Correlation whose token expired.
    pub(super) correlation: LifecycleCorrelation,
}

/// Ordered mailbox item. Terminal results and expiry share one queue.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub(super) enum MailboxMessage {
    /// Owner terminal result.
    Terminal(TerminalResult),
    /// Token liveness signal.
    TokenExpired(TokenExpired),
}

/// Result of resolving a node mailbox for one tick.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum MailboxResolution {
    /// No terminal or expiry signal is available yet.
    Pending,
    /// The owner completed.
    Terminal(Box<TerminalResult>),
    /// Expiry was observed before a terminal result.
    Expired,
}

/// Flow-local ordered mailbox.
#[derive(Clone, Debug, Default)]
pub(super) struct NodeMailbox {
    messages: VecDeque<MailboxMessage>,
}

impl NodeMailbox {
    pub(super) fn snapshot(&self) -> Vec<MailboxMessage> {
        self.messages.iter().cloned().collect()
    }

    /// Enqueues a message in FIFO order.
    pub(super) fn push(&mut self, message: MailboxMessage) {
        self.messages.push_back(message);
    }

    /// Resolves the first signal for a node execution in enqueue order.
    ///
    /// Owner results and token expiry are deliberately serialized through the
    /// same queue: expiry cannot overwrite a terminal result already enqueued.
    pub(super) fn resolve(&mut self, correlation: LifecycleCorrelation) -> MailboxResolution {
        let position = self.messages.iter().position(|message| match message {
            MailboxMessage::Terminal(result) => result.correlation == correlation,
            MailboxMessage::TokenExpired(expired) => expired.correlation == correlation,
        });
        let Some(position) = position else {
            return MailboxResolution::Pending;
        };
        let message = self
            .messages
            .remove(position)
            .expect("mailbox position exists");
        match message {
            MailboxMessage::Terminal(result) => MailboxResolution::Terminal(Box::new(result)),
            MailboxMessage::TokenExpired(_) => MailboxResolution::Expired,
        }
    }

    /// Removes all messages for an execution during abort.
    pub(super) fn discard(&mut self, node_execution_id: NodeExecutionId) {
        self.messages.retain(|message| match message {
            MailboxMessage::Terminal(result) => result.correlation.node_execution_id != node_execution_id,
            MailboxMessage::TokenExpired(expired) => expired.correlation.node_execution_id != node_execution_id,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::{FlowExecutionId, FlowId, FlowNodeId, NodeExecutionId, NodeKind};
    use super::*;
    use uuid::Uuid;

    fn correlation() -> LifecycleCorrelation {
        LifecycleCorrelation {
            node_kind: NodeKind::parse("ScriptNode").expect("static node kind is valid"),
            flow_id: FlowId(Uuid::new_v4()),
            flow_node_id: FlowNodeId(Uuid::new_v4()),
            flow_execution_id: FlowExecutionId(Uuid::new_v4()),
            node_execution_id: NodeExecutionId(Uuid::new_v4()),
        }
    }

    #[test]
    fn resolves_in_enqueue_order_and_discards_late_signals() {
        let correlation = correlation();
        let mut mailbox = NodeMailbox::default();
        assert_eq!(mailbox.resolve(correlation.clone()), MailboxResolution::Pending);
        mailbox.push(MailboxMessage::TokenExpired(TokenExpired {
            correlation: correlation.clone(),
        }));
        mailbox.push(MailboxMessage::Terminal(TerminalResult {
            correlation: correlation.clone(),
            status: TerminalStatus::Success,
            outputs: BoundOutputs::default(),
            state: serde_json::Value::Null,
            error: None,
        }));
        assert_eq!(mailbox.resolve(correlation.clone()), MailboxResolution::Expired);
        assert!(matches!(
            mailbox.resolve(correlation.clone()),
            MailboxResolution::Terminal(_)
        ));
        mailbox.push(MailboxMessage::TokenExpired(TokenExpired {
            correlation: correlation.clone(),
        }));
        mailbox.discard(correlation.node_execution_id);
        assert_eq!(mailbox.resolve(correlation), MailboxResolution::Pending);
    }
}

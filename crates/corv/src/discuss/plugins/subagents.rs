//! Provider-neutral subagent tracking. Subagents arrive as ACP `subagent_update`s, as provider
//! lifecycle notifications, or (as a fallback) as the provider's delegating tool calls. The
//! tracker keeps the identity learned from earlier updates so later partial updates still yield a
//! complete record. A tool-call guess never moves a finished subagent back to running; a reported
//! lifecycle may, because providers reuse a child session for later work.

use std::collections::{BTreeMap, VecDeque};

// The schema crate exports MaybeUndefined only at its root, not through v1.
use agent_client_protocol::schema::MaybeUndefined;
use agent_client_protocol::v1::schema::{IdleStopReason, StateUpdate, SubagentUpdate, ToolCallStatus};
use chrono::Utc;

use super::super::types::{DiscussSubagentRecord, DiscussSubagentSurfaceState, DiscussUtilityPayload};

/// Most subagents remembered per session; the oldest finished ones are forgotten first.
const TRACKED_LIMIT: usize = 64;

/// One observation of a subagent from a provider payload.
pub(super) struct SubagentObservation<'a> {
    pub capture_id: &'a str,
    pub child_session_id: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub description: Option<&'a str>,
    /// `None` keeps the last known state (or `Loading` for a new subagent).
    pub state: Option<DiscussSubagentSurfaceState>,
}

/// Worker-local subagent records for one session.
#[derive(Default)]
pub struct SubagentTracker {
    known: BTreeMap<String, DiscussSubagentRecord>,
    /// Changes observed beyond the one a single capture can return, delivered on later captures.
    pending: VecDeque<DiscussSubagentRecord>,
    /// The session reports subagent lifecycles directly, so tool-call fallbacks stand down.
    reported: bool,
}

impl SubagentTracker {
    /// Whether this session reports subagent lifecycles directly.
    pub(super) fn reports_lifecycle(&self) -> bool {
        self.reported
    }

    /// Merge a lifecycle the provider reported itself; it may reopen a finished subagent.
    pub(super) fn observe_reported(&mut self, observation: SubagentObservation<'_>) {
        self.reported = true;
        self.merge(observation, true);
    }

    /// Merge an observation inferred from a tool call or partial update.
    pub(super) fn observe(&mut self, observation: SubagentObservation<'_>) {
        self.merge(observation, false);
    }

    /// Whether the capture id already names a tracked subagent.
    pub(super) fn knows(&self, capture_id: &str) -> bool {
        self.known.contains_key(capture_id)
    }

    /// Merge an observation; queue the updated record when anything visible changed.
    fn merge(&mut self, observation: SubagentObservation<'_>, reopen: bool) {
        if observation.capture_id.is_empty() {
            return;
        }
        let previous = self.known.get(observation.capture_id);
        let state = match (previous.map(|record| record.surface_state), observation.state) {
            (
                Some(kept @ (DiscussSubagentSurfaceState::Done | DiscussSubagentSurfaceState::Error)),
                Some(DiscussSubagentSurfaceState::Loading | DiscussSubagentSurfaceState::Live),
            ) if !reopen => kept,
            (_, Some(next)) => next,
            (Some(kept), None) => kept,
            (None, None) => DiscussSubagentSurfaceState::Loading,
        };
        let text = |value: Option<&str>| value.filter(|value| !value.is_empty()).map(str::to_string);
        let record = DiscussSubagentRecord {
            capture_id: observation.capture_id.to_string(),
            child_session_id: text(observation.child_session_id)
                .or_else(|| previous.and_then(|record| record.child_session_id.clone())),
            agent_name: text(observation.agent_name)
                .or_else(|| previous.map(|record| record.agent_name.clone()))
                .unwrap_or_else(|| "subagent".to_string()),
            description: text(observation.description)
                .or_else(|| previous.and_then(|record| record.description.clone())),
            fallback_capability: None,
            surface_state: state,
            last_updated_at: previous.map_or(0, |record| record.last_updated_at),
        };
        if previous.is_some_and(|previous| *previous == record) {
            return;
        }
        let record = DiscussSubagentRecord {
            last_updated_at: Utc::now().timestamp_millis(),
            ..record
        };
        self.known.insert(record.capture_id.clone(), record.clone());
        self.pending.push_back(record);
        while self.known.len() > TRACKED_LIMIT {
            let Some(id) = self
                .known
                .iter()
                .find(|(_, record)| {
                    matches!(
                        record.surface_state,
                        DiscussSubagentSurfaceState::Done | DiscussSubagentSurfaceState::Error
                    )
                })
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            self.known.remove(&id);
        }
    }

    /// The next queued change, as the utility payload a capture returns.
    pub(super) fn next(&mut self) -> Option<DiscussUtilityPayload> {
        self.pending
            .pop_front()
            .map(|subagent| DiscussUtilityPayload::Subagent { subagent })
    }
}

/// ACP `subagent_update` on the parent session: `sessionId` names the child, `title` and
/// `description` are display metadata, and `state` is `running` | `requires_action` | `unknown` |
/// `idle` (with `stopReason`; `error` or `cancelled` means the work did not finish). Omitted or
/// null fields keep their previous values.
pub(super) fn capture_standard_update(update: &SubagentUpdate, tracker: &mut SubagentTracker) {
    let child = update.session_id.0.as_ref();
    let state = match &update.state {
        MaybeUndefined::Value(StateUpdate::Running(_) | StateUpdate::RequiresAction(_)) => {
            Some(DiscussSubagentSurfaceState::Live)
        }
        MaybeUndefined::Value(StateUpdate::Unknown(_)) => Some(DiscussSubagentSurfaceState::Loading),
        MaybeUndefined::Value(StateUpdate::Idle(idle)) => Some(
            if matches!(
                &idle.stop_reason,
                Some(IdleStopReason::Cancelled | IdleStopReason::Error(_))
            ) {
                DiscussSubagentSurfaceState::Error
            } else {
                DiscussSubagentSurfaceState::Done
            },
        ),
        _ => None,
    };
    tracker.observe_reported(SubagentObservation {
        capture_id: child,
        child_session_id: Some(child),
        agent_name: match &update.title {
            MaybeUndefined::Value(value) => Some(value.as_str()),
            _ => None,
        },
        description: match &update.description {
            MaybeUndefined::Value(value) => Some(value.as_str()),
            _ => None,
        },
        state,
    });
}

/// The surface state for an ACP tool-call status.
pub(super) fn tool_status_state(status: Option<&ToolCallStatus>) -> Option<DiscussSubagentSurfaceState> {
    match status? {
        ToolCallStatus::Pending => Some(DiscussSubagentSurfaceState::Loading),
        ToolCallStatus::InProgress => Some(DiscussSubagentSurfaceState::Live),
        ToolCallStatus::Completed => Some(DiscussSubagentSurfaceState::Done),
        ToolCallStatus::Failed => Some(DiscussSubagentSurfaceState::Error),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe(tracker: &mut SubagentTracker, state: Option<DiscussSubagentSurfaceState>, name: Option<&str>) {
        tracker.observe(SubagentObservation {
            capture_id: "call-1",
            child_session_id: None,
            agent_name: name,
            description: None,
            state,
        });
    }

    fn state(payload: Option<DiscussUtilityPayload>) -> (DiscussSubagentSurfaceState, String) {
        match payload {
            Some(DiscussUtilityPayload::Subagent { subagent }) => (subagent.surface_state, subagent.agent_name),
            other => panic!("expected a subagent update, got {other:?}"),
        }
    }

    #[test]
    fn partial_updates_keep_identity_and_finished_subagents_stay_finished() {
        let mut tracker = SubagentTracker::default();
        observe(
            &mut tracker,
            Some(DiscussSubagentSurfaceState::Loading),
            Some("Explore"),
        );
        assert_eq!(
            state(tracker.next()),
            (DiscussSubagentSurfaceState::Loading, "Explore".to_string())
        );
        observe(&mut tracker, Some(DiscussSubagentSurfaceState::Live), None);
        assert_eq!(
            state(tracker.next()),
            (DiscussSubagentSurfaceState::Live, "Explore".to_string())
        );
        observe(&mut tracker, None, None);
        assert!(tracker.next().is_none());
        observe(&mut tracker, Some(DiscussSubagentSurfaceState::Done), None);
        assert_eq!(state(tracker.next()).0, DiscussSubagentSurfaceState::Done);
        observe(&mut tracker, Some(DiscussSubagentSurfaceState::Live), None);
        assert!(tracker.next().is_none());
        assert!(!tracker.reports_lifecycle());
    }

    #[test]
    fn standard_updates_report_and_reopen_the_child() {
        let mut tracker = SubagentTracker::default();
        let mut capture = |update: serde_json::Value| {
            capture_standard_update(&serde_json::from_value(update).expect("subagent update"), &mut tracker);
            match tracker.next() {
                Some(DiscussUtilityPayload::Subagent { subagent }) => {
                    Some((subagent.capture_id, subagent.agent_name, subagent.surface_state))
                }
                _ => None,
            }
        };
        let running = serde_json::json!({"sessionUpdate": "subagent_update", "sessionId": "child",
            "title": "Explore auth", "state": {"state": "running"}});
        assert_eq!(
            capture(running.clone()),
            Some(("child".into(), "Explore auth".into(), DiscussSubagentSurfaceState::Live))
        );
        let idle = serde_json::json!({"sessionUpdate": "subagent_update", "sessionId": "child",
            "state": {"state": "idle", "stopReason": "end_turn"}});
        assert_eq!(capture(idle).map(|row| row.2), Some(DiscussSubagentSurfaceState::Done));
        assert_eq!(
            capture(running).map(|row| row.2),
            Some(DiscussSubagentSurfaceState::Live)
        );
        let failed = serde_json::json!({"sessionUpdate": "subagent_update", "sessionId": "child",
            "state": {"state": "idle", "stopReason": "error", "error": {"code": -32603, "message": "x"}}});
        assert_eq!(
            capture(failed).map(|row| row.2),
            Some(DiscussSubagentSurfaceState::Error)
        );
        let unknown = serde_json::json!({"sessionUpdate": "subagent_update", "sessionId": "child",
            "state": {"state": "unknown"}});
        assert_eq!(
            capture(unknown).map(|row| row.2),
            Some(DiscussSubagentSurfaceState::Loading)
        );
        assert!(tracker.reports_lifecycle());
    }
}

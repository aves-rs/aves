//! Put all the stuff to talk the agents here

use std::collections::BTreeMap;
use std::mem::take;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use agent_client_protocol::v1::{
    SessionMessage,
    schema::{
        AvailableCommandInput, ContentBlock, CreateElicitationRequest, PermissionOptionId, RequestPermissionOutcome,
        RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome, SessionConfigKind, SessionId,
        SessionNotification, SessionUpdate, StopReason,
    },
};
use agent_client_protocol::{Dispatch, Error as AcpCrateError, Responder};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::database::PersistenceJob;
use super::elicitation::classify_elicitation_schema;
use super::error::DiscussError;
use super::plugins::{DiscussAgentPlugin, PluginCaptureState};
use super::report_types::DiscussCompletionKind;
use super::types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, DiscussElicitationAnswer,
    DiscussPermissionOption, DiscussSessionId, DiscussStreamEvent, DiscussTurnUsage, DiscussUtilityPayload,
    DiscussUtilityUpdate, ElicitationRequestId, PendingElicitation, PermissionRequestId,
};
use super::usage::{UsageCaptureEvent, UsageTurnAccumulator};

/// Handles an incoming permission request from the agent.
///
/// The actor must register the pending entry before publishing the request,
/// and remove it and cancel the responder if publication fails.
pub fn prepare_permission_request(
    _session: &SessionId,
    request: &RequestPermissionRequest,
) -> (
    PermissionRequestId,
    Vec<PermissionOptionId>,
    oneshot::Receiver<DiscussPermissionOption>,
    oneshot::Sender<DiscussPermissionOption>,
) {
    let permission_request_id = PermissionRequestId::new();
    let offered_option_ids = request
        .options
        .iter()
        .map(|option| option.option_id.clone())
        .collect();
    let (responder_tx, responder_rx) = oneshot::channel();
    (permission_request_id, offered_option_ids, responder_rx, responder_tx)
}

/// Prepare an incoming ACP elicitation for actor-owned registration.
///
/// The actor must insert the returned pending entry before publishing the
/// elicitation stream event. Keeping the responder sender in that entry gives
/// answer handling the same race-free ordering as permission requests.
pub(crate) fn prepare_elicitation_request(
    request: &CreateElicitationRequest,
    position_allocator: &TranscriptPositionAllocator,
) -> (
    ElicitationRequestId,
    PendingElicitation,
    oneshot::Receiver<DiscussElicitationAnswer>,
) {
    let elicitation_request_id = ElicitationRequestId::new();
    let classification = classify_elicitation_schema(request);
    let (responder_tx, responder_rx) = oneshot::channel();
    let pending = PendingElicitation {
        position: position_allocator.next(),
        responder_tx,
        raw_request: request.clone(),
        classification,
    };
    (elicitation_request_id, pending, responder_rx)
}

/// Allocates unique, session-owned transcript positions for every persisted
/// user, assistant, and elicitation record.
#[derive(Clone)]
pub(crate) struct TranscriptPositionAllocator {
    next_position: Arc<AtomicI64>,
}

impl TranscriptPositionAllocator {
    /// Start at the first position not present in the persisted transcript.
    pub(crate) fn new(next_position: i64) -> Self {
        Self {
            next_position: Arc::new(AtomicI64::new(next_position)),
        }
    }

    /// Reserve the next position atomically across the worker and ACP callbacks.
    pub(crate) fn next(&self) -> i64 {
        self.next_position.fetch_add(1, Ordering::Relaxed)
    }
}

/// Resolve a provider permission responder with the user's answer.
///
/// Converts the `DiscussPermissionOption` into the ACP SDK
/// `RequestPermissionResponse` and sends it to the provider.
pub async fn resolve_permission_responder(
    responder: Responder<RequestPermissionResponse>,
    option: DiscussPermissionOption,
) -> Result<(), AcpCrateError> {
    let response = match option {
        DiscussPermissionOption::Select { option_id } => RequestPermissionResponse::new(
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(PermissionOptionId::new(option_id))),
        ),
        DiscussPermissionOption::Cancel => RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled),
    };
    responder.respond(response)?;
    Ok(())
}

// ── Assistant-turn buffer ──────────────────────────────────────────────────

/// Owned flush payload produced by [`AssistantTurnBuffer::flush`] on a
/// `StopReason` turn boundary.
#[derive(Clone, Debug)]
pub(super) struct FlushPayload {
    pub(super) aggregated_text: String,
    pub(super) aggregated_thought: String,
    /// Ordered transcript parts. Tool-call updates are retained as raw ACP
    /// update objects so the client can apply its existing stable-id merge.
    pub(super) parts: Vec<Value>,
    pub(super) stop_reason: String,
    pub(super) usage: Option<DiscussTurnUsage>,
}

/// Per-session assistant-turn accumulator split by ACP semantic part type.
#[derive(Debug, Default)]
pub(super) struct AssistantTurnBuffer {
    accumulated_text: String,
    accumulated_thought: String,
    parts: Vec<Value>,
    metadata_updates: Vec<Value>,
}

impl AssistantTurnBuffer {
    pub(super) fn append_text(&mut self, text: &str) {
        self.accumulated_text.push_str(text);
        self.parts
            .push(serde_json::json!({"kind": "text", "text": text}));
    }

    pub(super) fn append_thought(&mut self, thought: &str) {
        self.accumulated_thought.push_str(thought);
        self.parts
            .push(serde_json::json!({"kind": "thought", "text": thought}));
    }

    pub(super) fn append_image(&mut self, data: &str, mime_type: &str) {
        self.parts
            .push(serde_json::json!({"kind": "image", "mimeType": mime_type, "data": data}));
    }

    fn append_content_block(&mut self, block: &ContentBlock) {
        if let Ok(block) = serde_json::to_value(block) {
            self.parts
                .push(serde_json::json!({"kind": "content", "block": block}));
        }
    }

    pub(super) fn append_tool_call(&mut self, update: Value, is_update: bool) {
        self.parts.push(serde_json::json!({
            "kind": "tool_call",
            "update": update,
            "isUpdate": is_update,
        }));
    }

    pub(super) fn tool_call_count(&self) -> u64 {
        self.parts
            .iter()
            .filter(|part| {
                part.get("kind").and_then(Value::as_str) == Some("tool_call")
                    && part.get("isUpdate").and_then(Value::as_bool) != Some(true)
            })
            .count() as u64
    }

    pub(super) fn flush(&mut self, stop_reason: String) -> FlushPayload {
        let aggregated_text = take(&mut self.accumulated_text);
        let aggregated_thought = take(&mut self.accumulated_thought);
        let parts = take(&mut self.parts);
        FlushPayload {
            aggregated_text,
            aggregated_thought,
            parts,
            stop_reason,
            usage: None,
        }
    }

    pub(super) fn push_metadata_update(&mut self, update: Value) {
        self.metadata_updates.push(update);
    }

    pub(super) fn take_metadata_updates(&mut self) -> Vec<Value> {
        take(&mut self.metadata_updates)
    }
}

// ── Forwarder outcome ──────────────────────────────────────────────────────

/// Outcome of forwarding one `SessionMessage` to the user.
#[derive(Debug)]
pub(super) enum AgentMessageForwardOutcome {
    /// A dispatch was forwarded through the stream. The worker loop continues.
    Continue {
        utility_payload: Option<DiscussUtilityPayload>,
    },
    /// A `StopReason` was forwarded and the assistant-turn buffer was flushed.
    /// The worker must enter its between-turns path.
    TurnBoundary {
        flush: Box<FlushPayload>,
        utility_payload: Option<DiscussUtilityPayload>,
    },
}

pub(super) struct AgentMessageForwardContext<'a> {
    pub(super) stream_tx: &'a mpsc::Sender<DiscussStreamEvent>,
    pub(super) discuss_session_id: DiscussSessionId,
    pub(super) session: &'a SessionId,
    pub(super) agent_plugin: &'a dyn DiscussAgentPlugin,
    pub(super) capture_state: &'a mut PluginCaptureState,
    pub(super) usage: Option<(&'a mut UsageTurnAccumulator, &'a mpsc::Sender<PersistenceJob>)>,
}

/// Handles an incoming ACP message from the agent.
///
/// The forwarder publishes `DiscussStreamEvent::AgentMessage` on the bounded
/// stream sender. It does not read any user-turn route — the session worker
/// owns that wait.
pub(super) async fn send_message_to_user(
    context: AgentMessageForwardContext<'_>,
    message: SessionMessage,
    buffer: &mut AssistantTurnBuffer,
    suppress_transcript: bool,
) -> Result<AgentMessageForwardOutcome, AcpCrateError> {
    let AgentMessageForwardContext {
        stream_tx,
        discuss_session_id,
        session,
        agent_plugin,
        capture_state,
        mut usage,
    } = context;
    let mut turn_usage = None;
    let mut typed_notification = None;
    let stop_reason = match &message {
        SessionMessage::PromptResponse(Ok(response)) => Some(response.stop_reason),
        _ => None,
    };

    let payload = match message {
        SessionMessage::Message(dispatch) => {
            let raw = dispatch.into_raw();
            if let Dispatch::Notification(notification) = &raw {
                let (method, params) = notification.clone().into_parts();
                if method == "session/update" {
                    typed_notification = serde_json::from_value::<SessionNotification>(params.clone()).ok();
                }
                let event = if method == "session/update" {
                    typed_notification
                        .as_ref()
                        .map(|notification| UsageCaptureEvent::Update(&notification.update))
                } else {
                    Some(UsageCaptureEvent::Notification {
                        method: &method,
                        params: &params,
                    })
                };
                if let Some(evidence) = event
                    .as_ref()
                    .and_then(|event| agent_plugin.capture_usage_evidence(event))
                    && let Some((accumulator, queue)) = usage.as_mut()
                {
                    let correlatable = evidence.turn_correlatable;
                    accumulator.capture(evidence, queue).await;
                    turn_usage = accumulator.evidence_snapshot(correlatable);
                }
            }
            AcpAgentMessagePayload::Dispatch {
                session_id: session.to_string(),
                dispatch: dispatch_payload(raw),
            }
        }
        SessionMessage::PromptResponse(result) => {
            let response = result?;
            if let Some(evidence) = agent_plugin.capture_usage_evidence(&UsageCaptureEvent::PromptResponse(&response))
                && let Some((accumulator, queue)) = usage.as_mut()
            {
                accumulator.capture(evidence, queue).await;
            }
            if let Some((accumulator, queue)) = usage.as_mut() {
                if accumulator.has_active_turn() {
                    let reason = stop_reason_to_string(response.stop_reason);
                    let kind = if reason == "cancelled" {
                        DiscussCompletionKind::Cancelled
                    } else {
                        DiscussCompletionKind::Completed
                    };
                    accumulator.finish(kind, Some(reason), queue).await;
                }
                turn_usage = accumulator.finalized_snapshot();
            }
            acp_message_payload(session, SessionMessage::PromptResponse(Ok(response)))?
        }
        other => acp_message_payload(session, other)?,
    };

    if suppress_transcript
        && (stop_reason.is_some()
            || typed_notification
                .as_ref()
                .is_some_and(|notification| is_transcript_dispatch(&notification.update)))
    {
        return Ok(AgentMessageForwardOutcome::Continue { utility_payload: None });
    }

    let utility_payload = if let AcpAgentMessagePayload::Dispatch { dispatch, .. } = &payload {
        if let Some(notification) = &typed_notification {
            if let AcpDispatchPayload::Notification { params, .. } = dispatch {
                append_dispatch_delta(buffer, &notification.update, params);
            }
            append_metadata_update(buffer, notification);
        }
        capture_utility_payload(agent_plugin, dispatch, typed_notification.as_ref(), capture_state)
    } else {
        None
    };

    // Forward the typed payload to Zeus via the bounded stream.
    stream_tx
        .send(DiscussStreamEvent::AgentMessage {
            discuss_session_id,
            acp_session_id: session.clone(),
            payload,
            turn_usage,
        })
        .await
        .map_err(|error| stream_send_error("send_message_to_user.stream", error))?;

    let utility_payload_for_persistence = utility_payload.clone();
    if let Some(payload) = utility_payload {
        stream_tx
            .send(DiscussStreamEvent::UtilityUpdate {
                discuss_session_id,
                acp_session_id: session.clone(),
                utility: DiscussUtilityUpdate {
                    discuss_session_id,
                    payload,
                },
            })
            .await
            .map_err(|error| stream_send_error("send_message_to_user.utility_update", error))?;
    }

    match stop_reason {
        None => Ok(AgentMessageForwardOutcome::Continue {
            utility_payload: utility_payload_for_persistence,
        }),
        Some(reason) => {
            let flush_payload = buffer.flush(stop_reason_to_string(reason));
            Ok(AgentMessageForwardOutcome::TurnBoundary {
                flush: Box::new(flush_payload),
                utility_payload: utility_payload_for_persistence,
            })
        }
    }
}

fn stream_send_error(operation: &'static str, error: mpsc::error::SendError<DiscussStreamEvent>) -> AcpCrateError {
    AcpCrateError::into_internal_error(DiscussError::Infrastructure {
        kind: "channel_send".to_string(),
        target_domain: "discuss".to_string(),
        operation: operation.to_string(),
        message: error.to_string(),
    })
}

fn capture_utility_payload(
    agent_plugin: &dyn DiscussAgentPlugin,
    dispatch: &AcpDispatchPayload,
    notification: Option<&SessionNotification>,
    capture_state: &mut PluginCaptureState,
) -> Option<DiscussUtilityPayload> {
    let AcpDispatchPayload::Notification { method, params } = dispatch else {
        return None;
    };
    if method != "session/update" {
        return agent_plugin.capture_extension_notification(method, params, capture_state);
    }
    match &notification?.update {
        SessionUpdate::SubagentUpdate(update) => capture_state.capture_standard_subagent(update),
        SessionUpdate::ToolCall(call) => agent_plugin.capture_tool_call(call, capture_state),
        SessionUpdate::ToolCallUpdate(update) => agent_plugin.capture_tool_call_update(update, capture_state),
        _ => None,
    }
}

fn is_transcript_dispatch(update: &SessionUpdate) -> bool {
    matches!(
        update,
        SessionUpdate::UserMessageChunk(_)
            | SessionUpdate::AgentThoughtChunk(_)
            | SessionUpdate::AgentMessageChunk(_)
            | SessionUpdate::ToolCall(_)
            | SessionUpdate::ToolCallUpdate(_)
    )
}

/// Serialize a `StopReason` to the snake_case string the provider emitted.
fn stop_reason_to_string(reason: StopReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{reason:?}").to_lowercase())
}

fn append_dispatch_delta(buffer: &mut AssistantTurnBuffer, update: &SessionUpdate, params: &Value) {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) | SessionUpdate::AgentThoughtChunk(chunk) => match &chunk.content {
            ContentBlock::Text(text) if matches!(update, SessionUpdate::AgentThoughtChunk(_)) => {
                buffer.append_thought(&text.text);
            }
            ContentBlock::Text(text) => buffer.append_text(&text.text),
            ContentBlock::Image(image) if matches!(update, SessionUpdate::AgentMessageChunk(_)) => {
                buffer.append_image(&image.data, &image.mime_type);
            }
            ContentBlock::Resource(_) | ContentBlock::ResourceLink(_) => buffer.append_content_block(&chunk.content),
            _ => {}
        },
        SessionUpdate::ToolCall(_) | SessionUpdate::ToolCallUpdate(_) => {
            if let Some(raw_update) = params.get("update") {
                buffer.append_tool_call(raw_update.clone(), matches!(update, SessionUpdate::ToolCallUpdate(_)));
            }
        }
        _ => {}
    }
}

fn append_metadata_update(buffer: &mut AssistantTurnBuffer, notification: &SessionNotification) {
    let session_id = notification.session_id.to_string();
    let value = match &notification.update {
        SessionUpdate::AvailableCommandsUpdate(update) => {
            let commands: Vec<_> = update
                .available_commands
                .iter()
                .map(|command| {
                    let mut value = serde_json::json!({"name": command.name, "description": command.description});
                    if let Some(hint) = command.input.as_ref().and_then(|input| match input {
                        AvailableCommandInput::Unstructured(input) => Some(input.hint.clone()),
                        _ => None,
                    }) {
                        value["inputHint"] = Value::String(hint);
                    }
                    value
                })
                .collect();
            serde_json::json!({"sessionId": session_id, "availableCommands": commands})
        }
        SessionUpdate::SessionInfoUpdate(update) => {
            let mut value = serde_json::json!({"sessionId": session_id, "title": update.title.value(), "updatedAt": update.updated_at.value()});
            if let Some(meta) = &update.meta {
                value["_meta"] = serde_json::json!(meta);
            }
            value
        }
        SessionUpdate::ConfigOptionUpdate(update) => {
            let selected: BTreeMap<_, _> = update
                .config_options
                .iter()
                .filter_map(|option| match &option.kind {
                    SessionConfigKind::Select(select) => {
                        Some((option.id.to_string(), select.current_value.to_string()))
                    }
                    _ => None,
                })
                .collect();
            serde_json::json!({"sessionId": session_id, "options": update.config_options, "selected": selected})
        }
        _ => return,
    };
    buffer.push_metadata_update(value);
}

fn acp_message_payload(session: &SessionId, message: SessionMessage) -> Result<AcpAgentMessagePayload, AcpCrateError> {
    match message {
        SessionMessage::Message(dispatch) => Ok(AcpAgentMessagePayload::Dispatch {
            session_id: session.to_string(),
            dispatch: dispatch_payload(dispatch.into_raw()),
        }),
        SessionMessage::PromptResponse(result) => Ok(AcpAgentMessagePayload::StopReason {
            session_id: session.to_string(),
            stop_reason: result?.stop_reason,
        }),
        _ => Err(AcpCrateError::into_internal_error(DiscussError::ProtocolError {
            message: "unsupported ACP session message variant is not forwarded as an agent message".to_string(),
        })),
    }
}

fn dispatch_payload(dispatch: Dispatch) -> AcpDispatchPayload {
    match dispatch {
        Dispatch::Request(request, _) => {
            let (method, params) = request.into_parts();
            AcpDispatchPayload::Request { method, params }
        }
        Dispatch::Notification(notification) => {
            let (method, params) = notification.into_parts();
            AcpDispatchPayload::Notification { method, params }
        }
        Dispatch::Response(result, _) => AcpDispatchPayload::Response {
            result: match result {
                Ok(value) => AcpDispatchResponsePayload::Ok { value },
                Err(error) => AcpDispatchResponsePayload::Error {
                    message: error.to_string(),
                },
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::v1::SessionMessage;
    use agent_client_protocol::v1::schema::{
        ContentBlock, ElicitationFormMode, PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason,
    };
    use agent_client_protocol::{Dispatch, UntypedMessage};
    use serde_json::Value;
    use tokio::sync::mpsc;

    use super::super::elicitation::{DiscussRestrictedFormFieldType, sdk_to_restricted_schema};
    use super::super::types::{
        AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, DiscussSessionId, DiscussStreamEvent,
    };
    use super::{
        AgentMessageForwardContext, AgentMessageForwardOutcome, AssistantTurnBuffer, PluginCaptureState,
        acp_message_payload, dispatch_payload, send_message_to_user,
    };

    #[test]
    fn restricted_schema_projects_titled_multi_select_fields() {
        let mode = serde_json::from_value::<ElicitationFormMode>(serde_json::json!({
            "sessionId": "session-1",
            "requestedSchema": {
                "type": "object",
                "properties": {
                    "channels": {
                        "type": "array",
                        "title": "Channels",
                        "description": "Select delivery channels",
                        "minItems": 1,
                        "maxItems": 2,
                        "items": {
                            "type": "string",
                            "anyOf": [
                                { "const": "email", "title": "Email", "description": "Send email" },
                                { "const": "slack", "title": "Slack", "description": "Send Slack message" }
                            ]
                        },
                        "default": ["email"]
                    }
                },
                "required": ["channels"]
            }
        }))
        .expect("valid ACP multi-select form");

        let schema = sdk_to_restricted_schema(&mode).expect("restricted schema");
        let field = schema.fields.first().expect("multi-select field");

        assert_eq!(field.field_type, DiscussRestrictedFormFieldType::Array);
        assert_eq!(field.min, Some(1.0));
        assert_eq!(field.max, Some(2.0));
        assert_eq!(field.default, Some(serde_json::json!(["email"])));
        assert_eq!(field.options.len(), 2);
        assert_eq!(field.options[0].value, "email");
        assert_eq!(field.options[0].title.as_deref(), Some("Email"));
        assert_eq!(field.options[1].description.as_deref(), Some("Send Slack message"));
    }

    // ── dispatch_payload serialization ───────────────────────────────────────

    #[test]
    fn dispatch_payload_notification_variant() {
        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 75})).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification { method, params } => {
                assert_eq!(method, "tools/progress");
                assert_eq!(params, serde_json::json!({"pct": 75}));
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_notification_serializes_correctly() {
        let notification =
            UntypedMessage::new("session/update", serde_json::json!({"data": "x"})).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        let json = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(
            json.get("kind").and_then(|v| v.as_str()),
            Some("notification"),
            "kind must be 'notification'"
        );
        assert_eq!(json.get("method").and_then(|v| v.as_str()), Some("session/update"));
    }

    #[test]
    fn dispatch_response_ok_payload_serializes_correctly() {
        let payload = AcpDispatchPayload::Response {
            result: AcpDispatchResponsePayload::Ok {
                value: serde_json::json!({"answer": 42}),
            },
        };

        let json = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(json.get("kind").and_then(|v| v.as_str()), Some("response"));
        let result = json.get("result").expect("result field");
        assert_eq!(result.get("status").and_then(|v| v.as_str()), Some("ok"));
        assert_eq!(
            result
                .get("value")
                .and_then(|v| v.get("answer"))
                .and_then(|v| v.as_i64()),
            Some(42)
        );
    }

    #[test]
    fn dispatch_response_error_payload_serializes_correctly() {
        let payload = AcpDispatchPayload::Response {
            result: AcpDispatchResponsePayload::Error {
                message: "tool execution failed".to_string(),
            },
        };

        let json = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(json.get("kind").and_then(|v| v.as_str()), Some("response"));
        let result = json.get("result").expect("result field");
        assert_eq!(result.get("status").and_then(|v| v.as_str()), Some("error"));
        assert_eq!(
            result.get("message").and_then(|v| v.as_str()),
            Some("tool execution failed")
        );
    }

    // ── acp_message_payload dispatch variant ─────────────────────────────────

    #[test]
    fn acp_message_payload_dispatch_notification_variant() {
        let session = SessionId::from("dispatch-session");
        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 90})).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let message = SessionMessage::Message(dispatch);

        let payload = acp_message_payload(&session, message).expect("payload ok");

        match payload {
            AcpAgentMessagePayload::Dispatch { session_id, dispatch } => {
                assert_eq!(session_id, "dispatch-session");
                assert!(
                    matches!(dispatch, AcpDispatchPayload::Notification { .. }),
                    "dispatch must be Notification variant"
                );
            }
            other => panic!("expected Dispatch payload, got {other:?}"),
        }
    }

    #[test]
    fn acp_message_payload_dispatch_serializes_to_bridge_shape() {
        let session = SessionId::from("bridge-shape-session");
        let notification =
            UntypedMessage::new("session/update", serde_json::json!({"data": "x"})).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let message = SessionMessage::Message(dispatch);

        let payload = acp_message_payload(&session, message).expect("payload ok");
        let json = serde_json::to_value(&payload).expect("serialize");

        assert_eq!(
            json.get("kind").and_then(|v| v.as_str()),
            Some("dispatch"),
            "kind must be 'dispatch' for SessionMessage::Message"
        );
        assert_eq!(
            json.get("sessionId").and_then(|v| v.as_str()),
            Some("bridge-shape-session"),
            "sessionId must be camelCase"
        );
    }

    #[test]
    fn stop_reason_payload_serializes_to_bridge_shape() {
        let session = SessionId::from("serde-session");
        let payload = super::acp_message_payload(
            &session,
            SessionMessage::PromptResponse(Ok(PromptResponse::new(StopReason::Refusal))),
        )
        .expect("payload ok");

        let json = serde_json::to_value(&payload).expect("serialize");
        let obj = json.as_object().expect("object");
        assert_eq!(
            obj.get("kind").and_then(Value::as_str),
            Some("stopReason"),
            "kind must be camelCase stopReason for bridge compatibility"
        );
        assert_eq!(obj.get("sessionId").and_then(Value::as_str), Some("serde-session"));
        assert_eq!(
            obj.get("stopReason").and_then(Value::as_str),
            Some("refusal"),
            "StopReason::Refusal must serialize as snake_case 'refusal'"
        );
    }

    // ── session-update passthrough ───────────────────────────────────────────

    #[tokio::test]
    async fn non_transcript_updates_forward_unchanged_without_turn_parts() {
        let updates = [
            serde_json::json!({"sessionUpdate":"plan_update", "plan":{"type":"items", "planId":"p", "entries":[]}}),
            serde_json::json!({"sessionUpdate":"plan_removed", "planId":"p"}),
            serde_json::json!({"sessionUpdate":"notice", "severity":"info", "title":"Notice"}),
            serde_json::json!({"sessionUpdate":"compaction_update", "compactionId":"c", "status":"completed"}),
            serde_json::json!({"sessionUpdate":"compaction_summary_chunk", "compactionId":"c", "content":{"type":"text", "text":"summary"}}),
            serde_json::json!({"sessionUpdate":"current_mode_update", "currentModeId":"mode"}),
        ];
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        let session = SessionId::from("s");
        let plugin = super::super::plugins::for_agent("custom-agent");
        let mut capture_state = PluginCaptureState::for_plugin(plugin);
        let mut buffer = AssistantTurnBuffer::default();
        for (index, update) in updates.into_iter().enumerate() {
            let params = serde_json::json!({"sessionId":"s", "update":update});
            let typed: SessionNotification = serde_json::from_value(params.clone()).expect("typed update");
            assert!(match (index, &typed.update) {
                (0, SessionUpdate::PlanUpdate(_))
                | (1, SessionUpdate::PlanRemoved(_))
                | (2, SessionUpdate::Notice(_))
                | (3, SessionUpdate::CompactionUpdate(_))
                | (4, SessionUpdate::CompactionSummaryChunk(_))
                | (5, SessionUpdate::CurrentModeUpdate(_)) => true,
                _ => false,
            });
            assert!(!super::is_transcript_dispatch(&typed.update));
            let dispatch =
                Dispatch::Notification(UntypedMessage::new("session/update", params.clone()).expect("notification"));
            let outcome = send_message_to_user(
                AgentMessageForwardContext {
                    stream_tx: &stream_tx,
                    discuss_session_id: DiscussSessionId::new(),
                    session: &session,
                    agent_plugin: plugin,
                    capture_state: &mut capture_state,
                    usage: None,
                },
                SessionMessage::Message(dispatch),
                &mut buffer,
                true,
            )
            .await
            .expect("forwarded");
            assert!(matches!(outcome, AgentMessageForwardOutcome::Continue { .. }));
            match stream_rx.recv().await.expect("stream event") {
                DiscussStreamEvent::AgentMessage {
                    payload:
                        AcpAgentMessagePayload::Dispatch {
                            dispatch:
                                AcpDispatchPayload::Notification {
                                    method,
                                    params: forwarded,
                                },
                            ..
                        },
                    ..
                } => {
                    assert_eq!(method, "session/update");
                    assert_eq!(forwarded, params);
                }
                other => panic!("unexpected event: {other:?}"),
            }
            assert!(buffer.flush("end_turn".to_string()).parts.is_empty());
            assert!(buffer.take_metadata_updates().is_empty());
        }
    }

    #[test]
    fn resource_message_chunks_preserve_serialized_content_blocks() {
        for block in [
            serde_json::json!({"type":"resource", "resource":{"uri":"file:///a", "text":"contents"}}),
            serde_json::json!({"type":"resource_link", "uri":"file:///b", "name":"file", "mimeType":"text/plain"}),
        ] {
            let content: ContentBlock = serde_json::from_value(block).expect("content block");
            let expected = serde_json::to_value(&content).expect("serialize content");
            for kind in ["agent_message_chunk", "agent_thought_chunk"] {
                let params = serde_json::json!({"sessionId":"s", "update":{"sessionUpdate":kind, "content":expected}});
                let notification: SessionNotification = serde_json::from_value(params.clone()).expect("typed update");
                assert!(super::is_transcript_dispatch(&notification.update));
                let mut buffer = AssistantTurnBuffer::default();
                super::append_dispatch_delta(&mut buffer, &notification.update, &params);
                assert_eq!(
                    buffer.flush("end_turn".to_string()).parts,
                    vec![serde_json::json!({"kind":"content", "block":expected})]
                );
            }
        }
    }

    #[test]
    fn metadata_projection_preserves_persistence_shape() {
        let mut buffer = AssistantTurnBuffer::default();
        for params in [
            serde_json::json!({"sessionId":"s", "update": {
                "sessionUpdate":"available_commands_update", "availableCommands":[
                    {"name":"plan", "description":"Plan", "input":{"hint":"Goal"}}
                ]
            }}),
            serde_json::json!({"sessionId":"s", "update": {
                "sessionUpdate":"session_info_update", "title":"Title", "_meta":{"key":"value"}
            }}),
            serde_json::json!({"sessionId":"s", "update": {
                "sessionUpdate":"config_option_update", "configOptions":[{
                    "id":"model", "name":"Model", "type":"select", "currentValue":"a",
                    "options":[{"value":"a", "name":"A"}]
                }]
            }}),
        ] {
            let notification = serde_json::from_value(params).expect("typed notification");
            super::append_metadata_update(&mut buffer, &notification);
        }
        let updates = buffer.take_metadata_updates();
        assert_eq!(
            updates[0],
            serde_json::json!({"sessionId":"s", "availableCommands":[
                {"name":"plan", "description":"Plan", "inputHint":"Goal"}
            ]})
        );
        assert_eq!(
            updates[1],
            serde_json::json!({
                "sessionId":"s", "title":"Title", "updatedAt":null, "_meta":{"key":"value"}
            })
        );
        assert_eq!(updates[2]["selected"], serde_json::json!({"model":"a"}));
        assert_eq!(updates[2]["options"][0]["currentValue"], "a");
    }

    #[test]
    fn dispatch_payload_forwards_available_commands_update() {
        let params = serde_json::json!({
            "sessionId": "acp-sess-1",
            "update": {
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "create_plan", "description": "Create a plan", "input": {"hint": "Describe the plan"}},
                    {"name": "research", "description": "Research codebase"}
                ]
            }
        });
        let notification = UntypedMessage::new("session/update", params.clone()).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification {
                method,
                params: forwarded,
            } => {
                assert_eq!(method, "session/update");
                assert_eq!(forwarded, params);
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_forwards_session_info_update() {
        let params = serde_json::json!({
            "sessionId": "acp-sess-2",
            "update": {
                "sessionUpdate": "session_info_update",
                "title": "My Chat",
                "updatedAt": "2026-07-08T00:00:00Z",
                "_meta": {"custom": "value"}
            }
        });
        let notification = UntypedMessage::new("session/update", params.clone()).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification {
                method,
                params: forwarded,
            } => {
                assert_eq!(method, "session/update");
                assert_eq!(forwarded, params);
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_forwards_config_option_update() {
        let params = serde_json::json!({
            "sessionId": "acp-sess-3",
            "update": {
                "sessionUpdate": "config_option_update",
                "configOptions": [
                    {
                        "id": "model",
                        "name": "Model",
                        "type": "select",
                        "currentValue": "provider-a",
                        "options": [{"value": "provider-a"}, {"value": "provider-b"}]
                    }
                ]
            }
        });
        let notification = UntypedMessage::new("session/update", params.clone()).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification {
                method,
                params: forwarded,
            } => {
                assert_eq!(method, "session/update");
                assert_eq!(forwarded, params);
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_forwards_non_metadata_update_unchanged() {
        let params = serde_json::json!({
            "sessionId": "acp-sess-4",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hello"}
            }
        });
        let notification = UntypedMessage::new("session/update", params.clone()).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification {
                method,
                params: forwarded,
            } => {
                assert_eq!(method, "session/update");
                assert_eq!(forwarded, params, "non-metadata updates must be forwarded as raw JSON");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    // ── AssistantTurnBuffer ──────────────────────────────────────────────────

    #[test]
    fn buffer_accumulates_and_flushes() {
        use super::AssistantTurnBuffer;

        let mut buffer = AssistantTurnBuffer::default();
        buffer.append_text("hello ");
        buffer.append_text("world");
        buffer.append_thought("reasoning");
        let payload = buffer.flush("end_turn".to_string());
        assert_eq!(payload.aggregated_text, "hello world");
        assert_eq!(payload.aggregated_thought, "reasoning");
        assert_eq!(payload.stop_reason, "end_turn");

        let payload2 = buffer.flush("max_tokens".to_string());
        assert_eq!(payload2.aggregated_text, "", "buffer must be empty after flush");
        assert_eq!(
            payload2.aggregated_thought, "",
            "thought buffer must be empty after flush"
        );
        assert_eq!(payload2.stop_reason, "max_tokens");
    }

    #[test]
    fn buffer_empty_flush_produces_empty_text() {
        use super::AssistantTurnBuffer;

        let mut buffer = AssistantTurnBuffer::default();
        let payload = buffer.flush("refusal".to_string());
        assert_eq!(payload.aggregated_text, "");
        assert_eq!(payload.stop_reason, "refusal");
    }

    #[test]
    fn buffer_preserves_tool_calls_between_ordered_text_parts() {
        let mut buffer = AssistantTurnBuffer::default();
        buffer.append_text("before");
        buffer.append_tool_call(serde_json::json!({"toolCallId": "tool-1", "title": "Read"}), false);
        buffer.append_thought("thinking");
        buffer.append_text("after");

        let payload = buffer.flush("end_turn".to_string());
        let kinds = payload
            .parts
            .iter()
            .map(|part| part.get("kind").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![Some("text"), Some("tool_call"), Some("thought"), Some("text")]
        );
        assert_eq!(payload.parts[1]["update"]["toolCallId"], "tool-1");
    }

    // ── send_message_to_user with stream sender ──────────────────────────────

    /// `send_message_to_user` on `StopReason` must flush and return
    /// `TurnBoundary` and forward the payload via `DiscussStreamEvent::AgentMessage`.
    #[tokio::test]
    async fn send_message_to_user_stop_reason_returns_turn_boundary() {
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        let discuss_id = DiscussSessionId::new();
        let session = SessionId::from("stop-session");
        let mut buffer = AssistantTurnBuffer::default();
        buffer.append_text("assistant reply");
        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                discuss_session_id: discuss_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
                usage: None,
            },
            SessionMessage::PromptResponse(Ok(PromptResponse::new(StopReason::EndTurn))),
            &mut buffer,
            false,
        )
        .await
        .expect("forwarder must succeed");

        match outcome {
            AgentMessageForwardOutcome::TurnBoundary {
                flush,
                utility_payload: _,
            } => {
                assert_eq!(flush.aggregated_text, "assistant reply");
                assert_eq!(flush.stop_reason, "end_turn");
            }
            other => panic!("expected TurnBoundary, got {other:?}"),
        }

        // The stream event must carry the discuss_session_id and the payload.
        let event = stream_rx.recv().await.expect("stream event forwarded");
        match event {
            DiscussStreamEvent::AgentMessage {
                discuss_session_id,
                acp_session_id,
                payload,
                ..
            } => {
                assert_eq!(discuss_session_id, discuss_id);
                assert_eq!(acp_session_id, session);
                match payload {
                    AcpAgentMessagePayload::StopReason { stop_reason, .. } => {
                        assert_eq!(stop_reason, StopReason::EndTurn);
                    }
                    other => panic!("expected StopReason payload, got {other:?}"),
                }
            }
            other => panic!("expected AgentMessage event, got {other:?}"),
        }

        // Buffer was cleared by the flush.
        let empty = buffer.flush("max_tokens".to_string());
        assert_eq!(empty.aggregated_text, "", "buffer must be empty after StopReason flush");
    }

    /// A terminal ACP update is authoritative even when the turn produced no
    /// assistant text. Persistence decides separately whether an assistant row
    /// is useful; it must not suppress the boundary event.
    #[tokio::test]
    async fn send_message_to_user_empty_stop_reason_forwards_turn_boundary() {
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        let discuss_id = DiscussSessionId::new();
        let session = SessionId::from("empty-stop-session");
        let mut buffer = AssistantTurnBuffer::default();
        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                discuss_session_id: discuss_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
                usage: None,
            },
            SessionMessage::PromptResponse(Ok(PromptResponse::new(StopReason::EndTurn))),
            &mut buffer,
            false,
        )
        .await
        .expect("empty terminal update must be forwarded");

        let AgentMessageForwardOutcome::TurnBoundary { flush, .. } = outcome else {
            panic!("expected empty terminal update to close the turn");
        };
        assert!(flush.aggregated_text.is_empty());
        assert!(flush.parts.is_empty());

        let DiscussStreamEvent::AgentMessage { payload, .. } = stream_rx.recv().await.expect("boundary event") else {
            panic!("expected an AgentMessage boundary event");
        };
        assert!(matches!(payload, AcpAgentMessagePayload::StopReason { .. }));
    }

    /// Provider history replay during `session/load` does not forward or flush
    /// transcript events.
    #[tokio::test]
    async fn replayed_stop_reason_is_not_forwarded_or_flushed() {
        let (stream_tx, mut stream_rx) = mpsc::channel(1);
        let discuss_id = DiscussSessionId::new();
        let session = SessionId::from("replayed-session");
        let mut buffer = AssistantTurnBuffer::default();
        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));

        let replayed_dispatch = UntypedMessage::new(
            "session/update",
            serde_json::json!({
                "sessionId": "replayed-session",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "replayed assistant text"}
                }
            }),
        )
        .expect("valid replayed dispatch");
        let dispatch_outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                discuss_session_id: discuss_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
                usage: None,
            },
            SessionMessage::Message(Dispatch::Notification(replayed_dispatch)),
            &mut buffer,
            true,
        )
        .await
        .expect("replayed dispatch is ignored");
        assert!(matches!(dispatch_outcome, AgentMessageForwardOutcome::Continue { .. }));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                discuss_session_id: discuss_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
                usage: None,
            },
            SessionMessage::PromptResponse(Ok(PromptResponse::new(StopReason::EndTurn))),
            &mut buffer,
            true,
        )
        .await
        .expect("replayed stop reason is ignored");

        assert!(matches!(outcome, AgentMessageForwardOutcome::Continue { .. }));
        assert!(
            stream_rx.try_recv().is_err(),
            "replayed stop reason must not reach the stream"
        );
        assert_eq!(
            buffer.flush("end_turn".to_string()).aggregated_text,
            "",
            "replay suppression must not produce a persistence flush"
        );
    }

    /// Non-StopReason dispatch still returns `Continue` and does not flush.
    #[tokio::test]
    async fn send_message_to_user_dispatch_returns_continue() {
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        let discuss_id = DiscussSessionId::new();
        let session = SessionId::from("dispatch-session");

        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 50})).expect("valid untyped message");
        let dispatch = Dispatch::Notification(notification);

        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));
        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                discuss_session_id: discuss_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
                usage: None,
            },
            SessionMessage::Message(dispatch),
            &mut AssistantTurnBuffer::default(),
            false,
        )
        .await
        .expect("forwarder must succeed");

        assert!(matches!(outcome, AgentMessageForwardOutcome::Continue { .. }));

        let event = stream_rx.recv().await.expect("stream event forwarded");
        match event {
            DiscussStreamEvent::AgentMessage { discuss_session_id, .. } => {
                assert_eq!(discuss_session_id, discuss_id);
            }
            other => panic!("expected AgentMessage event, got {other:?}"),
        }
    }
}

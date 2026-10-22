//! Put all the stuff to talk the agents here

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use agent_client_protocol::{
    Dispatch, Error as AcpCrateError, Responder, SessionMessage,
    schema::v1::{
        CreateElicitationRequest, PermissionOptionId, RequestPermissionRequest, RequestPermissionResponse, SessionId,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::elicitation::classify_elicitation_schema;
use super::plugins::{DiscussAgentPlugin, PluginCaptureState};
use super::types::{
    AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, DiscussElicitationAnswer, DiscussError,
    DiscussPermissionOption, DiscussStreamEvent, DiscussUtilityPayload, DiscussUtilityUpdate, ElicitationRequestId,
    PendingElicitation, PermissionRequestId, SluiceSessionId,
};

/// Handles an incoming permission request from the agent.
///
/// Creates the worker-response oneshot, generates an opaque
/// `PermissionRequestId`, and returns both so the actor can register the
/// pending entry in its registry **before** publishing
/// `DiscussStreamEvent::PermissionRequested`. This ordering prevents an
/// early-answer race where an answer arrives before the pending entry exists.
///
/// The actor is responsible for:
/// 1. Inserting the `PendingPermission` into its registry keyed by
///    `(sluice_session_id, permission_request_id)`.
/// 2. Publishing `DiscussStreamEvent::PermissionRequested` on the stream.
/// 3. If the stream send fails, removing the just-inserted pending entry and
///    resolving the provider responder as cancelled.
///
/// Returns the `PermissionRequestId`, the offered option ids (for later
/// validation), and the oneshot receiver that the actor resolves when the
/// user answers through `AnswerPermission`.
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
    use agent_client_protocol::schema::v1::{PermissionOptionId, RequestPermissionOutcome, SelectedPermissionOutcome};

    let response = match option {
        DiscussPermissionOption::Select { option_id } => RequestPermissionResponse::new(
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(PermissionOptionId::new(option_id))),
        ),
        DiscussPermissionOption::Cancel => RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled),
    };
    responder.respond(response)?;
    Ok(())
}

// ── Session-update DTOs ────────────────────────────────────────────────────
//
// Typed reshaped projections of the three metadata-only `session/update`
// discriminators (`available_commands_update`, `session_info_update`,
// `config_option_update`). They are declared `pub(crate)` so the actor and
// handle layers can read the typed shape when they wire metadata persistence,
// but they are not part of the public API surface. They live next to the
// message payload per the subtask contract — no new module.

/// One provider-exposed available slash command.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AvailableCommandDto {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_hint: Option<String>,
}

/// Typed reshape of an `available_commands_update` session update.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AvailableCommandsUpdate {
    pub session_id: String,
    pub available_commands: Vec<AvailableCommandDto>,
}

/// Typed reshape of a `session_info_update` session update.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionInfoUpdate {
    pub session_id: String,
    pub title: Option<String>,
    pub updated_at: Option<String>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Typed reshape of a `config_option_update` session update.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConfigOptionUpdate {
    pub session_id: String,
    pub options: Vec<serde_json::Value>,
    pub selected: std::collections::BTreeMap<String, String>,
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
    pub(super) parts: Vec<serde_json::Value>,
    pub(super) stop_reason: String,
}

/// Per-session assistant-turn accumulator split by ACP semantic part type.
#[derive(Debug, Default)]
pub(super) struct AssistantTurnBuffer {
    accumulated_text: String,
    accumulated_thought: String,
    parts: Vec<serde_json::Value>,
    metadata_updates: Vec<serde_json::Value>,
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

    pub(super) fn append_tool_call(&mut self, update: serde_json::Value, is_update: bool) {
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
                part.get("kind").and_then(serde_json::Value::as_str) == Some("tool_call")
                    && part.get("isUpdate").and_then(serde_json::Value::as_bool) != Some(true)
            })
            .count() as u64
    }

    pub(super) fn flush(&mut self, stop_reason: String) -> FlushPayload {
        let aggregated_text = std::mem::take(&mut self.accumulated_text);
        let aggregated_thought = std::mem::take(&mut self.accumulated_thought);
        let parts = std::mem::take(&mut self.parts);
        FlushPayload {
            aggregated_text,
            aggregated_thought,
            parts,
            stop_reason,
        }
    }

    pub(super) fn push_metadata_update(&mut self, update: serde_json::Value) {
        self.metadata_updates.push(update);
    }

    pub(super) fn take_metadata_updates(&mut self) -> Vec<serde_json::Value> {
        std::mem::take(&mut self.metadata_updates)
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
        flush: FlushPayload,
        utility_payload: Option<DiscussUtilityPayload>,
    },
}

pub(super) struct AgentMessageForwardContext<'a> {
    pub(super) stream_tx: &'a mpsc::Sender<DiscussStreamEvent>,
    pub(super) sluice_session_id: SluiceSessionId,
    pub(super) session: &'a SessionId,
    pub(super) agent_plugin: &'a dyn DiscussAgentPlugin,
    pub(super) capture_state: &'a mut PluginCaptureState,
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
        sluice_session_id,
        session,
        agent_plugin,
        capture_state,
    } = context;
    let stop_reason = match &message {
        SessionMessage::StopReason(reason) => Some(*reason),
        _ => None,
    };

    let payload = acp_message_payload(session, message)?;

    if suppress_transcript && is_transcript_event(&payload) {
        return Ok(AgentMessageForwardOutcome::Continue { utility_payload: None });
    }

    let utility_payload = if let AcpAgentMessagePayload::Dispatch { dispatch, .. } = &payload {
        append_dispatch_delta(buffer, dispatch);
        append_metadata_update(buffer, dispatch);
        capture_utility_payload(agent_plugin, dispatch, capture_state)
    } else {
        None
    };

    // Forward the typed payload to Zeus via the bounded stream.
    stream_tx
        .send(DiscussStreamEvent::AgentMessage {
            sluice_session_id,
            acp_session_id: session.clone(),
            payload,
        })
        .await
        .map_err(|error| stream_send_error("send_message_to_user.stream", error))?;

    let utility_payload_for_persistence = utility_payload.clone();
    if let Some(payload) = utility_payload {
        stream_tx
            .send(DiscussStreamEvent::UtilityUpdate {
                sluice_session_id,
                acp_session_id: session.clone(),
                utility: DiscussUtilityUpdate {
                    sluice_session_id,
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
                flush: flush_payload,
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
    capture_state: &mut PluginCaptureState,
) -> Option<DiscussUtilityPayload> {
    let AcpDispatchPayload::Notification { method, params } = dispatch else {
        return None;
    };
    if method != "session/update" {
        return None;
    }
    let update = params.get("update")?;
    let session_update = update.get("sessionUpdate")?.as_str()?;
    let agent_scope_key = agent_plugin.agent_scope_key();
    match session_update {
        "tool_call" => agent_plugin.capture_tool_call_with_state(update, agent_scope_key, capture_state),
        "tool_call_update" => agent_plugin.capture_tool_call_update_with_state(update, agent_scope_key, capture_state),
        _ => None,
    }
}

fn is_transcript_event(payload: &AcpAgentMessagePayload) -> bool {
    match payload {
        AcpAgentMessagePayload::StopReason { .. } => true,
        AcpAgentMessagePayload::Dispatch { dispatch, .. } => is_transcript_dispatch(dispatch),
    }
}

fn is_transcript_dispatch(dispatch: &AcpDispatchPayload) -> bool {
    let AcpDispatchPayload::Notification { method, params } = dispatch else {
        return false;
    };

    if method != "session/update" {
        return false;
    }

    let session_update = params
        .get("update")
        .and_then(|update| update.get("sessionUpdate"))
        .and_then(|value| value.as_str());

    matches!(
        session_update,
        Some("user_message_chunk" | "agent_thought_chunk" | "agent_message_chunk" | "tool_call" | "tool_call_update")
    )
}

/// Serialize a `StopReason` to the snake_case string the provider emitted.
fn stop_reason_to_string(reason: agent_client_protocol::schema::v1::StopReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{reason:?}").to_lowercase())
}

fn append_dispatch_delta(buffer: &mut AssistantTurnBuffer, dispatch: &AcpDispatchPayload) {
    if let AcpDispatchPayload::Notification { method, params } = dispatch {
        if method != "session/update" {
            return;
        }
        if let Some(delta) = extract_assistant_delta(params) {
            match delta {
                AssistantDelta::Text(text) => buffer.append_text(&text),
                AssistantDelta::Thought(thought) => buffer.append_thought(&thought),
            }
            return;
        }
        let Some(update) = params.get("update") else {
            return;
        };
        let Some(session_update) = update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)
        else {
            return;
        };
        if matches!(session_update, "tool_call" | "tool_call_update") {
            buffer.append_tool_call(update.clone(), session_update == "tool_call_update");
        }
    }
}

fn append_metadata_update(buffer: &mut AssistantTurnBuffer, dispatch: &AcpDispatchPayload) {
    if let AcpDispatchPayload::Notification { method, params } = dispatch
        && method == "session/update"
        && (params.get("availableCommands").is_some()
            || params.get("options").is_some()
            || params.get("title").is_some()
            || params.get("updatedAt").is_some())
    {
        buffer.push_metadata_update(params.clone());
    }
}

enum AssistantDelta {
    Text(String),
    Thought(String),
}

fn extract_assistant_delta(params: &serde_json::Value) -> Option<AssistantDelta> {
    let update = params.get("update")?;
    let session_update = update.get("sessionUpdate")?.as_str()?;
    match session_update {
        "agent_message_chunk" => extract_text_from_content_block(update.get("content")?).map(AssistantDelta::Text),
        "agent_thought_chunk" => extract_text_from_content_block(update.get("content")?).map(AssistantDelta::Thought),
        _ => None,
    }
}

fn extract_text_from_content_block(content: &serde_json::Value) -> Option<String> {
    let content_type = content.get("type")?.as_str()?;
    if content_type == "text" {
        content.get("text")?.as_str().map(str::to_string)
    } else {
        None
    }
}

fn acp_message_payload(session: &SessionId, message: SessionMessage) -> Result<AcpAgentMessagePayload, AcpCrateError> {
    match message {
        SessionMessage::SessionMessage(dispatch) => Ok(AcpAgentMessagePayload::Dispatch {
            session_id: session.to_string(),
            dispatch: dispatch_payload(dispatch),
        }),
        SessionMessage::StopReason(stop_reason) => Ok(AcpAgentMessagePayload::StopReason {
            session_id: session.to_string(),
            stop_reason,
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
            if method == "session/update"
                && let Some(reshaped) = reshape_session_update(&params)
            {
                return AcpDispatchPayload::Notification {
                    method,
                    params: reshaped,
                };
            }
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

/// Reshape a serialized `SessionNotification` params blob into a typed
/// DTO when the `update.sessionUpdate` discriminator is one of the three
/// metadata-only session-update kinds.
fn reshape_session_update(params: &serde_json::Value) -> Option<serde_json::Value> {
    let update = params.get("update")?;
    let session_update = update.get("sessionUpdate")?.as_str()?;
    let session_id = params
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    match session_update {
        "available_commands_update" => {
            let dto = parse_available_commands_update(session_id, update)?;
            Some(serde_json::to_value(dto).ok()?)
        }
        "session_info_update" => {
            let dto = parse_session_info_update(session_id, update)?;
            Some(serde_json::to_value(dto).ok()?)
        }
        "config_option_update" => {
            let dto = parse_config_option_update(session_id, update)?;
            Some(serde_json::to_value(dto).ok()?)
        }
        _ => None,
    }
}

fn parse_available_commands_update(
    session_id: Option<String>,
    update: &serde_json::Value,
) -> Option<AvailableCommandsUpdate> {
    let commands = update.get("availableCommands")?;
    let available_commands = commands
        .as_array()?
        .iter()
        .filter_map(parse_available_command)
        .collect::<Vec<_>>();
    Some(AvailableCommandsUpdate {
        session_id: session_id.unwrap_or_default(),
        available_commands,
    })
}

fn parse_available_command(value: &serde_json::Value) -> Option<AvailableCommandDto> {
    let name = value.get("name")?.as_str()?.to_string();
    let description = value.get("description")?.as_str()?.to_string();
    let input_hint = value
        .get("input")
        .and_then(|input| input.get("hint"))
        .and_then(|hint| hint.as_str())
        .map(str::to_string);
    Some(AvailableCommandDto {
        name,
        description,
        input_hint,
    })
}

fn parse_session_info_update(session_id: Option<String>, update: &serde_json::Value) -> Option<SessionInfoUpdate> {
    let title = update
        .get("title")
        .and_then(|v| if v.is_null() { None } else { v.as_str() })
        .map(str::to_string);
    let updated_at = update
        .get("updatedAt")
        .and_then(|v| if v.is_null() { None } else { v.as_str() })
        .map(str::to_string);
    let meta = update
        .get("_meta")
        .and_then(|v| if v.is_null() { None } else { v.as_object() })
        .cloned();
    Some(SessionInfoUpdate {
        session_id: session_id.unwrap_or_default(),
        title,
        updated_at,
        meta,
    })
}

fn parse_config_option_update(session_id: Option<String>, update: &serde_json::Value) -> Option<ConfigOptionUpdate> {
    // Keep the provider's flattened wire shape intact. The ACP SDK's typed
    // `SessionConfigOption` decoder expects its internal representation, while
    // session/update uses the protocol's flattened `type`/`currentValue` form.
    let options = update.get("configOptions")?.as_array()?.to_vec();
    let mut selected = std::collections::BTreeMap::new();
    for option in &options {
        let Some(id) = option.get("id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if let Some(value) = option
            .get("currentValue")
            .and_then(serde_json::Value::as_str)
        {
            selected.insert(id.to_string(), value.to_string());
        }
    }
    Some(ConfigOptionUpdate {
        session_id: session_id.unwrap_or_default(),
        options,
        selected,
    })
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::SessionMessage;
    use agent_client_protocol::schema::v1::{SessionId, StopReason};
    use tokio::sync::mpsc;

    use super::super::elicitation::{DiscussRestrictedFormFieldType, sdk_to_restricted_schema};
    use super::super::types::{
        AcpAgentMessagePayload, AcpDispatchPayload, AcpDispatchResponsePayload, DiscussStreamEvent, SluiceSessionId,
    };
    use super::{
        AgentMessageForwardContext, AgentMessageForwardOutcome, AssistantTurnBuffer, PluginCaptureState,
        acp_message_payload, dispatch_payload, send_message_to_user,
    };

    #[test]
    fn restricted_schema_projects_titled_multi_select_fields() {
        let mode =
            serde_json::from_value::<agent_client_protocol::schema::v1::ElicitationFormMode>(serde_json::json!({
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
        use agent_client_protocol::UntypedMessage;

        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 75})).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
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
        use agent_client_protocol::UntypedMessage;

        let notification =
            UntypedMessage::new("session/update", serde_json::json!({"data": "x"})).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
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
        use agent_client_protocol::UntypedMessage;

        let session = SessionId::from("dispatch-session");
        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 90})).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
        let message = SessionMessage::SessionMessage(dispatch);

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
        use agent_client_protocol::UntypedMessage;

        let session = SessionId::from("bridge-shape-session");
        let notification =
            UntypedMessage::new("session/update", serde_json::json!({"data": "x"})).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
        let message = SessionMessage::SessionMessage(dispatch);

        let payload = acp_message_payload(&session, message).expect("payload ok");
        let json = serde_json::to_value(&payload).expect("serialize");

        assert_eq!(
            json.get("kind").and_then(|v| v.as_str()),
            Some("dispatch"),
            "kind must be 'dispatch' for SessionMessage::SessionMessage"
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
        let payload =
            super::acp_message_payload(&session, SessionMessage::StopReason(StopReason::Refusal)).expect("payload ok");

        let json = serde_json::to_value(&payload).expect("serialize");
        let obj = json.as_object().expect("object");
        assert_eq!(
            obj.get("kind").and_then(serde_json::Value::as_str),
            Some("stopReason"),
            "kind must be camelCase stopReason for bridge compatibility"
        );
        assert_eq!(
            obj.get("sessionId").and_then(serde_json::Value::as_str),
            Some("serde-session")
        );
        assert_eq!(
            obj.get("stopReason").and_then(serde_json::Value::as_str),
            Some("refusal"),
            "StopReason::Refusal must serialize as snake_case 'refusal'"
        );
    }

    // ── session-update reshape ───────────────────────────────────────────────

    #[test]
    fn dispatch_payload_reshapes_available_commands_update() {
        use agent_client_protocol::UntypedMessage;

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
        let notification = UntypedMessage::new("session/update", params).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification { method, params } => {
                assert_eq!(method, "session/update");
                assert_eq!(
                    params.get("sessionId").and_then(|v| v.as_str()),
                    Some("acp-sess-1"),
                    "session_id must be the ACP protocol session id"
                );
                let commands = params
                    .get("availableCommands")
                    .and_then(|v| v.as_array())
                    .expect("availableCommands array");
                assert_eq!(commands.len(), 2);
                assert_eq!(commands[0].get("name").and_then(|v| v.as_str()), Some("create_plan"));
                assert_eq!(
                    commands[0].get("inputHint").and_then(|v| v.as_str()),
                    Some("Describe the plan"),
                    "inputHint must be camelCase"
                );
                assert_eq!(
                    commands[1].get("inputHint"),
                    None,
                    "missing input must produce absent inputHint, not null"
                );
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_reshapes_session_info_update() {
        use agent_client_protocol::UntypedMessage;

        let params = serde_json::json!({
            "sessionId": "acp-sess-2",
            "update": {
                "sessionUpdate": "session_info_update",
                "title": "My Chat",
                "updatedAt": "2026-07-08T00:00:00Z",
                "_meta": {"custom": "value"}
            }
        });
        let notification = UntypedMessage::new("session/update", params).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification { method, params } => {
                assert_eq!(method, "session/update");
                assert_eq!(params.get("sessionId").and_then(|v| v.as_str()), Some("acp-sess-2"));
                assert_eq!(params.get("title").and_then(|v| v.as_str()), Some("My Chat"));
                assert_eq!(
                    params.get("updatedAt").and_then(|v| v.as_str()),
                    Some("2026-07-08T00:00:00Z")
                );
                assert_eq!(
                    params
                        .get("_meta")
                        .and_then(|v| v.get("custom"))
                        .and_then(|v| v.as_str()),
                    Some("value"),
                    "_meta must be propagated verbatim"
                );
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_reshapes_config_option_update() {
        use agent_client_protocol::UntypedMessage;

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
        let notification = UntypedMessage::new("session/update", params).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
        let payload = dispatch_payload(dispatch);

        match payload {
            AcpDispatchPayload::Notification { method, params } => {
                assert_eq!(method, "session/update");
                assert_eq!(params.get("sessionId").and_then(|v| v.as_str()), Some("acp-sess-3"));
                let options = params
                    .get("options")
                    .and_then(|v| v.as_array())
                    .expect("options array");
                assert_eq!(options.len(), 1);
                assert_eq!(options[0].get("id").and_then(|v| v.as_str()), Some("model"));
                let selected = params.get("selected").expect("selected map");
                assert_eq!(
                    selected.get("model").and_then(|v| v.as_str()),
                    Some("provider-a"),
                    "selected must map option id to current value"
                );
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_payload_forwards_non_metadata_update_unchanged() {
        use agent_client_protocol::UntypedMessage;

        let params = serde_json::json!({
            "sessionId": "acp-sess-4",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hello"}
            }
        });
        let notification = UntypedMessage::new("session/update", params.clone()).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);
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
            .map(|part| part.get("kind").and_then(serde_json::Value::as_str))
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
        let sluice_id = SluiceSessionId::new();
        let session = SessionId::from("stop-session");
        let mut buffer = AssistantTurnBuffer::default();
        buffer.append_text("assistant reply");
        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                sluice_session_id: sluice_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
            },
            SessionMessage::StopReason(StopReason::EndTurn),
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

        // The stream event must carry the sluice_session_id and the payload.
        let event = stream_rx.recv().await.expect("stream event forwarded");
        match event {
            DiscussStreamEvent::AgentMessage {
                sluice_session_id,
                acp_session_id,
                payload,
            } => {
                assert_eq!(sluice_session_id, sluice_id);
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
        let sluice_id = SluiceSessionId::new();
        let session = SessionId::from("empty-stop-session");
        let mut buffer = AssistantTurnBuffer::default();
        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                sluice_session_id: sluice_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
            },
            SessionMessage::StopReason(StopReason::EndTurn),
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
        use agent_client_protocol::UntypedMessage;

        let (stream_tx, mut stream_rx) = mpsc::channel(1);
        let sluice_id = SluiceSessionId::new();
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
                sluice_session_id: sluice_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
            },
            SessionMessage::SessionMessage(agent_client_protocol::Dispatch::Notification(replayed_dispatch)),
            &mut buffer,
            true,
        )
        .await
        .expect("replayed dispatch is ignored");
        assert!(matches!(dispatch_outcome, AgentMessageForwardOutcome::Continue { .. }));

        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                sluice_session_id: sluice_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
            },
            SessionMessage::StopReason(StopReason::EndTurn),
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
        use agent_client_protocol::UntypedMessage;

        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        let sluice_id = SluiceSessionId::new();
        let session = SessionId::from("dispatch-session");

        let notification =
            UntypedMessage::new("tools/progress", serde_json::json!({"pct": 50})).expect("valid untyped message");
        let dispatch = agent_client_protocol::Dispatch::Notification(notification);

        let mut capture_state = PluginCaptureState::for_plugin(super::super::plugins::for_agent("custom-agent"));
        let outcome = send_message_to_user(
            AgentMessageForwardContext {
                stream_tx: &stream_tx,
                sluice_session_id: sluice_id,
                session: &session,
                agent_plugin: super::super::plugins::for_agent("custom-agent"),
                capture_state: &mut capture_state,
            },
            SessionMessage::SessionMessage(dispatch),
            &mut AssistantTurnBuffer::default(),
            false,
        )
        .await
        .expect("forwarder must succeed");

        assert!(matches!(outcome, AgentMessageForwardOutcome::Continue { .. }));

        let event = stream_rx.recv().await.expect("stream event forwarded");
        match event {
            DiscussStreamEvent::AgentMessage { sluice_session_id, .. } => {
                assert_eq!(sluice_session_id, sluice_id);
            }
            other => panic!("expected AgentMessage event, got {other:?}"),
        }
    }
}

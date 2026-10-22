use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use tokio::sync::{mpsc, oneshot};
use turso::Connection;

use aves::bus::BusRpcContext;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::agents::FlushPayload;
use super::boot::load_discuss_settings;
use super::config::{config_options_from_json, config_settings_from_json};
use super::library::{AgentId, AssetLibrary, SkillId, TeamDefinition};
use super::plugins;
use super::session::{SessionAnchorClear, SessionAnchorMetadata};
use super::storage::{
    DiscussChatTurnInsert, DiscussPersistence, DiscussSessionUpsert, DiscussTeamInstallationRow,
    ElicitationPersistenceInput, insert_discuss_chat_turn, load_discuss_agent_config, load_discuss_session,
    prune_discuss_chat_turns_window, upsert_discuss_agent_config, upsert_discuss_session_with_task_tag,
};
use super::types::{
    CleanupWarning, ConfigSettings, DiscussConfigOption, DiscussElicitationAnswer, DiscussError,
    DiscussMemberOverrides, DiscussSubagentRecord, DiscussUtilityPayload, RetainedTeamAssetsByScope,
};

/// One ordered persistence unit for a session's serialized write queue.
///
/// Each variant owns its data so the job can be moved across the queue without
/// borrowing worker state; the drain task acquires a fresh connection per job
/// through the shared `DiscussPersistence` factory.
pub enum PersistenceJob {
    /// Anchor upsert for a live ACP session that has not yet received a user
    /// prompt. Writes `acp_session_id` so the parent row exists before any
    /// later config/turn writes, without inventing a user turn.
    SessionOpen {
        acp_session_id: String,
        completion_tx: Option<oneshot::Sender<Result<(), DiscussError>>>,
    },
    /// Anchor upsert + user turn. Position 0 follows the initial-turn path;
    /// restored sessions use their next loaded transcript position.
    InitialTurn {
        acp_session_id: String,
        position: i64,
        initial_prompt: String,
    },
    /// One or more metadata-only anchor updates, applied in order.
    MetadataUpdates { updates: Vec<serde_json::Value> },
    /// Canonical config options refreshed by a successful
    /// `session/set_config_option`, persisted to `latest_config_options_json`.
    ///
    /// Unlike [`PersistenceJob::MetadataUpdates`], the payload is already the
    /// canonical `Vec<DiscussConfigOption>` (normalized from the provider's
    /// post-response `config_options` through the provider config extractor),
    /// so the drain task serializes it verbatim and overwrites the persisted
    /// fallback. The response always carries the provider's full explicit option
    /// set, so an empty vec is persisted as an explicit `[]` (never a
    /// carry-forward), keeping the no-live-worker fallback in lock-step with the
    /// live worker cache after a set.
    ConfigOptions {
        options: Vec<DiscussConfigOption>,
        session_settings: Option<ConfigSettings>,
        completion_tx: Option<oneshot::Sender<Result<(), DiscussError>>>,
    },
    /// Assistant turn persisted exactly at its `StopReason` boundary.
    AssistantTurn {
        assistant_position: i64,
        flush: FlushPayload,
    },
    /// Latest parent-session todo snapshot, serialized as JSON.
    TodoSnapshot { snapshot_json: String },
    /// Typed parent-session subagent capture, upserted and retained atomically.
    Subagent { subagent: DiscussSubagentRecord },
}

// ── Serialized per-session persistence queue ──────────────────────────────
//
// All fire-and-forget persistence writes for a single session are funnelled
// through one ordered mpsc queue drained by a dedicated task. This makes the
// per-session write path single-writer and strictly ordered:
//   - the initial anchor+turn job is enqueued first, so the parent anchor
//     always exists before any child `acp_chat_turns` insert (the FK from
//     migration 0023 is never violated by a racing child insert);
//   - independent load-then-upsert metadata writes can no longer interleave
//     and overwrite newer metadata, because they run sequentially in enqueue
//     order rather than in independently-spawned tasks;
//   - each job still logs DB failures via `tracing` and continues, so a live
//     DB error never tears down the session loop.

/// Spawn the serialized per-session persistence drain task and return its
/// ordered sender. The task exits when every sender is dropped (the worker
/// drops its sender on loop exit).
pub(super) fn spawn_persistence_queue(
    persistence: DiscussPersistence,
    sluice_session_id: String,
    agent_name: String,
    session_name: String,
    root_context: BusRpcContext,
) -> (mpsc::Sender<PersistenceJob>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<PersistenceJob>(DEFAULT_BUFFER_SIZE);
    let drain = tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            match job {
                PersistenceJob::SessionOpen {
                    acp_session_id,
                    completion_tx,
                } => {
                    let result = persist_session_anchor_inner(
                        &persistence,
                        &sluice_session_id,
                        SessionAnchorMetadata {
                            acp_session_id: Some(&acp_session_id),
                            ..SessionAnchorMetadata::default()
                        },
                    )
                    .await;
                    if let Some(completion_tx) = completion_tx {
                        let _ = completion_tx.send(result);
                    } else if let Err(error) = result {
                        tracing::warn!(?error, "Discuss worker: failed to persist session open");
                    }
                }
                PersistenceJob::InitialTurn {
                    acp_session_id,
                    position,
                    initial_prompt,
                } => {
                    if position == 0 {
                        persist_initial_turn(&persistence, &sluice_session_id, &acp_session_id, &initial_prompt).await;
                    } else {
                        persist_subsequent_user_turn(&persistence, &sluice_session_id, position, &initial_prompt).await;
                    }
                }
                PersistenceJob::MetadataUpdates { updates } => {
                    for metadata in &updates {
                        persist_metadata_update(&persistence, &sluice_session_id, &agent_name, &session_name, metadata)
                            .await;
                    }
                }
                PersistenceJob::ConfigOptions {
                    options,
                    session_settings,
                    completion_tx,
                } => {
                    let result = persist_config_options(
                        &persistence,
                        &sluice_session_id,
                        &agent_name,
                        &options,
                        session_settings.as_ref(),
                    )
                    .await;
                    if let Some(completion_tx) = completion_tx {
                        let _ = completion_tx.send(result);
                    } else if let Err(error) = result {
                        tracing::warn!(?error, "Discuss worker: failed to persist config state");
                    }
                }
                PersistenceJob::AssistantTurn {
                    assistant_position,
                    flush,
                } => {
                    persist_flush_payload(&persistence, &sluice_session_id, assistant_position, &flush).await;
                }
                PersistenceJob::TodoSnapshot { snapshot_json } => {
                    if let Err(error) = persist_todo_snapshot(&persistence, &sluice_session_id, &snapshot_json).await {
                        tracing::warn!(?error, "Discuss worker: failed to persist todo snapshot");
                    }
                }
                PersistenceJob::Subagent { subagent } => {
                    let result = async {
                        let config = load_discuss_settings(&root_context).await?;
                        persist_subagent(
                            &persistence,
                            &sluice_session_id,
                            &subagent,
                            config.subagent_retention_limit,
                        )
                        .await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(?error, "Discuss worker: failed to persist subagent capture");
                    }
                }
            }
        }
    });
    (tx, drain)
}

/// Enqueue a persistence job on the ordered per-session queue (fire-and-forget).
///
/// A closed queue (drain task gone) or a full queue only logs; it never blocks
/// the live session loop or propagates an error into the forward path.
pub(super) async fn enqueue_persistence_job(queue: &mpsc::Sender<PersistenceJob>, job: PersistenceJob) {
    if queue.send(job).await.is_err() {
        tracing::warn!("ACP worker: persistence queue closed; dropping write");
    }
}

/// Dispatch typed utility payloads to their ordered parent-session persistence jobs.
pub(super) async fn enqueue_utility_persistence(
    queue: &mpsc::Sender<PersistenceJob>,
    utility_payload: Option<DiscussUtilityPayload>,
) {
    let Some(utility_payload) = utility_payload else {
        return;
    };
    match utility_payload {
        DiscussUtilityPayload::TodoSnapshot { snapshot } => match serde_json::to_string(&snapshot) {
            Ok(snapshot_json) => {
                enqueue_persistence_job(queue, PersistenceJob::TodoSnapshot { snapshot_json }).await;
            }
            Err(error) => {
                tracing::warn!(?error, "Discuss worker: failed to serialize todo snapshot");
            }
        },
        DiscussUtilityPayload::Subagent { subagent } => {
            enqueue_persistence_job(queue, PersistenceJob::Subagent { subagent }).await;
        }
    }
}

// ── Worker persistence helpers ─────────────────────────────────────────────

/// Persist a user prompt as a `discuss_chat_turns` row (fire-and-forget).
///
/// Best-effort: DB failures log via `tracing` and continue. The worker is the
/// single site that issues `discuss_chat_turns_*` writes.
pub async fn persist_user_turn(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    position: i64,
    text: &str,
) -> Result<(), DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let content = serde_json::json!({"role": "user", "text": text}).to_string();
    insert_discuss_chat_turn(
        &conn,
        DiscussChatTurnInsert {
            sluice_session_id,
            position,
            role: "user",
            acp_session_id: None,
            content_json: &content,
            created_at: now_ms,
        },
    )
    .await?;
    prune_discuss_chat_turns_window(&conn, sluice_session_id).await?;
    Ok(())
}

/// Persist a subsequent user turn (fire-and-forget).
///
/// Inserts the `role='user'` `discuss_chat_turns` row at `position`, prunes the
/// bounded window, then bumps `last_user_prompt` + `last_interaction_at` on the
/// `discuss_sessions` anchor. Best-effort: DB failures log via `tracing` and
/// continue.
pub async fn persist_subsequent_user_turn(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    position: i64,
    text: &str,
) {
    if let Err(error) = persist_user_turn(persistence, sluice_session_id, position, text).await {
        tracing::warn!(?error, "Discuss worker: failed to persist user turn");
    }
    // Bump last_user_prompt + last_interaction_at on the chat anchor. The
    // `persist_session_anchor` load-and-carry-forward path preserves the
    // existing acp_session_id / display_title / config / commands columns.
    persist_session_anchor(
        persistence,
        sluice_session_id,
        SessionAnchorMetadata {
            last_user_prompt: Some(text),
            ..SessionAnchorMetadata::default()
        },
    )
    .await;
}

/// Persist the session anchor row and the initial user prompt together
/// (fire-and-forget).
///
/// Runs the `discuss_sessions` upsert (with `last_user_prompt` to the initial prompt, and
/// `first_prompt_at` / `last_interaction_at` bumped to now) and the initial
/// `role='user'` `discuss_chat_turns` insert at position 0, then prunes the bounded
/// window so the initial prompt participates in the same bounded-window
/// invariant as every later turn. Best-effort: DB failures log via `tracing`
/// and continue, so the live stream is never blocked.
pub(super) async fn persist_initial_turn(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    acp_session_id: &str,
    initial_prompt: &str,
) {
    persist_session_anchor(
        persistence,
        sluice_session_id,
        SessionAnchorMetadata {
            acp_session_id: Some(acp_session_id),
            last_user_prompt: Some(initial_prompt),
            ..SessionAnchorMetadata::default()
        },
    )
    .await;

    if let Err(error) = persist_user_turn(persistence, sluice_session_id, 0, initial_prompt).await {
        tracing::warn!(?error, "Discuss worker: failed to persist initial user turn");
    }
}

/// Persist the latest parent-session todo snapshot without touching transcript
/// rows, metadata, or child utility state.
pub async fn persist_todo_snapshot(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    snapshot_json: &str,
) -> Result<(), DiscussError> {
    let conn = persistence.acquire_connection().await?;
    super::storage::update_discuss_session_todo_snapshot(&conn, sluice_session_id, snapshot_json).await
}

pub async fn persist_subagent(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    subagent: &DiscussSubagentRecord,
    retention_limit: u32,
) -> Result<(), DiscussError> {
    let mut conn = persistence.acquire_connection().await?;
    super::storage::upsert_and_prune_discuss_subagent(&mut conn, sluice_session_id, subagent, retention_limit).await
}

pub async fn persist_session_anchor(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    metadata: SessionAnchorMetadata<'_>,
) {
    if let Err(error) = persist_session_anchor_inner(persistence, sluice_session_id, metadata).await {
        tracing::warn!(?error, "Discuss worker: failed to persist session metadata");
    }
}

pub async fn persist_session_anchor_inner(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    metadata: SessionAnchorMetadata<'_>,
) -> Result<(), DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let existing = load_discuss_session(&conn, sluice_session_id).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    upsert_discuss_session_with_task_tag(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id,
            team_leader_id: metadata.team_leader_id.or(existing
                .as_ref()
                .and_then(|row| row.team_leader_id.as_deref())),
            agent_name: metadata
                .agent_name
                .or(existing.as_ref().map(|r| r.agent_name.as_str()))
                .unwrap_or(""),
            session_name: metadata
                .session_name
                .or(existing.as_ref().map(|r| r.session_name.as_str()))
                .unwrap_or(""),
            acp_session_id: metadata.acp_session_id.or(existing
                .as_ref()
                .and_then(|row| row.acp_session_id.as_deref())),
            display_title: if metadata.clear.display_title {
                None
            } else {
                metadata.display_title.or(existing
                    .as_ref()
                    .and_then(|row| row.display_title.as_deref()))
            },
            last_user_prompt: metadata.last_user_prompt.or(existing
                .as_ref()
                .and_then(|row| row.last_user_prompt.as_deref())),
            latest_todo_snapshot_json: metadata.latest_todo_snapshot_json.or(existing
                .as_ref()
                .and_then(|row| row.latest_todo_snapshot_json.as_deref())),
            latest_config_options_json: if metadata.clear.latest_config_options_json {
                None
            } else {
                metadata.latest_config_options_json.or(existing
                    .as_ref()
                    .and_then(|row| row.latest_config_options_json.as_deref()))
            },
            config_settings_json: metadata.config_settings_json.or(existing
                .as_ref()
                .and_then(|row| row.config_settings_json.as_deref())),
            available_commands_json: if metadata.clear.available_commands_json {
                None
            } else {
                metadata.available_commands_json.or(existing
                    .as_ref()
                    .and_then(|row| row.available_commands_json.as_deref()))
            },
            first_prompt_at: existing
                .as_ref()
                .and_then(|row| row.first_prompt_at)
                .or(Some(now_ms)),
            last_interaction_at: Some(now_ms),
        },
        metadata.is_task_session,
    )
    .await
}

pub async fn persist_metadata_update(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    agent_name: &str,
    session_name: &str,
    metadata: &serde_json::Value,
) {
    if let Err(error) =
        persist_metadata_update_inner(persistence, sluice_session_id, agent_name, session_name, metadata).await
    {
        tracing::warn!(?error, "Discuss worker: failed to persist metadata update");
    }
}

/// Fallible core of [`persist_metadata_update`].
///
/// Splitting the classification + anchor write out of the fire-and-forget
/// wrapper lets persistence tests assert the real DB result at the persist site
/// instead of observing a downstream "row missing" symptom after
/// [`persist_metadata_update`] / [`persist_session_anchor`] have already
/// swallowed the error into a `tracing::warn!`.
pub async fn persist_metadata_update_inner(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    agent_name: &str,
    session_name: &str,
    metadata: &serde_json::Value,
) -> Result<(), DiscussError> {
    let acp_session_id = metadata.get("sessionId").and_then(|value| value.as_str());

    // Provider titles stay metadata-only. Local Sluice session names are
    // user-controlled, so metadata updates never overwrite display_title.
    // Config options are persisted as canonical `Vec<DiscussConfigOption>` JSON — the
    // exact shape the no-live-worker `handle_get_config_options` fallback
    // deserializes — never the raw provider `options` blob. A present config
    // array is normalized through the same provider config extractor
    // path the live worker cache uses, so persisted and live reads agree.
    let agent_plugin = plugins::for_agent(agent_name);
    let (latest_config_options_json, clear_config) = classify_clearable_config_options(agent_plugin, metadata);
    let (available_commands_json, clear_commands) = classify_clearable_json(metadata.get("availableCommands"));

    persist_session_anchor_inner(
        persistence,
        sluice_session_id,
        SessionAnchorMetadata {
            agent_name: Some(agent_name),
            session_name: Some(session_name),
            acp_session_id,
            latest_config_options_json: latest_config_options_json.as_deref(),
            available_commands_json: available_commands_json.as_deref(),
            clear: SessionAnchorClear {
                display_title: false,
                latest_config_options_json: clear_config,
                available_commands_json: clear_commands,
            },
            ..SessionAnchorMetadata::default()
        },
    )
    .await?;

    let agent_menu = if clear_config {
        Some(Vec::new())
    } else {
        agent_plugin.metadata_config_options(metadata)
    };
    if let Some(options) = agent_menu {
        let conn = persistence.acquire_connection().await?;
        let defaults = load_discuss_agent_config(&conn, agent_name)
            .await?
            .map(|row| config_settings_from_json(&row.default_config_json))
            .transpose()?
            .unwrap_or_default();
        upsert_discuss_agent_config(&conn, agent_name, Some(&options), &defaults).await?;
    }
    Ok(())
}

/// Persist canonical config options refreshed by a successful
/// `session/set_config_option` to `latest_config_options_json` (fire-and-forget).
///
/// The `options` are already the canonical `Vec<DiscussConfigOption>` normalized from
/// the provider's **post-response** `config_options` through
/// the provider config extractor, so this helper serializes them verbatim and
/// **overwrites** the persisted fallback via the load-and-carry-forward anchor
/// path. The `session/set_config_option` response always carries the provider's
/// full explicit option set, so an empty slice serializes to `"[]"` and is
/// persisted as an explicit empty list rather than a carry-forward — the
/// persisted fallback therefore mirrors the live worker cache exactly after a
/// set, including the "provider now advertises no options" case.
///
pub async fn persist_config_options(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    agent_name: &str,
    options: &[DiscussConfigOption],
    session_settings: Option<&ConfigSettings>,
) -> Result<(), DiscussError> {
    let options_json = serde_json::to_string(options)?;
    let settings_json = session_settings.map(serde_json::to_string).transpose()?;
    persist_session_anchor_inner(
        persistence,
        sluice_session_id,
        SessionAnchorMetadata {
            latest_config_options_json: Some(&options_json),
            config_settings_json: settings_json.as_deref(),
            ..SessionAnchorMetadata::default()
        },
    )
    .await?;

    let conn = persistence.acquire_connection().await?;
    let defaults = load_discuss_agent_config(&conn, agent_name)
        .await?
        .map(|row| config_settings_from_json(&row.default_config_json))
        .transpose()?
        .unwrap_or_default();
    upsert_discuss_agent_config(&conn, agent_name, Some(options), &defaults).await
}

/// Persist a flush payload as a `discuss_chat_turns` assistant row
/// (fire-and-forget).
///
/// Best-effort: DB failures log via `tracing` and continue. The buffer carries
/// no position; the worker owns the per-session `position` counter.
pub async fn persist_flush_payload(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    position: i64,
    payload: &FlushPayload,
) {
    if let Err(error) = persist_assistant_turn(persistence, sluice_session_id, position, payload).await {
        tracing::warn!(?error, "Discuss worker: failed to persist assistant turn");
    }
    // Bump last_interaction_at on the chat anchor so the session dropdown
    // orders by the protocol-level turn boundary. The load-and-carry-forward
    // path preserves every other metadata column.
    persist_session_anchor(persistence, sluice_session_id, SessionAnchorMetadata::default()).await;
}

pub async fn persist_assistant_turn(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    position: i64,
    payload: &FlushPayload,
) -> Result<(), DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let content = serde_json::json!({
        "role": "assistant",
        "text": payload.aggregated_text,
        "thought": payload.aggregated_thought,
        "parts": payload.parts,
        "stopReason": payload.stop_reason,
    })
    .to_string();
    insert_discuss_chat_turn(
        &conn,
        DiscussChatTurnInsert {
            sluice_session_id,
            position,
            role: "assistant",
            acp_session_id: None,
            content_json: &content,
            created_at: now_ms,
        },
    )
    .await?;
    prune_discuss_chat_turns_window(&conn, sluice_session_id).await?;
    Ok(())
}

/// Persist only content-bearing assistant turns. A repeated `StopReason` after
/// a buffer flush has no text, so it cannot create a duplicate transcript row.
pub fn should_persist_assistant_turn(payload: &FlushPayload) -> bool {
    !payload.aggregated_text.is_empty() || !payload.parts.is_empty()
}

/// Persist one elicitation question and its terminal answer as a dedicated
/// transcript role. The raw ACP schema is intentionally excluded from this
/// durable projection; cancellation is represented by `status: "cancelled"`.
pub async fn persist_elicitation_turn_inner(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    elicitation_request_id: &str,
    input: ElicitationPersistenceInput,
    answer: DiscussElicitationAnswer,
) -> Result<(), DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let (status, answer_json) = match answer {
        DiscussElicitationAnswer::Accept { content } => ("accepted", Some(content)),
        DiscussElicitationAnswer::Decline => ("declined", None),
        DiscussElicitationAnswer::Cancel => ("cancelled", None),
    };
    let content = serde_json::json!({
        "elicitationRequestId": elicitation_request_id,
        "message": input.message,
        "answer": answer_json,
        "status": status,
    })
    .to_string();
    insert_discuss_chat_turn(
        &conn,
        DiscussChatTurnInsert {
            sluice_session_id,
            position: input.position,
            role: "elicitation",
            acp_session_id: None,
            content_json: &content,
            created_at: chrono::Utc::now().timestamp_millis(),
        },
    )
    .await?;
    prune_discuss_chat_turns_window(&conn, sluice_session_id).await?;
    Ok(())
}

/// Classify a metadata JSON field (serialized as a string blob) into
/// (present serialized value, clear flag).
///
/// An explicit `null` clears the column rather than persisting the literal
/// string `"null"`.
fn classify_clearable_json(value: Option<&serde_json::Value>) -> (Option<String>, bool) {
    match value {
        None => (None, false),
        Some(v) if v.is_null() => (None, true),
        Some(v) => (Some(v.to_string()), false),
    }
}

/// Classify the config-options field of a metadata update into
/// (canonical serialized value, clear flag).
///
/// Unlike [`classify_clearable_json`], the persisted value is the **canonical**
/// `Vec<DiscussConfigOption>` JSON — the exact shape the no-live-worker
/// `handle_get_config_options` fallback deserializes — rather than the raw
/// provider `options` blob. This keeps the persisted fallback consistent with
/// the live-worker cache, which is refreshed from the same normalization
/// (`DiscussAgentPlugin::metadata_config_options`).
///
/// Tri-state semantics mirror [`classify_clearable_json`], keyed on the config
/// field (`options`, or the raw `configOptions` key):
/// - Neither key present -> `(None, false)`: carry the existing DB value forward.
/// - Explicit `null` -> `(None, true)`: clear the DB column.
/// - Present array -> `(Some(canonical_json), false)`: overwrite with the
///   normalized canonical options.
fn classify_clearable_config_options(
    agent_plugin: &dyn plugins::DiscussAgentPlugin,
    metadata: &serde_json::Value,
) -> (Option<String>, bool) {
    let raw = metadata
        .get("configOptions")
        .or_else(|| metadata.get("options"));
    match raw {
        None => (None, false),
        Some(v) if v.is_null() => (None, true),
        Some(_) => {
            // Normalize to canonical `Vec<DiscussConfigOption>` and serialize. A
            // present-but-unnormalizable array (no recognizable select options)
            // yields an empty canonical array rather than persisting raw JSON the
            // fallback cannot deserialize into typed options.
            let canonical = agent_plugin
                .metadata_config_options(metadata)
                .unwrap_or_default();
            match serde_json::to_string(&canonical) {
                Ok(json) => (Some(json), false),
                Err(error) => {
                    tracing::warn!(?error, "ACP worker: failed to serialize canonical config options");
                    (None, false)
                }
            }
        }
    }
}

/// Persist the live session anchor without a user turn (fire-and-forget).
///
/// Used when an ACP protocol session is created before the first prompt so the
/// parent `discuss_sessions` row exists for later config-option and turn
/// writes. Best-effort: DB failures log via `tracing` and continue.
#[cfg(test)]
pub(super) async fn persist_session_open(
    persistence: &DiscussPersistence,
    sluice_session_id: &str,
    acp_session_id: &str,
) {
    persist_session_anchor(
        persistence,
        sluice_session_id,
        SessionAnchorMetadata {
            acp_session_id: Some(acp_session_id),
            ..SessionAnchorMetadata::default()
        },
    )
    .await;
}

pub(super) fn retained_team_assets(
    library: AssetLibrary,
    installations: &[DiscussTeamInstallationRow],
    current_leader_id: &str,
    agent_scope_key: &str,
    skill_scope_key: &str,
) -> (BTreeSet<AgentId>, BTreeSet<SkillId>) {
    let mut retain_agents = BTreeSet::new();
    let mut retain_skills = BTreeSet::new();
    for installation in installations
        .iter()
        .filter(|installation| installation.leader_id != current_leader_id)
    {
        if let Some(team) = library
            .teams_for()
            .find(|team| team.leader.as_ref() == installation.leader_id)
        {
            let plugin = plugins::for_agent(&installation.provider_name);
            if plugin.agent_scope_key() == agent_scope_key {
                retain_agents.insert(team.leader.clone());
                retain_agents.extend(team.followers.iter().cloned());
            }
            if plugin.skill_scope_key() == skill_scope_key {
                retain_skills.extend(team.skills.iter().cloned());
            }
        }
    }
    (retain_agents, retain_skills)
}

pub(super) fn retained_team_assets_by_scope(
    library: AssetLibrary,
    installations: &[DiscussTeamInstallationRow],
) -> RetainedTeamAssetsByScope {
    let mut retained_agents = BTreeMap::new();
    let mut retained_skills = BTreeMap::new();
    for installation in installations {
        let Some(team) = library
            .teams_for()
            .find(|team| team.leader.as_ref() == installation.leader_id)
        else {
            continue;
        };
        let plugin = plugins::for_agent(&installation.provider_name);
        let retain_agents = retained_agents
            .entry(plugin.agent_scope_key())
            .or_insert_with(BTreeSet::new);
        retain_agents.insert(team.leader.clone());
        retain_agents.extend(team.followers.iter().cloned());
        let retain_skills = retained_skills
            .entry(plugin.skill_scope_key())
            .or_insert_with(BTreeSet::new);
        retain_skills.extend(team.skills.iter().cloned());
    }
    (retained_agents, retained_skills)
}

pub(super) fn team_member_ids(team: &TeamDefinition) -> Vec<String> {
    std::iter::once(team.leader.to_string())
        .chain(team.followers.iter().map(ToString::to_string))
        .collect()
}

pub async fn validate_member_overrides(
    conn: &Connection,
    provider_name: &str,
    team: &TeamDefinition,
    overrides: DiscussMemberOverrides,
) -> Result<DiscussMemberOverrides, DiscussError> {
    let valid_members = team_member_ids(team).into_iter().collect::<BTreeSet<_>>();
    let options = load_discuss_agent_config(conn, provider_name)
        .await?
        .and_then(|row| row.config_options_json)
        .map(|json| config_options_from_json(&json))
        .transpose()?
        .unwrap_or_default();
    let advertised = options
        .iter()
        .filter(|option| option.category != "mode" && option.id != "mode")
        .map(|option| {
            (
                option.id.as_str(),
                option
                    .options
                    .iter()
                    .map(|value| value.value.as_str())
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut normalized = DiscussMemberOverrides::new();
    for (member_id, mut values) in overrides {
        if !valid_members.contains(&member_id) {
            return Err(DiscussError::WorkspaceInvalidAssetId {
                asset_id: member_id,
                violation: "member is not the Team leader or a canonical follower".to_string(),
            });
        }
        values.remove("mode");
        for (config_id, value) in &values {
            let Some(valid_values) = advertised.get(config_id.as_str()) else {
                return Err(DiscussError::Validation {
                    message: format!("Provider option `{config_id}` is not advertised for Team member `{member_id}`"),
                });
            };
            if !valid_values.contains(value.as_str()) {
                return Err(DiscussError::Validation {
                    message: format!("Provider value `{value}` is not advertised for option `{config_id}`"),
                });
            }
        }
        if !values.is_empty() {
            normalized.insert(member_id, values);
        }
    }
    Ok(normalized)
}

pub(super) fn persistence_context(error: DiscussError, context: String) -> DiscussError {
    match error {
        DiscussError::Persistence { code, message } => DiscussError::Persistence {
            code,
            message: format!("{message}; {context}"),
        },
        other => DiscussError::Persistence {
            code: "db_query".to_string(),
            message: format!("{other}; {context}"),
        },
    }
}

pub(super) fn cleanup_warning(provider_name: &str, leader_id: &str, error: &DiscussError) -> CleanupWarning {
    let path = match error {
        DiscussError::WorkspaceIo { path, .. } | DiscussError::WorkspaceSerialize { path, .. } => Some(path.clone()),
        _ => None,
    };
    CleanupWarning {
        kind: "workspace_cleanup".to_string(),
        provider_name: provider_name.to_string(),
        agent_id: Some(leader_id.to_string()),
        path,
        message: error.to_string(),
    }
}

pub(super) fn with_install_compensation_context(
    error: DiscussError,
    agent_id: &str,
    provider_name: &str,
    workspace: &Path,
    cleanup_error: &DiscussError,
) -> DiscussError {
    let context = format!(
        "installation file may remain after failed persistence compensation: agent_id={agent_id}, provider={provider_name}, path={}, cleanup_error={cleanup_error}",
        workspace.display()
    );
    match error {
        DiscussError::Persistence { code, message } => DiscussError::Persistence {
            code,
            message: format!("{message}; {context}"),
        },
        other => DiscussError::Persistence {
            code: "db_query".to_string(),
            message: format!("{other}; {context}"),
        },
    }
}

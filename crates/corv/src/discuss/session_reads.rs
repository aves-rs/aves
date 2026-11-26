use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use agent_client_protocol::v1::schema::{SessionId, SessionOptionsRequest};
use serde_json::Value;
use tokio::sync::oneshot;

use super::actor::Actor;
use super::config::{config_options_from_json, provider_config_options_json};
use super::error::DiscussError;
use super::messages::DiscussResponse;
use super::plugins;
use super::storage::{
    DiscussSessionRow, LoadedSessionRefresh, list_recent_discuss_sessions, load_discuss_agent_config,
    load_discuss_chat_turns_window, load_discuss_session, load_discuss_subagents, merge_refresh_discuss_session,
    present_acp_session_id, provider_updated_at_ms,
};
use super::supervisor::ProviderSupervisorResources;
use super::types::{
    DiscussChatTurnRecord, DiscussConfigOption, DiscussSessionId, DiscussSessionInfo, DiscussTranscriptCursor,
    DiscussTranscriptPage, WorkerCommand,
};

/// Cuts saved turns, oldest first, down to one page: the newest `part_limit` entries before the
/// page's cursor. Returns the page and, when older entries remain, the cursor that continues it.
pub(super) fn page_turns(
    turns: Vec<DiscussChatTurnRecord>,
    page: DiscussTranscriptPage,
) -> (Vec<DiscussChatTurnRecord>, Option<DiscussTranscriptCursor>) {
    let mut budget = page.part_limit.max(1) as usize;
    let mut kept = Vec::new();
    let mut first = None;
    let mut more = false;
    for mut turn in turns.into_iter().rev() {
        let position = turn.position;
        let cut = match page.before {
            Some(before) if position > before.position => continue,
            Some(before) if position == before.position => Some(before.part as usize),
            _ => None,
        };
        if cut == Some(0) {
            continue;
        }
        if budget == 0 {
            more = true;
            break;
        }
        let mut content = serde_json::from_str::<Value>(&turn.content_json).unwrap_or(Value::Null);
        let Some(parts) = content.get_mut("parts").and_then(Value::as_array_mut) else {
            budget -= 1;
            first = Some(DiscussTranscriptCursor { position, part: 0 });
            kept.push(turn);
            continue;
        };
        let end = cut.unwrap_or(parts.len()).min(parts.len());
        let start = tool_safe_start(parts, end.saturating_sub(budget), end);
        budget = budget.saturating_sub(end - start);
        if start > 0 || end < parts.len() {
            parts.truncate(end);
            parts.drain(..start);
            if let Some(record) = content.as_object_mut() {
                record.remove("text");
                record.remove("thought");
            }
            turn.content_json = content.to_string();
        }
        first = Some(DiscussTranscriptCursor {
            position,
            part: u32::try_from(start).unwrap_or(u32::MAX),
        });
        kept.push(turn);
        if start > 0 {
            more = true;
            break;
        }
    }
    kept.reverse();
    (kept, first.filter(|_| more))
}

/// Moves a page's first entry back until every tool call in `start..end` begins inside the page.
fn tool_safe_start(parts: &[Value], mut start: usize, end: usize) -> usize {
    fn tool_id(part: &Value) -> Option<&str> {
        if part.get("kind").and_then(Value::as_str) != Some("tool_call") {
            return None;
        }
        part.get("update")?.get("toolCallId")?.as_str()
    }
    let mut begins: HashMap<&str, usize> = HashMap::new();
    for (index, part) in parts[..end].iter().enumerate() {
        if let Some(id) = tool_id(part) {
            begins.entry(id).or_insert(index);
        }
    }
    let mut scanned = end;
    while start < scanned {
        let earliest = parts[start..scanned]
            .iter()
            .filter_map(tool_id)
            .filter_map(|id| begins.get(id).copied())
            .min()
            .unwrap_or(start);
        scanned = start;
        start = start.min(earliest);
    }
    start
}

impl Actor {
    /// List local ACP chat sessions ordered by most recent interaction.
    ///
    /// Reads local `acp_agent_sessions` rows only as the session identity
    /// source. For every local row that has a non-empty `acp_session_id`,
    /// checks the matching worker's server-side capability state and, when the
    /// `list` bit is set, forwards a `WorkerCommand::ListProviderSessions` to
    /// the live worker so provider `SessionInfo` can refresh freshness metadata
    /// (`last_interaction_at`) on **already-local** rows matched by
    /// `acp_session_id`. Provider titles remain metadata-only; local
    /// display labels are user-controlled.
    ///
    /// Provider-listed sessions that do not already have a local
    /// `discuss_sessions` row are **not** materialized. A provider list row
    /// is a candidate only; local identity is created solely by explicit
    /// create / load / import / persist paths.
    pub(super) async fn handle_list_sessions(&mut self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let local_rows = list_recent_discuss_sessions(&conn).await?;

        let mut sessions: Vec<DiscussSessionInfo> = local_rows
            .iter()
            .filter_map(|row| {
                Some(DiscussSessionInfo {
                    launch_limitations: row.launch_limitations.clone(),
                    active_turn: DiscussSessionId::from_str(&row.discuss_session_id)
                        .ok()
                        .and_then(|id| self.worker_registry.get(&id))
                        .is_some_and(|entry| !entry.join_handle.is_finished() && entry.active_turn),
                    is_task_session: row.is_task_session,
                    team_leader_id: row.team_leader_id.clone(),
                    discuss_session_id: DiscussSessionId::from_str(&row.discuss_session_id).ok()?,
                    session_name: row.session_name.clone(),
                    agent_name: row.agent_name.clone(),
                    acp_session_id: present_acp_session_id(row.acp_session_id.as_deref()),
                    display_title: row.display_title.clone().filter(|t| !t.is_empty()),
                    last_user_prompt: row.last_user_prompt.clone().filter(|p| !p.is_empty()),
                    latest_todo_snapshot_json: row.latest_todo_snapshot_json.clone(),
                    latest_config_options_json: row.latest_config_options_json.clone(),
                    available_commands_json: row.available_commands_json.clone(),
                    first_prompt_at: row.first_prompt_at,
                    last_interaction_at: row.last_interaction_at,
                })
            })
            .collect();

        // Defensive dedupe by local identity first, then by provider session id
        // so a corrupted DB cannot surface the same chat twice in the dropdown.
        sessions = dedupe_session_list(sessions);

        // For every persisted row with a non-empty acp_session_id, check the
        // server-side capability state and forward ListProviderSessions to
        // the live worker when the list bit is set. An empty-string id is the
        // canonical "no provider session id yet" sentinel, so it is treated
        // the same as an absent id here.
        //
        // Provider-only rows are intentionally ignored: list/refresh must never
        // insert into `discuss_sessions`.
        for row in &local_rows {
            if present_acp_session_id(row.acp_session_id.as_deref()).is_none() {
                continue;
            }
            let Ok(discuss_session_id) = DiscussSessionId::from_str(&row.discuss_session_id) else {
                continue;
            };
            if let Some(entry) = self.worker_registry.get(&discuss_session_id)
                && let Some(caps) = self.worker_capabilities.get(&discuss_session_id)
                && caps.list
            {
                let command_tx = entry.command_tx.clone();
                let (reply, reply_rx) = oneshot::channel();
                if command_tx
                    .send(WorkerCommand::ListProviderSessions { reply })
                    .await
                    .is_err()
                {
                    self.remove_worker(&discuss_session_id).await;
                    self.worker_capabilities.remove(&discuss_session_id);
                } else if let Ok(Ok(provider_sessions)) = reply_rx.await {
                    // Merge provider-listed freshness metadata onto already-local
                    // rows matched by acp_session_id. Provider-only rows are not
                    // materialized.
                    for ps in provider_sessions {
                        // Provider session id match: update last_interaction_at.
                        if let Some(ref acp_id) = row.acp_session_id
                            && ps.session_id.to_string() == *acp_id
                        {
                            let now_ms = provider_updated_at_ms(ps.updated_at.as_deref());
                            let _ = merge_refresh_discuss_session(
                                &conn,
                                &row.discuss_session_id,
                                &LoadedSessionRefresh {
                                    acp_session_id: None,
                                    // ProviderSessionInfo does not expose a title;
                                    // retain the locally persisted session metadata.
                                    display_title: None,
                                    latest_config_options_json: None,
                                    last_interaction_at: now_ms,
                                },
                            )
                            .await;
                        }
                    }
                }
            }
        }

        Ok(DiscussResponse::SessionList { sessions })
    }

    /// Load a single chat session and its bounded recent transcript turns.
    ///
    /// Missing-load is always a graceful fallback: when the provider does not
    /// advertise `load` (live-worker or short-lived branch), the local row +
    /// bounded turns are returned and `UnsupportedCapability` is never raised.
    /// When a provider refresh succeeds, the refreshed metadata is persisted
    /// and the response is rebuilt from the re-read row so the client always
    /// sees the freshest persisted state rather than the pre-refresh snapshot.
    pub(super) async fn handle_load_session(
        &mut self,
        discuss_session_id: DiscussSessionId,
        page: Option<DiscussTranscriptPage>,
    ) -> Result<DiscussResponse, DiscussError> {
        tracing::debug!(
            target: "application_acp",
            discuss_session_id = %discuss_session_id,
            has_live_worker = self.worker_registry.contains_key(&discuss_session_id),
            "loading session (metadata + bounded transcript window)"
        );
        let conn = self.persistence.acquire_connection().await?;
        let mut row = load_discuss_session(&conn, &discuss_session_id.to_string())
            .await?
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;

        let turns = load_discuss_chat_turns_window(&conn, &discuss_session_id.to_string()).await?;
        let subagents = load_discuss_subagents(&conn, &discuss_session_id.to_string()).await?;
        let turn_records: Vec<DiscussChatTurnRecord> = turns
            .iter()
            .filter_map(|t| {
                Some(DiscussChatTurnRecord {
                    discuss_session_id: DiscussSessionId::from_str(&t.discuss_session_id).ok()?,
                    position: t.position,
                    role: t.role.clone().unwrap_or_default(),
                    acp_session_id: t.acp_session_id.clone(),
                    content_json: t.content_json.clone(),
                    created_at: t.created_at,
                })
            })
            .collect();
        let (turn_records, earlier) = match page {
            Some(page) => page_turns(turn_records, page),
            None => (turn_records, None),
        };

        // Without a live worker the saved row is the answer; the reconnect that follows refreshes it.
        if self.worker_registry.contains_key(&discuss_session_id)
            && let Some(acp_session_id) = present_acp_session_id(row.acp_session_id.as_deref())
            && let Some(refreshed) = self
                .refresh_from_live_worker(discuss_session_id, &acp_session_id)
                .await?
        {
            row = refreshed;
        }

        Ok(DiscussResponse::SessionLoad {
            session: self.loaded_session_info(discuss_session_id, row)?,
            turns: turn_records,
            subagents,
            earlier,
        })
    }

    /// Ask the live worker to reload the provider session when it advertises `load`, then
    /// persist the refreshed metadata. A closed worker channel drops the stale registration.
    async fn refresh_from_live_worker(
        &mut self,
        discuss_session_id: DiscussSessionId,
        acp_session_id: &str,
    ) -> Result<Option<DiscussSessionRow>, DiscussError> {
        let load_supported = self
            .worker_capabilities
            .get(&discuss_session_id)
            .is_some_and(|capabilities| capabilities.load);
        let Some(entry) = self
            .worker_registry
            .get(&discuss_session_id)
            .filter(|_| load_supported)
        else {
            return Ok(None);
        };
        let command_tx = entry.command_tx.clone();
        let (reply, reply_rx) = oneshot::channel();
        if command_tx
            .send(WorkerCommand::LoadProviderSession {
                acp_session_id: SessionId::from(acp_session_id.to_owned()),
                reply,
            })
            .await
            .is_err()
        {
            self.remove_worker(&discuss_session_id).await;
            self.worker_capabilities.remove(&discuss_session_id);
            return Ok(None);
        }
        let Ok(Ok(ps)) = reply_rx.await else {
            return Ok(None);
        };
        let now_ms = provider_updated_at_ms(ps.updated_at.as_deref());
        let refreshed_options_json = provider_config_options_json(&ps.config_options)?;
        self.refresh_loaded_session_row(
            discuss_session_id,
            LoadedSessionRefresh {
                acp_session_id: Some(ps.session_id.to_string()),
                display_title: None,
                latest_config_options_json: refreshed_options_json,
                last_interaction_at: now_ms,
            },
        )
        .await
        .map(Some)
    }

    /// Project a loaded anchor row into the session info the load response carries.
    fn loaded_session_info(
        &self,
        discuss_session_id: DiscussSessionId,
        row: DiscussSessionRow,
    ) -> Result<DiscussSessionInfo, DiscussError> {
        Ok(DiscussSessionInfo {
            launch_limitations: row.launch_limitations,
            active_turn: self
                .worker_registry
                .get(&discuss_session_id)
                .is_some_and(|entry| !entry.join_handle.is_finished() && entry.active_turn),
            is_task_session: row.is_task_session,
            team_leader_id: row.team_leader_id,
            discuss_session_id: DiscussSessionId::from_str(&row.discuss_session_id).map_err(|_| {
                DiscussError::Serialization {
                    message: "invalid discuss_session_id in Discuss session row".to_string(),
                }
            })?,
            session_name: row.session_name,
            agent_name: row.agent_name,
            acp_session_id: present_acp_session_id(row.acp_session_id.as_deref()),
            display_title: row.display_title.filter(|t| !t.is_empty()),
            last_user_prompt: row.last_user_prompt.filter(|p| !p.is_empty()),
            latest_todo_snapshot_json: row.latest_todo_snapshot_json,
            latest_config_options_json: row.latest_config_options_json,
            available_commands_json: row.available_commands_json,
            first_prompt_at: row.first_prompt_at,
            last_interaction_at: row.last_interaction_at,
        })
    }

    /// Persist a provider-refreshed metadata delta onto the chat anchor row and
    /// re-read the row so the load response reflects the freshest persisted
    /// state.
    ///
    /// R4: this uses a merge-safe **partial** `UPDATE`
    /// (`discuss_sessions_load_refresh.sql`) rather than the full-column
    /// upsert. Only the refreshed columns are touched, each via
    /// `COALESCE(new, existing)`, and `last_interaction_at` moves monotonically
    /// via `MAX(...)`. A `None` field in `refresh` therefore carries the
    /// existing value forward — a provider that omits a title or config options
    /// never erases known-good state — and a stale refresh interleaved with a
    /// newer queued write from the worker's per-session persistence queue can
    /// never clobber the fresher metadata or move the timestamp backward.
    ///
    /// The `row` snapshot is intentionally not written back: because the update
    /// is partial and merge-safe, carry-forward columns (`last_user_prompt`,
    /// `available_commands_json`, `first_prompt_at`) are left untouched in the
    /// database instead of being re-written from a possibly-stale snapshot.
    async fn refresh_loaded_session_row(
        &self,
        discuss_session_id: DiscussSessionId,
        refresh: LoadedSessionRefresh,
    ) -> Result<DiscussSessionRow, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let discuss_session_id_string = discuss_session_id.to_string();
        merge_refresh_discuss_session(&conn, &discuss_session_id_string, &refresh).await?;
        let refreshed = load_discuss_session(&conn, &discuss_session_id_string)
            .await?
            .ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
        Ok(refreshed)
    }

    /// Get the database-backed provider menu for a session.
    ///
    /// Workers eagerly replace the persisted snapshot whenever the provider
    /// publishes a complete menu. Reads never synchronously query a provider.
    pub(super) async fn handle_get_config_options(
        &mut self,
        discuss_session_id: DiscussSessionId,
    ) -> Result<DiscussResponse, DiscussError> {
        // An inspector viewer has no anchor row; it reports the child session's own options.
        if let Some((provider_name, options)) = self.inspector_config_options.get(&discuss_session_id) {
            return Ok(DiscussResponse::ConfigOptions {
                options: options.clone(),
                agent_config_option_id: plugins::agent_config_option_id(plugins::for_agent(provider_name)),
            });
        }
        let conn = self.persistence.acquire_connection().await?;
        let row = load_discuss_session(&conn, &discuss_session_id.to_string()).await?;
        let row = row.ok_or(DiscussError::SessionNotFound { discuss_session_id })?;
        let mut options = match row.latest_config_options_json.as_deref() {
            Some(json) => config_options_from_json(json)?,
            None => load_discuss_agent_config(&conn, &row.agent_name)
                .await?
                .and_then(|config| config.config_options_json)
                .as_deref()
                .map(config_options_from_json)
                .transpose()?
                .unwrap_or_default(),
        };

        if options.is_empty()
            && !self.worker_registry.contains_key(&discuss_session_id)
            && let Some(shared) = self.shared_session_options(&row.agent_name).await
        {
            options = shared?;
        }

        let agent_config_option_id = plugins::agent_config_option_id(plugins::for_agent(&row.agent_name));
        Ok(DiscussResponse::ConfigOptions {
            options,
            agent_config_option_id,
        })
    }

    /// Reads a provider's options over its running shared connection with `session/options`,
    /// which opens no session. `None` when there is no such connection or it lacks the method.
    pub(super) async fn shared_session_options(
        &self,
        provider_name: &str,
    ) -> Option<Result<Vec<DiscussConfigOption>, DiscussError>> {
        let Some(ProviderSupervisorResources::Shared {
            connection: Some(connection),
            capabilities,
            ..
        }) = self
            .provider_supervisors
            .get(provider_name)
            .map(|supervisor| &supervisor.resources)
        else {
            return None;
        };
        capabilities.session_capabilities.options.as_ref()?;
        let response = connection
            .send_request(SessionOptionsRequest::new(self.agents_workspace.clone()))
            .block_task()
            .await;
        Some(response.map_err(DiscussError::from).map(|response| {
            plugins::for_agent(provider_name)
                .initial_config_options(response.config_options, response.meta.as_ref())
                .unwrap_or_default()
        }))
    }
}

/// Deduplicate session list rows by local identity, then by provider session id.
///
/// First-seen wins so the interaction-ordered list from SQL stays stable.
/// Empty-string / absent `acp_session_id` values do not participate in the
/// provider-id merge (they are not real provider identities).
pub(super) fn dedupe_session_list(sessions: Vec<DiscussSessionInfo>) -> Vec<DiscussSessionInfo> {
    let mut seen_identity = HashSet::<DiscussSessionId>::new();
    let mut seen_acp_id = HashSet::<String>::new();
    let mut out = Vec::with_capacity(sessions.len());

    for session in sessions {
        let identity = session.discuss_session_id;
        if !seen_identity.insert(identity)
            || present_acp_session_id(session.acp_session_id.as_deref())
                .is_some_and(|acp_id| !seen_acp_id.insert(acp_id))
        {
            continue;
        }
        out.push(session);
    }

    out
}

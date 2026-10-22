use std::sync::Arc;

use turso::{Connection, Database};

use aves::utils::database::{DatabaseError, connect_database};

use super::types::{
    ConfigSettings, DiscussConfigOption, DiscussError, DiscussSubagentRecord, DiscussSubagentSurfaceState,
    SluiceSessionId,
};

pub const DISCUSS_SESSIONS_UPSERT_SQL: &str = include_str!("queries/discuss_sessions_upsert.sql");
pub const DISCUSS_SESSIONS_UPDATE_SQL: &str = include_str!("queries/discuss_sessions_update.sql");
pub const DISCUSS_SESSIONS_TODO_SNAPSHOT_UPDATE_SQL: &str = include_str!("queries/todo_snapshot_update.sql");
pub const DISCUSS_SESSIONS_LOAD_REFRESH_SQL: &str = include_str!("queries/discuss_sessions_load_refresh.sql");
pub const DISCUSS_SESSIONS_LIST_RECENT_SQL: &str = include_str!("queries/discuss_sessions_list_recent.sql");
pub const DISCUSS_SESSIONS_LOAD_SQL: &str = include_str!("queries/discuss_sessions_load.sql");
pub const DISCUSS_SESSIONS_DELETE_SQL: &str = include_str!("queries/session_delete.sql");
pub const DISCUSS_SESSIONS_RENAME_SQL: &str = include_str!("queries/discuss_sessions_rename.sql");
pub const DISCUSS_CHAT_TURNS_INSERT_SQL: &str = include_str!("queries/discuss_chat_turns_insert.sql");
pub const DISCUSS_CHAT_TURNS_PRUNE_WINDOW_SQL: &str = include_str!("queries/discuss_chat_turns_prune_window.sql");
pub const DISCUSS_CHAT_TURNS_LOAD_WINDOW_SQL: &str = include_str!("queries/discuss_chat_turns_load_window.sql");
pub const DISCUSS_AGENT_CONFIG_LOAD_SQL: &str = include_str!("queries/discuss_agent_config_load.sql");
pub const DISCUSS_AGENT_CONFIG_UPSERT_SQL: &str = include_str!("queries/discuss_agent_config_upsert.sql");
pub const DISCUSS_AGENT_CONFIG_DELETE_SQL: &str = include_str!("queries/discuss_agent_config_delete.sql");
pub const DISCUSS_TEAM_INSTALLATION_LIST_SQL: &str = include_str!("queries/discuss_team_installation_list.sql");
pub const DISCUSS_TEAM_INSTALLATION_LIST_BY_PROVIDER_SQL: &str =
    include_str!("queries/discuss_team_installation_list_by_provider.sql");
pub const DISCUSS_TEAM_INSTALLATION_LOAD_SQL: &str = include_str!("queries/discuss_team_installation_load.sql");
pub const DISCUSS_TEAM_INSTALLATION_DELETE_SQL: &str = include_str!("queries/discuss_team_installation_delete.sql");
pub const DISCUSS_TEAM_INSTALLATION_UPSERT_SQL: &str = include_str!("queries/discuss_team_installation_upsert.sql");
pub const DISCUSS_SUBAGENTS_UPSERT_SQL: &str = include_str!("queries/discuss_subagents_upsert.sql");
pub const DISCUSS_SUBAGENTS_PRUNE_DONE_SQL: &str = include_str!("queries/discuss_subagents_prune_done.sql");
pub const DISCUSS_SUBAGENTS_LOAD_SQL: &str = include_str!("queries/discuss_subagents_load.sql");

/// Row projection for `discuss_sessions` chat anchor rows.
///
/// Carries the full metadata column set so the session dropdown and load path
/// can restore cached state in one round-trip. `sluice_session_id` is the
/// canonical identity; `session_name` and `agent_name` are metadata only.
pub struct DiscussSessionRow {
    pub(super) is_task_session: bool,
    pub(super) team_leader_id: Option<String>,
    pub(super) sluice_session_id: String,
    pub(super) agent_name: String,
    pub(super) session_name: String,
    pub(super) acp_session_id: Option<String>,
    pub(super) display_title: Option<String>,
    pub(super) last_user_prompt: Option<String>,
    pub(super) latest_todo_snapshot_json: Option<String>,
    pub(super) latest_config_options_json: Option<String>,
    pub(super) config_settings_json: Option<String>,
    pub(super) available_commands_json: Option<String>,
    pub(super) first_prompt_at: Option<i64>,
    pub(super) last_interaction_at: Option<i64>,
}

/// Row projection for `discuss_chat_turns` bounded transcript rows.
pub struct DiscussChatTurnRow {
    pub(super) sluice_session_id: String,
    pub(super) position: i64,
    pub(super) role: Option<String>,
    pub(super) acp_session_id: Option<String>,
    pub(super) content_json: String,
    pub(super) created_at: i64,
}

/// Upsert parameters for the `discuss_sessions` chat anchor.
///
/// The same tuple binds both the UPDATE and the INSERT-if-absent halves of
/// the session upsert, covering every metadata column. Identity is
/// `sluice_session_id` alone; `session_name` and `agent_name` are metadata.
pub struct DiscussSessionUpsert<'a> {
    pub(super) team_leader_id: Option<&'a str>,
    pub(super) sluice_session_id: &'a str,
    pub(super) agent_name: &'a str,
    pub(super) session_name: &'a str,
    pub(super) acp_session_id: Option<&'a str>,
    pub(super) display_title: Option<&'a str>,
    pub(super) last_user_prompt: Option<&'a str>,
    pub(super) latest_todo_snapshot_json: Option<&'a str>,
    pub(super) latest_config_options_json: Option<&'a str>,
    pub(super) config_settings_json: Option<&'a str>,
    pub(super) available_commands_json: Option<&'a str>,
    pub(super) first_prompt_at: Option<i64>,
    pub(super) last_interaction_at: Option<i64>,
}

pub struct DiscussAgentConfigRow {
    pub(super) config_options_json: Option<String>,
    pub(super) default_config_json: String,
}

impl Clone for DiscussAgentConfigRow {
    fn clone(&self) -> Self {
        Self {
            config_options_json: self.config_options_json.clone(),
            default_config_json: self.default_config_json.clone(),
        }
    }
}

pub struct DiscussTeamInstallationRow {
    pub(super) leader_id: String,
    pub(super) provider_name: String,
    pub(super) installed_version: String,
    pub(super) member_overrides_json: String,
    pub(super) members_json: String,
    pub(super) theme_color_json: Option<String>,
    pub(super) _updated_at: i64,
}

/// Provider-refreshed metadata delta applied to the chat anchor row during
/// `acp.loadSession`. Owned values so the delta can be moved into
/// `refresh_loaded_session_row` without borrowing the pre-refresh row.
///
/// The three `Option` fields are merge deltas, not a full snapshot: `Some(v)`
/// refreshes the column to `v`, while `None` carries the existing persisted
/// value forward (via `COALESCE` in `acp_agent_sessions_load_refresh.sql`). A
/// provider response that omits a title or config options therefore leaves the
/// known-good persisted value intact instead of erasing it (R3). Callers must
/// pass `None` — never a stale snapshot value — for any column the provider did
/// not actually refresh. `last_interaction_at` is always supplied and is
/// applied monotonically (`MAX`) so a stale timestamp cannot move the anchor
/// backward.
pub struct LoadedSessionRefresh {
    pub(super) acp_session_id: Option<String>,
    pub(super) display_title: Option<String>,
    pub(super) latest_config_options_json: Option<String>,
    pub(super) last_interaction_at: i64,
}

/// Insert parameters for `discuss_chat_turns_insert`.
pub struct DiscussChatTurnInsert<'a> {
    pub(super) sluice_session_id: &'a str,
    pub(super) position: i64,
    pub(super) role: &'a str,
    pub(super) acp_session_id: Option<&'a str>,
    pub(super) content_json: &'a str,
    pub(super) created_at: i64,
}

pub struct ElicitationPersistenceInput {
    pub(super) position: i64,
    pub(super) message: String,
}

// ── DiscussPersistence ───────────────────────────────────────────────────

/// Private cloneable DB-connection factory for the Discuss domain.
///
/// Wraps a clone of the actor-owned `turso::Database` handle (held behind an
/// `Arc` so the helper clones cheaply without duplicating the underlying Turso
/// handle). Both the actor's read-side query helpers and the worker pipeline's
/// fire-and-forget writes go through this factory, each acquiring a fresh
/// per-operation `turso::Connection` via `connect_database`.
#[derive(Clone)]
pub struct DiscussPersistence {
    pub(super) database: Arc<Database>,
}

impl DiscussPersistence {
    /// Acquire a fresh connection with Aves' connection setup applied.
    pub(super) async fn acquire_connection(&self) -> Result<Connection, DiscussError> {
        Ok(connect_database(&self.database).await?)
    }
}

impl From<DatabaseError> for DiscussError {
    fn from(error: DatabaseError) -> Self {
        Self::Persistence {
            code: "db_query".to_string(),
            message: error.to_string(),
        }
    }
}

fn row_get<T>(row: &turso::Row, index: usize, label: &str) -> Result<T, DiscussError>
where
    T: turso::core::types::FromValue,
{
    row.get(index).map_err(|error| DiscussError::Persistence {
        code: "db_query".to_string(),
        message: format!("failed to decode {label}: {error}"),
    })
}
// ── Typed async query helpers ─────────────────────────────────────────────
//
// Each helper takes an already-acquired `&turso::Connection` plus typed
// parameters and returns `Result<..., DiscussError>`. Driver errors are
// `?`-propagated through `From<turso::Error> for DiscussError` into the
// serializable `DiscussError::Persistence` variant. These helpers do not open,
// hold, clone, or expose a `turso::Database`, do not define or accept an
// `DiscussPersistence` factory, and have no `acquire_connection()` method. DB
// ownership and worker wiring belong to subtask 07.

/// Upsert a Discuss session anchor row keyed by `sluice_session_id`.
///
/// The new `discuss_sessions` table declares `sluice_session_id TEXT NOT NULL
/// PRIMARY KEY`, so a plain `INSERT ... ON CONFLICT(sluice_session_id) DO
/// UPDATE` has a matching conflict target. However, the caller still uses an
/// explicit **UPDATE-first, INSERT-if-absent** upsert to preserve the
/// load-and-carry-forward semantics of the persistence queue: the UPDATE half
/// reads the existing row to resolve carry-forward vs explicit-clear before
/// writing, and the INSERT-if-absent half only runs when no row was updated.
/// Both statements bind the same parameter tuple.
#[cfg(test)]
pub async fn upsert_discuss_session(conn: &Connection, params: DiscussSessionUpsert<'_>) -> Result<(), DiscussError> {
    upsert_discuss_session_with_task_tag(conn, params, false).await
}

pub(super) async fn upsert_discuss_session_with_task_tag(
    conn: &Connection,
    params: DiscussSessionUpsert<'_>,
    is_task_session: bool,
) -> Result<(), DiscussError> {
    let bindings = (
        params.sluice_session_id.to_string(),
        params.agent_name.to_string(),
        params.session_name.to_string(),
        params.display_title.map(str::to_string),
        params.acp_session_id.map(str::to_string),
        params.last_user_prompt.map(str::to_string),
        params.latest_todo_snapshot_json.map(str::to_string),
        params.latest_config_options_json.map(str::to_string),
        params.config_settings_json.map(str::to_string),
        params.available_commands_json.map(str::to_string),
        params.first_prompt_at,
        params.last_interaction_at,
        params.team_leader_id.map(str::to_string),
        i64::from(is_task_session),
    );

    let updated = conn
        .execute(DISCUSS_SESSIONS_UPDATE_SQL, bindings.clone())
        .await?;
    if updated == 0 {
        conn.execute(DISCUSS_SESSIONS_UPSERT_SQL, bindings).await?;
    }
    Ok(())
}

/// Replace the persisted typed todo snapshot for a Discuss session.
pub(super) async fn update_discuss_session_todo_snapshot(
    conn: &Connection,
    sluice_session_id: &str,
    snapshot_json: &str,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_TODO_SNAPSHOT_UPDATE_SQL,
        (sluice_session_id.to_string(), snapshot_json.to_string()),
    )
    .await?;
    Ok(())
}

/// Merge-safe partial refresh of an existing Discuss session anchor row
/// during `discuss.loadSession` (R4).
///
/// Runs `DISCUSS_SESSIONS_LOAD_REFRESH_SQL`, an atomic partial `UPDATE` that
/// only touches the refreshed columns via `COALESCE(new, existing)` and moves
/// `last_interaction_at` monotonically via `MAX(...)`. A `None` field in
/// `refresh` therefore carries the existing value forward, and a stale refresh
/// can never clobber fresher metadata queued by the worker's per-session
/// persistence queue. No row is inserted: the anchor is guaranteed to exist
/// because `handle_load_session` already loaded it before refreshing.
pub async fn merge_refresh_discuss_session(
    conn: &Connection,
    sluice_session_id: &str,
    refresh: &LoadedSessionRefresh,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_LOAD_REFRESH_SQL,
        (
            sluice_session_id.to_string(),
            refresh.acp_session_id.clone(),
            refresh.display_title.clone(),
            refresh.latest_config_options_json.clone(),
            refresh.last_interaction_at,
        ),
    )
    .await?;
    Ok(())
}

/// List Discuss session anchor rows ordered by most recent interaction
/// (NULLs last).
pub async fn list_recent_discuss_sessions(conn: &Connection) -> Result<Vec<DiscussSessionRow>, DiscussError> {
    let mut rows = conn.query(DISCUSS_SESSIONS_LIST_RECENT_SQL, ()).await?;
    let mut sessions = Vec::new();
    while let Some(row) = rows.next().await? {
        sessions.push(decode_discuss_session_row(&row)?);
    }
    Ok(sessions)
}

/// Load a single Discuss session anchor row by `sluice_session_id`.
///
/// Returns `Ok(None)` when no row matches so callers can branch into
/// `DiscussError::SessionNotFound` at a higher layer.
pub async fn load_discuss_session(
    conn: &Connection,
    sluice_session_id: &str,
) -> Result<Option<DiscussSessionRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_SESSIONS_LOAD_SQL, (sluice_session_id.to_string(),))
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(decode_discuss_session_row(&row)?)),
        None => Ok(None),
    }
}

pub async fn load_discuss_subagents(
    conn: &Connection,
    sluice_session_id: &str,
) -> Result<Vec<DiscussSubagentRecord>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_SUBAGENTS_LOAD_SQL, (sluice_session_id.to_string(),))
        .await?;
    let mut subagents = Vec::new();
    while let Some(row) = rows.next().await? {
        subagents.push(DiscussSubagentRecord {
            capture_id: row_get(&row, 0, "discuss_subagents.capture_id")?,
            child_session_id: row_get_optional_string(&row, 1, "discuss_subagents.child_session_id")?,
            agent_name: row_get(&row, 2, "discuss_subagents.agent_name")?,
            description: row_get_optional_string(&row, 3, "discuss_subagents.description")?,
            fallback_capability: row_get_optional_string(&row, 4, "discuss_subagents.fallback_capability")?,
            surface_state: subagent_surface_state_from_value(&row_get::<String>(
                &row,
                5,
                "discuss_subagents.surface_state",
            )?)?,
            last_updated_at: row_get(&row, 6, "discuss_subagents.last_updated_at")?,
        });
    }
    Ok(subagents)
}

pub async fn load_discuss_agent_config(
    conn: &Connection,
    agent_name: &str,
) -> Result<Option<DiscussAgentConfigRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_AGENT_CONFIG_LOAD_SQL, [agent_name.to_string()])
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(DiscussAgentConfigRow {
        config_options_json: row_get::<Option<String>>(&row, 1, "discuss_agent_config.config_options_json")?,
        default_config_json: row_get::<String>(&row, 2, "discuss_agent_config.default_config_json")?,
    }))
}

pub async fn upsert_discuss_agent_config(
    conn: &Connection,
    agent_name: &str,
    config_options: Option<&[DiscussConfigOption]>,
    defaults: &ConfigSettings,
) -> Result<(), DiscussError> {
    let config_options_json = config_options.map(serde_json::to_string).transpose()?;
    let default_config_json = serde_json::to_string(defaults)?;
    conn.execute(
        DISCUSS_AGENT_CONFIG_UPSERT_SQL,
        (
            agent_name.to_string(),
            config_options_json,
            default_config_json,
            chrono::Utc::now().timestamp_millis(),
        ),
    )
    .await?;
    Ok(())
}

pub async fn list_discuss_team_installations(
    conn: &Connection,
) -> Result<Vec<DiscussTeamInstallationRow>, DiscussError> {
    let mut rows = conn.query(DISCUSS_TEAM_INSTALLATION_LIST_SQL, ()).await?;
    let mut installations = Vec::new();
    while let Some(row) = rows.next().await? {
        installations.push(decode_discuss_team_installation_row(&row)?);
    }
    Ok(installations)
}

pub async fn list_discuss_team_installations_by_provider(
    conn: &Connection,
    provider_name: &str,
) -> Result<Vec<DiscussTeamInstallationRow>, DiscussError> {
    let mut rows = conn
        .query(
            DISCUSS_TEAM_INSTALLATION_LIST_BY_PROVIDER_SQL,
            (provider_name.to_string(),),
        )
        .await?;
    let mut installations = Vec::new();
    while let Some(row) = rows.next().await? {
        installations.push(decode_discuss_team_installation_row(&row)?);
    }
    Ok(installations)
}

pub async fn load_discuss_team_installation(
    conn: &Connection,
    leader_id: &str,
) -> Result<Option<DiscussTeamInstallationRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_TEAM_INSTALLATION_LOAD_SQL, (leader_id.to_string(),))
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(decode_discuss_team_installation_row(&row)?))
}

fn decode_discuss_team_installation_row(row: &turso::Row) -> Result<DiscussTeamInstallationRow, DiscussError> {
    Ok(DiscussTeamInstallationRow {
        leader_id: row_get::<String>(row, 0, "discuss_team_installation.leader_id")?,
        provider_name: row_get::<String>(row, 1, "discuss_team_installation.provider_name")?,
        installed_version: row_get::<String>(row, 2, "discuss_team_installation.installed_version")?,
        member_overrides_json: row_get::<String>(row, 3, "discuss_team_installation.member_overrides_json")?,
        members_json: row_get::<String>(row, 4, "discuss_team_installation.members_json")?,
        theme_color_json: row_get::<Option<String>>(row, 5, "discuss_team_installation.theme_color_json")?,
        _updated_at: row_get::<i64>(row, 6, "discuss_team_installation.updated_at")?,
    })
}

/// Upsert a captured subagent and prune older captures to the configured bound.
pub(super) async fn upsert_and_prune_discuss_subagent(
    conn: &mut Connection,
    sluice_session_id: &str,
    subagent: &DiscussSubagentRecord,
    retention_limit: u32,
) -> Result<(), DiscussError> {
    let transaction = conn.transaction().await?;
    transaction
        .execute(
            DISCUSS_SUBAGENTS_UPSERT_SQL,
            (
                sluice_session_id.to_string(),
                subagent.capture_id.clone(),
                subagent.child_session_id.clone(),
                subagent.agent_name.clone(),
                subagent.description.clone(),
                subagent.fallback_capability.clone(),
                subagent_surface_state_value(subagent.surface_state).to_string(),
                subagent.last_updated_at,
            ),
        )
        .await?;
    transaction
        .execute(
            DISCUSS_SUBAGENTS_PRUNE_DONE_SQL,
            (sluice_session_id.to_string(), i64::from(retention_limit)),
        )
        .await?;
    transaction.commit().await?;
    Ok(())
}

pub async fn upsert_discuss_team_installation(
    conn: &Connection,
    leader_id: &str,
    provider_name: &str,
    installed_version: &str,
    member_overrides_json: &str,
    members_json: &str,
    theme_color_json: Option<&str>,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_TEAM_INSTALLATION_UPSERT_SQL,
        (
            leader_id.to_string(),
            provider_name.to_string(),
            installed_version.to_string(),
            member_overrides_json.to_string(),
            members_json.to_string(),
            theme_color_json.map(ToOwned::to_owned),
            chrono::Utc::now().timestamp_millis(),
        ),
    )
    .await?;
    Ok(())
}

fn subagent_surface_state_value(state: DiscussSubagentSurfaceState) -> &'static str {
    match state {
        DiscussSubagentSurfaceState::Loading => "loading",
        DiscussSubagentSurfaceState::Live => "live",
        DiscussSubagentSurfaceState::Done => "done",
        DiscussSubagentSurfaceState::Error => "error",
    }
}

fn subagent_surface_state_from_value(value: &str) -> Result<DiscussSubagentSurfaceState, DiscussError> {
    match value {
        "loading" => Ok(DiscussSubagentSurfaceState::Loading),
        "live" => Ok(DiscussSubagentSurfaceState::Live),
        "done" => Ok(DiscussSubagentSurfaceState::Done),
        "error" => Ok(DiscussSubagentSurfaceState::Error),
        value => Err(DiscussError::Validation {
            message: format!("unknown Discuss subagent surface state `{value}`"),
        }),
    }
}
pub async fn delete_discuss_team_installation(conn: &Connection, leader_id: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_TEAM_INSTALLATION_DELETE_SQL, (leader_id.to_string(),))
        .await?;
    Ok(())
}

pub async fn delete_discuss_agent_config(conn: &Connection, agent_name: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_AGENT_CONFIG_DELETE_SQL, [agent_name.to_string()])
        .await?;
    Ok(())
}

fn row_get_optional_string(row: &turso::Row, index: usize, label: &str) -> Result<Option<String>, DiscussError> {
    match row_get::<Option<String>>(row, index, label) {
        Ok(value) => Ok(value),
        Err(_) => row_get::<String>(row, index, label).map(Some),
    }
}
/// Delete a local Discuss session anchor by canonical `sluice_session_id`.
///
/// Child `discuss_chat_turns` rows cascade via the FK from migration 0030
/// (`ON DELETE CASCADE`) when foreign_keys are enabled on the connection.
/// Idempotent: deleting a missing row is success.
pub async fn delete_discuss_session(conn: &Connection, sluice_session_id: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_SESSIONS_DELETE_SQL, (sluice_session_id.to_string(),))
        .await?;
    Ok(())
}

/// Update the local user-controlled display title for an existing Discuss
/// session anchor, keyed by `sluice_session_id`.
pub async fn rename_discuss_session(
    conn: &Connection,
    sluice_session_id: SluiceSessionId,
    display_title: &str,
) -> Result<(), DiscussError> {
    let updated = conn
        .execute(
            DISCUSS_SESSIONS_RENAME_SQL,
            (
                sluice_session_id.to_string(),
                display_title.to_string(),
                chrono::Utc::now().timestamp_millis(),
            ),
        )
        .await?;
    if updated == 0 {
        return Err(DiscussError::SessionNotFound { sluice_session_id });
    }
    Ok(())
}

/// Insert one bounded transcript turn keyed by `(sluice_session_id, position)`.
pub async fn insert_discuss_chat_turn(
    conn: &Connection,
    params: DiscussChatTurnInsert<'_>,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_CHAT_TURNS_INSERT_SQL,
        (
            params.sluice_session_id.to_string(),
            params.position,
            params.role.to_string(),
            params.acp_session_id.map(str::to_string),
            params.content_json.to_string(),
            params.created_at,
        ),
    )
    .await?;
    Ok(())
}

/// Prune the bounded transcript window to the latest 10 turns for a given
/// `sluice_session_id`.
pub async fn prune_discuss_chat_turns_window(conn: &Connection, sluice_session_id: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_CHAT_TURNS_PRUNE_WINDOW_SQL, (sluice_session_id.to_string(),))
        .await?;
    Ok(())
}

/// Load the latest 10 transcript turns for a given `sluice_session_id`,
/// ordered by position ASC.
pub async fn load_discuss_chat_turns_window(
    conn: &Connection,
    sluice_session_id: &str,
) -> Result<Vec<DiscussChatTurnRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_CHAT_TURNS_LOAD_WINDOW_SQL, (sluice_session_id.to_string(),))
        .await?;
    let mut turns = Vec::new();
    while let Some(row) = rows.next().await? {
        turns.push(decode_discuss_chat_turn_row(&row)?);
    }
    Ok(turns)
}

// ── Row decoders ──────────────────────────────────────────────────────────

/// Decode a `discuss_sessions` row into `DiscussSessionRow`.
///
/// Column order matches `DISCUSS_SESSIONS_LIST_RECENT_SQL` /
/// `DISCUSS_SESSIONS_LOAD_SQL`:
/// `sluice_session_id, agent_name, session_name, display_title, acp_session_id,
/// last_user_prompt, latest_todo_snapshot_json, latest_config_options_json, config_settings_json,
/// available_commands_json, first_prompt_at, last_interaction_at`.
fn decode_discuss_session_row(row: &turso::Row) -> Result<DiscussSessionRow, DiscussError> {
    Ok(DiscussSessionRow {
        is_task_session: row_get::<i64>(row, 13, "discuss_sessions.is_task_session")? != 0,
        team_leader_id: row_get::<Option<String>>(row, 12, "discuss_sessions.team_leader_id")?,
        sluice_session_id: row_get::<String>(row, 0, "discuss_sessions.sluice_session_id")?,
        agent_name: row_get::<String>(row, 1, "discuss_sessions.agent_name")?,
        session_name: row_get::<String>(row, 2, "discuss_sessions.session_name")?,
        display_title: row_get::<Option<String>>(row, 3, "discuss_sessions.display_title")?,
        acp_session_id: row_get::<Option<String>>(row, 4, "discuss_sessions.acp_session_id")?,
        last_user_prompt: row_get::<Option<String>>(row, 5, "discuss_sessions.last_user_prompt")?,
        latest_todo_snapshot_json: row_get::<Option<String>>(row, 6, "discuss_sessions.latest_todo_snapshot_json")?,
        latest_config_options_json: row_get::<Option<String>>(row, 7, "discuss_sessions.latest_config_options_json")?,
        config_settings_json: row_get::<Option<String>>(row, 8, "discuss_sessions.config_settings_json")?,
        available_commands_json: row_get::<Option<String>>(row, 9, "discuss_sessions.available_commands_json")?,
        first_prompt_at: row_get::<Option<i64>>(row, 10, "discuss_sessions.first_prompt_at")?,
        last_interaction_at: row_get::<Option<i64>>(row, 11, "discuss_sessions.last_interaction_at")?,
    })
}

/// Decode a `discuss_chat_turns` row into `DiscussChatTurnRow`.
///
/// Column order matches `DISCUSS_CHAT_TURNS_LOAD_WINDOW_SQL`:
/// `sluice_session_id, position, role, acp_session_id, content_json,
/// created_at`.
fn decode_discuss_chat_turn_row(row: &turso::Row) -> Result<DiscussChatTurnRow, DiscussError> {
    Ok(DiscussChatTurnRow {
        sluice_session_id: row_get::<String>(row, 0, "discuss_chat_turns.sluice_session_id")?,
        position: row_get::<i64>(row, 1, "discuss_chat_turns.position")?,
        role: row_get::<Option<String>>(row, 2, "discuss_chat_turns.role")?,
        acp_session_id: row_get::<Option<String>>(row, 3, "discuss_chat_turns.acp_session_id")?,
        content_json: row_get::<String>(row, 4, "discuss_chat_turns.content_json")?,
        created_at: row_get::<i64>(row, 5, "discuss_chat_turns.created_at")?,
    })
}

/// Normalize a persisted `acp_session_id` into an owned "present" id.
///
/// A row that has no provider session id yet reads back as `None` (the new
/// schema declares `acp_session_id` NULL, so no empty-string sentinel is
/// written). This helper collapses an empty string to `None` as a defensive
/// guard so provider list/load refresh gates only fire for a real, non-empty
/// provider session id.
pub(super) fn present_acp_session_id(acp_session_id: Option<&str>) -> Option<String> {
    acp_session_id
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

/// Resolve a provider `updatedAt` ISO 8601 timestamp to epoch milliseconds for
/// the `session/list` merge path, using the canonical
/// The RFC 3339 conversion is performed at the call site.
///
/// An absent, empty, or unparseable timestamp falls back to the current instant
/// so presentation merges onto already-local rows still order sensibly — the
/// same behavior the previous local helper's `0` fallback produced in practice
/// once a `MAX(..., now)` guard was applied, but expressed at the call site with
/// typed error handling instead of a bespoke parser.
pub(super) fn provider_updated_at_ms(updated_at: Option<&str>) -> i64 {
    updated_at
        .filter(|iso| !iso.is_empty())
        .and_then(|iso| chrono::DateTime::parse_from_rfc3339(iso).ok())
        .map(|timestamp| timestamp.timestamp_millis())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
}

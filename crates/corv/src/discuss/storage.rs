use std::sync::Arc;

use agent_client_protocol::v1::schema::McpServer;
use turso::{Connection, Database};

use aves::utils::database::{DatabaseError, connect_database};

use super::types::{
    ConfigSettings, DiscussCompletionKind, DiscussConfigOption, DiscussError, DiscussHarnessCostKind,
    DiscussModelUsageEntry, DiscussSessionId, DiscussSubagentRecord, DiscussSubagentSurfaceState, DiscussTurnRecord,
    DiscussUsageEvidenceSource, LaunchLimitation, SessionLaunchConfig,
};

pub(super) const INSERT_IDENTITY_BINDING_SQL: &str = include_str!("queries/insert_identity_binding.sql");
pub(super) const READ_IDENTITY_BINDING_SQL: &str = include_str!("queries/read_identity_binding.sql");

pub const DISCUSS_SESSIONS_UPSERT_SQL: &str = include_str!("queries/discuss_sessions_upsert.sql");
pub const DISCUSS_SESSIONS_UPDATE_SQL: &str = include_str!("queries/discuss_sessions_update.sql");
pub const DISCUSS_SESSIONS_TODO_SNAPSHOT_UPDATE_SQL: &str = include_str!("queries/todo_snapshot_update.sql");
pub const DISCUSS_SESSIONS_LOAD_REFRESH_SQL: &str = include_str!("queries/discuss_sessions_load_refresh.sql");
pub const DISCUSS_SESSIONS_LIST_RECENT_SQL: &str = include_str!("queries/discuss_sessions_list_recent.sql");
pub const DISCUSS_SESSIONS_LOAD_SQL: &str = include_str!("queries/discuss_sessions_load.sql");
const DISCUSS_SESSIONS_SETUP_KIND_SQL: &str = include_str!("queries/discuss_sessions_setup_kind.sql");
const DISCUSS_SESSIONS_LAUNCH_CONTEXT_SQL: &str = include_str!("queries/discuss_sessions_launch_context.sql");
const DISCUSS_SESSIONS_LAUNCH_CONFIG_SQL: &str = include_str!("queries/discuss_sessions_launch_config.sql");
const DISCUSS_SESSIONS_LAUNCH_LIMITATIONS_SQL: &str = include_str!("queries/update_session_launch_limitations.sql");
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
const USAGE_TURNS_INSERT_SQL: &str = include_str!("queries/discuss_usage_turns_insert.sql");
const USAGE_MODEL_ENTRIES_INSERT_SQL: &str = include_str!("queries/discuss_usage_model_entries_insert.sql");
const USAGE_MODEL_ENTRIES_UPSERT_SQL: &str = include_str!("queries/discuss_usage_model_entries_upsert.sql");
const USAGE_TURNS_UPDATE_SQL: &str = include_str!("queries/discuss_usage_turns_update.sql");
const USAGE_RANGE_SELECT_SQL: &str = include_str!("queries/discuss_usage_range_select.sql");
const USAGE_MODEL_ENTRIES_RANGE_SELECT_SQL: &str = include_str!("queries/discuss_usage_model_entries_range_select.sql");
const USAGE_NEXT_ORDINAL_SQL: &str = include_str!("queries/discuss_usage_next_ordinal.sql");
const ALLOWANCE_INSERT_SQL: &str = include_str!("queries/discuss_allowance_insert.sql");
const ALLOWANCE_RANGE_SELECT_SQL: &str = include_str!("queries/discuss_allowance_range_select.sql");
const MODELS_CATALOG_LOAD_SQL: &str = include_str!("queries/discuss_models_catalog_load.sql");
const MODELS_CATALOG_REPLACE_SQL: &str = include_str!("queries/discuss_models_catalog_replace.sql");
const LEDGER_PRUNE_MODEL_ENTRIES_SQL: &str = include_str!("queries/discuss_ledger_prune_model_entries.sql");
const LEDGER_PRUNE_TURNS_SQL: &str = include_str!("queries/discuss_ledger_prune_turns.sql");
const LEDGER_PRUNE_ALLOWANCE_SQL: &str = include_str!("queries/discuss_ledger_prune_allowance.sql");
const CUSTOM_AGENTS_UPSERT_SQL: &str = include_str!("queries/discuss_custom_agents_upsert.sql");
const CUSTOM_AGENTS_LOAD_SQL: &str = include_str!("queries/discuss_custom_agents_load.sql");
const CUSTOM_AGENTS_LIST_SQL: &str = include_str!("queries/discuss_custom_agents_list.sql");
const CUSTOM_AGENTS_DELETE_SQL: &str = include_str!("queries/discuss_custom_agents_delete.sql");
const CUSTOM_AGENTS_PULL_FAILED_SQL: &str = include_str!("queries/discuss_custom_agents_pull_failed.sql");
const CUSTOM_SKILLS_UPSERT_SQL: &str = include_str!("queries/discuss_custom_skills_upsert.sql");
const CUSTOM_SKILLS_LOAD_SQL: &str = include_str!("queries/discuss_custom_skills_load.sql");
const CUSTOM_SKILLS_LIST_SQL: &str = include_str!("queries/discuss_custom_skills_list.sql");
const CUSTOM_SKILLS_DELETE_SQL: &str = include_str!("queries/discuss_custom_skills_delete.sql");
const CUSTOM_SKILLS_PULL_FAILED_SQL: &str = include_str!("queries/discuss_custom_skills_pull_failed.sql");
const CUSTOM_AGENT_INSTALLATIONS_UPSERT_SQL: &str =
    include_str!("queries/discuss_custom_agent_installations_upsert.sql");
const CUSTOM_AGENT_INSTALLATIONS_LOAD_SQL: &str = include_str!("queries/discuss_custom_agent_installations_load.sql");
const CUSTOM_AGENT_INSTALLATIONS_LIST_SQL: &str = include_str!("queries/discuss_custom_agent_installations_list.sql");
const CUSTOM_AGENT_INSTALLATIONS_DELETE_SQL: &str =
    include_str!("queries/discuss_custom_agent_installations_delete.sql");
const CUSTOM_SKILL_INSTALLATIONS_UPSERT_SQL: &str =
    include_str!("queries/discuss_custom_skill_installations_upsert.sql");
const CUSTOM_SKILL_INSTALLATIONS_LOAD_SQL: &str = include_str!("queries/discuss_custom_skill_installations_load.sql");
const CUSTOM_SKILL_INSTALLATIONS_LIST_SQL: &str = include_str!("queries/discuss_custom_skill_installations_list.sql");
const CUSTOM_SKILL_INSTALLATIONS_DELETE_SQL: &str =
    include_str!("queries/discuss_custom_skill_installations_delete.sql");
const CUSTOM_TEAMS_UPSERT_SQL: &str = include_str!("queries/discuss_custom_teams_upsert.sql");
const CUSTOM_TEAMS_LOAD_SQL: &str = include_str!("queries/discuss_custom_teams_load.sql");
const CUSTOM_TEAMS_LIST_SQL: &str = include_str!("queries/discuss_custom_teams_list.sql");
const CUSTOM_TEAMS_DELETE_SQL: &str = include_str!("queries/discuss_custom_teams_delete.sql");
const ASSET_INSTRUCTIONS_UPSERT_SQL: &str = include_str!("queries/discuss_asset_instructions_upsert.sql");
const ASSET_INSTRUCTIONS_LOAD_SQL: &str = include_str!("queries/discuss_asset_instructions_load.sql");
const CUSTOM_MCP_UPSERT_SQL: &str = include_str!("queries/discuss_custom_mcp_upsert.sql");
const CUSTOM_MCP_LOAD_SQL: &str = include_str!("queries/discuss_custom_mcp_load.sql");
const CUSTOM_MCP_LIST_SQL: &str = include_str!("queries/discuss_custom_mcp_list.sql");
const CUSTOM_MCP_DELETE_SQL: &str = include_str!("queries/discuss_custom_mcp_delete.sql");
const MCP_ASSIGNMENT_INSERT_SQL: &str = include_str!("queries/discuss_mcp_assignment_insert.sql");
const MCP_ASSIGNMENT_DELETE_SQL: &str = include_str!("queries/discuss_mcp_assignment_delete.sql");
const MCP_ASSIGNMENT_LIST_BY_SCOPE_SQL: &str = include_str!("queries/discuss_mcp_assignment_list_by_scope.sql");
const MCP_ASSIGNMENT_DELETE_BY_SCOPE_SQL: &str = include_str!("queries/discuss_mcp_assignment_delete_by_scope.sql");
const MCP_ASSIGNMENT_DELETE_BY_NAME_SQL: &str = include_str!("queries/discuss_mcp_assignment_delete_by_name.sql");

/// Stored descriptor; it may contain sensitive environment variables or headers.
pub(super) struct CustomMcpRow {
    pub(super) name: String,
    pub(super) descriptor: McpServer,
    pub(super) updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum McpAssignmentScope {
    Provider,
    Team,
    Session,
}

impl McpAssignmentScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Team => "team",
            Self::Session => "session",
        }
    }
}

fn custom_mcp_error() -> DiscussError {
    DiscussError::Persistence {
        code: "db_query".to_owned(),
        message: "custom MCP storage operation failed".to_owned(),
    }
}

fn decode_custom_mcp(row: &turso::Row) -> Result<CustomMcpRow, DiscussError> {
    let descriptor_json: String = row.get(1).map_err(|_| custom_mcp_error())?;
    Ok(CustomMcpRow {
        name: row.get(0).map_err(|_| custom_mcp_error())?,
        descriptor: serde_json::from_str(&descriptor_json).map_err(|_| custom_mcp_error())?,
        updated_at_ms: row.get(2).map_err(|_| custom_mcp_error())?,
    })
}

pub(super) async fn upsert_custom_mcp(
    conn: &Connection,
    name: &str,
    descriptor: &McpServer,
    updated_at_ms: i64,
) -> Result<(), DiscussError> {
    let descriptor_json = serde_json::to_string(descriptor).map_err(|_| custom_mcp_error())?;
    conn.execute(CUSTOM_MCP_UPSERT_SQL, (name.to_owned(), descriptor_json, updated_at_ms))
        .await
        .map_err(|_| custom_mcp_error())?;
    Ok(())
}

pub(super) async fn load_custom_mcp(conn: &Connection, name: &str) -> Result<Option<CustomMcpRow>, DiscussError> {
    let mut rows = conn
        .query(CUSTOM_MCP_LOAD_SQL, (name.to_owned(),))
        .await
        .map_err(|_| custom_mcp_error())?;
    rows.next()
        .await
        .map_err(|_| custom_mcp_error())?
        .as_ref()
        .map(decode_custom_mcp)
        .transpose()
}

pub(super) async fn list_custom_mcps(conn: &Connection) -> Result<Vec<CustomMcpRow>, DiscussError> {
    let mut rows = conn
        .query(CUSTOM_MCP_LIST_SQL, ())
        .await
        .map_err(|_| custom_mcp_error())?;
    let mut servers = Vec::new();
    while let Some(row) = rows.next().await.map_err(|_| custom_mcp_error())? {
        servers.push(decode_custom_mcp(&row)?);
    }
    Ok(servers)
}

pub(super) async fn delete_custom_mcp(conn: &Connection, name: &str) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_MCP_DELETE_SQL, (name.to_owned(),))
        .await
        .map_err(|_| custom_mcp_error())?;
    Ok(())
}

pub(super) async fn insert_mcp_assignment(
    conn: &Connection,
    scope: McpAssignmentScope,
    scope_id: &str,
    name: &str,
) -> Result<(), DiscussError> {
    conn.execute(
        MCP_ASSIGNMENT_INSERT_SQL,
        (scope.as_str().to_owned(), scope_id.to_owned(), name.to_owned()),
    )
    .await?;
    Ok(())
}

pub(super) async fn delete_mcp_assignment(
    conn: &Connection,
    scope: McpAssignmentScope,
    scope_id: &str,
    name: &str,
) -> Result<(), DiscussError> {
    conn.execute(
        MCP_ASSIGNMENT_DELETE_SQL,
        (scope.as_str().to_owned(), scope_id.to_owned(), name.to_owned()),
    )
    .await?;
    Ok(())
}

pub(super) async fn list_mcp_assignments_by_scope(
    conn: &Connection,
    scope: McpAssignmentScope,
    scope_id: &str,
) -> Result<Vec<String>, DiscussError> {
    let mut rows = conn
        .query(
            MCP_ASSIGNMENT_LIST_BY_SCOPE_SQL,
            (scope.as_str().to_owned(), scope_id.to_owned()),
        )
        .await?;
    let mut names = Vec::new();
    while let Some(row) = rows.next().await? {
        names.push(row_get(&row, 0, "mcp_name")?);
    }
    Ok(names)
}

pub(super) async fn delete_mcp_assignments_by_scope(
    conn: &Connection,
    scope: McpAssignmentScope,
    scope_id: &str,
) -> Result<(), DiscussError> {
    conn.execute(
        MCP_ASSIGNMENT_DELETE_BY_SCOPE_SQL,
        (scope.as_str().to_owned(), scope_id.to_owned()),
    )
    .await?;
    Ok(())
}

pub(super) async fn delete_mcp_assignments_by_name(conn: &Connection, name: &str) -> Result<(), DiscussError> {
    conn.execute(MCP_ASSIGNMENT_DELETE_BY_NAME_SQL, (name.to_owned(),))
        .await?;
    Ok(())
}

/// Row projection for `discuss_sessions` chat anchor rows.
///
/// Carries the full metadata column set so the session dropdown and load path
/// can restore cached state in one round-trip. `discuss_session_id` is the
/// canonical identity; `session_name` and `agent_name` are metadata only.
pub struct DiscussSessionRow {
    pub(super) launch_limitations: Vec<LaunchLimitation>,
    pub(super) is_task_session: bool,
    pub(super) session_cwd_json: Option<String>,
    pub(super) session_mcp_enabled: Option<i64>,
    pub(super) session_launch_config_json: Option<String>,
    pub(super) session_setup_kind: Option<String>,
    pub(super) team_leader_id: Option<String>,
    pub(super) discuss_session_id: String,
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
    pub(super) discuss_session_id: String,
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
/// `discuss_session_id` alone; `session_name` and `agent_name` are metadata.
pub struct DiscussSessionUpsert<'a> {
    pub(super) team_leader_id: Option<&'a str>,
    pub(super) discuss_session_id: &'a str,
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
    pub(super) discuss_session_id: &'a str,
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

/// Upsert a Discuss session anchor row keyed by `discuss_session_id`.
///
/// The new `discuss_sessions` table declares `discuss_session_id TEXT NOT NULL
/// PRIMARY KEY`, so a plain `INSERT ... ON CONFLICT(discuss_session_id) DO
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
        params.discuss_session_id.to_string(),
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
    discuss_session_id: &str,
    snapshot_json: &str,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_TODO_SNAPSHOT_UPDATE_SQL,
        (discuss_session_id.to_string(), snapshot_json.to_string()),
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
    discuss_session_id: &str,
    refresh: &LoadedSessionRefresh,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_LOAD_REFRESH_SQL,
        (
            discuss_session_id.to_string(),
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

/// Load a single Discuss session anchor row by `discuss_session_id`.
///
/// Returns `Ok(None)` when no row matches so callers can branch into
/// `DiscussError::SessionNotFound` at a higher layer.
pub async fn load_discuss_session(
    conn: &Connection,
    discuss_session_id: &str,
) -> Result<Option<DiscussSessionRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_SESSIONS_LOAD_SQL, (discuss_session_id.to_string(),))
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(decode_discuss_session_row(&row)?)),
        None => Ok(None),
    }
}

pub(super) async fn update_session_launch_context(
    conn: &Connection,
    session_id: &str,
    cwd_json: Option<String>,
    mcp_enabled: Option<bool>,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_LAUNCH_CONTEXT_SQL,
        (session_id.to_string(), cwd_json, mcp_enabled.map(i64::from)),
    )
    .await?;
    Ok(())
}

/// Persist the restorable setup kind (for example a Team materialized at the session root).
pub(super) async fn update_session_setup_kind(
    conn: &Connection,
    session_id: &str,
    setup_kind: Option<&str>,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_SESSIONS_SETUP_KIND_SQL,
        (session_id.to_string(), setup_kind.map(str::to_string)),
    )
    .await?;
    Ok(())
}

/// Replace all launch limitations on an existing anchor.
pub(super) async fn update_session_launch_limitations(
    conn: &Connection,
    session_id: &str,
    limitations: &[LaunchLimitation],
) -> Result<(), DiscussError> {
    let json = serde_json::to_string(limitations)?;
    let updated = conn
        .execute(DISCUSS_SESSIONS_LAUNCH_LIMITATIONS_SQL, (session_id.to_string(), json))
        .await?;
    if updated == 0 {
        return Err(DiscussError::Persistence {
            code: "db_query".to_string(),
            message: "session anchor missing while persisting launch limitations".to_string(),
        });
    }
    Ok(())
}

/// Persist the explicitly supplied launch configuration on an existing anchor.
pub(super) async fn update_session_launch_config(
    conn: &Connection,
    session_id: &str,
    launch_config: Option<&SessionLaunchConfig>,
) -> Result<(), DiscussError> {
    let launch_config_json = launch_config.map(serde_json::to_string).transpose()?;
    conn.execute(
        DISCUSS_SESSIONS_LAUNCH_CONFIG_SQL,
        (session_id.to_string(), launch_config_json),
    )
    .await?;
    Ok(())
}

pub async fn load_discuss_subagents(
    conn: &Connection,
    discuss_session_id: &str,
) -> Result<Vec<DiscussSubagentRecord>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_SUBAGENTS_LOAD_SQL, (discuss_session_id.to_string(),))
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
    discuss_session_id: &str,
    subagent: &DiscussSubagentRecord,
    retention_limit: u32,
) -> Result<(), DiscussError> {
    let transaction = conn.transaction().await?;
    transaction
        .execute(
            DISCUSS_SUBAGENTS_UPSERT_SQL,
            (
                discuss_session_id.to_string(),
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
            (discuss_session_id.to_string(), i64::from(retention_limit)),
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
/// Delete a local Discuss session anchor by canonical `discuss_session_id`.
///
/// Child `discuss_chat_turns` rows cascade via the FK from migration 0030
/// (`ON DELETE CASCADE`) when foreign_keys are enabled on the connection.
/// Idempotent: deleting a missing row is success.
pub async fn delete_discuss_session(conn: &Connection, discuss_session_id: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_SESSIONS_DELETE_SQL, (discuss_session_id.to_string(),))
        .await?;
    Ok(())
}

/// Update the local user-controlled display title for an existing Discuss
/// session anchor, keyed by `discuss_session_id`.
pub async fn rename_discuss_session(
    conn: &Connection,
    discuss_session_id: DiscussSessionId,
    display_title: &str,
) -> Result<(), DiscussError> {
    let updated = conn
        .execute(
            DISCUSS_SESSIONS_RENAME_SQL,
            (
                discuss_session_id.to_string(),
                display_title.to_string(),
                chrono::Utc::now().timestamp_millis(),
            ),
        )
        .await?;
    if updated == 0 {
        return Err(DiscussError::SessionNotFound { discuss_session_id });
    }
    Ok(())
}

/// Insert one bounded transcript turn keyed by `(discuss_session_id, position)`.
pub async fn insert_discuss_chat_turn(
    conn: &Connection,
    params: DiscussChatTurnInsert<'_>,
) -> Result<(), DiscussError> {
    conn.execute(
        DISCUSS_CHAT_TURNS_INSERT_SQL,
        (
            params.discuss_session_id.to_string(),
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
/// `discuss_session_id`.
pub async fn prune_discuss_chat_turns_window(conn: &Connection, discuss_session_id: &str) -> Result<(), DiscussError> {
    conn.execute(DISCUSS_CHAT_TURNS_PRUNE_WINDOW_SQL, (discuss_session_id.to_string(),))
        .await?;
    Ok(())
}

/// Load the latest 10 transcript turns for a given `discuss_session_id`,
/// ordered by position ASC.
pub async fn load_discuss_chat_turns_window(
    conn: &Connection,
    discuss_session_id: &str,
) -> Result<Vec<DiscussChatTurnRow>, DiscussError> {
    let mut rows = conn
        .query(DISCUSS_CHAT_TURNS_LOAD_WINDOW_SQL, (discuss_session_id.to_string(),))
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
/// `discuss_session_id, agent_name, session_name, display_title, acp_session_id,
/// last_user_prompt, latest_todo_snapshot_json, latest_config_options_json, config_settings_json,
/// available_commands_json, first_prompt_at, last_interaction_at, team_leader_id,
/// is_task_session, session_cwd_json, session_mcp_enabled, session_launch_config_json,
/// session_setup_kind, launch_limitations_json`.
fn decode_discuss_session_row(row: &turso::Row) -> Result<DiscussSessionRow, DiscussError> {
    let limitations_json = row_get_optional_string(row, 18, "discuss_sessions.launch_limitations_json")?;
    let launch_limitations = limitations_json
        .as_deref()
        .map(|json| {
            serde_json::from_str(json).map_err(|error| DiscussError::Persistence {
                code: "db_query".to_string(),
                message: format!("failed to decode discuss_sessions.launch_limitations_json: {error}"),
            })
        })
        .transpose()?
        .unwrap_or_default();
    Ok(DiscussSessionRow {
        launch_limitations,
        is_task_session: row_get::<i64>(row, 13, "discuss_sessions.is_task_session")? != 0,
        session_cwd_json: row_get(row, 14, "discuss_sessions.session_cwd_json")?,
        session_mcp_enabled: row_get(row, 15, "discuss_sessions.session_mcp_enabled")?,
        session_launch_config_json: row_get(row, 16, "discuss_sessions.session_launch_config_json")?,
        session_setup_kind: row_get(row, 17, "discuss_sessions.session_setup_kind")?,
        team_leader_id: row_get::<Option<String>>(row, 12, "discuss_sessions.team_leader_id")?,
        discuss_session_id: row_get::<String>(row, 0, "discuss_sessions.discuss_session_id")?,
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
/// `discuss_session_id, position, role, acp_session_id, content_json,
/// created_at`.
fn decode_discuss_chat_turn_row(row: &turso::Row) -> Result<DiscussChatTurnRow, DiscussError> {
    Ok(DiscussChatTurnRow {
        discuss_session_id: row_get::<String>(row, 0, "discuss_chat_turns.discuss_session_id")?,
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

/// One sampled allowance observation.
#[derive(Clone, Debug)]
pub(super) struct DiscussAllowanceObservation {
    pub provider: String,
    pub account_key: Option<String>,
    pub fetched_at_ms: Option<i64>,
    pub observed_at_ms: i64,
    pub stale_since_ms: Option<i64>,
    pub error_class: Option<String>,
    pub entry_json: String,
    pub created_at_ms: i64,
}

/// Persisted model catalog and HTTP cache metadata.
#[derive(Clone, Debug)]
pub(super) struct DiscussModelsCatalog {
    pub body: String,
    pub metadata_json: String,
    pub fetched_at_ms: i64,
}

fn ledger_count(value: Option<u64>, field: &'static str) -> Option<i64> {
    value.and_then(|value| match i64::try_from(value) {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(
                field,
                "Discuss accounting count exceeds SQLite integer range; storing NULL"
            );
            None
        }
    })
}

fn decode_ledger_count(row: &turso::Row, index: usize, label: &str) -> Result<Option<u64>, DiscussError> {
    row_get::<Option<i64>>(row, index, label)?
        .map(|value| u64::try_from(value).map_err(|_| DiscussError::UsageValueOutOfRange))
        .transpose()
}

fn completion_value(value: DiscussCompletionKind) -> &'static str {
    match value {
        DiscussCompletionKind::Completed => "completed",
        DiscussCompletionKind::Cancelled => "cancelled",
        DiscussCompletionKind::Failed => "failed",
    }
}

fn cost_kind_value(value: DiscussHarnessCostKind) -> &'static str {
    match value {
        DiscussHarnessCostKind::PerTurn => "per_turn",
        DiscussHarnessCostKind::SessionCumulative => "session_cumulative",
    }
}

fn evidence_value(value: DiscussUsageEvidenceSource) -> &'static str {
    match value {
        DiscussUsageEvidenceSource::Response => "prompt_response",
        DiscussUsageEvidenceSource::ResponseMeta => "prompt_response_meta",
        DiscussUsageEvidenceSource::Complete => "prompt_complete",
    }
}

async fn write_usage_turn(conn: &Connection, sql: &str, turn: &DiscussTurnRecord) -> Result<u64, DiscussError> {
    Ok(conn
        .execute(
            sql,
            turso::params![
                turn.discuss_session_id.clone(),
                turn.turn_ordinal,
                turn.provider_name.clone(),
                turn.model_id.clone(),
                turn.turn_started_at_ms,
                turn.turn_completed_at_ms,
                turn.end_to_end_ms,
                completion_value(turn.completion_kind),
                turn.stop_reason.clone(),
                ledger_count(turn.context_used_tokens, "context_used_tokens"),
                ledger_count(turn.context_window_tokens, "context_window_tokens"),
                turn.harness_cost_amount.clone(),
                turn.harness_cost_currency.clone(),
                turn.harness_cost_kind.map(cost_kind_value),
                i64::from(turn.usage_is_incomplete),
                i64::from(turn.cost_is_partial),
                evidence_value(turn.evidence_source),
                turn.evidence_json.clone(),
                turn.created_at_ms,
            ],
        )
        .await?)
}

async fn write_model_entry(conn: &Connection, sql: &str, entry: &DiscussModelUsageEntry) -> Result<(), DiscussError> {
    conn.execute(
        sql,
        turso::params![
            entry.discuss_session_id.clone(),
            entry.turn_ordinal,
            entry.provider_name.clone(),
            entry.turn_started_at_ms,
            entry.model_id.clone(),
            ledger_count(entry.input_tokens, "input_tokens"),
            ledger_count(entry.output_tokens, "output_tokens"),
            ledger_count(entry.thought_tokens, "thought_tokens"),
            ledger_count(entry.cached_read_tokens, "cached_read_tokens"),
            ledger_count(entry.cached_write_tokens, "cached_write_tokens"),
            ledger_count(entry.total_tokens, "total_tokens"),
            entry.calculated_cost_usd.clone(),
            entry.incomplete_categories,
            i64::from(entry.usage_is_incomplete),
            i64::from(entry.cost_is_partial),
            entry.pricing_basis.clone(),
            entry.price_snapshot_json.clone(),
        ],
    )
    .await?;
    Ok(())
}

fn validate_usage_entries(turn: &DiscussTurnRecord, entries: &[DiscussModelUsageEntry]) -> Result<(), DiscussError> {
    if entries.iter().any(|entry| {
        entry.discuss_session_id != turn.discuss_session_id
            || entry.turn_ordinal != turn.turn_ordinal
            || entry.provider_name != turn.provider_name
            || entry.turn_started_at_ms != turn.turn_started_at_ms
    }) {
        return Err(DiscussError::Validation {
            message: "accounting model entry does not match its turn".to_owned(),
        });
    }
    Ok(())
}

/// Insert one terminal turn and its children atomically, ignoring duplicate turns.
pub(super) async fn insert_usage_turn(
    conn: &mut Connection,
    turn: &DiscussTurnRecord,
    entries: &[DiscussModelUsageEntry],
) -> Result<(), DiscussError> {
    validate_usage_entries(turn, entries)?;
    let transaction = conn.transaction().await?;
    if write_usage_turn(&transaction, USAGE_TURNS_INSERT_SQL, turn).await? != 0 {
        for entry in entries {
            write_model_entry(&transaction, USAGE_MODEL_ENTRIES_INSERT_SQL, entry).await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

/// Update late turn evidence and upsert its model rows atomically.
pub(super) async fn update_turn_usage(
    conn: &mut Connection,
    turn: &DiscussTurnRecord,
    entries: &[DiscussModelUsageEntry],
) -> Result<(), DiscussError> {
    validate_usage_entries(turn, entries)?;
    let transaction = conn.transaction().await?;
    if write_usage_turn(&transaction, USAGE_TURNS_UPDATE_SQL, turn).await? != 0 {
        for entry in entries {
            write_model_entry(&transaction, USAGE_MODEL_ENTRIES_UPSERT_SQL, entry).await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

fn decode_usage_turn(row: &turso::Row) -> Result<DiscussTurnRecord, DiscussError> {
    let decode = |index, label| -> Result<DiscussCompletionKind, DiscussError> {
        row_get::<String>(row, index, label)
            .and_then(|value| Ok(serde_json::from_value(serde_json::Value::String(value))?))
    };
    let kind = row_get::<Option<String>>(row, 13, "harness_cost_kind")?
        .map(|value| serde_json::from_value(serde_json::Value::String(value)))
        .transpose()?;
    Ok(DiscussTurnRecord {
        discuss_session_id: row_get(row, 0, "discuss_session_id")?,
        turn_ordinal: row_get(row, 1, "turn_ordinal")?,
        provider_name: row_get(row, 2, "provider_name")?,
        model_id: row_get(row, 3, "model_id")?,
        turn_started_at_ms: row_get(row, 4, "turn_started_at_ms")?,
        turn_completed_at_ms: row_get(row, 5, "turn_completed_at_ms")?,
        end_to_end_ms: row_get(row, 6, "end_to_end_ms")?,
        completion_kind: decode(7, "completion_kind")?,
        stop_reason: row_get(row, 8, "stop_reason")?,
        context_used_tokens: decode_ledger_count(row, 9, "context_used_tokens")?,
        context_window_tokens: decode_ledger_count(row, 10, "context_window_tokens")?,
        harness_cost_amount: row_get(row, 11, "harness_cost_amount")?,
        harness_cost_currency: row_get(row, 12, "harness_cost_currency")?,
        harness_cost_kind: kind,
        usage_is_incomplete: row_get::<i64>(row, 14, "usage_is_incomplete")? != 0,
        cost_is_partial: row_get::<i64>(row, 15, "cost_is_partial")? != 0,
        evidence_source: serde_json::from_value(serde_json::Value::String(row_get(row, 16, "evidence_source")?))?,
        evidence_json: row_get(row, 17, "evidence_json")?,
        created_at_ms: row_get(row, 18, "created_at_ms")?,
    })
}

fn decode_model_entry(row: &turso::Row) -> Result<DiscussModelUsageEntry, DiscussError> {
    Ok(DiscussModelUsageEntry {
        discuss_session_id: row_get(row, 0, "discuss_session_id")?,
        turn_ordinal: row_get(row, 1, "turn_ordinal")?,
        provider_name: row_get(row, 2, "provider_name")?,
        turn_started_at_ms: row_get(row, 3, "turn_started_at_ms")?,
        model_id: row_get(row, 4, "model_id")?,
        input_tokens: decode_ledger_count(row, 5, "input_tokens")?,
        output_tokens: decode_ledger_count(row, 6, "output_tokens")?,
        thought_tokens: decode_ledger_count(row, 7, "thought_tokens")?,
        cached_read_tokens: decode_ledger_count(row, 8, "cached_read_tokens")?,
        cached_write_tokens: decode_ledger_count(row, 9, "cached_write_tokens")?,
        total_tokens: decode_ledger_count(row, 10, "total_tokens")?,
        calculated_cost_usd: row_get(row, 11, "calculated_cost_usd")?,
        incomplete_categories: row_get(row, 12, "incomplete_categories")?,
        usage_is_incomplete: row_get::<i64>(row, 13, "usage_is_incomplete")? != 0,
        cost_is_partial: row_get::<i64>(row, 14, "cost_is_partial")? != 0,
        pricing_basis: row_get(row, 15, "pricing_basis")?,
        price_snapshot_json: row_get(row, 16, "price_snapshot_json")?,
    })
}

/// Load turn and model observations in an inclusive UTC millisecond range.
pub(super) async fn select_usage_range(
    conn: &Connection,
    from_ms: i64,
    to_ms: i64,
) -> Result<(Vec<DiscussTurnRecord>, Vec<DiscussModelUsageEntry>), DiscussError> {
    let mut rows = conn.query(USAGE_RANGE_SELECT_SQL, (from_ms, to_ms)).await?;
    let mut turns = Vec::new();
    while let Some(row) = rows.next().await? {
        turns.push(decode_usage_turn(&row)?);
    }
    let mut rows = conn
        .query(USAGE_MODEL_ENTRIES_RANGE_SELECT_SQL, (from_ms, to_ms))
        .await?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        entries.push(decode_model_entry(&row)?);
    }
    Ok((turns, entries))
}

/// Resolve the next surviving ledger ordinal for a session.
pub(super) async fn next_usage_ordinal(conn: &Connection, session_id: &str) -> Result<i64, DiscussError> {
    let mut rows = conn
        .query(USAGE_NEXT_ORDINAL_SQL, (session_id.to_owned(),))
        .await?;
    match rows.next().await? {
        Some(row) => row_get(&row, 0, "next turn ordinal"),
        None => Err(DiscussError::Persistence {
            code: "db_query".to_owned(),
            message: "ordinal query returned no row".to_owned(),
        }),
    }
}

/// Insert an allowance snapshot without changing session state.
pub(super) async fn insert_allowance_observation(
    conn: &Connection,
    value: &DiscussAllowanceObservation,
) -> Result<(), DiscussError> {
    conn.execute(
        ALLOWANCE_INSERT_SQL,
        (
            value.provider.clone(),
            value.account_key.clone(),
            value.fetched_at_ms,
            value.observed_at_ms,
            value.stale_since_ms,
            value.error_class.clone(),
            value.entry_json.clone(),
            value.created_at_ms,
        ),
    )
    .await?;
    Ok(())
}

/// Read allowance snapshots in an inclusive UTC millisecond range.
pub(super) async fn select_allowance_range(
    conn: &Connection,
    from_ms: i64,
    to_ms: i64,
) -> Result<Vec<DiscussAllowanceObservation>, DiscussError> {
    let mut rows = conn
        .query(ALLOWANCE_RANGE_SELECT_SQL, (from_ms, to_ms))
        .await?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().await? {
        values.push(DiscussAllowanceObservation {
            provider: row_get(&row, 0, "provider")?,
            account_key: row_get(&row, 1, "account_key")?,
            fetched_at_ms: row_get(&row, 2, "fetched_at_ms")?,
            observed_at_ms: row_get(&row, 3, "observed_at_ms")?,
            stale_since_ms: row_get(&row, 4, "stale_since_ms")?,
            error_class: row_get(&row, 5, "error_class")?,
            entry_json: row_get(&row, 6, "entry_json")?,
            created_at_ms: row_get(&row, 7, "created_at_ms")?,
        });
    }
    Ok(values)
}

/// Load the last successfully persisted models catalog, if any.
pub(super) async fn load_models_catalog(conn: &Connection) -> Result<Option<DiscussModelsCatalog>, DiscussError> {
    let mut rows = conn.query(MODELS_CATALOG_LOAD_SQL, ()).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(DiscussModelsCatalog {
            body: row_get(&row, 0, "body")?,
            metadata_json: row_get(&row, 1, "metadata_json")?,
            fetched_at_ms: row_get(&row, 2, "fetched_at_ms")?,
        })),
        None => Ok(None),
    }
}

/// Replace the persisted singleton models catalog.
pub(super) async fn replace_models_catalog(
    conn: &Connection,
    value: &DiscussModelsCatalog,
) -> Result<(), DiscussError> {
    conn.execute(
        MODELS_CATALOG_REPLACE_SQL,
        (value.body.clone(), value.metadata_json.clone(), value.fetched_at_ms),
    )
    .await?;
    Ok(())
}

/// Prune all ledger families at the same cutoff in one transaction.
pub(super) async fn prune_accounting_ledger(conn: &mut Connection, cutoff_ms: i64) -> Result<(), DiscussError> {
    let transaction = conn.transaction().await?;
    transaction
        .execute(LEDGER_PRUNE_MODEL_ENTRIES_SQL, (cutoff_ms,))
        .await?;
    transaction
        .execute(LEDGER_PRUNE_TURNS_SQL, (cutoff_ms,))
        .await?;
    transaction
        .execute(LEDGER_PRUNE_ALLOWANCE_SQL, (cutoff_ms,))
        .await?;
    transaction.commit().await?;
    Ok(())
}

/// Origin of a custom asset's last successful content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CustomSourceKind {
    LocalMarkdown,
    UrlMarkdown,
    GithubFile,
    GithubDirectory,
}

impl CustomSourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LocalMarkdown => "local_markdown",
            Self::UrlMarkdown => "url_markdown",
            Self::GithubFile => "github_file",
            Self::GithubDirectory => "github_directory",
        }
    }
    fn parse(value: &str) -> Result<Self, DiscussError> {
        match value {
            "local_markdown" => Ok(Self::LocalMarkdown),
            "url_markdown" => Ok(Self::UrlMarkdown),
            "github_file" => Ok(Self::GithubFile),
            "github_directory" => Ok(Self::GithubDirectory),
            _ => Err(DiscussError::Persistence {
                code: "db_query".into(),
                message: format!("invalid custom source kind: {value}"),
            }),
        }
    }
}

/// Custom asset metadata and source provenance.
#[derive(Clone, Debug)]
pub(super) struct CustomAssetRow {
    pub id: String,
    pub name: String,
    pub job_title: Option<String>,
    pub description: String,
    pub version: String,
    pub source_kind: CustomSourceKind,
    pub source_url: Option<String>,
    pub last_pull_at_ms: Option<i64>,
    pub last_pull_ok: Option<bool>,
    pub pull_error: Option<String>,
    pub updated_at_ms: i64,
}

/// Persisted custom team metadata.
#[derive(Clone, Debug)]
pub(super) struct CustomTeamRow {
    pub leader_id: String,
    pub display_name: Option<String>,
    pub followers_json: String,
    pub skills_json: String,
    pub version: String,
    pub updated_at_ms: i64,
}

/// Installed version for one provider and custom asset.
#[derive(Clone, Debug)]
pub(super) struct CustomInstallationRow {
    pub provider_name: String,
    pub asset_id: String,
    pub installed_version: String,
}

/// Instructions associated with a physical workspace path.
#[derive(Clone, Debug)]
pub(super) struct AssetInstructionsRow {
    pub asset_path: String,
    pub instructions: String,
    pub updated_at_ms: i64,
}

/// Persist an agent row after its cache swap succeeds.
pub(super) async fn upsert_custom_agent(conn: &Connection, row: &CustomAssetRow) -> Result<(), DiscussError> {
    let title = row
        .job_title
        .as_ref()
        .ok_or_else(|| DiscussError::Validation {
            message: "agent job title is missing".into(),
        })?;
    conn.execute(
        CUSTOM_AGENTS_UPSERT_SQL,
        turso::params![
            row.id.clone(),
            row.name.clone(),
            title.clone(),
            row.description.clone(),
            row.version.clone(),
            row.source_kind.as_str(),
            row.source_url.clone(),
            row.last_pull_at_ms,
            row.last_pull_ok.map(i64::from),
            row.pull_error.clone(),
            row.updated_at_ms
        ],
    )
    .await?;
    Ok(())
}

/// Persist a skill row after its cache swap succeeds.
pub(super) async fn upsert_custom_skill(conn: &Connection, row: &CustomAssetRow) -> Result<(), DiscussError> {
    conn.execute(
        CUSTOM_SKILLS_UPSERT_SQL,
        turso::params![
            row.id.clone(),
            row.name.clone(),
            row.description.clone(),
            row.version.clone(),
            row.source_kind.as_str(),
            row.source_url.clone(),
            row.last_pull_at_ms,
            row.last_pull_ok.map(i64::from),
            row.pull_error.clone(),
            row.updated_at_ms
        ],
    )
    .await?;
    Ok(())
}

fn decode_custom_asset(row: &turso::Row, agent: bool) -> Result<CustomAssetRow, DiscussError> {
    let offset = usize::from(agent);
    let kind: String = row_get(row, 4 + offset, "source_kind")?;
    let pull_ok: Option<i64> = row_get(row, 7 + offset, "last_pull_ok")?;
    Ok(CustomAssetRow {
        id: row_get(row, 0, "id")?,
        name: row_get(row, 1, "name")?,
        job_title: if agent {
            Some(row_get(row, 2, "job_title")?)
        } else {
            None
        },
        description: row_get(row, 2 + offset, "description")?,
        version: row_get(row, 3 + offset, "version")?,
        source_kind: CustomSourceKind::parse(&kind)?,
        source_url: row_get(row, 5 + offset, "source_url")?,
        last_pull_at_ms: row_get(row, 6 + offset, "last_pull_at_ms")?,
        last_pull_ok: pull_ok.map(|v| v != 0),
        pull_error: row_get(row, 8 + offset, "pull_error")?,
        updated_at_ms: row_get(row, 9 + offset, "updated_at_ms")?,
    })
}

async fn load_custom_asset(
    conn: &Connection,
    sql: &str,
    id: &str,
    agent: bool,
) -> Result<Option<CustomAssetRow>, DiscussError> {
    let mut rows = conn.query(sql, (id.to_owned(),)).await?;
    rows.next()
        .await?
        .map(|row| decode_custom_asset(&row, agent))
        .transpose()
}

async fn list_custom_assets(conn: &Connection, sql: &str, agent: bool) -> Result<Vec<CustomAssetRow>, DiscussError> {
    let mut rows = conn.query(sql, ()).await?;
    let mut assets = Vec::new();
    while let Some(row) = rows.next().await? {
        assets.push(decode_custom_asset(&row, agent)?);
    }
    Ok(assets)
}

/// List cached agent metadata and provenance in identity order.
pub(super) async fn list_custom_agents(conn: &Connection) -> Result<Vec<CustomAssetRow>, DiscussError> {
    list_custom_assets(conn, CUSTOM_AGENTS_LIST_SQL, true).await
}

/// List cached skill metadata and provenance in identity order.
pub(super) async fn list_custom_skills(conn: &Connection) -> Result<Vec<CustomAssetRow>, DiscussError> {
    list_custom_assets(conn, CUSTOM_SKILLS_LIST_SQL, false).await
}

/// Load one agent's metadata.
pub(super) async fn load_custom_agent(conn: &Connection, id: &str) -> Result<Option<CustomAssetRow>, DiscussError> {
    load_custom_asset(conn, CUSTOM_AGENTS_LOAD_SQL, id, true).await
}
/// Load one skill's metadata.
pub(super) async fn load_custom_skill(conn: &Connection, id: &str) -> Result<Option<CustomAssetRow>, DiscussError> {
    load_custom_asset(conn, CUSTOM_SKILLS_LOAD_SQL, id, false).await
}
/// Delete one agent's metadata.
pub(super) async fn delete_custom_agent(conn: &Connection, id: &str) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_AGENTS_DELETE_SQL, (id.to_owned(),))
        .await?;
    Ok(())
}
/// Delete one skill's metadata.
pub(super) async fn delete_custom_skill(conn: &Connection, id: &str) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_SKILLS_DELETE_SQL, (id.to_owned(),))
        .await?;
    Ok(())
}
/// Record a failed agent pull without changing last successful pull time.
pub(super) async fn fail_custom_agent_pull(
    conn: &Connection,
    id: &str,
    error: &str,
    now: i64,
) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_AGENTS_PULL_FAILED_SQL, (id.to_owned(), error.to_owned(), now))
        .await?;
    Ok(())
}
/// Record a failed skill pull without changing last successful pull time.
pub(super) async fn fail_custom_skill_pull(
    conn: &Connection,
    id: &str,
    error: &str,
    now: i64,
) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_SKILLS_PULL_FAILED_SQL, (id.to_owned(), error.to_owned(), now))
        .await?;
    Ok(())
}

/// Persist custom team metadata.
pub(super) async fn upsert_custom_team(conn: &Connection, row: &CustomTeamRow) -> Result<(), DiscussError> {
    conn.execute(
        CUSTOM_TEAMS_UPSERT_SQL,
        (
            row.leader_id.clone(),
            row.display_name.clone(),
            row.followers_json.clone(),
            row.skills_json.clone(),
            row.version.clone(),
            row.updated_at_ms,
        ),
    )
    .await?;
    Ok(())
}
/// Load custom team metadata.
pub(super) async fn load_custom_team(conn: &Connection, id: &str) -> Result<Option<CustomTeamRow>, DiscussError> {
    let mut rows = conn.query(CUSTOM_TEAMS_LOAD_SQL, (id.to_owned(),)).await?;
    let Some(row) = rows.next().await? else { return Ok(None) };
    Ok(Some(decode_custom_team(&row)?))
}

/// List custom teams in leader identity order.
pub(super) async fn list_custom_teams(conn: &Connection) -> Result<Vec<CustomTeamRow>, DiscussError> {
    let mut rows = conn.query(CUSTOM_TEAMS_LIST_SQL, ()).await?;
    let mut teams = Vec::new();
    while let Some(row) = rows.next().await? {
        teams.push(decode_custom_team(&row)?);
    }
    Ok(teams)
}

fn decode_custom_team(row: &turso::Row) -> Result<CustomTeamRow, DiscussError> {
    Ok(CustomTeamRow {
        leader_id: row_get(row, 0, "leader_id")?,
        display_name: row_get(row, 1, "display_name")?,
        followers_json: row_get(row, 2, "followers_json")?,
        skills_json: row_get(row, 3, "skills_json")?,
        version: row_get(row, 4, "version")?,
        updated_at_ms: row_get(row, 5, "updated_at_ms")?,
    })
}
/// Delete custom team metadata.
pub(super) async fn delete_custom_team(conn: &Connection, id: &str) -> Result<(), DiscussError> {
    conn.execute(CUSTOM_TEAMS_DELETE_SQL, (id.to_owned(),))
        .await?;
    Ok(())
}

/// Persist instructions for one physical path.
pub(super) async fn upsert_asset_instructions(
    conn: &Connection,
    row: &AssetInstructionsRow,
) -> Result<(), DiscussError> {
    conn.execute(
        ASSET_INSTRUCTIONS_UPSERT_SQL,
        (row.asset_path.clone(), row.instructions.clone(), row.updated_at_ms),
    )
    .await?;
    Ok(())
}
/// Load instructions for one physical path.
pub(super) async fn load_asset_instructions(
    conn: &Connection,
    path: &str,
) -> Result<Option<AssetInstructionsRow>, DiscussError> {
    let mut rows = conn
        .query(ASSET_INSTRUCTIONS_LOAD_SQL, (path.to_owned(),))
        .await?;
    let Some(row) = rows.next().await? else { return Ok(None) };
    Ok(Some(AssetInstructionsRow {
        asset_path: row_get(&row, 0, "asset_path")?,
        instructions: row_get(&row, 1, "instructions")?,
        updated_at_ms: row_get(&row, 2, "updated_at_ms")?,
    }))
}

/// Selects the appropriate installation table without using dynamic SQL.
#[derive(Clone, Copy)]
pub(super) enum CustomInstallationKind {
    Agent,
    Skill,
}
impl CustomInstallationKind {
    fn statements(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Agent => (
                CUSTOM_AGENT_INSTALLATIONS_UPSERT_SQL,
                CUSTOM_AGENT_INSTALLATIONS_LOAD_SQL,
                CUSTOM_AGENT_INSTALLATIONS_DELETE_SQL,
            ),
            Self::Skill => (
                CUSTOM_SKILL_INSTALLATIONS_UPSERT_SQL,
                CUSTOM_SKILL_INSTALLATIONS_LOAD_SQL,
                CUSTOM_SKILL_INSTALLATIONS_DELETE_SQL,
            ),
        }
    }
}
/// Persist a provider's installed asset version.
pub(super) async fn upsert_custom_installation(
    conn: &Connection,
    kind: CustomInstallationKind,
    row: &CustomInstallationRow,
) -> Result<(), DiscussError> {
    conn.execute(
        kind.statements().0,
        (
            row.provider_name.clone(),
            row.asset_id.clone(),
            row.installed_version.clone(),
        ),
    )
    .await?;
    Ok(())
}
/// Load a provider's installed asset version.
pub(super) async fn load_custom_installation(
    conn: &Connection,
    kind: CustomInstallationKind,
    provider: &str,
    id: &str,
) -> Result<Option<CustomInstallationRow>, DiscussError> {
    let mut rows = conn
        .query(kind.statements().1, (provider.to_owned(), id.to_owned()))
        .await?;
    let Some(row) = rows.next().await? else { return Ok(None) };
    Ok(Some(CustomInstallationRow {
        provider_name: row_get(&row, 0, "provider_name")?,
        asset_id: row_get(&row, 1, "asset_id")?,
        installed_version: row_get(&row, 2, "installed_version")?,
    }))
}
/// Delete a provider's installed asset version.
pub(super) async fn delete_custom_installation(
    conn: &Connection,
    kind: CustomInstallationKind,
    provider: &str,
    id: &str,
) -> Result<(), DiscussError> {
    conn.execute(kind.statements().2, (provider.to_owned(), id.to_owned()))
        .await?;
    Ok(())
}

/// List installation records across all providers, including removed providers.
pub(super) async fn list_custom_installations(
    conn: &Connection,
    kind: CustomInstallationKind,
) -> Result<Vec<CustomInstallationRow>, DiscussError> {
    let sql = match kind {
        CustomInstallationKind::Agent => CUSTOM_AGENT_INSTALLATIONS_LIST_SQL,
        CustomInstallationKind::Skill => CUSTOM_SKILL_INSTALLATIONS_LIST_SQL,
    };
    let mut rows = conn.query(sql, ()).await?;
    let mut installations = Vec::new();
    while let Some(row) = rows.next().await? {
        installations.push(CustomInstallationRow {
            provider_name: row_get(&row, 0, "provider_name")?,
            asset_id: row_get(&row, 1, "asset_id")?,
            installed_version: row_get(&row, 2, "installed_version")?,
        });
    }
    Ok(installations)
}

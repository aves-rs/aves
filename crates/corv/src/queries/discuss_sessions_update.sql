-- Existing anchor metadata update keyed by `sluice_session_id`.
-- Caller binds final values: NULL explicitly clears nullable metadata fields.
-- ACP session ID is never cleared; NULL/empty input preserves the stored ID.
-- Task origin is sticky: once true, `is_task_session` remains true.
UPDATE discuss_sessions
SET
    agent_name                  = ?2,
    session_name                = ?3,
    display_title               = ?4,
    acp_session_id              = COALESCE(NULLIF(?5, ''), acp_session_id),
    last_user_prompt            = ?6,
    latest_todo_snapshot_json   = ?7,
    latest_config_options_json  = ?8,
    config_settings_json        = ?9,
    available_commands_json     = ?10,
    first_prompt_at             = ?11,
    last_interaction_at          = ?12,
    team_leader_id               = ?13,
    is_task_session              = is_task_session OR ?14
WHERE sluice_session_id = ?1

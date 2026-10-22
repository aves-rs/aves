-- Upsert a Discuss session anchor keyed by `sluice_session_id`; names are metadata.
-- Nullable metadata accepts NULL. Callers bind final values; ACP identity is retained
-- when absent, and task-session origin remains sticky.
INSERT INTO discuss_sessions (
    sluice_session_id,
    agent_name,
    session_name,
    display_title,
    acp_session_id,
    last_user_prompt,
    latest_todo_snapshot_json,
    latest_config_options_json,
    config_settings_json,
    available_commands_json,
    first_prompt_at,
    last_interaction_at,
    team_leader_id,
    is_task_session
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
ON CONFLICT(sluice_session_id) DO UPDATE SET
    agent_name                  = excluded.agent_name,
    session_name                = excluded.session_name,
    display_title               = excluded.display_title,
    acp_session_id              = COALESCE(NULLIF(excluded.acp_session_id, ''), discuss_sessions.acp_session_id),
    last_user_prompt            = excluded.last_user_prompt,
    latest_todo_snapshot_json   = excluded.latest_todo_snapshot_json,
    latest_config_options_json  = excluded.latest_config_options_json,
    config_settings_json        = excluded.config_settings_json,
    available_commands_json     = excluded.available_commands_json,
    first_prompt_at             = excluded.first_prompt_at,
    last_interaction_at         = excluded.last_interaction_at,
    team_leader_id              = excluded.team_leader_id,
    is_task_session             = discuss_sessions.is_task_session OR excluded.is_task_session

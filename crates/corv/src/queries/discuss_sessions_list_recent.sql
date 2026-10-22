-- List Discuss session anchor rows ordered by most recent interaction
-- (NULLs last), for the recent-sessions dropdown.
SELECT
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
FROM discuss_sessions
ORDER BY last_interaction_at DESC NULLS LAST

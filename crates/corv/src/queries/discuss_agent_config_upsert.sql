INSERT INTO discuss_agent_config (
    agent_name,
    config_options_json,
    default_config_json,
    updated_at
)
VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(agent_name) DO UPDATE SET
    config_options_json = COALESCE(excluded.config_options_json, discuss_agent_config.config_options_json),
    default_config_json = excluded.default_config_json,
    updated_at = excluded.updated_at

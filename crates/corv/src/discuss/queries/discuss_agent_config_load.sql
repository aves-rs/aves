SELECT
    agent_name,
    config_options_json,
    default_config_json,
    updated_at
FROM discuss_agent_config
WHERE agent_name = ?1

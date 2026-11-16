INSERT INTO discuss_custom_agent_installations VALUES (?1, ?2, ?3) ON CONFLICT(provider_name, agent_id) DO UPDATE SET installed_version=excluded.installed_version;

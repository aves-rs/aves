INSERT INTO discuss_custom_skill_installations VALUES (?1, ?2, ?3) ON CONFLICT(provider_name, skill_id) DO UPDATE SET installed_version=excluded.installed_version;

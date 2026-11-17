INSERT INTO discuss_custom_mcp_servers (name, descriptor_json, updated_at_ms)
VALUES (?1, ?2, ?3)
ON CONFLICT (name) DO UPDATE SET descriptor_json = excluded.descriptor_json, updated_at_ms = excluded.updated_at_ms;

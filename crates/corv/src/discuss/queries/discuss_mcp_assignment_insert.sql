INSERT INTO discuss_mcp_assignments (scope_kind, scope_id, mcp_name) VALUES (?1, ?2, ?3) ON CONFLICT (scope_kind, scope_id, mcp_name) DO NOTHING;

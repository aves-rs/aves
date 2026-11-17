CREATE TABLE discuss_custom_mcp_servers (
    name TEXT PRIMARY KEY,
    descriptor_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE discuss_mcp_assignments (
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('provider', 'team', 'session')),
    scope_id TEXT NOT NULL,
    mcp_name TEXT NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, mcp_name)
);

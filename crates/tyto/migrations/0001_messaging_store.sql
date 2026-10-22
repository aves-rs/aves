CREATE TABLE messaging_connections (
    id TEXT PRIMARY KEY NOT NULL,
    display_name TEXT NOT NULL,
    provider_type TEXT NOT NULL CHECK (provider_type IN ('Email', 'Slack', 'Telegram')),
    credential_id TEXT NOT NULL,
    transport_settings TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    retention_seconds INTEGER NOT NULL
);

CREATE TABLE permission_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    connection_id TEXT NOT NULL,
    provider_message_id TEXT,
    destination TEXT NOT NULL,
    request_text TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    flow_execution_id TEXT NOT NULL,
    flow_node_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'pending', 'approved', 'denied', 'expired', 'cancelled', 'deleted', 'failed')),
    provider_receipt TEXT,
    decided_at INTEGER,
    outstanding_edit INTEGER NOT NULL DEFAULT 0 CHECK (outstanding_edit IN (0, 1))
);
CREATE INDEX permission_requests_unfinished ON permission_requests(status, expires_at);
CREATE INDEX permission_requests_connection ON permission_requests(connection_id, status);

CREATE TABLE inbound_messages (
    id INTEGER PRIMARY KEY,
    raw_payload BLOB NOT NULL,
    normalized_body TEXT,
    normalized_metadata TEXT,
    connection_id TEXT NOT NULL,
    channel_id TEXT,
    received_at INTEGER NOT NULL,
    retention_seconds INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE permission_callbacks (
    id INTEGER PRIMARY KEY,
    raw_payload TEXT NOT NULL,
    request_id TEXT,
    action TEXT CHECK (action IS NULL OR action IN ('approve', 'deny', 'unknown')),
    decided_at INTEGER,
    connection_id TEXT,
    received_at INTEGER NOT NULL,
    retention_seconds INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE accepted_sends (
    id INTEGER PRIMARY KEY,
    normalized_body TEXT NOT NULL,
    normalized_metadata TEXT,
    provider_receipt TEXT NOT NULL,
    raw_payload TEXT CHECK (raw_payload IS NULL),
    connection_id TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    retention_seconds INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

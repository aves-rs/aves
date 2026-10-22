CREATE TABLE IF NOT EXISTS flow_mobility_state (
    flow_id TEXT PRIMARY KEY NOT NULL REFERENCES flows(flow_id) ON DELETE CASCADE,
    authority_state TEXT NOT NULL CHECK (authority_state IN ('authoritative', 'synchronizing', 'inactive_committed', 'yielded', 'active', 'cleanup_pending')),
    destination_peer_json TEXT,
    destination_commit_ack_json TEXT,
    activation_ack_json TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flow_mobility_snapshots (
    flow_id TEXT PRIMARY KEY NOT NULL REFERENCES flows(flow_id) ON DELETE CASCADE,
    snapshot_json TEXT NOT NULL,
    snapshot_generation INTEGER NOT NULL DEFAULT 1,
    captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

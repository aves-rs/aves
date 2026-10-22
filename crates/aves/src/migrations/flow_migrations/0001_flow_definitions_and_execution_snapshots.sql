-- Flow definitions are immutable revisions. The catalog is the mutable pointer
-- used to decide whether a new execution may start.
CREATE TABLE IF NOT EXISTS flows (
    flow_id TEXT PRIMARY KEY NOT NULL,
    active_revision INTEGER,
    deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS flow_definition_revisions (
    flow_id TEXT NOT NULL REFERENCES flows(flow_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    name TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    blackboard_defaults_json TEXT NOT NULL,
    blackboard_bindings_json TEXT NOT NULL,
    editor_metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (flow_id, revision)
);

CREATE TABLE IF NOT EXISTS flow_nodes (
    flow_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    parent_node_id TEXT,
    node_kind TEXT NOT NULL,
    label TEXT NOT NULL,
    constraints_json TEXT NOT NULL,
    data_space_json TEXT NOT NULL,
    configuration_json TEXT NOT NULL,
    PRIMARY KEY (flow_id, revision, node_id),
    FOREIGN KEY (flow_id, revision)
        REFERENCES flow_definition_revisions(flow_id, revision)
);

CREATE TABLE IF NOT EXISTS flow_triggers (
    trigger_id TEXT PRIMARY KEY NOT NULL,
    flow_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    adapter_kind TEXT NOT NULL,
    adapter_config_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    next_fire_at TEXT,
    last_fired_at TEXT,
    FOREIGN KEY (flow_id, revision)
        REFERENCES flow_definition_revisions(flow_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_flow_triggers_flow_revision
    ON flow_triggers (flow_id, revision);

CREATE TABLE IF NOT EXISTS flow_executions (
    execution_id TEXT PRIMARY KEY NOT NULL,
    flow_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'succeeded', 'failed', 'aborted')),
    blackboard_json TEXT NOT NULL,
    workspace_roots_json TEXT NOT NULL,
    reconstruction_state_json TEXT NOT NULL,
    scratchpad_json TEXT NOT NULL,
    observer_state_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    error_json TEXT,
    FOREIGN KEY (flow_id, revision)
        REFERENCES flow_definition_revisions(flow_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_flow_executions_status
    ON flow_executions (status);

CREATE TABLE IF NOT EXISTS flow_node_executions (
    execution_id TEXT NOT NULL REFERENCES flow_executions(execution_id) ON DELETE CASCADE,
    node_execution_id TEXT NOT NULL,
    flow_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ready', 'running', 'succeeded', 'failed', 'aborted')),
    state_json TEXT NOT NULL,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    error_json TEXT,
    PRIMARY KEY (execution_id, node_execution_id),
    FOREIGN KEY (flow_id, revision, node_id)
        REFERENCES flow_nodes(flow_id, revision, node_id)
);

CREATE INDEX IF NOT EXISTS idx_flow_node_executions_execution
    ON flow_node_executions (execution_id);

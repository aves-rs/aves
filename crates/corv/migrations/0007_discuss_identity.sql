CREATE TABLE discuss_identity_bindings (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    discuss_session_id TEXT NOT NULL,
    agent_provider TEXT,
    agent_name TEXT,
    node_execution_id TEXT,
    created_at INTEGER NOT NULL,
    ambiguous INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (issuer, subject),
    FOREIGN KEY (discuss_session_id)
        REFERENCES discuss_sessions(discuss_session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flow_execution_documents (
    execution_id TEXT PRIMARY KEY NOT NULL REFERENCES flow_executions(execution_id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

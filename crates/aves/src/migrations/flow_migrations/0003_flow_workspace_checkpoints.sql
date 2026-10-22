ALTER TABLE flow_executions ADD COLUMN workspace_checkpoint_json TEXT;
ALTER TABLE flow_executions ADD COLUMN workspace_entries_json TEXT NOT NULL DEFAULT '{}';

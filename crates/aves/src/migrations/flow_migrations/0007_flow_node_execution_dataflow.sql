ALTER TABLE flow_node_executions ADD COLUMN inputs_json TEXT NOT NULL DEFAULT '{"values":{}}';
ALTER TABLE flow_node_executions ADD COLUMN outputs_json TEXT NOT NULL DEFAULT '{"values":{}}';

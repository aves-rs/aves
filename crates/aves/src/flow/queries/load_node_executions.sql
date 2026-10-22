SELECT node_id, node_execution_id, status, state_json, inputs_json, outputs_json, started_at, updated_at, completed_at, error_json FROM flow_node_executions WHERE execution_id=?1 ORDER BY node_id

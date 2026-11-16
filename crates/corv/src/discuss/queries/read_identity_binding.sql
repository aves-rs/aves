SELECT discuss_session_id, agent_provider, agent_name, node_execution_id, ambiguous
FROM discuss_identity_bindings WHERE issuer = ? AND subject = ?;

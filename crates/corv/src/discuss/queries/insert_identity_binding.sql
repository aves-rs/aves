INSERT INTO discuss_identity_bindings
    (issuer, subject, discuss_session_id, agent_provider, agent_name, node_execution_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (issuer, subject) DO UPDATE SET
    ambiguous = CASE WHEN discuss_identity_bindings.discuss_session_id <> excluded.discuss_session_id
                     THEN 1 ELSE discuss_identity_bindings.ambiguous END;

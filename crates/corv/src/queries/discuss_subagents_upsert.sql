INSERT INTO discuss_subagents (
    sluice_session_id,
    capture_id,
    child_session_id,
    agent_name,
    description,
    fallback_capability,
    surface_state,
    last_updated_at
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT (sluice_session_id, capture_id) DO UPDATE SET
    child_session_id = COALESCE(excluded.child_session_id, discuss_subagents.child_session_id),
    agent_name = excluded.agent_name,
    description = COALESCE(excluded.description, discuss_subagents.description),
    fallback_capability = COALESCE(excluded.fallback_capability, discuss_subagents.fallback_capability),
    surface_state = CASE
        WHEN discuss_subagents.surface_state = 'error'
            OR (
                discuss_subagents.surface_state = 'done'
                AND excluded.surface_state IN ('loading', 'live')
            )
        THEN discuss_subagents.surface_state
        ELSE excluded.surface_state
    END,
    last_updated_at = CASE
        WHEN discuss_subagents.surface_state = 'error'
            OR (
                discuss_subagents.surface_state = 'done'
                AND excluded.surface_state IN ('loading', 'live')
            )
        THEN discuss_subagents.last_updated_at
        ELSE excluded.last_updated_at
    END

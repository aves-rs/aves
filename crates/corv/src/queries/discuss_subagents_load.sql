SELECT
    capture_id,
    child_session_id,
    agent_name,
    description,
    fallback_capability,
    surface_state,
    last_updated_at
FROM discuss_subagents
WHERE sluice_session_id = ?1
ORDER BY last_updated_at ASC, capture_id ASC

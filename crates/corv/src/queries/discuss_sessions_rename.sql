-- Rename a Discuss session's display title and advance its last interaction time.
UPDATE discuss_sessions
SET
    display_title      = ?2,
    last_interaction_at = MAX(COALESCE(last_interaction_at, 0), ?3)
WHERE sluice_session_id = ?1

-- Keep the ten greatest positions for this session; positions are unique per
-- session, and empty turns still occupy a position.
DELETE FROM discuss_chat_turns
WHERE sluice_session_id = ?1
  AND position NOT IN (
      SELECT position
      FROM discuss_chat_turns
      WHERE sluice_session_id = ?1
      ORDER BY position DESC
      LIMIT 10
  )

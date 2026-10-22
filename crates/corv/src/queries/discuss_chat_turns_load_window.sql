SELECT
    sluice_session_id,
    position,
    role,
    acp_session_id,
    content_json,
    created_at
FROM discuss_chat_turns
WHERE sluice_session_id = ?1
  AND position IN (
      SELECT position
      FROM discuss_chat_turns
      WHERE sluice_session_id = ?1
      ORDER BY position DESC
      LIMIT 10
  )
ORDER BY position ASC

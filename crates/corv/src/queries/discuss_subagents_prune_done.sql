DELETE FROM discuss_subagents
WHERE sluice_session_id = ?1
  AND capture_id NOT IN (
      SELECT capture_id
      FROM discuss_subagents
      WHERE sluice_session_id = ?1
      ORDER BY last_updated_at DESC, capture_id DESC
      LIMIT ?2
  )

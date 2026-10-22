SELECT key, updated_at FROM knowledge_data
WHERE (?1 IS NULL OR updated_at >= ?1)
  AND (?2 IS NULL OR updated_at <= ?2)
ORDER BY updated_at DESC, key COLLATE BINARY ASC
LIMIT ?3 OFFSET ?4;

INSERT INTO knowledge_data (key, value_json, updated_at)
VALUES (?1, ?2, ?3)
ON CONFLICT(key) DO UPDATE SET
    value_json = excluded.value_json,
    updated_at = excluded.updated_at;

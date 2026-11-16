INSERT INTO discuss_models_catalog (id, body, metadata_json, fetched_at_ms) VALUES (1, ?1, ?2, ?3)
ON CONFLICT(id) DO UPDATE SET body = excluded.body, metadata_json = excluded.metadata_json, fetched_at_ms = excluded.fetched_at_ms;

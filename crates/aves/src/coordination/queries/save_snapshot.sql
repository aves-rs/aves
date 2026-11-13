INSERT INTO coordination_snapshot (singleton, state_json) VALUES (1, ?1)
ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json;

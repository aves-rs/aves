INSERT INTO flow_mobility_snapshots (flow_id, snapshot_json, snapshot_generation)
VALUES (?1, ?2, ?3)
ON CONFLICT(flow_id) DO UPDATE SET
    snapshot_json = excluded.snapshot_json,
    snapshot_generation = excluded.snapshot_generation,
    captured_at = CURRENT_TIMESTAMP;

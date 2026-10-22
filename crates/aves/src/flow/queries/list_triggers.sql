SELECT trigger_id, flow_id, revision, adapter_kind, adapter_config_json, state_json, enabled, next_fire_at, last_fired_at FROM flow_triggers WHERE flow_id = ?1 ORDER BY trigger_id

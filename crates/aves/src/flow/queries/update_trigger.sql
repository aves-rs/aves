UPDATE flow_triggers SET flow_id=?2, revision=?3, adapter_kind=?4, adapter_config_json=?5, state_json=?6, enabled=?7, next_fire_at=?8, last_fired_at=?9 WHERE trigger_id=?1

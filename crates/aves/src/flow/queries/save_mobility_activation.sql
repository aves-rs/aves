UPDATE flow_mobility_state
SET activation_ack_json = ?2, updated_at = CURRENT_TIMESTAMP
WHERE flow_id = ?1

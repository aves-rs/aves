UPDATE flow_mobility_state
SET destination_peer_json = ?2, destination_commit_ack_json = ?3, updated_at = CURRENT_TIMESTAMP
WHERE flow_id = ?1

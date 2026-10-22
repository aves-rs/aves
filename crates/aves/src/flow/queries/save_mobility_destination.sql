UPDATE flow_mobility_state
SET destination_peer_json = ?2, destination_commit_ack_json = NULL, activation_ack_json = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE flow_id = ?1

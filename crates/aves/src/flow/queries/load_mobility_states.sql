SELECT flow_id, authority_state, destination_peer_json, destination_commit_ack_json, activation_ack_json
FROM flow_mobility_state
ORDER BY flow_id

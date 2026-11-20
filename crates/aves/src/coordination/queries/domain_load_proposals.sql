SELECT height, value_id, payload_json FROM coordination_domain_proposals WHERE domain = ?1 ORDER BY height, value_id;

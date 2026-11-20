INSERT INTO coordination_domain_pending SELECT ?1, sequence, event_json, body_json FROM coordination_pending;

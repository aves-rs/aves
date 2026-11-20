INSERT INTO coordination_domain_snapshot (domain, state_json) VALUES (?1, ?2) ON CONFLICT(domain) DO UPDATE SET state_json = excluded.state_json;

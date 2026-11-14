INSERT INTO coordination_retired (domain, recovery_json) VALUES (?1, ?2)
ON CONFLICT(domain) DO NOTHING;

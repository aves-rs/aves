SELECT height, value_bytes, certificate_json FROM coordination_domain_decided WHERE domain = ?1 ORDER BY height;

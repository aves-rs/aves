CREATE TABLE coordination_domain_snapshot (
    domain BLOB PRIMARY KEY,
    state_json TEXT NOT NULL
);

CREATE TABLE coordination_domain_pending (
    domain BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    body_json TEXT NOT NULL,
    PRIMARY KEY (domain, sequence)
);

CREATE TABLE coordination_domain_decided (
    domain BLOB NOT NULL,
    height BLOB NOT NULL,
    value_bytes BLOB NOT NULL,
    certificate_json TEXT NOT NULL,
    PRIMARY KEY (domain, height)
);

CREATE TABLE coordination_domain_proposals (
    domain BLOB NOT NULL,
    height BLOB NOT NULL,
    value_id BLOB NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (domain, height, value_id)
);

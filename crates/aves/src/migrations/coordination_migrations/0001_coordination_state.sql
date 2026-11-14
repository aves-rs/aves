CREATE TABLE coordination_snapshot (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state_json TEXT NOT NULL
);

CREATE TABLE coordination_pending (
    sequence INTEGER PRIMARY KEY,
    event_json TEXT NOT NULL,
    body_json TEXT NOT NULL
);

CREATE TABLE coordination_decided (
    height BLOB PRIMARY KEY,
    value_bytes BLOB NOT NULL,
    certificate_json TEXT NOT NULL
);

CREATE TABLE coordination_proposals (
    height BLOB NOT NULL,
    value_id BLOB NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (height, value_id)
);

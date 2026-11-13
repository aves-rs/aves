CREATE TABLE coordination_retired (
    domain BLOB PRIMARY KEY CHECK (length(domain) = 32),
    recovery_json TEXT NOT NULL
);

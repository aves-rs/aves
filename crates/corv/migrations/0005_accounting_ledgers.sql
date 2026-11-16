CREATE TABLE discuss_usage_turns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discuss_session_id TEXT NOT NULL,
    turn_ordinal INTEGER NOT NULL,
    provider_name TEXT NOT NULL,
    model_id TEXT,
    turn_started_at_ms INTEGER NOT NULL,
    turn_completed_at_ms INTEGER,
    end_to_end_ms INTEGER,
    completion_kind TEXT NOT NULL CHECK (completion_kind IN ('completed', 'cancelled', 'failed')),
    stop_reason TEXT,
    context_used_tokens INTEGER,
    context_window_tokens INTEGER,
    harness_cost_amount TEXT,
    harness_cost_currency TEXT,
    harness_cost_kind TEXT CHECK (harness_cost_kind IN ('per_turn', 'session_cumulative')),
    usage_is_incomplete INTEGER NOT NULL DEFAULT 0,
    cost_is_partial INTEGER NOT NULL DEFAULT 0,
    evidence_source TEXT NOT NULL,
    evidence_json TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (discuss_session_id, turn_ordinal)
);
CREATE INDEX discuss_usage_turns_provider_started ON discuss_usage_turns (provider_name, turn_started_at_ms);
CREATE INDEX discuss_usage_turns_started ON discuss_usage_turns (turn_started_at_ms);

CREATE TABLE discuss_usage_model_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discuss_session_id TEXT NOT NULL,
    turn_ordinal INTEGER NOT NULL,
    provider_name TEXT NOT NULL,
    turn_started_at_ms INTEGER NOT NULL,
    model_id TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    thought_tokens INTEGER,
    cached_read_tokens INTEGER,
    cached_write_tokens INTEGER,
    total_tokens INTEGER,
    calculated_cost_usd TEXT,
    incomplete_categories INTEGER NOT NULL DEFAULT 0,
    usage_is_incomplete INTEGER NOT NULL DEFAULT 0,
    cost_is_partial INTEGER NOT NULL DEFAULT 0,
    pricing_basis TEXT,
    price_snapshot_json TEXT
);
CREATE UNIQUE INDEX discuss_usage_model_entries_turn_model ON discuss_usage_model_entries (discuss_session_id, turn_ordinal, IFNULL(model_id, ''));
CREATE INDEX discuss_usage_model_entries_provider_model_started ON discuss_usage_model_entries (provider_name, model_id, turn_started_at_ms);
CREATE INDEX discuss_usage_model_entries_turn ON discuss_usage_model_entries (discuss_session_id, turn_ordinal);

CREATE TABLE discuss_allowance_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    account_key TEXT,
    fetched_at_ms INTEGER,
    observed_at_ms INTEGER NOT NULL,
    stale_since_ms INTEGER,
    error_class TEXT,
    entry_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX discuss_allowance_observations_provider_observed ON discuss_allowance_observations (provider, observed_at_ms);
CREATE INDEX discuss_allowance_observations_provider_fetched ON discuss_allowance_observations (provider, fetched_at_ms);

CREATE TABLE discuss_models_catalog (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    body TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    fetched_at_ms INTEGER NOT NULL
);

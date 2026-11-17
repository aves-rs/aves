INSERT INTO discuss_usage_model_entries (discuss_session_id, turn_ordinal, provider_name, turn_started_at_ms, model_id, input_tokens, output_tokens, thought_tokens, cached_read_tokens, cached_write_tokens, total_tokens, calculated_cost_usd, incomplete_categories, usage_is_incomplete, cost_is_partial, pricing_basis, price_snapshot_json)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
ON CONFLICT(discuss_session_id, turn_ordinal, IFNULL(model_id,'')) DO NOTHING;

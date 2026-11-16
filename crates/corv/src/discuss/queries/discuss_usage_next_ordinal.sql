SELECT COALESCE(MAX(turn_ordinal) + 1, 0) FROM discuss_usage_turns WHERE discuss_session_id = ?1;

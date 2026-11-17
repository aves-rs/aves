UPDATE discuss_custom_skills SET last_pull_ok = 0, pull_error = ?2, updated_at_ms = ?3 WHERE skill_id = ?1;

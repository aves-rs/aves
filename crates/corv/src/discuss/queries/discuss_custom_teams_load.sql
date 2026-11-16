SELECT leader_id, display_name, followers_json, skills_json, version, updated_at_ms FROM discuss_custom_teams WHERE leader_id = ?1;

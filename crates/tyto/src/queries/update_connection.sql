UPDATE messaging_connections SET display_name = ?2, provider_type = ?3, credential_id = ?4, transport_settings = ?5, retention_seconds = ?6 WHERE id = ?1;

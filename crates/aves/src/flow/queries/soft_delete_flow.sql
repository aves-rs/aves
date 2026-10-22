UPDATE flows SET deleted_at = CURRENT_TIMESTAMP WHERE flow_id = ?1 AND active_revision = ?2 AND deleted_at IS NULL

UPDATE flows SET active_revision = ?2 WHERE flow_id = ?1 AND active_revision = ?3 AND deleted_at IS NULL

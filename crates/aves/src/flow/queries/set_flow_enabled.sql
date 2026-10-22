UPDATE flow_definition_revisions SET automatic_enabled = ?3
WHERE flow_id = ?1 AND revision = ?2
AND EXISTS (SELECT 1 FROM flows WHERE flow_id = ?1 AND active_revision = ?2 AND deleted_at IS NULL)

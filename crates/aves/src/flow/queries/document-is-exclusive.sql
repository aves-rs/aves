SELECT 1 FROM flow_execution_documents WHERE namespace_id = ?1 AND author_id = ?2 AND execution_id <> ?3 LIMIT 1

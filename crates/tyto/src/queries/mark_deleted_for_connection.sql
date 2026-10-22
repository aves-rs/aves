UPDATE permission_requests SET status = 'deleted', decided_at = ?2, outstanding_edit = 0 WHERE connection_id = ?1 AND status IN ('prepared', 'pending');

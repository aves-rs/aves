UPDATE permission_requests SET status = ?2, decided_at = ?3, outstanding_edit = ?4 WHERE request_id = ?1 AND status IN ('prepared', 'pending');

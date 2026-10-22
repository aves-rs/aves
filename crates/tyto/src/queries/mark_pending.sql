UPDATE permission_requests SET provider_message_id = ?2, provider_receipt = ?3, status = 'pending' WHERE request_id = ?1 AND status = 'prepared';

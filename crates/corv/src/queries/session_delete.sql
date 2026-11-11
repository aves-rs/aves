-- Child rows cascade through the parent-child foreign key.
DELETE FROM discuss_sessions
WHERE discuss_session_id = ?1

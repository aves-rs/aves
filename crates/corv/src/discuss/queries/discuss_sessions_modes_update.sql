UPDATE discuss_sessions
SET session_modes_json = ?2
WHERE discuss_session_id = ?1

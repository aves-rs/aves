UPDATE discuss_sessions
SET session_launch_config_json = ?2
WHERE discuss_session_id = ?1

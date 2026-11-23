-- Load the last session modes a worker reported; NULL when none were recorded.
SELECT session_modes_json
FROM discuss_sessions
WHERE discuss_session_id = ?1

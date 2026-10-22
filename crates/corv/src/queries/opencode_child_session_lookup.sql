SELECT json_extract(data, '$.state.metadata.sessionId') AS session_id
FROM part
WHERE session_id = {{parent_session_id}}
  AND json_extract(data, '$.tool') = 'task'
  AND json_extract(data, '$.callID') = {{capture_id}}
  AND json_extract(data, '$.state.metadata.sessionId') IS NOT NULL
LIMIT 1

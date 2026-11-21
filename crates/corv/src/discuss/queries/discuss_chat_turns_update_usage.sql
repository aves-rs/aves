UPDATE discuss_chat_turns
SET content_json = json_set(content_json, '$.usage', json(?3))
WHERE discuss_session_id = ?1 AND position = ?2 AND role = 'assistant'

-- Parent deletion cascades; role values are constrained by the table CHECK.
INSERT INTO discuss_chat_turns (
    sluice_session_id,
    position,
    role,
    acp_session_id,
    content_json,
    created_at
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)

UPDATE discuss_sessions
SET session_cwd_json = COALESCE(?2, session_cwd_json),
    session_mcp_enabled = COALESCE(?3, session_mcp_enabled),
    requires_mcp_isolation = COALESCE(?4, requires_mcp_isolation)
WHERE discuss_session_id = ?1

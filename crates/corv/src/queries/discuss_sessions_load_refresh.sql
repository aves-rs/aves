-- Merge-safe refresh of an existing session anchor. Update only refreshed fields:
-- concurrent writes retain untouched metadata; nullable deltas carry forward and
-- last_interaction_at remains monotonic. No row is inserted.
UPDATE discuss_sessions
SET
    acp_session_id              = COALESCE(?2, acp_session_id),
    display_title               = COALESCE(?3, display_title),
    latest_config_options_json  = COALESCE(?4, latest_config_options_json),
    last_interaction_at         = MAX(?5, COALESCE(last_interaction_at, ?5))
WHERE sluice_session_id = ?1

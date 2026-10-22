INSERT INTO discuss_team_installation (
    leader_id,
    provider_name,
    installed_version,
    member_overrides_json,
    members_json,
    theme_color_json,
    updated_at
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT(leader_id) DO UPDATE SET
    provider_name = excluded.provider_name,
    installed_version = excluded.installed_version,
    member_overrides_json = excluded.member_overrides_json,
    members_json = excluded.members_json,
    theme_color_json = excluded.theme_color_json,
    updated_at = excluded.updated_at

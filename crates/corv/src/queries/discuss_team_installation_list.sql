SELECT
    leader_id,
    provider_name,
    installed_version,
    member_overrides_json,
    members_json,
    theme_color_json,
    updated_at
FROM discuss_team_installation
ORDER BY leader_id

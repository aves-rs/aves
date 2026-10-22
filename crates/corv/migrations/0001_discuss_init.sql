CREATE TABLE discuss_sessions (
    sluice_session_id           TEXT    NOT NULL PRIMARY KEY,
    agent_name                  TEXT    NOT NULL,
    session_name                TEXT    NOT NULL,
    display_title               TEXT    NULL,
    acp_session_id              TEXT    NULL,
    last_user_prompt            TEXT    NULL,
    latest_config_options_json  TEXT    NULL,
    available_commands_json     TEXT    NULL,
    first_prompt_at             INTEGER NULL,
    last_interaction_at         INTEGER NULL,
    config_settings_json        TEXT    NULL,
    latest_todo_snapshot_json   TEXT    NULL,
    team_leader_id              TEXT    NULL,
    is_task_session             INTEGER NOT NULL DEFAULT 0 CHECK (is_task_session IN (0, 1))
);

CREATE INDEX idx_discuss_sessions_interaction
    ON discuss_sessions (last_interaction_at DESC);

CREATE TABLE discuss_chat_turns (
    sluice_session_id TEXT NOT NULL,
    position          INTEGER NOT NULL,
    role              TEXT CHECK (role IN ('user', 'assistant', 'permission', 'notification', 'system', 'elicitation')),
    acp_session_id    TEXT NULL,
    content_json      TEXT NOT NULL,
    created_at        INTEGER NOT NULL,
    PRIMARY KEY (sluice_session_id, position),
    FOREIGN KEY (sluice_session_id)
        REFERENCES discuss_sessions(sluice_session_id) ON DELETE CASCADE
);

CREATE TABLE discuss_agent_config (
    agent_name          TEXT    NOT NULL PRIMARY KEY,
    config_options_json TEXT    NULL,
    default_config_json TEXT    NOT NULL DEFAULT '{}',
    updated_at          INTEGER NOT NULL
);

CREATE TABLE discuss_team_installation (
    leader_id             TEXT    NOT NULL PRIMARY KEY,
    provider_name         TEXT    NOT NULL,
    installed_version     TEXT    NOT NULL,
    member_overrides_json TEXT    NOT NULL DEFAULT '{}',
    members_json          TEXT    NOT NULL DEFAULT '[]',
    updated_at            INTEGER NOT NULL,
    theme_color_json      TEXT    NULL
);

CREATE TABLE discuss_subagents (
    sluice_session_id    TEXT    NOT NULL,
    capture_id           TEXT    NOT NULL,
    child_session_id     TEXT    NULL,
    agent_name           TEXT    NOT NULL,
    description          TEXT    NULL,
    fallback_capability  TEXT    NULL,
    surface_state        TEXT    NOT NULL CHECK (surface_state IN ('loading', 'live', 'done', 'error')),
    last_updated_at      INTEGER NOT NULL,
    PRIMARY KEY (sluice_session_id, capture_id),
    FOREIGN KEY (sluice_session_id)
        REFERENCES discuss_sessions(sluice_session_id) ON DELETE CASCADE
);

CREATE INDEX idx_discuss_subagents_retention
    ON discuss_subagents (sluice_session_id, surface_state, last_updated_at DESC, capture_id);

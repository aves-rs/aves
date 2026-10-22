use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Stdio,
};

use agent_client_protocol::schema::v1::{McpServer, SessionId};
use chrono::Utc;
use serde::Serialize;
use tokio::{
    process::Command,
    sync::mpsc,
    time::{Duration, sleep, timeout},
};

use super::super::library::{
    AgentDefinition, AgentId, AgentRole, AssetLibrary, SkillDefinition, SkillId, TeamDefinition,
};
use super::super::types::{
    DiscussError, DiscussSubagentRecord, DiscussSubagentSurfaceState, DiscussTodoItem, DiscussTodoSnapshot,
    DiscussTodoStatus, DiscussUtilityPayload,
};
use super::{
    ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, PluginCaptureState, render_skill_file, resolve_agent,
    resolve_skill,
};

pub const OPENCODE_PLUGIN_NAME: &str = "opencode";

const AGENTS_DIRECTORY: &str = ".opencode/agents";
const CONFIG_PATH: &str = ".opencode/opencode.json";
const CHILD_SESSION_LOOKUP_ATTEMPTS: usize = 3;
const CHILD_SESSION_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

pub static PLUGIN: OpenCodePlugin = OpenCodePlugin;

/// OpenCode Discuss integration.
pub struct OpenCodePlugin;

#[derive(Default)]
pub struct OpenCodeCaptureState {
    subagents: BTreeMap<String, OpenCodeSubagentCapture>,
    db_lookup: Option<OpenCodeDbLookup>,
}

struct OpenCodeSubagentCapture {
    agent_name: String,
    description: Option<String>,
    child_session_id: Option<String>,
    lookup_started: bool,
}

#[derive(Clone)]
struct OpenCodeDbLookup {
    command: PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
    parent_session_id: String,
    resolved_subagent_tx: mpsc::Sender<DiscussSubagentRecord>,
}

impl OpenCodeCaptureState {
    pub fn configure_db_lookup(
        &mut self,
        launch_server: &McpServer,
        parent_session_id: &SessionId,
        resolved_subagent_tx: mpsc::Sender<DiscussSubagentRecord>,
    ) {
        self.db_lookup = build_db_lookup(launch_server, parent_session_id, resolved_subagent_tx);
    }
}

#[derive(Serialize)]
struct RenderedFile {
    path: PathBuf,
    contents: String,
}

#[derive(Serialize)]
struct AgentFrontmatter<'a> {
    name: &'a str,
    mode: &'static str,
    description: &'a str,
    #[serde(flatten)]
    settings: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct EnabledAgent {
    disable: bool,
}

#[derive(Serialize)]
struct OpenCodeBuiltinAgents {
    build: EnabledAgent,
    plan: EnabledAgent,
    explore: EnabledAgent,
    general: EnabledAgent,
}

#[derive(Serialize)]
struct OpenCodeConfig {
    #[serde(rename = "$schema")]
    schema: &'static str,
    agent: OpenCodeBuiltinAgents,
}

impl DiscussAgentPlugin for OpenCodePlugin {
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Shared
    }

    // Source-verified OpenCode contracts: `todowrite` accepts `{ todos }`, and
    // `task` accepts `{ description, prompt, subagent_type, task_id? }`.
    // OpenCode emits task lifecycle statuses through ACP `status` updates:
    // `pending`, `in_progress`, `completed`, and `failed`.
    fn capture_tool_call(&self, payload: &serde_json::Value, agent_scope_key: &str) -> Option<DiscussUtilityPayload> {
        if agent_scope_key != self.agent_scope_key() {
            return None;
        }
        classify_tool_payload(payload)
    }

    fn supports_child_session_load(&self) -> bool {
        // OpenCode advertises ACP loadSession and loads the supplied provider
        // session id through its session service.
        true
    }

    fn capture_tool_call_update(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
    ) -> Option<DiscussUtilityPayload> {
        if agent_scope_key != self.agent_scope_key() {
            return None;
        }
        classify_tool_payload(payload)
    }

    fn capture_tool_call_with_state(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        if agent_scope_key != self.agent_scope_key() {
            return None;
        }
        let PluginCaptureState::OpenCode(state) = state else {
            return None;
        };
        capture_subagent_update(payload, state)
    }

    fn capture_tool_call_update_with_state(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        self.capture_tool_call_with_state(payload, agent_scope_key, state)
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.opencode"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.opencode"
    }

    /// Materialize this Provider's shared configuration in the Agent workspace.
    fn install_config(&self, root: &Path) -> Result<(), DiscussError> {
        let config = render_config()?;
        write_file(root, Path::new(CONFIG_PATH), &config)
    }

    /// Remove this Provider's shared configuration from the Agent workspace.
    fn remove_config(&self, root: &Path) -> Result<(), DiscussError> {
        let path = root.join(CONFIG_PATH);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(capture_write_error(&path, error)),
        }
    }

    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        let file = render_agent(agent, None)?;
        write_file(root, &file.path, &file.contents)
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        remove_file(
            root,
            &Path::new(AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination())),
        )
    }

    fn install_team_with_member_overrides(
        &self,
        library: AssetLibrary,
        root: &Path,
        team: &TeamDefinition,
        member_settings: &BTreeMap<AgentId, ConfigSettings>,
    ) -> Result<(), DiscussError> {
        install_team_members(library, self, root, team, member_settings)?;
        for skill_id in &team.skills {
            let skill = resolve_skill(library, skill_id)?;
            self.install_skill(root, skill)?;
        }
        Ok(())
    }

    fn install_team_member(
        &self,
        library: AssetLibrary,
        root: &Path,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        let file = render_agent(agent, member_settings)?;
        write_file(root, &file.path, &file.contents)
    }

    fn destroy_team(
        &self,
        library: AssetLibrary,
        root: &Path,
        team: &TeamDefinition,
        retain_agents: &BTreeSet<AgentId>,
        retain_skills: &BTreeSet<SkillId>,
    ) -> Result<(), DiscussError> {
        if !retain_agents.contains(&team.leader) {
            self.destroy_agent(root, &team.leader)?;
        }
        for follower in &team.followers {
            if !retain_agents.contains(follower) {
                self.destroy_agent(root, follower)?;
            }
        }
        for skill_id in &team.skills {
            if !retain_skills.contains(skill_id) {
                let skill = resolve_skill(library, skill_id)?;
                self.destroy_skill(root, skill)?;
            }
        }
        Ok(())
    }

    fn install_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(".opencode/skills")
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(OPENCODE_PLUGIN_NAME, &relative_path, skill, file)?;
            write_file(root, &relative_path, &contents)?;
        }
        Ok(())
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(".opencode/skills")
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            remove_file(root, &relative_path)?;
        }
        Ok(())
    }
}

fn classify_tool_payload(payload: &serde_json::Value) -> Option<DiscussUtilityPayload> {
    let update = payload
        .get("sessionUpdate")
        .and_then(serde_json::Value::as_str)?;
    if !matches!(update, "tool_call" | "tool_call_update") {
        return None;
    }
    if payload.get("title").and_then(serde_json::Value::as_str) == Some("todowrite") {
        return parse_todo_snapshot(payload);
    }
    if payload.get("kind").and_then(serde_json::Value::as_str) == Some("think") {
        return parse_subagent(payload);
    }
    None
}

fn parse_todo_snapshot(update: &serde_json::Value) -> Option<DiscussUtilityPayload> {
    let todos = update
        .get("rawInput")
        .and_then(|input| input.get("todos"))
        .or_else(|| {
            update
                .get("rawOutput")
                .and_then(|output| output.get("metadata"))
                .and_then(|metadata| metadata.get("todos"))
        })?
        .as_array()?;
    let items = todos
        .iter()
        .map(|todo| {
            Some(DiscussTodoItem {
                content: todo.get("content")?.as_str()?.to_string(),
                status: parse_todo_status(todo.get("status")?.as_str()?)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(DiscussUtilityPayload::TodoSnapshot {
        snapshot: DiscussTodoSnapshot { items },
    })
}

fn parse_todo_status(status: &str) -> Option<DiscussTodoStatus> {
    match status {
        "pending" => Some(DiscussTodoStatus::Pending),
        "in_progress" => Some(DiscussTodoStatus::InProgress),
        "completed" => Some(DiscussTodoStatus::Completed),
        "cancelled" => Some(DiscussTodoStatus::Cancelled),
        _ => None,
    }
}

fn parse_subagent(update: &serde_json::Value) -> Option<DiscussUtilityPayload> {
    let input = update.get("rawInput")?;
    let metadata = update.get("rawOutput")?.get("metadata")?;
    let child_session_id = metadata.get("sessionId")?.as_str()?;
    let agent_name = input.get("subagent_type")?.as_str()?;
    let surface_state = match update.get("status").and_then(serde_json::Value::as_str) {
        // OpenCode's ACP adapter maps its pending/running/completed/error task
        // states to ACP pending/in_progress/completed/failed statuses.
        Some("pending") | None => DiscussSubagentSurfaceState::Loading,
        Some("in_progress") | Some("running") => DiscussSubagentSurfaceState::Live,
        Some("completed") => DiscussSubagentSurfaceState::Done,
        Some("failed") => DiscussSubagentSurfaceState::Error,
        Some(_) => return None,
    };
    if child_session_id.is_empty() || agent_name.is_empty() {
        return None;
    }
    let description = input
        .get("description")
        .and_then(serde_json::Value::as_str)
        .filter(|description| !description.is_empty())
        .map(str::to_string)
        .or_else(|| Some(format!("{agent_name} subagent")));
    Some(DiscussUtilityPayload::Subagent {
        subagent: DiscussSubagentRecord {
            capture_id: update
                .get("toolCallId")
                .and_then(serde_json::Value::as_str)
                .filter(|capture_id| !capture_id.is_empty())?
                .to_string(),
            child_session_id: Some(child_session_id.to_string()),
            agent_name: agent_name.to_string(),
            description,
            fallback_capability: None,
            surface_state,
            last_updated_at: Utc::now().timestamp_millis(),
        },
    })
}

fn capture_subagent_update(
    update: &serde_json::Value,
    state: &mut OpenCodeCaptureState,
) -> Option<DiscussUtilityPayload> {
    state.capture_update(update)
}

impl OpenCodeCaptureState {
    fn capture_update(&mut self, update: &serde_json::Value) -> Option<DiscussUtilityPayload> {
        if update.get("title").and_then(serde_json::Value::as_str) == Some("todowrite") {
            return parse_todo_snapshot(update);
        }
        let tool_call_id = update
            .get("toolCallId")
            .and_then(serde_json::Value::as_str)?;
        if tool_call_id.is_empty() {
            return None;
        }
        self.update_subagent_input(update, tool_call_id)?;
        self.update_child_session_id(update, tool_call_id);
        let surface_state = parse_subagent_state(update)?;
        self.start_child_lookup(tool_call_id, surface_state);
        let payload = self.subagent_payload(tool_call_id, surface_state)?;
        self.remove_finished_subagent(update, tool_call_id);
        Some(payload)
    }

    fn update_subagent_input(&mut self, update: &serde_json::Value, capture_id: &str) -> Option<()> {
        let is_tracked = self.subagents.contains_key(capture_id);
        if !is_tracked && update.get("kind").and_then(serde_json::Value::as_str) != Some("think") {
            return None;
        }
        if let Some(input) = update.get("rawInput") {
            let agent_name = input
                .get("subagent_type")
                .and_then(serde_json::Value::as_str)?;
            if agent_name.is_empty() {
                return None;
            }
            let description = input
                .get("description")
                .and_then(serde_json::Value::as_str)
                .filter(|description| !description.is_empty())
                .map(str::to_string)
                .or_else(|| Some(format!("{agent_name} subagent")));
            let entry = self
                .subagents
                .entry(capture_id.to_string())
                .or_insert_with(|| OpenCodeSubagentCapture {
                    agent_name: agent_name.to_string(),
                    description: description.clone(),
                    child_session_id: None,
                    lookup_started: false,
                });
            entry.agent_name = agent_name.to_string();
            entry.description = description;
        } else if !is_tracked {
            return None;
        }
        Some(())
    }

    fn update_child_session_id(&mut self, update: &serde_json::Value, capture_id: &str) {
        if let Some(child_session_id) = update
            .get("rawOutput")
            .and_then(|output| output.get("metadata"))
            .and_then(|metadata| metadata.get("sessionId"))
            .and_then(serde_json::Value::as_str)
            .filter(|session_id| !session_id.is_empty())
            && let Some(entry) = self.subagents.get_mut(capture_id)
        {
            entry.child_session_id = Some(child_session_id.to_string());
        }
    }

    fn start_child_lookup(&mut self, capture_id: &str, surface_state: DiscussSubagentSurfaceState) {
        let Some(entry) = self.subagents.get_mut(capture_id) else {
            return;
        };
        if entry.child_session_id.is_some()
            || entry.lookup_started
            || surface_state != DiscussSubagentSurfaceState::Live
        {
            return;
        }
        let Some(lookup) = self.db_lookup.clone() else { return };
        entry.lookup_started = true;
        let capture_id = capture_id.to_string();
        let resolved_agent_name = entry.agent_name.clone();
        let resolved_description = entry.description.clone();
        let resolved_subagent_tx = lookup.resolved_subagent_tx.clone();
        tokio::spawn(async move {
            if let Some(child_session_id) = lookup_child_session(&lookup, &capture_id).await {
                let _ = resolved_subagent_tx
                    .send(DiscussSubagentRecord {
                        capture_id,
                        child_session_id: Some(child_session_id),
                        agent_name: resolved_agent_name,
                        description: resolved_description,
                        fallback_capability: None,
                        surface_state,
                        last_updated_at: Utc::now().timestamp_millis(),
                    })
                    .await;
            }
        });
    }

    fn subagent_payload(
        &self,
        capture_id: &str,
        surface_state: DiscussSubagentSurfaceState,
    ) -> Option<DiscussUtilityPayload> {
        let entry = self.subagents.get(capture_id)?;
        Some(DiscussUtilityPayload::Subagent {
            subagent: DiscussSubagentRecord {
                capture_id: capture_id.to_string(),
                child_session_id: entry.child_session_id.clone(),
                agent_name: entry.agent_name.clone(),
                description: entry.description.clone(),
                fallback_capability: None,
                surface_state,
                last_updated_at: Utc::now().timestamp_millis(),
            },
        })
    }

    fn remove_finished_subagent(&mut self, update: &serde_json::Value, capture_id: &str) {
        if matches!(
            update.get("status").and_then(serde_json::Value::as_str),
            Some("completed" | "failed")
        ) && self
            .subagents
            .get(capture_id)
            .is_some_and(|entry| entry.child_session_id.is_some())
        {
            self.subagents.remove(capture_id);
        }
    }
}

fn build_db_lookup(
    launch_server: &McpServer,
    parent_session_id: &SessionId,
    resolved_subagent_tx: mpsc::Sender<DiscussSubagentRecord>,
) -> Option<OpenCodeDbLookup> {
    let McpServer::Stdio(server) = launch_server else {
        return None;
    };
    let mut args = server.args.clone();
    let acp_index = args.iter().position(|arg| arg == "acp")?;
    if args.iter().filter(|arg| *arg == "acp").count() != 1 {
        return None;
    }
    args.truncate(acp_index);
    args.push("db".to_string());
    Some(OpenCodeDbLookup {
        command: server.command.clone(),
        args,
        env: server
            .env
            .iter()
            .map(|variable| (variable.name.clone(), variable.value.clone()))
            .collect(),
        parent_session_id: parent_session_id.to_string(),
        resolved_subagent_tx,
    })
}

fn quote_sql(value: &str) -> Option<String> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return None;
    }
    Some(format!("'{}'", value.replace('\'', "''")))
}

fn build_db_query(parent_session_id: &str, capture_id: &str) -> Option<String> {
    Some(
        include_str!("../../queries/opencode_child_session_lookup.sql")
            .trim()
            .replace("{{parent_session_id}}", &quote_sql(parent_session_id)?)
            .replace("{{capture_id}}", &quote_sql(capture_id)?),
    )
}

async fn lookup_child_session(lookup: &OpenCodeDbLookup, capture_id: &str) -> Option<String> {
    let query = build_db_query(&lookup.parent_session_id, capture_id)?;
    for attempt in 0..CHILD_SESSION_LOOKUP_ATTEMPTS {
        if attempt > 0 {
            sleep(Duration::from_millis(50 * attempt as u64)).await;
        }
        let mut command = Command::new(&lookup.command);
        command
            .args(&lookup.args)
            .arg(query.as_str())
            .arg("--format")
            .arg("json")
            .envs(lookup.env.iter().map(|(name, value)| (name, value)))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let Ok(Ok(output)) = timeout(CHILD_SESSION_LOOKUP_TIMEOUT, command.output()).await else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let Ok(rows) = serde_json::from_slice::<Vec<serde_json::Value>>(&output.stdout) else {
            continue;
        };
        if let Some(session_id) = rows
            .first()
            .and_then(|row| row.get("session_id"))
            .and_then(serde_json::Value::as_str)
            .filter(|session_id| !session_id.is_empty())
        {
            return Some(session_id.to_string());
        }
    }
    None
}

fn parse_subagent_state(update: &serde_json::Value) -> Option<DiscussSubagentSurfaceState> {
    match update.get("status").and_then(serde_json::Value::as_str) {
        Some("pending") | None => Some(DiscussSubagentSurfaceState::Loading),
        Some("in_progress") | Some("running") => Some(DiscussSubagentSurfaceState::Live),
        Some("completed") => Some(DiscussSubagentSurfaceState::Done),
        Some("failed") => Some(DiscussSubagentSurfaceState::Error),
        Some(_) => None,
    }
}

fn render_agent(
    agent: &AgentDefinition,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = PathBuf::from(AGENTS_DIRECTORY).join(format!("{}.md", agent.id.normalized_destination()));
    let frontmatter = AgentFrontmatter {
        name: agent.id.as_ref(),
        description: agent.description,
        mode: match agent.role {
            AgentRole::Leader => "primary",
            AgentRole::Follower => "subagent",
        },
        settings: member_settings.cloned().unwrap_or_default(),
    };
    render_markdown(&path, &frontmatter, agent.body)
}

fn install_team_members(
    library: AssetLibrary,
    plugin: &OpenCodePlugin,
    root: &Path,
    team: &TeamDefinition,
    member_settings: &BTreeMap<AgentId, ConfigSettings>,
) -> Result<(), DiscussError> {
    plugin.install_team_member(library, root, &team.leader, member_settings.get(&team.leader))?;
    for follower in &team.followers {
        plugin.install_team_member(library, root, follower, member_settings.get(follower))?;
    }
    Ok(())
}

fn render_config() -> Result<String, DiscussError> {
    serde_json::to_string_pretty(&OpenCodeConfig {
        schema: "https://opencode.ai/config.json",
        agent: OpenCodeBuiltinAgents {
            build: EnabledAgent { disable: true },
            plan: EnabledAgent { disable: false },
            explore: EnabledAgent { disable: false },
            general: EnabledAgent { disable: true },
        },
    })
    .map(|json| format!("{json}\n"))
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: OPENCODE_PLUGIN_NAME.to_string(),
        path: CONFIG_PATH.to_string(),
        format: "json".to_string(),
        message: source.to_string(),
    })
}

fn render_markdown<T: Serialize>(path: &Path, frontmatter: &T, body: &str) -> Result<RenderedFile, DiscussError> {
    let yaml = yaml_serde::to_string(frontmatter).map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: OPENCODE_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;
    Ok(RenderedFile {
        path: path.to_path_buf(),
        contents: format!("---\n{yaml}---\n\n{body}"),
    })
}

fn write_file(root: &Path, relative_path: &Path, contents: &str) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| capture_write_error(&path, error))?;
    }
    fs::write(&path, contents).map_err(|error| capture_write_error(&path, error))
}

fn remove_file(root: &Path, relative_path: &Path) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(capture_write_error(&path, error)),
    }
}

fn capture_write_error(path: &Path, error: std::io::Error) -> DiscussError {
    DiscussError::WorkspaceIo {
        plugin: OPENCODE_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::McpServerStdio;

    use super::super::super::test_library;
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn captures_todos_from_raw_input_object() {
        let payload = serde_json::json!({
            "sessionUpdate": "tool_call",
            "title": "todowrite",
            "rawInput": {
                "todos": [{"content": "Ship it", "status": "in_progress"}]
            }
        });

        assert!(matches!(
            classify_tool_payload(&payload),
            Some(DiscussUtilityPayload::TodoSnapshot { .. })
        ));
    }

    #[test]
    fn captures_verified_subagent_lifecycle_states() {
        for (status, expected) in [
            ("pending", DiscussSubagentSurfaceState::Loading),
            ("in_progress", DiscussSubagentSurfaceState::Live),
            ("completed", DiscussSubagentSurfaceState::Done),
            ("running", DiscussSubagentSurfaceState::Live),
            ("failed", DiscussSubagentSurfaceState::Error),
        ] {
            let payload = serde_json::json!({
                "sessionUpdate": "tool_call_update",
                "kind": "think",
                "toolCallId": "tool-1",
                "status": status,
                "rawInput": {
                    "description": "Inspect the code",
                    "subagent_type": "InternalScout"
                },
                "rawOutput": {"metadata": {"sessionId": "child-1"}}
            });

            let Some(DiscussUtilityPayload::Subagent { subagent }) = classify_tool_payload(&payload) else {
                panic!("expected subagent payload for {status}");
            };
            assert_eq!(subagent.child_session_id.as_deref(), Some("child-1"));
            assert_eq!(subagent.surface_state, expected);
            assert!(subagent.last_updated_at > 0);
        }
    }

    #[test]
    fn captures_running_subagent_as_live() {
        let payload = serde_json::json!({
            "sessionUpdate": "tool_call_update",
            "kind": "think",
            "toolCallId": "tool-1",
            "status": "running",
            "rawInput": {"subagent_type": "InternalScout"},
            "rawOutput": {"metadata": {"sessionId": "child-1"}}
        });

        let Some(DiscussUtilityPayload::Subagent { subagent }) = classify_tool_payload(&payload) else {
            panic!("expected running subagent payload");
        };
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Live);

        let mut unknown_status = payload;
        unknown_status["status"] = serde_json::json!("queued");
        assert!(classify_tool_payload(&unknown_status).is_none());
    }

    #[test]
    fn retains_loading_when_subagent_status_is_absent() {
        let payload = serde_json::json!({
            "sessionUpdate": "tool_call",
            "kind": "think",
            "toolCallId": "tool-1",
            "rawInput": {"subagent_type": "InternalScout"},
            "rawOutput": {"metadata": {"sessionId": "child-1"}}
        });

        let Some(DiscussUtilityPayload::Subagent { subagent }) = classify_tool_payload(&payload) else {
            panic!("expected loading subagent payload");
        };
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Loading);
    }

    #[test]
    fn captures_provisional_subagent_by_tool_call_id() {
        let payload = serde_json::json!({
            "sessionUpdate": "tool_call_update",
            "kind": "think",
            "toolCallId": "tool-provisional",
            "status": "in_progress",
            "rawInput": {"subagent_type": "InternalScout"}
        });
        let mut state = OpenCodeCaptureState::default();

        let Some(DiscussUtilityPayload::Subagent { subagent }) = capture_subagent_update(&payload, &mut state) else {
            panic!("expected provisional subagent payload");
        };

        assert_eq!(subagent.capture_id, "tool-provisional");
        assert_eq!(subagent.child_session_id, None);
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Live);
        assert!(subagent.last_updated_at > 0);
    }

    #[test]
    fn db_query_quotes_exact_provider_ids() {
        let query = build_db_query("parent'1", "tool'1").expect("safe SQL query");
        assert!(query.contains("session_id = 'parent''1'"));
        assert!(query.contains("json_extract(data, '$.callID') = 'tool''1'"));
        assert!(build_db_query("", "tool").is_none());
        assert!(build_db_query("parent\n", "tool").is_none());
    }

    #[test]
    fn db_lookup_transforms_only_supported_stdio_acp_launches() {
        let server = McpServer::Stdio(McpServerStdio::new("OpenCode", "opencode").args(vec![
            "--config".to_string(),
            "test".to_string(),
            "acp".to_string(),
        ]));

        let parent_session_id: SessionId =
            serde_json::from_value(serde_json::json!("synthetic-parent")).expect("synthetic local ACP parent id");
        let (resolved_subagent_tx, _resolved_subagent_rx) = mpsc::channel(4);
        let lookup =
            build_db_lookup(&server, &parent_session_id, resolved_subagent_tx).expect("stdio ACP launch is supported");

        assert_eq!(lookup.command, PathBuf::from("opencode"));
        assert_eq!(lookup.args, vec!["--config", "test", "db"]);
    }

    #[test]
    fn install_is_idempotent_and_destroy_is_absent_noop() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let mut agents = test_library::library().agents_for();
        let agent = agents.next().expect("target agent");
        let peer = agents.next().expect("peer agent");
        let peer_path = directory
            .path()
            .join(format!("{AGENTS_DIRECTORY}/{}.md", peer.id.normalized_destination()));
        let skill_path = directory.path().join(".opencode/skills/keep.md");
        let command_path = directory.path().join(".opencode/commands/keep.md");
        fs::create_dir_all(peer_path.parent().expect("peer parent")).expect("agent directory");
        fs::create_dir_all(skill_path.parent().expect("skill parent")).expect("skill directory");
        fs::create_dir_all(command_path.parent().expect("command parent")).expect("command directory");
        fs::write(&peer_path, "peer agent").expect("peer file");
        fs::write(&skill_path, "peer skill").expect("skill file");
        fs::write(&command_path, "peer command").expect("command file");
        PLUGIN
            .install_agent(library, directory.path(), &agent.id)
            .expect("install succeeds");
        let first = fs::read(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", agent.id.normalized_destination())),
        )
        .expect("agent file exists");
        assert_eq!(fs::read_to_string(&peer_path).expect("peer remains"), "peer agent");
        assert_eq!(fs::read_to_string(&skill_path).expect("skill remains"), "peer skill");
        assert_eq!(
            fs::read_to_string(&command_path).expect("command remains"),
            "peer command"
        );
        fs::write(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", agent.id.normalized_destination())),
            "stale target",
        )
        .expect("stale target");
        PLUGIN
            .install_agent(library, directory.path(), &agent.id)
            .expect("repeat install succeeds");
        let second = fs::read(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", agent.id.normalized_destination())),
        )
        .expect("agent file exists");
        assert_ne!(second, b"stale target");
        assert_eq!(first, second);
        PLUGIN
            .destroy_agent(directory.path(), &agent.id)
            .expect("destroy succeeds");
        assert!(
            !directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", agent.id.normalized_destination()))
                .exists()
        );
        assert_eq!(fs::read_to_string(&peer_path).expect("peer remains"), "peer agent");
        assert_eq!(fs::read_to_string(&skill_path).expect("skill remains"), "peer skill");
        assert_eq!(
            fs::read_to_string(&command_path).expect("command remains"),
            "peer command"
        );
        PLUGIN
            .destroy_agent(directory.path(), &agent.id)
            .expect("absent destroy is a no-op");
    }

    #[test]
    fn team_member_overrides_are_rendered_independently() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let team = test_library::library()
            .teams_for()
            .find(|team| !team.followers.is_empty())
            .expect("canonical team with follower");
        let follower = team.followers.first().expect("team follower");
        let untouched = team.followers.get(1).expect("second team follower");
        let settings = BTreeMap::from([
            (
                team.leader.clone(),
                ConfigSettings::from([(String::from("model"), String::from("leader-model"))]),
            ),
            (
                follower.clone(),
                ConfigSettings::from([(String::from("model"), String::from("follower-model"))]),
            ),
        ]);
        PLUGIN
            .install_team_with_member_overrides(library, directory.path(), team, &settings)
            .expect("team installs");
        let leader = fs::read_to_string(directory.path().join(format!(
            "{AGENTS_DIRECTORY}/{}.md",
            team.leader.normalized_destination()
        )))
        .expect("leader file exists");
        assert!(leader.contains("model: leader-model"));
        let follower_file = fs::read_to_string(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", follower.normalized_destination())),
        )
        .expect("follower file exists");
        assert!(follower_file.contains("model: follower-model"));
        let untouched_file = fs::read_to_string(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.md", untouched.normalized_destination())),
        )
        .expect("untouched follower file exists");
        assert!(!untouched_file.contains("model:"));
    }
}

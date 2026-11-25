pub(super) mod permissions;

use std::{
    collections::{BTreeMap, BTreeSet},
    env::{self, VarError},
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
};

use agent_client_protocol::v1::schema::{EnvVariable, McpServer, ToolCall, ToolCallStatus, ToolCallUpdate, ToolKind};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::super::error::DiscussError;
use super::super::launch_types::{
    LaunchLimitation, LaunchLimitationReason, LaunchLimitationSubject, SessionCliLogin, SessionLaunchConfig,
};
use super::super::library::{
    AgentId, AgentRole, AgentSelectionView, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition,
    SkillDefinition, SkillId, TeamDefinition,
};
use super::super::types::{
    DiscussSubagentRecord, DiscussSubagentSurfaceState, DiscussTodoItem, DiscussTodoSnapshot, DiscussTodoStatus,
    DiscussUtilityPayload,
};
use super::files::{RenderedFile, write_rendered_files};
use super::subagents::{SubagentObservation, SubagentTracker, tool_status_state};
use super::{
    BILLING_INPUT, BILLING_OUTPUT, BILLING_TOTAL, BillingCategorySemantics, ConfigSettings, ConnectionStrategy,
    DiscussAgentPlugin, LaunchContext, LaunchPreparation, ManagedConfig, PluginCaptureState, PrimaryAgentSelection,
    json_object, markdown_metadata, render_skill_contents, render_skill_file, resolve_agent, resolve_skill,
};

pub const OPENCODE_PLUGIN_NAME: &str = "opencode";

const AGENTS_DIRECTORY: &str = ".opencode/agents";
const CONFIG_PATH: &str = ".opencode/opencode.json";
const SESSION_CONFIG_PATH: &str = ".opencode/corv-session.json";
const SESSION_PERMISSIONS_SETTING: &str = "__corv_session_permissions";
/// OpenCode's session config option selecting the primary agent.
const MODE_CONFIG_ID: &str = "mode";
/// Most `subagent` tool calls remembered per session.
const TOOL_LIMIT: usize = 64;
/// OpenCode's subagent lifecycle notification, enabled by `opencode/child-session-updates`.
const CHILD_UPDATE_METHOD: &str = "opencode/session/child_update";

pub static PLUGIN: OpenCodePlugin = OpenCodePlugin;

/// OpenCode Discuss integration.
pub struct OpenCodePlugin;

#[derive(Default)]
pub struct OpenCodeCaptureState {
    /// v1 `task` tool calls, keyed by tool-call id.
    subagents: BTreeMap<String, OpenCodeSubagentCapture>,
    /// v2 `subagent` tool inputs, keyed by tool-call id.
    v2_tools: BTreeMap<String, V2SubagentInput>,
    tracker: SubagentTracker,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V2SubagentInput {
    agent: String,
    description: Option<String>,
    #[serde(rename = "sessionID")]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct V2SubagentOutput {
    metadata: Option<Value>,
    result: Option<Value>,
    #[serde(rename = "sessionID")]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct V1SubagentInput {
    subagent_type: String,
    description: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1SubagentMetadata {
    session_id: Option<String>,
}

struct OpenCodeSubagentCapture {
    agent_name: String,
    description: Option<String>,
    child_session_id: Option<String>,
}

/// OpenCode's `child_update` status for a direct child of the session (`created` | `running` |
/// `completed` | `failed` | `interrupted`). Nested children and transcript updates are ignored;
/// the subagent inspector loads the child session itself.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChildUpdate {
    root_session_id: Option<String>,
    parent_session_id: Option<String>,
    child_session_id: Option<String>,
    title: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    status: Option<String>,
}

fn capture_child_update(params: &Value, tracker: &mut SubagentTracker) {
    let Ok(update) = serde_json::from_value::<ChildUpdate>(params.clone()) else {
        return;
    };
    if update.kind.as_deref() != Some("status") || update.parent_session_id != update.root_session_id {
        return;
    }
    let Some(child) = update.child_session_id.as_deref() else {
        return;
    };
    let state = match update.status.as_deref() {
        Some("created") => DiscussSubagentSurfaceState::Loading,
        Some("running") => DiscussSubagentSurfaceState::Live,
        Some("completed") => DiscussSubagentSurfaceState::Done,
        Some("failed" | "interrupted") => DiscussSubagentSurfaceState::Error,
        _ => return,
    };
    tracker.observe_reported(SubagentObservation {
        capture_id: child,
        child_session_id: Some(child),
        agent_name: None,
        description: update.title.as_deref(),
        state: Some(state),
    });
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

#[derive(Default, Deserialize, Serialize)]
struct SessionConfigOwnership {
    mcp_servers: BTreeSet<String>,
    permissions: bool,
}

impl DiscussAgentPlugin for OpenCodePlugin {
    fn is_missing_saved_session(&self, error: &agent_client_protocol::Error, session_id: &str) -> bool {
        let message = error
            .message
            .strip_prefix("Invalid params: ")
            .unwrap_or(&error.message);
        error.code == agent_client_protocol::ErrorCode::INVALID_PARAMS
            && message.strip_prefix("session not found: ") == Some(session_id)
    }

    /// OpenCode's ACP usage reports non-cached input and visible output; cache and reasoning
    /// counts are separate and omitted when zero (`acp/translate.ts`, `session/usage.ts`).
    fn billing_semantics(&self) -> BillingCategorySemantics {
        BillingCategorySemantics {
            guaranteed_categories: BILLING_INPUT | BILLING_OUTPUT | BILLING_TOTAL,
            input_includes_cache: Some(false),
            output_excludes_thought: true,
        }
    }

    /// OpenCode's Claude Agent provider runs Anthropic models and prices them from Anthropic.
    fn models_dev_provider_aliases(&self) -> &'static [(&'static str, &'static str)] {
        &[("claude-agent", "anthropic")]
    }

    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Shared
    }

    /// OpenCode lists primary agents as values of its `mode` option.
    fn primary_agent_selection(&self, _agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        PrimaryAgentSelection::ConfigOption(MODE_CONFIG_ID)
    }

    fn uses_mcp_launch_env(&self) -> bool {
        true
    }

    fn materializes_session_config(&self) -> bool {
        true
    }

    fn session_member_settings(
        &self,
        settings: &ConfigSettings,
        config: &SessionLaunchConfig,
    ) -> Result<ConfigSettings, DiscussError> {
        let mut merged = settings.clone();
        if ["permission", "permissions", "policies", SESSION_PERMISSIONS_SETTING]
            .iter()
            .any(|key| merged.contains_key(*key))
            && !config.rule_sources.is_empty()
        {
            return Err(DiscussError::Validation {
                message: "OpenCode agent security policy conflicts with session rules".to_string(),
            });
        }
        if !config.rule_sources.is_empty()
            && ["name", "temperature"]
                .iter()
                .any(|key| merged.contains_key(*key))
        {
            return Err(DiscussError::Validation {
                message: "OpenCode V1 agent settings cannot be mixed with session permissions".to_string(),
            });
        }
        if !config.rule_sources.is_empty() {
            let translation = permissions::translate(&config.rule_sources);
            if translation.resolution.is_err() {
                return Err(DiscussError::Validation {
                    message: "Incomplete session rule sources".to_string(),
                });
            }
            let mut rules = Vec::new();
            if let Some(tools) = merged.remove("tools") {
                let tools: BTreeMap<String, bool> =
                    yaml_serde::from_str(&tools).map_err(|error| DiscussError::Validation {
                        message: error.to_string(),
                    })?;
                for (tool, enabled) in tools {
                    let action = match tool.as_str() {
                        "bash" => "shell",
                        "write" | "patch" => "edit",
                        "task" => "subagent",
                        _ => tool.as_str(),
                    };
                    rules.push(serde_json::json!({
                        "action": action,
                        "resource": "*",
                        "effect": if enabled { "allow" } else { "deny" },
                    }));
                }
            }
            // OpenCode uses the last matching rule, so session rules follow member tool rules.
            for rule in translation.config.permissions {
                rules.push(serde_json::to_value(rule)?);
            }
            merged.insert(SESSION_PERMISSIONS_SETTING.to_string(), serde_json::to_string(&rules)?);
        }
        Ok(merged)
    }

    fn materialize_session_config(
        &self,
        root: &Path,
        team: &[&AgentId],
        config: &SessionLaunchConfig,
    ) -> Result<Vec<LaunchLimitation>, DiscussError> {
        if config
            .cli_tools
            .iter()
            .any(|tool| tool.login == SessionCliLogin::Required)
        {
            return Err(DiscussError::SessionCliLoginUnsupported);
        }
        if !config.cli_tools.is_empty() {
            return Err(DiscussError::SessionCliToolUnsupported);
        }
        let path = root.join("opencode.json");
        let ownership_path = root.join(SESSION_CONFIG_PATH);
        let previous: SessionConfigOwnership = match fs::read_to_string(&ownership_path) {
            Ok(content) => serde_json::from_str(&content)?,
            Err(error) if error.kind() == ErrorKind::NotFound => SessionConfigOwnership::default(),
            Err(error) => {
                return Err(DiscussError::WorkspaceIo {
                    plugin: OPENCODE_PLUGIN_NAME.to_string(),
                    path: ownership_path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                });
            }
        };
        let mut project = match fs::read_to_string(&path) {
            Ok(content) => serde_json::from_str::<Value>(&content)?,
            Err(error) if error.kind() == ErrorKind::NotFound => serde_json::json!({}),
            Err(error) => {
                return Err(DiscussError::WorkspaceIo {
                    plugin: OPENCODE_PLUGIN_NAME.to_string(),
                    path: path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                });
            }
        };
        let object = project
            .as_object_mut()
            .ok_or_else(|| DiscussError::Validation {
                message: "OpenCode project configuration must be a JSON object".to_string(),
            })?;
        if previous.permissions {
            object.remove("permissions");
            object.remove("policies");
        }
        if !previous.mcp_servers.is_empty() {
            let servers = object
                .get_mut("mcp")
                .and_then(Value::as_object_mut)
                .and_then(|mcp| mcp.get_mut("servers"))
                .and_then(Value::as_object_mut)
                .ok_or_else(|| DiscussError::Validation {
                    message: "Unsupported OpenCode project MCP configuration".to_string(),
                })?;
            for name in &previous.mcp_servers {
                servers.remove(name);
            }
        }
        let mut ownership = SessionConfigOwnership::default();
        let mut files = Vec::new();
        if !config.rule_sources.is_empty() {
            let translation = permissions::translate(&config.rule_sources);
            if translation.resolution.is_err() {
                return Err(DiscussError::Validation {
                    message: "Incomplete session rule sources".to_string(),
                });
            }
            let permissions = serde_json::to_value(&translation.config.permissions)?;
            if team.is_empty() {
                let policies = serde_json::to_value(&translation.config.policies)?;
                if object.contains_key("permission")
                    || object
                        .get("permissions")
                        .is_some_and(|value| value != &permissions)
                    || object
                        .get("policies")
                        .is_some_and(|value| value != &policies)
                {
                    return Err(DiscussError::Validation {
                        message: "OpenCode project security policy conflicts with session rules".to_string(),
                    });
                }
                object.insert("permissions".to_string(), permissions.clone());
                object.insert("policies".to_string(), policies);
                ownership.permissions = true;
            }
        }
        if !config.mcp_servers.is_empty() {
            let mut incoming = Map::new();
            for descriptor in &config.mcp_servers {
                let (name, value) = match descriptor {
                    McpServer::Stdio(mcp) => {
                        let mut command = vec![mcp.command.to_string_lossy().into_owned()];
                        command.extend(mcp.args.iter().cloned());
                        let environment: BTreeMap<_, _> = mcp
                            .env
                            .iter()
                            .map(|entry| (entry.name.clone(), entry.value.clone()))
                            .collect();
                        (
                            mcp.name.clone(),
                            serde_json::json!({"type": "local", "command": command, "environment": environment}),
                        )
                    }
                    McpServer::Http(mcp) => {
                        let headers: BTreeMap<_, _> = mcp
                            .headers
                            .iter()
                            .map(|entry| (entry.name.clone(), entry.value.clone()))
                            .collect();
                        (
                            mcp.name.clone(),
                            serde_json::json!({"type": "remote", "url": mcp.url, "headers": headers}),
                        )
                    }
                    _ => {
                        return Err(DiscussError::Validation {
                            message: "Unsupported OpenCode MCP transport".to_string(),
                        });
                    }
                };
                if incoming.insert(name, value).is_some() {
                    return Err(DiscussError::Validation {
                        message: "Duplicate session MCP server name".to_string(),
                    });
                }
            }
            let mcp = object
                .entry("mcp".to_string())
                .or_insert_with(|| serde_json::json!({}));
            let servers = mcp
                .as_object_mut()
                .and_then(|mcp| {
                    mcp.entry("servers".to_string())
                        .or_insert_with(|| serde_json::json!({}))
                        .as_object_mut()
                })
                .ok_or_else(|| DiscussError::Validation {
                    message: "Unsupported OpenCode project MCP configuration".to_string(),
                })?;
            for (name, value) in incoming {
                if servers.contains_key(&name) {
                    return Err(DiscussError::Validation {
                        message: "OpenCode project MCP server conflicts with session server".to_string(),
                    });
                }
                ownership.mcp_servers.insert(name.clone());
                servers.insert(name, value);
            }
        }
        if !config.mcp_servers.is_empty()
            || (!config.rule_sources.is_empty() && team.is_empty())
            || !previous.mcp_servers.is_empty()
            || previous.permissions
        {
            files.push(RenderedFile {
                relative_path: PathBuf::from("opencode.json"),
                contents: serde_json::to_string_pretty(&project)?,
            });
        }
        files.push(RenderedFile {
            relative_path: PathBuf::from(SESSION_CONFIG_PATH),
            contents: serde_json::to_string_pretty(&ownership)?,
        });
        write_rendered_files(OPENCODE_PLUGIN_NAME, root, &files)?;
        Ok((0..config.plugin_references.len())
            .map(|index| LaunchLimitation {
                subject: LaunchLimitationSubject::PluginReference { index },
                reason: LaunchLimitationReason::UnsupportedPluginReference,
            })
            .collect())
    }

    fn launch_preparation(
        &self,
        _context: &LaunchContext,
        _config: &SessionLaunchConfig,
    ) -> Result<LaunchPreparation, DiscussError> {
        Ok(LaunchPreparation {
            meta: None,
            limitations: Vec::new(),
        })
    }

    fn configured_launch_server(
        &self,
        server: &McpServer,
        _workspace: &Path,
        config: &SessionLaunchConfig,
    ) -> Result<McpServer, DiscussError> {
        // ACP terminal authentication is provider-wide, not a per-session CLI login.
        if config
            .cli_tools
            .iter()
            .any(|tool| tool.login == SessionCliLogin::Required)
        {
            return Err(DiscussError::SessionCliLoginUnsupported);
        }
        if !config.cli_tools.is_empty() {
            return Err(DiscussError::SessionCliToolUnsupported);
        }
        // The Team's `.opencode` lives in the session's working directory, where OpenCode loads it
        // as project config on top of the user's own global configuration.
        let McpServer::Stdio(mut server) = server.clone() else {
            return Err(DiscussError::Validation {
                message: "OpenCode requires a stdio provider".to_string(),
            });
        };
        if config.rule_sources.is_empty() && config.mcp_servers.is_empty() && config.plugin_references.is_empty() {
            return Ok(McpServer::Stdio(server));
        }
        let inline = server
            .env
            .iter()
            .rev()
            .find(|entry| entry.name == "OPENCODE_CONFIG_CONTENT")
            .map(|entry| Ok(Some(entry.value.clone())))
            .unwrap_or_else(|| match env::var("OPENCODE_CONFIG_CONTENT") {
                Ok(value) => Ok(Some(value)),
                Err(VarError::NotPresent) => Ok(None),
                Err(VarError::NotUnicode(_)) => Err(DiscussError::Validation {
                    message: "OpenCode inline configuration is not UTF-8".to_string(),
                }),
            })?;
        let mut content = if let Some(inline) = inline {
            serde_json::from_str::<Value>(&inline)
                .ok()
                .and_then(|value| value.as_object().cloned())
                .ok_or_else(|| DiscussError::Validation {
                    message: "OpenCode inline configuration must be a JSON object".to_string(),
                })?
        } else {
            Map::new()
        };
        if !config.rule_sources.is_empty() {
            if ["permission", "permissions", "policies"]
                .iter()
                .any(|key| content.contains_key(*key))
            {
                return Err(DiscussError::Validation {
                    message: "OpenCode inline security policy conflicts with session rules".to_string(),
                });
            }
            let translation = permissions::translate(&config.rule_sources);
            if translation.resolution.is_err() {
                return Err(DiscussError::Validation {
                    message: "Incomplete session rule sources".to_string(),
                });
            }
            let fragment = serde_json::to_value(translation.config)?;
            if let Value::Object(fragment) = fragment {
                content.extend(fragment);
            }
        }
        if !config.mcp_servers.is_empty() {
            let mut servers = Map::new();
            for descriptor in &config.mcp_servers {
                let (name, value) = match descriptor {
                    McpServer::Stdio(mcp) => {
                        let mut command = vec![mcp.command.to_string_lossy().into_owned()];
                        command.extend(mcp.args.iter().cloned());
                        let environment: BTreeMap<_, _> = mcp
                            .env
                            .iter()
                            .map(|entry| (entry.name.clone(), entry.value.clone()))
                            .collect();
                        (
                            mcp.name.clone(),
                            serde_json::json!({"type": "local", "command": command, "environment": environment}),
                        )
                    }
                    McpServer::Http(mcp) => {
                        let headers: BTreeMap<_, _> = mcp
                            .headers
                            .iter()
                            .map(|entry| (entry.name.clone(), entry.value.clone()))
                            .collect();
                        (
                            mcp.name.clone(),
                            serde_json::json!({"type": "remote", "url": mcp.url, "headers": headers}),
                        )
                    }
                    _ => {
                        return Err(DiscussError::Validation {
                            message: "Unsupported OpenCode MCP transport".to_string(),
                        });
                    }
                };
                if servers.insert(name, value).is_some() {
                    return Err(DiscussError::Validation {
                        message: "Duplicate session MCP server name".to_string(),
                    });
                }
            }
            let mcp = content
                .entry("mcp".to_string())
                .or_insert_with(|| serde_json::json!({}));
            let mcp = mcp
                .as_object_mut()
                .ok_or_else(|| DiscussError::Validation {
                    message: "Unsupported OpenCode inline MCP configuration".to_string(),
                })?;
            let existing = mcp
                .entry("servers".to_string())
                .or_insert_with(|| serde_json::json!({}));
            let existing = existing
                .as_object_mut()
                .ok_or_else(|| DiscussError::Validation {
                    message: "Unsupported OpenCode inline MCP servers".to_string(),
                })?;
            for (name, value) in servers {
                if existing.contains_key(&name) {
                    return Err(DiscussError::Validation {
                        message: "OpenCode inline MCP server conflicts with session server".to_string(),
                    });
                }
                existing.insert(name, value);
            }
        }
        if !config.plugin_references.is_empty() {
            let plugins = content
                .entry("plugins".to_string())
                .or_insert_with(|| serde_json::json!([]));
            let plugins = plugins
                .as_array_mut()
                .ok_or_else(|| DiscussError::Validation {
                    message: "Unsupported OpenCode inline plugins configuration".to_string(),
                })?;
            for reference in &config.plugin_references {
                let value = Value::String(reference.clone());
                if !plugins.contains(&value) {
                    plugins.push(value);
                }
            }
        }
        server
            .env
            .retain(|entry| entry.name != "OPENCODE_CONFIG_CONTENT");
        server.env.push(EnvVariable::new(
            "OPENCODE_CONFIG_CONTENT",
            serde_json::to_string(&content)?,
        ));
        Ok(McpServer::Stdio(server))
    }

    // Source-verified OpenCode contracts: `todowrite` accepts `{ todos }`, and
    // `task` accepts `{ description, prompt, subagent_type, task_id? }`.
    // OpenCode emits task lifecycle statuses through ACP `status` updates:
    // `pending`, `in_progress`, `completed`, and `failed`.
    fn capture_tool_call(&self, call: &ToolCall, state: &mut PluginCaptureState) -> Option<DiscussUtilityPayload> {
        self.capture_tool_call_update(&call.clone().into(), state)
    }

    fn capture_extension_notification(
        &self,
        method: &str,
        params: &Value,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        let PluginCaptureState::OpenCode(state) = state else {
            return None;
        };
        if method.trim_start_matches('_') == CHILD_UPDATE_METHOD {
            capture_child_update(params, &mut state.tracker);
        }
        state.tracker.next()
    }

    fn supports_child_session_load(&self) -> bool {
        // OpenCode advertises ACP loadSession and loads the supplied provider
        // session id through its session service.
        true
    }

    fn capture_tool_call_update(
        &self,
        update: &ToolCallUpdate,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        let PluginCaptureState::OpenCode(state) = state else {
            return None;
        };
        state.capture_update(update)
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.opencode"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.opencode"
    }

    fn managed_config(&self) -> Option<ManagedConfig> {
        Some(ManagedConfig::WorkspaceFile {
            plugin: OPENCODE_PLUGIN_NAME,
            path: CONFIG_PATH,
            default: render_config,
            validate: |document| json_object(OPENCODE_PLUGIN_NAME, document).map(|_| ()),
        })
    }

    /// Remove this Provider's shared configuration from the Agent workspace.
    fn remove_config(&self, root: &Path) -> Result<(), DiscussError> {
        let path = root.join(CONFIG_PATH);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(capture_write_error(&path, error)),
        }
    }

    fn workspace_label(&self) -> &'static str {
        OPENCODE_PLUGIN_NAME
    }

    fn render_agent(
        &self,
        library: AssetLibrary,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<Vec<RenderedFile>, DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        Ok(vec![render_agent(
            &AgentSelectionView::from_builtin(agent),
            member_settings,
        )?])
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        remove_file(
            root,
            &Path::new(AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination())),
        )
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

    fn installed_agent_path(&self, id: &AgentId) -> PathBuf {
        Path::new(AGENTS_DIRECTORY).join(format!("{}.md", id.normalized_destination()))
    }

    fn installed_skill_path(&self, id: &SkillId) -> PathBuf {
        Path::new(".opencode/skills")
            .join(id.normalized_destination())
            .join("SKILL.md")
    }

    fn install_custom_agent(
        &self,
        root: &Path,
        agent: &CustomAgentDefinition,
        role: Option<AgentRole>,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let file = render_agent(&AgentSelectionView::from_custom(agent, role), member_settings)?;
        write_rendered_files(self.workspace_label(), root, &[file])
    }

    fn install_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let path = Path::new(".opencode/skills")
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            let body = if file.relative_path == "SKILL.md" {
                markdown_metadata(&file.contents)?.2
            } else {
                &file.contents
            };
            let contents = render_skill_contents(
                OPENCODE_PLUGIN_NAME,
                &path,
                &skill.id,
                &skill.description,
                &file.relative_path,
                body,
            )?;
            files.push(RenderedFile {
                relative_path: path,
                contents,
            });
        }
        write_rendered_files(self.workspace_label(), root, &files)
    }

    fn destroy_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let path = Path::new(".opencode/skills")
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            remove_file(root, &path)?;
        }
        Ok(())
    }

    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let relative_path = Path::new(".opencode/skills")
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(OPENCODE_PLUGIN_NAME, &relative_path, skill, file)?;
            files.push(RenderedFile {
                relative_path,
                contents,
            });
        }
        Ok(files)
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

#[derive(Deserialize)]
struct TodoInput {
    todos: Vec<TodoEntry>,
}
#[derive(Deserialize)]
struct TodoEntry {
    content: String,
    status: String,
}

fn parse_todo_snapshot(update: &ToolCallUpdate) -> Option<DiscussUtilityPayload> {
    let todos = update
        .fields
        .raw_input
        .as_ref()
        .and_then(|value| serde_json::from_value::<TodoInput>(value.clone()).ok())
        .or_else(|| {
            update
                .fields
                .raw_output
                .as_ref()
                .and_then(|value| value.get("metadata"))
                .and_then(|value| serde_json::from_value::<TodoInput>(value.clone()).ok())
        })?;
    let items = todos
        .todos
        .into_iter()
        .map(|todo| {
            Some(DiscussTodoItem {
                content: todo.content,
                status: parse_todo_status(&todo.status)?,
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

impl OpenCodeCaptureState {
    pub(super) fn subagent_tracker(&mut self) -> &mut SubagentTracker {
        &mut self.tracker
    }

    fn capture_update(&mut self, update: &ToolCallUpdate) -> Option<DiscussUtilityPayload> {
        if update.fields.title.as_deref() == Some("todowrite") {
            return parse_todo_snapshot(update);
        }
        let tool_call_id = update.tool_call_id.0.as_ref();
        if tool_call_id.is_empty() {
            return None;
        }
        if let Some(input) = update
            .fields
            .raw_input
            .as_ref()
            .and_then(|input| serde_json::from_value::<V2SubagentInput>(input.clone()).ok())
        {
            self.v2_tools.insert(tool_call_id.to_string(), input);
            if self.v2_tools.len() > TOOL_LIMIT {
                self.v2_tools.pop_first();
            }
        }
        // The v2 `subagent` tool returns as soon as the child is handed off; its lifecycle comes
        // from `opencode/session/child_update`. The call only names the child's agent.
        if let Some(input) = self.v2_tools.get(tool_call_id) {
            let child = update
                .fields
                .raw_output
                .as_ref()
                .and_then(|output| serde_json::from_value::<V2SubagentOutput>(output.clone()).ok())
                .and_then(|output| {
                    let session = |value: Option<Value>| -> Option<Option<String>> {
                        let Some(value) = value else { return Some(None) };
                        let object = value.as_object()?;
                        match object.get("sessionID") {
                            None | Some(Value::Null) => Some(None),
                            Some(Value::String(id)) => Some(Some(id.clone())),
                            _ => None,
                        }
                    };
                    let metadata = session(output.metadata)?;
                    let result = session(output.result)?;
                    metadata.or(result).or(output.session_id)
                })
                .or_else(|| input.session_id.clone())?;
            self.tracker.observe(SubagentObservation {
                capture_id: &child,
                child_session_id: Some(&child),
                agent_name: Some(&input.agent),
                description: input.description.as_deref(),
                state: None,
            });
            return self.tracker.next();
        }
        self.update_subagent_input(update, tool_call_id)?;
        self.update_child_session_id(update, tool_call_id);
        let surface_state = match update.fields.status.as_ref() {
            Some(status) => tool_status_state(Some(status))?,
            None => DiscussSubagentSurfaceState::Loading,
        };
        let payload = self.subagent_payload(tool_call_id, surface_state)?;
        self.remove_finished_subagent(update, tool_call_id);
        Some(payload)
    }

    fn update_subagent_input(&mut self, update: &ToolCallUpdate, capture_id: &str) -> Option<()> {
        let is_tracked = self.subagents.contains_key(capture_id);
        if !is_tracked && update.fields.kind.as_ref() != Some(&ToolKind::Think) {
            return None;
        }
        if let Some(input) = update.fields.raw_input.as_ref() {
            let input = serde_json::from_value::<V1SubagentInput>(input.clone()).ok()?;
            let agent_name = input.subagent_type.as_str();
            if agent_name.is_empty() {
                return None;
            }
            let description = input
                .description
                .as_deref()
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
                });
            entry.agent_name = agent_name.to_string();
            entry.description = description;
        } else if !is_tracked {
            return None;
        }
        Some(())
    }

    fn update_child_session_id(&mut self, update: &ToolCallUpdate, capture_id: &str) {
        if let Some(child_session_id) = update
            .fields
            .raw_output
            .as_ref()
            .and_then(|value| value.get("metadata"))
            .and_then(|value| serde_json::from_value::<V1SubagentMetadata>(value.clone()).ok())
            .and_then(|metadata| metadata.session_id)
            .filter(|session_id| !session_id.is_empty())
            && let Some(entry) = self.subagents.get_mut(capture_id)
        {
            entry.child_session_id = Some(child_session_id);
        }
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

    fn remove_finished_subagent(&mut self, update: &ToolCallUpdate, capture_id: &str) {
        if matches!(
            update.fields.status.as_ref(),
            Some(ToolCallStatus::Completed | ToolCallStatus::Failed)
        ) && self
            .subagents
            .get(capture_id)
            .is_some_and(|entry| entry.child_session_id.is_some())
        {
            self.subagents.remove(capture_id);
        }
    }
}

fn render_agent(
    agent: &AgentSelectionView<'_>,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = PathBuf::from(AGENTS_DIRECTORY).join(format!("{}.md", agent.id.normalized_destination()));
    let mut settings = member_settings.cloned().unwrap_or_default();
    let permissions = settings
        .remove(SESSION_PERMISSIONS_SETTING)
        .map(|value| serde_json::from_str::<Value>(&value))
        .transpose()?;
    let mut frontmatter = BTreeMap::new();
    let has_permissions = permissions.is_some();
    if !has_permissions {
        let name: &str = agent.id.as_ref();
        frontmatter.insert("name".to_string(), Value::String(name.to_string()));
    }
    frontmatter.insert("description".to_string(), Value::String(agent.description.to_string()));
    frontmatter.insert(
        "mode".to_string(),
        Value::String(
            match agent.role {
                Some(AgentRole::Leader) => "primary",
                Some(AgentRole::Follower) => "subagent",
                None => "all",
            }
            .to_string(),
        ),
    );
    frontmatter.extend(
        settings
            .into_iter()
            .map(|(key, value)| (key, Value::String(value))),
    );
    if let Some(permissions) = permissions {
        frontmatter.insert("permissions".to_string(), permissions);
    }
    render_markdown(&path, &frontmatter, agent.body)
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
        relative_path: path.to_path_buf(),
        contents: format!("---\n{yaml}---\n\n{body}"),
    })
}

fn remove_file(root: &Path, relative_path: &Path) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(capture_write_error(&path, error)),
    }
}

fn capture_write_error(path: &Path, error: IoError) -> DiscussError {
    DiscussError::WorkspaceIo {
        plugin: OPENCODE_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::v1::schema::McpServerStdio;

    use super::super::super::test_library;
    use super::*;
    use tempfile::tempdir;

    fn capture_fixture(mut fixture: serde_json::Value) -> Option<DiscussUtilityPayload> {
        let mut state = PluginCaptureState::OpenCode(OpenCodeCaptureState::default());
        if fixture["sessionUpdate"] == "tool_call_update" {
            let update: ToolCallUpdate = serde_json::from_value(fixture).expect("tool update");
            PLUGIN.capture_tool_call_update(&update, &mut state)
        } else {
            if fixture.get("toolCallId").is_none() {
                fixture["toolCallId"] = serde_json::json!("tool");
            }
            if fixture.get("title").is_none() {
                fixture["title"] = serde_json::json!("task");
            }
            let call: ToolCall = serde_json::from_value(fixture).expect("tool call");
            PLUGIN.capture_tool_call(&call, &mut state)
        }
    }

    #[test]
    fn materializes_permissions_and_mcp_in_session_root() {
        let root = tempdir().unwrap();
        let id = AgentId::try_from("reviewer".to_string()).unwrap();
        let agent_path = root.path().join(PLUGIN.installed_agent_path(&id));
        fs::write(
            root.path().join("opencode.json"),
            r#"{"theme":"night","mcp":{"servers":{"foreign":{"type":"local","command":["other"]}}}}"#,
        )
        .unwrap();
        let config = SessionLaunchConfig {
            rule_sources: vec![Default::default()],
            mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("example", "server"))],
            ..Default::default()
        };
        let overrides = ConfigSettings::from([
            ("model".to_string(), "example/model".to_string()),
            ("mode".to_string(), "subagent".to_string()),
            ("description".to_string(), "Custom description".to_string()),
        ]);
        let settings = PLUGIN.session_member_settings(&overrides, &config).unwrap();
        let agent = AgentSelectionView {
            id: &id,
            description: "Review",
            body: "Review code",
            role: Some(AgentRole::Leader),
        };
        let file = render_agent(&agent, Some(&settings)).unwrap();
        write_rendered_files(OPENCODE_PLUGIN_NAME, root.path(), &[file]).unwrap();
        PLUGIN
            .materialize_session_config(root.path(), &[&id], &config)
            .unwrap();
        let agent_text = fs::read_to_string(agent_path).unwrap();
        let yaml = agent_text
            .strip_prefix("---\n")
            .unwrap()
            .split_once("\n---\n")
            .unwrap()
            .0;
        let frontmatter: serde_json::Value = yaml_serde::from_str(yaml).unwrap();
        assert_eq!(frontmatter["permissions"][0]["effect"], "deny");
        assert_eq!(frontmatter["model"], "example/model");
        assert_eq!(frontmatter["mode"], "subagent");
        assert_eq!(frontmatter["description"], "Custom description");
        let project: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(root.path().join("opencode.json")).unwrap()).unwrap();
        assert_eq!(project["theme"], "night");
        assert_eq!(project["mcp"]["servers"]["example"]["command"][0], "server");
        let settings = PLUGIN
            .session_member_settings(&overrides, &SessionLaunchConfig::default())
            .unwrap();
        let file = render_agent(&agent, Some(&settings)).unwrap();
        write_rendered_files(OPENCODE_PLUGIN_NAME, root.path(), &[file]).unwrap();
        PLUGIN
            .materialize_session_config(root.path(), &[&id], &SessionLaunchConfig::default())
            .unwrap();
        assert!(
            !fs::read_to_string(root.path().join(PLUGIN.installed_agent_path(&id)))
                .unwrap()
                .contains("permissions:")
        );
        let project: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(root.path().join("opencode.json")).unwrap()).unwrap();
        assert_eq!(project["theme"], "night");
        assert!(project["mcp"]["servers"].get("example").is_none());
        assert_eq!(project["mcp"]["servers"]["foreign"]["command"][0], "other");
    }

    #[test]
    fn converts_legacy_tools_before_session_permissions() {
        let overrides = ConfigSettings::from([("tools".to_string(), r#"{"write":true,"bash":false}"#.to_string())]);
        let config = SessionLaunchConfig {
            rule_sources: vec![Default::default()],
            ..Default::default()
        };
        let settings = PLUGIN.session_member_settings(&overrides, &config).unwrap();
        let id = AgentId::try_from("reviewer".to_string()).unwrap();
        let agent = AgentSelectionView {
            id: &id,
            description: "Review",
            body: "Review code",
            role: Some(AgentRole::Leader),
        };
        let file = render_agent(&agent, Some(&settings)).unwrap();
        let yaml = file
            .contents
            .strip_prefix("---\n")
            .unwrap()
            .split_once("\n---\n")
            .unwrap()
            .0;
        let frontmatter: serde_json::Value = yaml_serde::from_str(yaml).unwrap();
        assert!(frontmatter.get("tools").is_none());
        let rules = frontmatter["permissions"].as_array().unwrap();
        assert_eq!(rules[0]["action"], "shell");
        assert_eq!(rules[0]["effect"], "deny");
        assert_eq!(rules[1]["action"], "edit");
        assert_eq!(rules[1]["effect"], "allow");
        let last_edit = rules
            .iter()
            .rev()
            .find(|rule| rule["action"] == "edit" || rule["action"] == "*")
            .unwrap();
        assert_eq!(last_edit["effect"], "deny");
        let unchanged = PLUGIN
            .session_member_settings(&overrides, &SessionLaunchConfig::default())
            .unwrap();
        assert_eq!(unchanged.get("tools"), overrides.get("tools"));
    }

    #[test]
    fn configured_launch_does_not_invent_permissions() {
        let workspace = tempdir().unwrap();
        let original = McpServer::Stdio(McpServerStdio::new("OpenCode", "opencode"));
        let config = SessionLaunchConfig {
            plugin_references: vec!["example-plugin".to_string()],
            mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("example", "server"))],
            ..Default::default()
        };
        let McpServer::Stdio(server) = PLUGIN
            .configured_launch_server(&original, workspace.path(), &config)
            .unwrap()
        else {
            panic!("expected stdio");
        };
        let content = &server
            .env
            .iter()
            .find(|entry| entry.name == "OPENCODE_CONFIG_CONTENT")
            .unwrap()
            .value;
        let content: serde_json::Value = serde_json::from_str(content).unwrap();
        assert!(content.get("permissions").is_none());
        assert!(content.get("policies").is_none());
        assert_eq!(content["plugins"][0], "example-plugin");
        assert_eq!(content["mcp"]["servers"]["example"]["command"][0], "server");
    }

    #[test]
    fn configured_launch_keeps_empty_overlay_inline_content() {
        let workspace = tempdir().unwrap();
        let mut provider = McpServerStdio::new("OpenCode", "opencode");
        let original = r#"{"permission":{"edit":"deny"},"theme":"night"}"#;
        provider
            .env
            .push(EnvVariable::new("OPENCODE_CONFIG_CONTENT", original));
        let McpServer::Stdio(server) = PLUGIN
            .configured_launch_server(
                &McpServer::Stdio(provider),
                workspace.path(),
                &SessionLaunchConfig::default(),
            )
            .unwrap()
        else {
            panic!("expected stdio")
        };
        assert!(
            server
                .env
                .iter()
                .any(|entry| entry.name == "OPENCODE_CONFIG_CONTENT" && entry.value == original)
        );
        assert!(
            !server
                .env
                .iter()
                .any(|entry| entry.name == "OPENCODE_CONFIG_DIR")
        );
    }

    #[test]
    fn configured_launch_merges_inline_content_and_rejects_policy_conflict() {
        let workspace = tempdir().unwrap();
        let mut provider = McpServerStdio::new("OpenCode", "opencode");
        provider.env.push(EnvVariable::new("OPENCODE_CONFIG_CONTENT", r#"{"permission":{"edit":"deny"},"theme":"night","plugins":["old"],"mcp":{"servers":{"old":{"type":"local"}}}}"#));
        let server = McpServer::Stdio(provider);
        let config = SessionLaunchConfig {
            plugin_references: vec!["new".to_string()],
            mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("new", "server"))],
            ..Default::default()
        };
        let McpServer::Stdio(launch) = PLUGIN
            .configured_launch_server(&server, workspace.path(), &config)
            .unwrap()
        else {
            panic!("expected stdio")
        };
        let inline = &launch
            .env
            .iter()
            .find(|entry| entry.name == "OPENCODE_CONFIG_CONTENT")
            .unwrap()
            .value;
        let content: serde_json::Value = serde_json::from_str(inline).unwrap();
        assert_eq!(content["permission"]["edit"], "deny");
        assert_eq!(content["theme"], "night");
        assert_eq!(content["plugins"], serde_json::json!(["old", "new"]));
        assert!(content["mcp"]["servers"].get("old").is_some());
        assert!(content["mcp"]["servers"].get("new").is_some());
        let conflicting = SessionLaunchConfig {
            rule_sources: vec![Default::default()],
            ..Default::default()
        };
        assert!(matches!(
            PLUGIN.configured_launch_server(&server, workspace.path(), &conflicting),
            Err(DiscussError::Validation { .. })
        ));
    }

    #[test]
    fn configured_launch_denies_omitted_operations_from_partial_rules() {
        use crate::discuss::launch_types::{Effect, Operation, Rule, RuleSet};

        let workspace = tempdir().unwrap();
        let original = McpServer::Stdio(McpServerStdio::new("OpenCode", "opencode"));
        let config = SessionLaunchConfig {
            rule_sources: vec![RuleSet {
                rules: vec![Rule {
                    operation: Operation::Read,
                    effect: Effect::Allow,
                }],
            }],
            ..Default::default()
        };
        let McpServer::Stdio(server) = PLUGIN
            .configured_launch_server(&original, workspace.path(), &config)
            .expect("partial rule sources launch")
        else {
            panic!("expected stdio");
        };
        let content = &server
            .env
            .iter()
            .find(|entry| entry.name == "OPENCODE_CONFIG_CONTENT")
            .unwrap()
            .value;
        let content: serde_json::Value = serde_json::from_str(content).unwrap();
        assert_eq!(content["permissions"][1]["action"], "read");
        assert_eq!(content["permissions"][1]["effect"], "allow");
        assert_eq!(content["permissions"][2]["action"], "edit");
        assert_eq!(content["permissions"][2]["effect"], "deny");
        assert_eq!(content["permissions"][3]["action"], "shell");
        assert_eq!(content["permissions"][3]["effect"], "deny");
    }

    #[test]
    fn configured_launch_rejects_explicit_cli_tools_without_exposing_credentials() {
        use super::super::super::launch_types::SessionCliTool;

        let workspace = tempdir().unwrap();
        let original = McpServer::Stdio(McpServerStdio::new("OpenCode", "opencode"));
        for (login, expected_login_error) in [(SessionCliLogin::None, false), (SessionCliLogin::Required, true)] {
            let config = SessionLaunchConfig {
                cli_tools: vec![SessionCliTool {
                    command: "example".into(),
                    login,
                }],
                ..Default::default()
            };
            let result = PLUGIN.configured_launch_server(&original, workspace.path(), &config);
            assert!(match result {
                Err(DiscussError::SessionCliLoginUnsupported) => expected_login_error,
                Err(DiscussError::SessionCliToolUnsupported) => !expected_login_error,
                _ => false,
            });
        }
    }

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
            capture_fixture(payload),
            Some(DiscussUtilityPayload::TodoSnapshot { .. })
        ));
    }

    #[test]
    fn captures_verified_subagent_lifecycle_states() {
        for (status, expected) in [
            ("pending", DiscussSubagentSurfaceState::Loading),
            ("in_progress", DiscussSubagentSurfaceState::Live),
            ("completed", DiscussSubagentSurfaceState::Done),
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

            let Some(DiscussUtilityPayload::Subagent { subagent }) = capture_fixture(payload) else {
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
            "status": "in_progress",
            "rawInput": {"subagent_type": "InternalScout"},
            "rawOutput": {"metadata": {"sessionId": "child-1"}}
        });

        let Some(DiscussUtilityPayload::Subagent { subagent }) = capture_fixture(payload.clone()) else {
            panic!("expected running subagent payload");
        };
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Live);

        let mut unknown_status = payload;
        unknown_status["status"] = serde_json::json!("queued");
        assert!(serde_json::from_value::<ToolCallStatus>(unknown_status["status"].clone()).is_err());
        let unknown: ToolCallUpdate = serde_json::from_value(unknown_status).expect("tool update");
        assert!(unknown.fields.status.is_none());
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

        let Some(DiscussUtilityPayload::Subagent { subagent }) = capture_fixture(payload) else {
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
        let mut state = PluginCaptureState::OpenCode(OpenCodeCaptureState::default());

        let update: ToolCallUpdate = serde_json::from_value(payload).expect("tool update");
        let Some(DiscussUtilityPayload::Subagent { subagent }) = PLUGIN.capture_tool_call_update(&update, &mut state)
        else {
            panic!("expected provisional subagent payload");
        };

        assert_eq!(subagent.capture_id, "tool-provisional");
        assert_eq!(subagent.child_session_id, None);
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Live);
        assert!(subagent.last_updated_at > 0);
    }

    #[test]
    fn captures_v2_child_from_each_output_shape() {
        for output in [
            serde_json::json!({"metadata": {"sessionID": "child"}}),
            serde_json::json!({"result": {"sessionID": "child"}}),
            serde_json::json!({"sessionID": "child"}),
        ] {
            let update: ToolCallUpdate = serde_json::from_value(serde_json::json!({
                "toolCallId": "call", "title": "subagent", "kind": "think",
                "rawInput": {"agent": "Scout"}, "rawOutput": output
            }))
            .expect("tool update");
            let mut state = PluginCaptureState::OpenCode(OpenCodeCaptureState::default());
            let Some(DiscussUtilityPayload::Subagent { subagent }) =
                PLUGIN.capture_tool_call_update(&update, &mut state)
            else {
                panic!("expected v2 child");
            };
            assert_eq!(subagent.child_session_id.as_deref(), Some("child"));
            assert_eq!(subagent.agent_name, "Scout");
        }
    }

    #[test]
    fn child_updates_report_the_subagent_lifecycle_with_the_tool_call_agent() {
        let mut state = PluginCaptureState::OpenCode(OpenCodeCaptureState::default());
        let mut child = |status: &str| {
            let params = serde_json::json!({"rootSessionId": "parent", "childSessionId": "child-1",
                "parentSessionId": "parent", "depth": 1, "title": "Explore code", "type": "status", "status": status});
            match PLUGIN.capture_extension_notification("opencode/session/child_update", &params, &mut state) {
                Some(DiscussUtilityPayload::Subagent { subagent }) => {
                    Some((subagent.agent_name, subagent.surface_state))
                }
                _ => None,
            }
        };
        assert_eq!(
            child("running"),
            Some(("subagent".to_string(), DiscussSubagentSurfaceState::Live))
        );
        assert_eq!(
            child("completed").map(|row| row.1),
            Some(DiscussSubagentSurfaceState::Done)
        );
        assert_eq!(
            child("running").map(|row| row.1),
            Some(DiscussSubagentSurfaceState::Live)
        );
        assert_eq!(
            child("interrupted").map(|row| row.1),
            Some(DiscussSubagentSurfaceState::Error)
        );

        let call = serde_json::json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-1", "title": "subagent",
            "kind": "think", "status": "completed",
            "rawInput": {"agent": "InternalScout", "description": "Explore code", "prompt": "look"},
            "rawOutput": {"metadata": {"sessionID": "child-1", "status": "running"}}});
        let call: ToolCallUpdate = serde_json::from_value(call).expect("tool update");
        let Some(DiscussUtilityPayload::Subagent { subagent }) = PLUGIN.capture_tool_call_update(&call, &mut state)
        else {
            panic!("expected the subagent's agent name from its tool call");
        };
        assert_eq!(subagent.capture_id, "child-1");
        assert_eq!(subagent.agent_name, "InternalScout");
        assert_eq!(subagent.surface_state, DiscussSubagentSurfaceState::Error);

        let nested = serde_json::json!({"rootSessionId": "parent", "childSessionId": "grandchild",
            "parentSessionId": "child-1", "depth": 2, "type": "status", "status": "running"});
        assert!(
            PLUGIN
                .capture_extension_notification("opencode/session/child_update", &nested, &mut state)
                .is_none()
        );
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
            .install_team_member(library, directory.path(), &agent.id, None)
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
            .install_team_member(library, directory.path(), &agent.id, None)
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

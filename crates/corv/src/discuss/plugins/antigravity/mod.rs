use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
};

use agent_client_protocol::v1::schema::McpServer;
use serde::Serialize;
use serde_json::Value;

use super::super::error::DiscussError;
use super::super::launch_types::{SessionCliLogin, SessionLaunchConfig};
use super::super::library::{
    AgentId, AgentSelectionView, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition, SkillDefinition, SkillId,
    TeamDefinition,
};
use super::files::{RenderedFile, write_rendered_files};
use super::{
    ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, ManagedConfig, PrimaryAgentSelection, json_object,
    markdown_metadata, render_skill_contents, render_skill_file, resolve_agent, resolve_skill,
};

pub(super) const ANTIGRAVITY_PLUGIN_NAME: &str = "antigravity-acp";

/// Antigravity discovers workspace agents and skills in the portable `.agents/` layout.
const AGENTS_DIRECTORY: &str = ".agents/agents";
/// Antigravity discovers workspace skills in the portable `.agents/skills` layout.
const SKILLS_DIRECTORY: &str = ".agents/skills";
/// `agy acp` flag naming the session's main agent.
const AGENT_FLAG: &str = "--agent";

pub(super) static PLUGIN: AntigravityPlugin = AntigravityPlugin;

/// Google Antigravity (`agy acp`) Discuss integration.
pub(super) struct AntigravityPlugin;

#[derive(Serialize)]
struct AgentFrontmatter<'a> {
    name: &'a str,
    description: &'a str,
    #[serde(flatten)]
    settings: BTreeMap<String, String>,
}

impl DiscussAgentPlugin for AntigravityPlugin {
    fn configured_launch_server(
        &self,
        server: &McpServer,
        _workspace: &Path,
        config: &SessionLaunchConfig,
    ) -> Result<McpServer, DiscussError> {
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
        let mut server = server.clone();
        if config
            .mcp_servers
            .iter()
            .any(|mcp| matches!(mcp, McpServer::Http(_)))
        {
            let McpServer::Stdio(server) = &mut server else {
                return Err(DiscussError::Validation {
                    message: "Antigravity HTTP MCP requires a stdio provider".to_owned(),
                });
            };
            if !server.args.iter().any(|arg| arg == "--process-per-session") {
                server.args.push("--process-per-session".to_owned());
            }
        }
        Ok(server)
    }

    fn managed_config(&self) -> Option<ManagedConfig> {
        Some(ManagedConfig::WorkspaceFile {
            plugin: ANTIGRAVITY_PLUGIN_NAME,
            path: ".agents/mcp_config.json",
            default: || Ok(r#"{"mcpServers":{}}"#.to_owned()),
            validate: validate_managed_config,
        })
    }
    /// Each session needs its own process because `--agent` is chosen per launch.
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Isolated
    }

    // Antigravity shares the portable `.agents/` directories with the default
    // plugin, so retention uses the default physical scopes.
    fn agent_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    /// `agy acp` advertises no ACP system prompt; it selects the main agent with `--agent <name>`.
    fn primary_agent_selection(&self, agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        PrimaryAgentSelection::LaunchArguments(vec![AGENT_FLAG.to_string(), agent.id.to_string()])
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

    fn workspace_label(&self) -> &'static str {
        ANTIGRAVITY_PLUGIN_NAME
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        remove_file(root, &agent_path(agent_id))
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

    fn install_custom_agent(
        &self,
        root: &Path,
        agent: &CustomAgentDefinition,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let file = render_agent(&AgentSelectionView::from_custom(agent), member_settings)?;
        write_rendered_files(ANTIGRAVITY_PLUGIN_NAME, root, &[file])
    }

    fn install_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            let body = if file.relative_path == "SKILL.md" {
                markdown_metadata(&file.contents)?.2
            } else {
                &file.contents
            };
            let contents = render_skill_contents(
                ANTIGRAVITY_PLUGIN_NAME,
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
        write_rendered_files(ANTIGRAVITY_PLUGIN_NAME, root, &files)
    }

    fn destroy_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            remove_file(root, &path)?;
        }
        Ok(())
    }

    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let relative_path = skill_path(skill, file.relative_path);
            let contents = render_skill_file(ANTIGRAVITY_PLUGIN_NAME, &relative_path, skill, file)?;
            files.push(RenderedFile {
                relative_path,
                contents,
            });
        }
        Ok(files)
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            remove_file(root, &skill_path(skill, file.relative_path))?;
        }
        Ok(())
    }
}

fn validate_managed_config(document: &str) -> Result<(), DiscussError> {
    let value = json_object(ANTIGRAVITY_PLUGIN_NAME, document)?;
    let valid = value
        .get("mcpServers")
        .and_then(Value::as_object)
        .is_some_and(|servers| {
            servers.values().all(|server| {
                let Some(server) = server.as_object() else { return false };
                let command = server.get("command").is_some_and(Value::is_string);
                let url = server.get("serverUrl").is_some_and(Value::is_string);
                let args = server.get("args").is_none_or(|value| {
                    value
                        .as_array()
                        .is_some_and(|args| args.iter().all(Value::is_string))
                });
                let env = server.get("env").is_none_or(|value| {
                    value
                        .as_object()
                        .is_some_and(|env| env.values().all(Value::is_string))
                });
                (command || url) && args && env
            })
        });
    if !valid {
        return Err(DiscussError::ManagedConfigValidation {
            plugin: ANTIGRAVITY_PLUGIN_NAME.to_owned(),
            reason: "expected mcpServers entries with command or serverUrl and valid args/env".to_owned(),
        });
    }
    Ok(())
}

fn agent_path(agent_id: &AgentId) -> PathBuf {
    PathBuf::from(AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination()))
}

fn skill_path(skill: &SkillDefinition, relative_path: &str) -> PathBuf {
    Path::new(SKILLS_DIRECTORY)
        .join(skill.id.normalized_destination())
        .join(relative_path)
}

fn render_agent(
    agent: &AgentSelectionView<'_>,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = agent_path(agent.id);
    let yaml = yaml_serde::to_string(&AgentFrontmatter {
        name: agent.id.as_ref(),
        description: agent.description,
        settings: member_settings.cloned().unwrap_or_default(),
    })
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: ANTIGRAVITY_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;
    Ok(RenderedFile {
        relative_path: path,
        contents: format!("---\n{yaml}---\n\n{}", agent.body),
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
        plugin: ANTIGRAVITY_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::launch_types::{RuleSet, SessionCliTool};
    use super::super::super::test_library;
    use super::*;
    use agent_client_protocol::v1::schema::{McpServerHttp, McpServerStdio};
    use tempfile::tempdir;

    #[test]
    fn http_mcp_requires_per_session_process() {
        let provider = McpServer::Stdio(McpServerStdio::new("provider", "agy"));
        let mut config = SessionLaunchConfig::default();
        config
            .mcp_servers
            .push(McpServer::Stdio(McpServerStdio::new("local", "server")));
        assert_eq!(
            PLUGIN
                .configured_launch_server(&provider, Path::new("."), &config)
                .unwrap(),
            provider
        );
        config
            .mcp_servers
            .push(McpServer::Http(McpServerHttp::new("remote", "https://example.test")));
        let McpServer::Stdio(launch) = PLUGIN
            .configured_launch_server(&provider, Path::new("."), &config)
            .unwrap()
        else {
            panic!("expected stdio")
        };
        assert_eq!(
            launch
                .args
                .iter()
                .filter(|arg| *arg == "--process-per-session")
                .count(),
            1
        );
        let repeated = PLUGIN
            .configured_launch_server(&McpServer::Stdio(launch), Path::new("."), &config)
            .unwrap();
        let McpServer::Stdio(repeated) = repeated else {
            panic!("expected stdio")
        };
        assert_eq!(
            repeated
                .args
                .iter()
                .filter(|arg| *arg == "--process-per-session")
                .count(),
            1
        );
    }

    #[test]
    fn rules_and_plugin_references_pass_through_without_mcp() {
        let provider = McpServer::Stdio(McpServerStdio::new("provider", "agy"));
        let config = SessionLaunchConfig {
            rule_sources: vec![RuleSet::default()],
            plugin_references: vec!["plugin".into()],
            ..Default::default()
        };
        assert_eq!(
            PLUGIN
                .configured_launch_server(&provider, Path::new("."), &config)
                .unwrap(),
            provider
        );
    }

    #[test]
    fn cli_tools_return_typed_errors() {
        let provider = McpServer::Stdio(McpServerStdio::new("provider", "agy"));
        let mut config = SessionLaunchConfig {
            cli_tools: vec![SessionCliTool {
                command: "tool".into(),
                login: SessionCliLogin::None,
            }],
            ..Default::default()
        };
        assert!(matches!(
            PLUGIN.configured_launch_server(&provider, Path::new("."), &config),
            Err(DiscussError::SessionCliToolUnsupported)
        ));
        config.cli_tools[0].login = SessionCliLogin::Required;
        assert!(matches!(
            PLUGIN.configured_launch_server(&provider, Path::new("."), &config),
            Err(DiscussError::SessionCliLoginUnsupported)
        ));
    }

    #[test]
    fn renders_portable_agent_and_skill_layout() {
        let library = test_library::library();
        let agent = library.agents_for().next().expect("agent");
        let files = PLUGIN
            .render_agent(library, &agent.id, None)
            .expect("agent rendering");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].relative_path, agent_path(&agent.id));
        assert!(files[0].contents.starts_with("---\n"));
        assert!(files[0].contents.ends_with(agent.body));
        let skill = library.skills_for().next().expect("skill");
        let files = PLUGIN.render_skill(skill).expect("skill rendering");
        assert_eq!(files.len(), skill.bundle.len());
        for (rendered, source) in files.iter().zip(&skill.bundle) {
            assert_eq!(rendered.relative_path, skill_path(skill, source.relative_path));
            assert_eq!(
                rendered.contents,
                render_skill_file(ANTIGRAVITY_PLUGIN_NAME, &rendered.relative_path, skill, source).unwrap()
            );
        }
        assert_eq!(PLUGIN.workspace_label(), "antigravity-acp");
    }

    #[test]
    fn install_is_idempotent_and_destroy_is_absent_noop() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let mut agents = test_library::library().agents_for();
        let agent = agents.next().expect("target agent");
        let peer = agents.next().expect("peer agent");
        let target_path = directory.path().join(agent_path(&agent.id));
        let peer_path = directory.path().join(agent_path(&peer.id));
        fs::create_dir_all(peer_path.parent().expect("peer parent")).expect("agent directory");
        fs::write(&peer_path, "peer agent").expect("peer file");

        PLUGIN
            .install_team_member(library, directory.path(), &agent.id, None)
            .expect("install succeeds");
        let first = fs::read_to_string(&target_path).expect("agent file exists");
        assert!(first.starts_with("---\n"));
        assert!(first.contains(&format!("name: {}\n", agent.id.as_ref())));
        assert!(first.contains(&format!("description: {}\n", agent.description)));
        assert!(first.ends_with(agent.body));

        fs::write(&target_path, "stale target").expect("stale target");
        PLUGIN
            .install_team_member(library, directory.path(), &agent.id, None)
            .expect("repeat install succeeds");
        assert_eq!(fs::read_to_string(&target_path).expect("agent file exists"), first);

        PLUGIN
            .destroy_agent(directory.path(), &agent.id)
            .expect("destroy succeeds");
        assert!(!target_path.exists());
        assert_eq!(fs::read_to_string(&peer_path).expect("peer remains"), "peer agent");
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
            .expect("team with follower");
        let follower = team.followers.first().expect("team follower");
        let settings = BTreeMap::from([(
            team.leader.clone(),
            ConfigSettings::from([(String::from("model"), String::from("pro"))]),
        )]);

        PLUGIN
            .install_team_with_member_overrides(library, directory.path(), team, &settings)
            .expect("team installs");

        let leader = fs::read_to_string(directory.path().join(agent_path(&team.leader))).expect("leader file");
        let follower = fs::read_to_string(directory.path().join(agent_path(follower))).expect("follower file");
        assert!(leader.contains("model: pro\n"));
        assert!(!follower.contains("model:"));
    }

    #[test]
    fn primary_agent_is_selected_with_the_agent_launch_flag() {
        let agent = test_library::library().agents_for().next().expect("agent");
        assert_eq!(
            PLUGIN.primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::LaunchArguments(vec!["--agent".to_string(), agent.id.to_string()])
        );
    }
}

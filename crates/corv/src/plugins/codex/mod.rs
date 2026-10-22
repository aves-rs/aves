use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

use super::super::DiscussError;
use super::super::library::{AgentDefinition, AgentId, AssetLibrary, SkillDefinition, TeamDefinition};
use super::{
    ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, destroy_default_skill, install_default_skill,
    resolve_agent, resolve_skill,
};

pub(super) const CODEX_PLUGIN_NAME: &str = "codex-acp";

const AGENTS_DIRECTORY: &str = ".codex/agents";

/// Codex ACP Discuss integration.
pub(super) struct CodexPlugin;

pub(super) static PLUGIN: CodexPlugin = CodexPlugin;

#[derive(Serialize)]
struct AgentToml<'a> {
    name: &'a str,
    description: &'a str,
    developer_instructions: &'a str,
    #[serde(flatten)]
    settings: BTreeMap<String, String>,
}

struct RenderedFile {
    path: PathBuf,
    contents: String,
}

impl DiscussAgentPlugin for CodexPlugin {
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Shared
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.codex-acp"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        let file = render_agent(agent, None)?;
        write_file(root, &file.path, &file.contents)
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let path = root
            .join(AGENTS_DIRECTORY)
            .join(format!("{}.toml", agent_id.normalized_destination()));
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(capture_write_error(&path, error)),
        }
    }

    fn install_team_with_member_overrides(
        &self,
        library: AssetLibrary,
        root: &Path,
        team: &TeamDefinition,
        member_settings: &BTreeMap<AgentId, ConfigSettings>,
    ) -> Result<(), DiscussError> {
        self.install_team_member(library, root, &team.leader, member_settings.get(&team.leader))?;
        for follower in &team.followers {
            self.install_team_member(library, root, follower, member_settings.get(follower))?;
        }
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

    fn install_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        install_default_skill(root, skill)
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        destroy_default_skill(root, skill)
    }
}

fn render_agent(
    agent: &AgentDefinition,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = PathBuf::from(AGENTS_DIRECTORY).join(format!("{}.toml", agent.id.normalized_destination()));
    let contents = toml::to_string(&AgentToml {
        name: agent.id.as_ref(),
        description: agent.description,
        developer_instructions: agent.body,
        settings: member_settings.cloned().unwrap_or_default(),
    })
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: CODEX_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        format: "toml".to_string(),
        message: source.to_string(),
    })?;
    Ok(RenderedFile { path, contents })
}

fn write_file(root: &Path, relative_path: &Path, contents: &str) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| capture_write_error(&path, error))?;
    }
    fs::write(&path, contents).map_err(|error| capture_write_error(&path, error))
}

fn capture_write_error(path: &Path, error: std::io::Error) -> DiscussError {
    DiscussError::WorkspaceIo {
        plugin: CODEX_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::test_library;
    use super::*;
    use agent_client_protocol::schema::v1::{McpServer, McpServerStdio};
    use tempfile::tempdir;

    #[test]
    fn uses_provider_advertised_launch_and_session_list_capability() {
        let server = McpServer::Stdio(
            McpServerStdio::new("Codex", "npx").args(vec!["-y".into(), "@agentclientprotocol/codex-acp@1.1.7".into()]),
        );

        assert_eq!(PLUGIN.launch_server(&server), server);
        assert!(PLUGIN.supports_session_list(true));
        assert!(!PLUGIN.supports_session_list(false));
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
            .join(format!("{AGENTS_DIRECTORY}/{}.toml", peer.id.normalized_destination()));
        let skill_path = directory.path().join(".agents/skills/keep.md");
        let command_path = directory.path().join(".agents/commands/keep.md");
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
                .join(format!("{AGENTS_DIRECTORY}/{}.toml", agent.id.normalized_destination())),
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
                .join(format!("{AGENTS_DIRECTORY}/{}.toml", agent.id.normalized_destination())),
            "stale target",
        )
        .expect("stale target");
        PLUGIN
            .install_agent(library, directory.path(), &agent.id)
            .expect("repeat install succeeds");
        let second = fs::read(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.toml", agent.id.normalized_destination())),
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
                .join(format!("{AGENTS_DIRECTORY}/{}.toml", agent.id.normalized_destination()))
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
            "{AGENTS_DIRECTORY}/{}.toml",
            team.leader.normalized_destination()
        )))
        .expect("leader file exists");
        assert!(leader.contains("model = \"leader-model\""));
        let follower_file = fs::read_to_string(
            directory
                .path()
                .join(format!("{AGENTS_DIRECTORY}/{}.toml", follower.normalized_destination())),
        )
        .expect("follower file exists");
        assert!(follower_file.contains("model = \"follower-model\""));
        let untouched_file = fs::read_to_string(directory.path().join(format!(
            "{AGENTS_DIRECTORY}/{}.toml",
            untouched.normalized_destination()
        )))
        .expect("untouched follower file exists");
        assert!(!untouched_file.contains("model ="));
    }
}

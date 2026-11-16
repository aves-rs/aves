use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

use super::super::library::{AgentDefinition, AgentId, AssetLibrary, SkillDefinition, SkillId, TeamDefinition};
use super::super::types::DiscussError;
use super::{
    ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, PrimaryAgentSelection, render_skill_file, resolve_agent,
    resolve_skill,
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

struct RenderedFile {
    path: PathBuf,
    contents: String,
}

impl DiscussAgentPlugin for AntigravityPlugin {
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
    fn primary_agent_selection(&self, agent: &AgentDefinition) -> PrimaryAgentSelection {
        PrimaryAgentSelection::LaunchArguments(vec![AGENT_FLAG.to_string(), agent.id.to_string()])
    }

    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        let file = render_agent(agent, None)?;
        write_file(root, &file.path, &file.contents)
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        remove_file(root, &agent_path(agent_id))
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
            let relative_path = skill_path(skill, file.relative_path);
            let contents = render_skill_file(ANTIGRAVITY_PLUGIN_NAME, &relative_path, skill, file)?;
            write_file(root, &relative_path, &contents)?;
        }
        Ok(())
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            remove_file(root, &skill_path(skill, file.relative_path))?;
        }
        Ok(())
    }
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
    agent: &AgentDefinition,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = agent_path(&agent.id);
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
        path,
        contents: format!("---\n{yaml}---\n\n{}", agent.body),
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
        plugin: ANTIGRAVITY_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::test_library;
    use super::*;
    use tempfile::tempdir;

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
            .install_agent(library, directory.path(), &agent.id)
            .expect("install succeeds");
        let first = fs::read_to_string(&target_path).expect("agent file exists");
        assert!(first.starts_with("---\n"));
        assert!(first.contains(&format!("name: {}\n", agent.id.as_ref())));
        assert!(first.contains(&format!("description: {}\n", agent.description)));
        assert!(first.ends_with(agent.body));

        fs::write(&target_path, "stale target").expect("stale target");
        PLUGIN
            .install_agent(library, directory.path(), &agent.id)
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
            PLUGIN.primary_agent_selection(agent),
            PrimaryAgentSelection::LaunchArguments(vec!["--agent".to_string(), agent.id.to_string()])
        );
    }
}

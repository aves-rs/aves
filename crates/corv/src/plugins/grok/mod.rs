use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::super::library::{AgentDefinition, AgentId, AssetLibrary, SkillDefinition, SkillId, TeamDefinition};
use super::super::types::{DiscussConfigOption, DiscussConfigOptionValue, DiscussError};
use super::{ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, render_skill_file, resolve_agent, resolve_skill};

pub const GROK_PLUGIN_NAME: &str = "grok-build";

const AGENTS_DIRECTORY: &str = ".grok/agents";
const SKILLS_DIRECTORY: &str = ".grok/skills";

const GROK_SESSION_CONFIG_META_KEY: &str = "x.ai/sessionConfig";
const GROK_MODEL_CATEGORY: &str = "model";
const GROK_EFFORT_CATEGORY: &str = "mode";

/// Grok Build Discuss integration.
pub struct GrokPlugin;

pub static PLUGIN: GrokPlugin = GrokPlugin;

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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GrokSessionConfig {
    options: Vec<GrokFlattenedOption>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GrokFlattenedOption {
    id: Option<String>,
    category: Option<String>,
    label: Option<String>,
    selected: Option<bool>,
}

struct GrokConfigField {
    id: &'static str,
    name: &'static str,
    category: &'static str,
}

impl DiscussAgentPlugin for GrokPlugin {
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Isolated
    }

    // Source verification: the local Grok plugin and ACP protocol sources do
    // not define a Grok-specific `todowrite` or `subagent-spawn` tool name or
    // payload contract, nor a provider-specific child-session/load guarantee.
    // Keep the inherited
    // no-op capture hooks and false child-session capability.
    fn agent_scope_key(&self) -> &'static str {
        "plugin.grok-build"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.grok-build"
    }

    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        let file = render_agent(agent, None)?;
        write_file(root, &file.path, &file.contents)
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let path = root
            .join(AGENTS_DIRECTORY)
            .join(format!("{}.md", agent_id.normalized_destination()));
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
            let relative_path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(GROK_PLUGIN_NAME, &relative_path, skill, file)?;
            write_file(root, &relative_path, &contents)?;
        }
        Ok(())
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            remove_file(root, &relative_path)?;
        }
        Ok(())
    }

    fn extension_config_options(
        &self,
        metadata: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<Vec<DiscussConfigOption>> {
        grok_session_config(metadata).and_then(grok_config_options)
    }
}

fn render_agent(
    agent: &AgentDefinition,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
    let path = PathBuf::from(AGENTS_DIRECTORY).join(format!("{}.md", agent.id.normalized_destination()));
    render_markdown(
        &path,
        &AgentFrontmatter {
            name: agent.id.as_ref(),
            description: agent.description,
            settings: member_settings.cloned().unwrap_or_default(),
        },
        agent.body,
    )
}

fn render_markdown<T: Serialize>(path: &Path, frontmatter: &T, body: &str) -> Result<RenderedFile, DiscussError> {
    let yaml = yaml_serde::to_string(frontmatter).map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: GROK_PLUGIN_NAME.to_string(),
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
        plugin: GROK_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

fn grok_session_config(metadata: &serde_json::Map<String, serde_json::Value>) -> Option<&serde_json::Value> {
    metadata.get(GROK_SESSION_CONFIG_META_KEY).or_else(|| {
        metadata
            .get("_meta")
            .and_then(serde_json::Value::as_object)
            .and_then(|meta| meta.get(GROK_SESSION_CONFIG_META_KEY))
    })
}

fn grok_config_options(value: &serde_json::Value) -> Option<Vec<DiscussConfigOption>> {
    let config = serde_json::from_value::<GrokSessionConfig>(value.clone()).ok()?;
    let mut categories = BTreeMap::<String, Vec<GrokFlattenedOption>>::new();

    for option in config.options {
        let Some(category) = option
            .category
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            continue;
        };
        categories
            .entry(category.to_string())
            .or_default()
            .push(option);
    }

    Some(
        categories
            .into_iter()
            .filter_map(|(category, options)| {
                grok_config_field(&category).and_then(|field| grok_config_option(field, options))
            })
            .collect(),
    )
}

fn grok_config_field(category: &str) -> Option<GrokConfigField> {
    match category {
        GROK_MODEL_CATEGORY => Some(GrokConfigField {
            id: "model",
            name: "Model",
            category: "model",
        }),
        GROK_EFFORT_CATEGORY => Some(GrokConfigField {
            // Grok calls this a mode, but its choices are reasoning-effort levels.
            // Retain the provider option id while exposing ACP's thought-level category.
            id: "mode",
            name: "Reasoning effort",
            category: "thought_level",
        }),
        _ => None,
    }
}

fn grok_config_option(field: GrokConfigField, options: Vec<GrokFlattenedOption>) -> Option<DiscussConfigOption> {
    if options.iter().any(|option| {
        matches!(option.id.as_deref(), None | Some(""))
            || option
                .id
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            || matches!(option.label.as_deref(), None | Some(""))
            || option
                .label
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
    }) {
        return None;
    }

    let mut selected = options
        .iter()
        .filter(|option| option.selected == Some(true));
    let value = selected.next()?.id.clone()?;
    if selected.next().is_some() {
        return None;
    }

    let options = options
        .into_iter()
        .map(|option| {
            Some(DiscussConfigOptionValue {
                value: option.id?,
                label: option.label?,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(DiscussConfigOption {
        id: field.id.to_string(),
        name: field.name.to_string(),
        category: field.category.to_string(),
        value,
        options,
    })
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
        let peer_path = directory
            .path()
            .join(format!("{AGENTS_DIRECTORY}/{}.md", peer.id.normalized_destination()));
        let skill_path = directory.path().join(".grok/skills/keep.md");
        let command_path = directory.path().join(".grok/commands/keep.md");
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

    fn session_new_metadata(options: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::from_iter([(
            "_meta".to_string(),
            serde_json::json!({ GROK_SESSION_CONFIG_META_KEY: options }),
        )])
    }

    #[test]
    fn standard_acp_options_remain_authoritative_over_grok_metadata() {
        let standard = serde_json::from_value(serde_json::json!([{
            "id": "standard-model",
            "name": "Standard model",
            "category": "model",
            "type": "select",
            "currentValue": "standard",
            "options": [{ "value": "standard", "name": "Standard" }]
        }]))
        .expect("valid ACP session config option");
        let metadata = session_new_metadata(serde_json::json!({
            "options": [{ "id": "grok-4.5", "category": "model", "label": "Grok 4.5", "selected": true }]
        }));

        let options = PLUGIN
            .initial_config_options(Some(standard), Some(&metadata))
            .expect("standard options are explicit");

        assert_eq!(options.len(), 1);
        assert_eq!(options[0].id, "standard-model");
        assert_eq!(options[0].value, "standard");
    }

    #[test]
    fn grok_flattened_metadata_becomes_canonical_model_and_reasoning_options() {
        let metadata = session_new_metadata(serde_json::json!({
            "options": [
                { "id": "grok-4.5", "category": "model", "label": "Grok 4.5", "description": "Latest", "selected": true },
                { "id": "grok-composer-2.5-fast", "category": "model", "label": "Composer 2.5", "selected": false },
                { "id": "high", "category": "mode", "label": "High Effort", "description": "Highest implementation quality with extensive reasoning", "selected": true },
                { "id": "medium", "category": "mode", "label": "Medium Effort", "description": "Balanced effort with standard implementation and testing", "selected": false },
                { "id": "low", "category": "mode", "label": "Low Effort", "description": "Quick, fast implementations", "selected": false }
            ]
        }));

        let options = PLUGIN
            .initial_config_options(None, Some(&metadata))
            .expect("Grok metadata is present");

        assert_eq!(options.len(), 2);
        assert_eq!(options[0].id, "mode");
        assert_eq!(options[0].name, "Reasoning effort");
        assert_eq!(options[0].category, "thought_level");
        assert_eq!(options[0].value, "high");
        assert_eq!(options[0].options.len(), 3);
        assert_eq!(options[0].options[0].value, "high");
        assert_eq!(options[0].options[0].label, "High Effort");
        assert_eq!(options[1].id, "model");
        assert_eq!(options[1].name, "Model");
        assert_eq!(options[1].category, "model");
        assert_eq!(options[1].value, "grok-4.5");
        assert_eq!(options[1].options.len(), 2);
        assert_eq!(options[1].options[0].value, "grok-4.5");
        assert_eq!(options[1].options[0].label, "Grok 4.5");
    }

    #[test]
    fn grok_groups_without_exactly_one_selected_choice_fail_closed() {
        let metadata = session_new_metadata(serde_json::json!({
            "options": [
                { "id": "grok-4.5", "category": "model", "label": "Grok 4.5", "selected": true },
                { "id": "grok-4.1", "category": "model", "label": "Grok 4.1", "selected": true },
                { "id": "high", "category": "mode", "label": "High Effort", "selected": false }
            ]
        }));

        let options = PLUGIN
            .initial_config_options(None, Some(&metadata))
            .expect("Grok metadata is present");

        assert!(options.is_empty());
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

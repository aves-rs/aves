//! Installed-agent plugin boundary for Discuss.
//!
//! The actor selects one plugin by exact registry id and uses this trait for
//! provider config normalization and plugin-owned workspace materialization.

mod claude;
mod codex;
mod grok;
mod opencode;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use agent_client_protocol::schema::v1::{
    McpServer, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOptions,
};

use super::library::{
    AgentDefinition, AgentId, AssetLibrary, SkillBundleFile, SkillDefinition, SkillId, TeamDefinition,
};
use super::types::{
    DiscussConfigOption, DiscussConfigOptionValue, DiscussError, DiscussMemberOverrides, DiscussUtilityPayload,
};

use claude::{CLAUDE_PLUGIN_NAME, PLUGIN as CLAUDE_AGENT_PLUGIN};
use codex::{CODEX_PLUGIN_NAME, PLUGIN as CODEX_AGENT_PLUGIN};
use grok::{GROK_PLUGIN_NAME, PLUGIN as GROK_AGENT_PLUGIN};
use opencode::{OPENCODE_PLUGIN_NAME, OpenCodeCaptureState, PLUGIN as OPENCODE_AGENT_PLUGIN};

/// Mutable, worker-local state used only by plugins whose utility payloads
/// arrive over multiple ACP updates.
pub enum PluginCaptureState {
    None,
    OpenCode(OpenCodeCaptureState),
}

impl PluginCaptureState {
    pub fn for_plugin(plugin: &dyn DiscussAgentPlugin) -> Self {
        if plugin.agent_scope_key() == "plugin.opencode" {
            Self::OpenCode(OpenCodeCaptureState::default())
        } else {
            Self::None
        }
    }
}

const DEFAULT_PLUGIN_NAME: &str = "default";
const DEFAULT_AGENTS_DIRECTORY: &str = ".agents/agents";
const DEFAULT_SKILLS_DIRECTORY: &str = ".agents/skills";

struct DefaultAgentPlugin;

/// Provider settings already resolved by the Discuss actor.
///
/// Plugins must not merge these values with Provider defaults. An entry is
/// present only when a Team member has an explicit, non-default override.
pub type ConfigSettings = BTreeMap<String, String>;

static DEFAULT_PLUGIN: DefaultAgentPlugin = DefaultAgentPlugin;

/// Declares whether a Provider's ACP connection may serve multiple sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStrategy {
    /// One ACP process and connection is shared by the Provider's sessions.
    Shared,
    /// Each session owns an independent ACP process and connection.
    Isolated,
}

#[derive(serde::Serialize)]
struct DefaultAgentFrontmatter<'a> {
    name: &'a str,
    description: &'a str,
    #[serde(flatten)]
    settings: ConfigSettings,
}

#[derive(serde::Serialize)]
struct SkillFrontmatter<'a> {
    name: &'a str,
    description: &'a str,
}

impl DiscussAgentPlugin for DefaultAgentPlugin {
    // Source verification: no provider harness contract exists for the
    // default/custom boundary: no `todowrite` or `subagent-spawn` tool name or
    // payload contract, and no child-session/load guarantee. Inherited capture
    // remains a no-op and child session/load remains false.
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Isolated
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        install_agent_file(root, agent, None)
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let relative_path =
            Path::new(DEFAULT_AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination()));
        remove_file(DEFAULT_PLUGIN_NAME, root, &relative_path)
    }

    fn install_team_member(
        &self,
        library: AssetLibrary,
        root: &Path,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        install_agent_file(root, agent, member_settings)
    }

    fn install_team_with_member_overrides(
        &self,
        library: AssetLibrary,
        root: &Path,
        team: &TeamDefinition,
        member_settings: &BTreeMap<AgentId, ConfigSettings>,
    ) -> Result<(), DiscussError> {
        install_team_member(library, self, root, &team.leader, member_settings.get(&team.leader))?;
        for follower in &team.followers {
            install_team_member(library, self, root, follower, member_settings.get(follower))?;
        }
        for skill_id in &team.skills {
            let skill = resolve_skill(library, skill_id)?;
            self.install_skill(root, skill)?;
        }
        Ok(())
    }

    fn install_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(DEFAULT_SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(DEFAULT_PLUGIN_NAME, &relative_path, skill, file)?;
            write_file(DEFAULT_PLUGIN_NAME, root, &relative_path, &contents)?;
        }
        Ok(())
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(DEFAULT_SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            remove_file(DEFAULT_PLUGIN_NAME, root, &relative_path)?;
        }
        Ok(())
    }
}

pub fn install_default_skill(root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
    DEFAULT_PLUGIN.install_skill(root, skill)
}

pub fn destroy_default_skill(root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
    DEFAULT_PLUGIN.destroy_skill(root, skill)
}

pub fn render_skill_file(
    plugin_name: &str,
    relative_path: &Path,
    skill: &SkillDefinition,
    file: &SkillBundleFile,
) -> Result<String, DiscussError> {
    if file.relative_path != "SKILL.md" {
        return Ok(file.contents.to_string());
    }

    let yaml = yaml_serde::to_string(&SkillFrontmatter {
        name: skill.id.normalized_destination(),
        description: skill.description,
    })
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: plugin_name.to_string(),
        path: relative_path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;

    Ok(format!("---\n{yaml}---\n\n{}", file.contents))
}

fn install_agent_file(
    root: &Path,
    agent: &AgentDefinition,
    member_settings: Option<&ConfigSettings>,
) -> Result<(), DiscussError> {
    let relative_path = Path::new(DEFAULT_AGENTS_DIRECTORY).join(format!("{}.md", agent.id.normalized_destination()));
    let yaml = yaml_serde::to_string(&DefaultAgentFrontmatter {
        name: agent.id.as_ref(),
        description: agent.description,
        settings: member_settings.cloned().unwrap_or_default(),
    })
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: DEFAULT_PLUGIN_NAME.to_string(),
        path: relative_path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;
    write_file(
        DEFAULT_PLUGIN_NAME,
        root,
        &relative_path,
        &format!("---\n{yaml}---\n\n{}", agent.body),
    )
}

/// Resolve an installed provider to its plugin. Unknown/custom providers use
/// the provider-neutral ACP implementation and portable Agent renderer.
pub fn for_agent(agent_name: &str) -> &'static dyn DiscussAgentPlugin {
    match agent_name {
        CLAUDE_PLUGIN_NAME => &CLAUDE_AGENT_PLUGIN,
        CODEX_PLUGIN_NAME => &CODEX_AGENT_PLUGIN,
        GROK_PLUGIN_NAME => &GROK_AGENT_PLUGIN,
        OPENCODE_PLUGIN_NAME => &OPENCODE_AGENT_PLUGIN,
        _ => &DEFAULT_PLUGIN,
    }
}

/// Resolve the optional built-in copy source beside the canonical Provider keys.
pub(super) fn default_workspace_copy_source(provider_name: &str) -> Option<&'static str> {
    match provider_name {
        OPENCODE_PLUGIN_NAME => Some(".opencode"),
        CLAUDE_PLUGIN_NAME => Some(".claude"),
        CODEX_PLUGIN_NAME => Some(".codex"),
        GROK_PLUGIN_NAME => Some(".grok"),
        _ => None,
    }
}

/// Provider-neutral contract used by the Discuss actor.
pub trait DiscussAgentPlugin: Sync {
    /// Declares the Provider's connection ownership model.
    ///
    /// Unverified Providers default to isolated connections; implementations
    /// must opt into sharing explicitly rather than relying on ACP capabilities.
    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Isolated
    }

    /// Classify and normalize one provider tool-call payload into a utility update.
    ///
    /// The actor supplies the physical Agent workspace scope so a plugin can
    /// distinguish harness-specific tool names without making that knowledge
    /// part of the provider-neutral DTOs. Implementations must remain pure:
    /// this hook only inspects the payload and returns a normalized value.
    fn capture_tool_call(&self, payload: &serde_json::Value, agent_scope_key: &str) -> Option<DiscussUtilityPayload> {
        let _ = (payload, agent_scope_key);
        None
    }

    /// Classify and normalize one provider tool-call update payload.
    ///
    /// Unverified providers intentionally return no utility update by default.
    fn capture_tool_call_update(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
    ) -> Option<DiscussUtilityPayload> {
        let _ = (payload, agent_scope_key);
        None
    }

    /// Classify a utility update with worker-local correlation state.
    fn capture_tool_call_with_state(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        let _ = state;
        self.capture_tool_call(payload, agent_scope_key)
    }

    /// Classify a utility update with worker-local correlation state.
    fn capture_tool_call_update_with_state(
        &self,
        payload: &serde_json::Value,
        agent_scope_key: &str,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        let _ = state;
        self.capture_tool_call_update(payload, agent_scope_key)
    }

    /// Whether this Provider can load a child ACP session by id.
    fn supports_child_session_load(&self) -> bool {
        false
    }

    /// Return the effective server launch specification for this Provider.
    ///
    /// Implementations may add process-local compatibility arguments, but must
    /// never mutate the persisted Provider configuration.
    fn launch_server(&self, server: &McpServer) -> McpServer {
        server.clone()
    }

    /// Reconcile an advertised `session/list` capability with provider-adapter
    /// compatibility knowledge.
    fn supports_session_list(&self, advertised: bool) -> bool {
        advertised
    }

    /// Identifies the physical Agent workspace retention scope for this plugin.
    fn agent_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    /// Identifies the physical Skill workspace retention scope for this plugin.
    fn skill_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    /// Materialize this Provider's shared configuration in the Agent workspace.
    fn install_config(&self, _root: &Path) -> Result<(), DiscussError> {
        Ok(())
    }

    /// Remove this Provider's shared configuration from the Agent workspace.
    fn remove_config(&self, _root: &Path) -> Result<(), DiscussError> {
        Ok(())
    }

    /// Materialize one canonical Agent in this provider's workspace.
    fn install_agent(&self, library: AssetLibrary, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError>;

    /// Remove one canonical Agent from this provider's workspace.
    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError>;

    /// Materialize a Team with actor-resolved settings for its members.
    fn install_team_with_member_overrides(
        &self,
        library: AssetLibrary,
        _root: &Path,
        team: &TeamDefinition,
        member_settings: &BTreeMap<AgentId, ConfigSettings>,
    ) -> Result<(), DiscussError>;

    /// Materialize one Team member through the Team-only configuration path.
    ///
    /// Empty settings mean Default and must remain config-free. Concrete
    /// plugins override this method to render non-empty actor-resolved
    /// settings in their native Agent format.
    fn install_team_member(
        &self,
        library: AssetLibrary,
        root: &Path,
        agent_id: &AgentId,
        _member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        self.install_agent(library, root, agent_id)
    }

    /// Remove a canonical Team while retaining assets shared by another Team.
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

    /// Materialize one canonical Skill bundle.
    fn install_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError>;

    /// Remove one canonical Skill bundle.
    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError>;

    /// Normalize initial provider configuration into the canonical menu.
    fn initial_config_options(
        &self,
        standard_options: Option<Vec<SessionConfigOption>>,
        metadata: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Option<Vec<DiscussConfigOption>> {
        standard_options
            .map(|options| self.standard_config_options(options))
            .or_else(|| metadata.and_then(|metadata| self.extension_config_options(metadata)))
    }

    /// Normalize standard ACP config options into the canonical Discuss menu.
    fn standard_config_options(&self, options: Vec<SessionConfigOption>) -> Vec<DiscussConfigOption> {
        options
            .into_iter()
            .filter_map(|option| self.standard_config_option(option))
            .collect()
    }

    /// Normalize one standard ACP select option.
    fn standard_config_option(&self, option: SessionConfigOption) -> Option<DiscussConfigOption> {
        let SessionConfigKind::Select(select) = option.kind else {
            return None;
        };
        let options = match select.options {
            SessionConfigSelectOptions::Ungrouped(options) => options
                .into_iter()
                .map(|option| DiscussConfigOptionValue {
                    value: option.value.to_string(),
                    label: option.name,
                })
                .collect(),
            SessionConfigSelectOptions::Grouped(groups) => groups
                .into_iter()
                .flat_map(|group| {
                    group
                        .options
                        .into_iter()
                        .map(move |option| DiscussConfigOptionValue {
                            value: option.value.to_string(),
                            label: format!("{} / {}", group.name, option.name),
                        })
                })
                .collect(),
            _ => Vec::new(),
        };
        Some(DiscussConfigOption {
            id: option.id.to_string(),
            name: option.name,
            category: option
                .category
                .map(|category| self.standard_config_option_category(category))
                .unwrap_or_else(|| "provider".to_string()),
            value: select.current_value.to_string(),
            options,
        })
    }

    /// Normalize one standard ACP option category.
    fn standard_config_option_category(&self, category: SessionConfigOptionCategory) -> String {
        match category {
            SessionConfigOptionCategory::Mode => "mode".to_string(),
            SessionConfigOptionCategory::Model => "model".to_string(),
            SessionConfigOptionCategory::ModelConfig => "model_config".to_string(),
            SessionConfigOptionCategory::ThoughtLevel => "thought_level".to_string(),
            SessionConfigOptionCategory::Other(category) => category,
            _ => "provider".to_string(),
        }
    }

    /// Normalize non-standard provider metadata after standard ACP parsing fails.
    fn extension_config_options(
        &self,
        _metadata: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<Vec<DiscussConfigOption>> {
        None
    }

    /// Normalize a provider config-update notification.
    fn metadata_config_options(&self, metadata: &serde_json::Value) -> Option<Vec<DiscussConfigOption>> {
        self.standard_metadata_config_options(metadata).or_else(|| {
            metadata
                .as_object()
                .and_then(|metadata| self.extension_config_options(metadata))
        })
    }

    /// Normalize standard ACP config metadata into the canonical Discuss menu.
    fn standard_metadata_config_options(&self, metadata: &serde_json::Value) -> Option<Vec<DiscussConfigOption>> {
        let options = metadata
            .get("configOptions")
            .or_else(|| metadata.get("options"))?
            .as_array()?
            .clone();
        Some(
            options
                .into_iter()
                .filter_map(|option| serde_json::from_value::<SessionConfigOption>(option).ok())
                .filter_map(|option| self.standard_config_option(option))
                .collect(),
        )
    }
}

fn install_team_member(
    library: AssetLibrary,
    plugin: &dyn DiscussAgentPlugin,
    root: &Path,
    agent_id: &AgentId,
    member_settings: Option<&ConfigSettings>,
) -> Result<(), DiscussError> {
    let member_settings = member_settings.filter(|settings| !settings.is_empty());
    plugin.install_team_member(library, root, agent_id, member_settings)
}

pub fn member_settings_for_team(
    overrides: DiscussMemberOverrides,
) -> Result<BTreeMap<AgentId, ConfigSettings>, DiscussError> {
    overrides
        .into_iter()
        .map(|(agent_id, settings)| {
            let agent_id =
                AgentId::try_from(agent_id.clone()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                    asset_id: agent_id,
                    violation: error.to_string(),
                })?;
            Ok((agent_id, settings))
        })
        .collect()
}

pub fn resolve_agent(library: AssetLibrary, agent_id: &AgentId) -> Result<&'static AgentDefinition, DiscussError> {
    library
        .agents_for()
        .find(|agent| agent.id == *agent_id)
        .ok_or_else(|| DiscussError::WorkspaceInvalidAssetId {
            asset_id: agent_id.to_string(),
            violation: "agent definition is not registered".to_string(),
        })
}

pub fn resolve_team(library: AssetLibrary, leader_id: &AgentId) -> Result<&'static TeamDefinition, DiscussError> {
    library
        .teams_for()
        .find(|team| team.leader == *leader_id)
        .ok_or_else(|| DiscussError::WorkspaceInvalidAssetId {
            asset_id: leader_id.to_string(),
            violation: "team definition is not registered".to_string(),
        })
}

pub fn resolve_skill(library: AssetLibrary, skill_id: &SkillId) -> Result<&'static SkillDefinition, DiscussError> {
    library
        .skills_for()
        .find(|skill| skill.id == *skill_id)
        .ok_or_else(|| DiscussError::WorkspaceInvalidAssetId {
            asset_id: skill_id.to_string(),
            violation: "skill definition is not registered".to_string(),
        })
}

fn write_file(plugin: &str, root: &Path, relative_path: &Path, contents: &str) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| DiscussError::WorkspaceIo {
            plugin: plugin.to_string(),
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    std::fs::write(&path, contents).map_err(|error| DiscussError::WorkspaceIo {
        plugin: plugin.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })
}

fn remove_file(plugin: &str, root: &Path, relative_path: &Path) -> Result<(), DiscussError> {
    let path = root.join(relative_path);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(DiscussError::WorkspaceIo {
            plugin: plugin.to_string(),
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::Path,
    };

    use agent_client_protocol::schema::v1::{SessionConfigOption, SessionConfigSelectOption};
    use tempfile::tempdir;

    use super::super::library::{AgentId, SkillId, TeamDefinition};
    use super::super::test_library;
    use super::*;

    struct ProviderLayout {
        name: &'static str,
        agents_directory: &'static str,
        agent_extension: &'static str,
        skills_directory: &'static str,
    }

    const PROVIDER_LAYOUTS: [ProviderLayout; 4] = [
        ProviderLayout {
            name: OPENCODE_PLUGIN_NAME,
            agents_directory: ".opencode/agents",
            agent_extension: "md",
            skills_directory: ".opencode/skills",
        },
        ProviderLayout {
            name: CLAUDE_PLUGIN_NAME,
            agents_directory: ".claude/agents",
            agent_extension: "md",
            skills_directory: ".claude/skills",
        },
        ProviderLayout {
            name: GROK_PLUGIN_NAME,
            agents_directory: ".grok/agents",
            agent_extension: "md",
            skills_directory: ".grok/skills",
        },
        ProviderLayout {
            name: CODEX_PLUGIN_NAME,
            agents_directory: ".codex/agents",
            agent_extension: "toml",
            skills_directory: ".agents/skills",
        },
    ];

    fn agent_path(layout: &ProviderLayout, root: &Path, agent_id: &AgentId) -> std::path::PathBuf {
        root.join(layout.agents_directory).join(format!(
            "{}.{}",
            agent_id.normalized_destination(),
            layout.agent_extension
        ))
    }

    fn skill_path(layout: &ProviderLayout, root: &Path, skill_id: &SkillId, relative_path: &str) -> std::path::PathBuf {
        root.join(layout.skills_directory)
            .join(skill_id.normalized_destination())
            .join(relative_path)
    }

    fn assert_agent_materialized(layout: &ProviderLayout, root: &Path, agent_id: &AgentId) {
        assert!(
            agent_path(layout, root, agent_id).is_file(),
            "{} should materialize {}",
            layout.name,
            agent_id
        );
    }

    #[test]
    fn custom_provider_uses_standard_acp_config() {
        let plugin = for_agent("custom-agent");
        let option = SessionConfigOption::select(
            "model",
            "Model",
            "model-a",
            SessionConfigSelectOptions::Ungrouped(vec![SessionConfigSelectOption::new("model-a", "Model A")]),
        );

        let options = plugin
            .initial_config_options(Some(vec![option]), None)
            .expect("explicit standard config menu");

        assert_eq!(options[0].id, "model");
    }

    #[test]
    fn provider_config_lifecycle_defaults_are_noop() {
        let root = tempfile::tempdir().expect("temp workspace");
        let plugin = for_agent("custom-provider");

        plugin
            .install_config(root.path())
            .expect("default install config");
        plugin
            .remove_config(root.path())
            .expect("default remove config");
        assert!(
            root.path()
                .read_dir()
                .expect("workspace entries")
                .next()
                .is_none()
        );
    }

    #[test]
    fn default_renderer_is_surgical_and_renders_portable_markdown() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let mut agents = test_library::library().agents_for();
        let target = agents.next().expect("target agent");
        let peer = agents.next().expect("peer agent");
        let target_path = directory.path().join(format!(
            "{DEFAULT_AGENTS_DIRECTORY}/{}.md",
            target.id.normalized_destination()
        ));
        let peer_path = directory.path().join(format!(
            "{DEFAULT_AGENTS_DIRECTORY}/{}.md",
            peer.id.normalized_destination()
        ));
        let skill_path = directory.path().join(".agents/skills/keep.md");
        let command_path = directory.path().join(".agents/commands/keep.md");
        fs::create_dir_all(peer_path.parent().expect("peer parent")).expect("agent directory");
        fs::create_dir_all(skill_path.parent().expect("skill parent")).expect("skill directory");
        fs::create_dir_all(command_path.parent().expect("command parent")).expect("command directory");
        fs::write(&peer_path, "peer agent").expect("peer file");
        fs::write(&skill_path, "peer skill").expect("skill file");
        fs::write(&command_path, "peer command").expect("command file");

        DEFAULT_PLUGIN
            .install_agent(library, directory.path(), &target.id)
            .expect("install succeeds");
        let rendered = fs::read_to_string(&target_path).expect("target file exists");
        assert!(rendered.starts_with("---\n"));
        assert!(rendered.contains(&format!("name: {}\n", target.id.as_ref())));
        assert!(rendered.contains(&format!("description: {}\n", target.description)));
        assert!(rendered.ends_with(target.body));
        assert_eq!(fs::read_to_string(&peer_path).expect("peer remains"), "peer agent");
        assert_eq!(fs::read_to_string(&skill_path).expect("skill remains"), "peer skill");
        assert_eq!(
            fs::read_to_string(&command_path).expect("command remains"),
            "peer command"
        );

        fs::write(&target_path, "stale target").expect("stale target");
        DEFAULT_PLUGIN
            .install_agent(library, directory.path(), &target.id)
            .expect("overwrite succeeds");
        assert_ne!(
            fs::read_to_string(&target_path).expect("target remains"),
            "stale target"
        );
        DEFAULT_PLUGIN
            .destroy_agent(directory.path(), &target.id)
            .expect("destroy succeeds");
        assert!(!target_path.exists());
        assert_eq!(fs::read_to_string(&peer_path).expect("peer remains"), "peer agent");
        assert_eq!(fs::read_to_string(&skill_path).expect("skill remains"), "peer skill");
        assert_eq!(
            fs::read_to_string(&command_path).expect("command remains"),
            "peer command"
        );
        DEFAULT_PLUGIN
            .destroy_agent(directory.path(), &target.id)
            .expect("absent destroy is a no-op");
    }

    #[test]
    fn default_team_override_renders_settings_only_for_explicit_member() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let team = test_library::library()
            .teams_for()
            .find(|team| !team.followers.is_empty())
            .expect("team with follower");
        let leader_settings = BTreeMap::from([(String::from("model"), String::from("model-a"))]);
        let member_settings = BTreeMap::from([(team.leader.clone(), leader_settings)]);

        DEFAULT_PLUGIN
            .install_team_with_member_overrides(library, directory.path(), team, &member_settings)
            .expect("team installs");

        let leader_path = directory.path().join(format!(
            "{DEFAULT_AGENTS_DIRECTORY}/{}.md",
            team.leader.normalized_destination()
        ));
        let follower_path = directory.path().join(format!(
            "{DEFAULT_AGENTS_DIRECTORY}/{}.md",
            team.followers[0].normalized_destination()
        ));
        let leader = fs::read_to_string(leader_path).expect("leader file exists");
        let follower = fs::read_to_string(follower_path).expect("follower file exists");

        assert!(leader.contains("model: model-a\n"));
        assert!(!follower.contains("model:"));
        assert!(!follower.contains("reasoning:"));
        assert!(!follower.contains("effort:"));
    }

    #[test]
    fn team_lifecycle_materializes_full_bundles_and_retains_shared_assets() {
        let library = test_library::library();
        let equity_team = test_library::library()
            .teams_for()
            .find(|team| team.leader.as_ref() == "equity-analyst")
            .expect("equity analyst team");
        let sector_team = test_library::library()
            .teams_for()
            .find(|team| team.leader.as_ref() == "sector-analyst")
            .expect("sector analyst team");
        let customizer_team = test_library::library()
            .teams_for()
            .find(|team| team.leader.as_ref() == "sluice-customizer")
            .expect("Sluice customizer team");
        // Keep the retained Team distinct from the removed Team's leader while
        // exercising retention with a Team member shared by the fixture.
        let shared_follower = equity_team
            .followers
            .iter()
            .find(|follower| follower.as_ref() == "equity-auditor")
            .expect("equity auditor follower");
        let retained_team = TeamDefinition {
            leader: AgentId::new("chief-of-staff").expect("canonical retained leader"),
            followers: vec![shared_follower.clone()],
            skills: sector_team.skills.clone(),
            version: sector_team.version.clone(),
        };
        let retained_followers: BTreeSet<_> = retained_team.followers.iter().cloned().collect();
        let retained_skills: BTreeSet<_> = retained_team
            .skills
            .iter()
            .chain(customizer_team.skills.iter())
            .cloned()
            .collect();

        for layout in PROVIDER_LAYOUTS {
            let directory = tempdir().expect("temporary workspace");
            let plugin = for_agent(layout.name);

            let member_settings = BTreeMap::from([(
                equity_team.leader.clone(),
                ConfigSettings::from([(String::from("model"), String::from("model-a"))]),
            )]);
            plugin
                .install_team_with_member_overrides(library, directory.path(), equity_team, &member_settings)
                .expect("equity team installs");
            plugin
                .install_team_with_member_overrides(library, directory.path(), &retained_team, &BTreeMap::new())
                .expect("sector team installs");
            plugin
                .install_team_with_member_overrides(library, directory.path(), customizer_team, &BTreeMap::new())
                .expect("Sluice customizer team installs");

            assert_agent_materialized(&layout, directory.path(), &equity_team.leader);
            for follower in &equity_team.followers {
                assert_agent_materialized(&layout, directory.path(), follower);
            }
            for skill in test_library::library().skills_for() {
                for file in &skill.bundle {
                    let path = skill_path(&layout, directory.path(), &skill.id, file.relative_path);
                    let contents = fs::read_to_string(&path).expect("skill bundle file exists");
                    if file.relative_path == "SKILL.md" {
                        let (frontmatter, body) = contents
                            .strip_prefix("---\n")
                            .and_then(|contents| contents.split_once("\n---\n\n"))
                            .expect("skill frontmatter delimiters");
                        let metadata = yaml_serde::from_str::<BTreeMap<String, String>>(frontmatter)
                            .expect("valid skill frontmatter");
                        assert_eq!(metadata.len(), 2);
                        assert_eq!(metadata.get("name").map(String::as_str), Some(skill.id.as_ref()));
                        assert_eq!(metadata.get("description").map(String::as_str), Some(skill.description));
                        assert_eq!(body, file.contents);
                    } else {
                        assert_eq!(contents, file.contents, "{} should preserve bundle files", layout.name);
                    }
                }
            }

            plugin
                .destroy_team(
                    library,
                    directory.path(),
                    equity_team,
                    &retained_followers,
                    &retained_skills,
                )
                .expect("equity team uninstalls");

            assert!(!agent_path(&layout, directory.path(), &equity_team.leader).exists());
            for follower in &equity_team.followers {
                let path = agent_path(&layout, directory.path(), follower);
                assert_eq!(
                    path.exists(),
                    retained_followers.contains(follower),
                    "{} should retain only followers used by the other team: {}",
                    layout.name,
                    follower
                );
            }
            for skill in test_library::library().skills_for() {
                let retained = retained_skills.contains(&skill.id);
                for file in &skill.bundle {
                    assert_eq!(
                        skill_path(&layout, directory.path(), &skill.id, file.relative_path).exists(),
                        retained,
                        "{} should {} skill {}",
                        layout.name,
                        if retained { "retain" } else { "remove" },
                        skill.id
                    );
                }
            }
        }
    }
}

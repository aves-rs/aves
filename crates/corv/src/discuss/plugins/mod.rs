//! Installed-agent plugin boundary for Discuss.
//!
//! The actor selects one plugin by exact registry id and uses this trait for
//! provider config normalization and plugin-owned workspace materialization.

mod antigravity;
mod claude;
mod codex;
mod files;
mod grok;
mod launch;
mod opencode;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use agent_client_protocol::v1::schema::{
    McpServer, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOptions,
};

use super::assets::markdown_metadata;
use super::library::{
    AgentDefinition, AgentId, AgentSelectionView, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition,
    SkillBundleFile, SkillDefinition, SkillId, TeamDefinition,
};
use super::types::{
    DiscussConfigOption, DiscussConfigOptionValue, DiscussError, DiscussMemberOverrides, DiscussUtilityPayload,
    LaunchLimitation, LaunchLimitationReason, LaunchLimitationSubject, LaunchPermissionOutcome, SessionLaunchConfig,
};
use super::usage::{DiscussUsageEvidence, UsageCaptureEvent, capture_standard_usage};
use files::{RenderedFile, write_rendered_files};
pub(super) use launch::{LaunchContext, LaunchPreparation, LaunchSelections, intersect_rules, merge_metadata};

/// Input category bit, shared by guaranteed and incomplete category masks.
pub(super) const BILLING_INPUT: u8 = 1 << 0;
/// Output token category.
pub(super) const BILLING_OUTPUT: u8 = 1 << 1;
/// Thought token category (included in output for billing).
pub(super) const BILLING_THOUGHT: u8 = 1 << 2;
/// Cache-read token category.
pub(super) const BILLING_CACHED_READ: u8 = 1 << 3;
/// Cache-write token category.
pub(super) const BILLING_CACHED_WRITE: u8 = 1 << 4;
/// Total token category.
pub(super) const BILLING_TOTAL: u8 = 1 << 5;

/// Verified category requirements and cache treatment for pricing.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct BillingCategorySemantics {
    pub guaranteed_categories: u8,
    pub input_includes_cache: Option<bool>,
}

impl BillingCategorySemantics {
    /// Whether a provider has declared its input/cache billing relationship.
    pub(super) fn declared(self) -> bool {
        self.input_includes_cache.is_some()
    }
}

use antigravity::{ANTIGRAVITY_PLUGIN_NAME, PLUGIN as ANTIGRAVITY_AGENT_PLUGIN};
use claude::{CLAUDE_PLUGIN_NAME, PLUGIN as CLAUDE_AGENT_PLUGIN};
use codex::{CODEX_PLUGIN_NAME, PLUGIN as CODEX_AGENT_PLUGIN};
use grok::{GROK_PLUGIN_NAME, PLUGIN as GROK_AGENT_PLUGIN};
use opencode::{OPENCODE_PLUGIN_NAME, OpenCodeCaptureState, PLUGIN as OPENCODE_AGENT_PLUGIN};

/// Provider settings already resolved by the Discuss actor.
///
/// Plugins must not merge these values with Provider defaults. An entry is
/// present only when a Team member has an explicit, non-default override.
pub type ConfigSettings = BTreeMap<String, String>;

pub(super) fn append_installed_instructions(
    plugin: &dyn DiscussAgentPlugin,
    root: &Path,
    path: &Path,
    instructions: &str,
    agent: bool,
) -> Result<(), DiscussError> {
    let path = root.join(path);
    let capture_error = |error: std::io::Error| DiscussError::WorkspaceIo {
        plugin: plugin.workspace_label().to_owned(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    };
    let rendered = std::fs::read_to_string(&path).map_err(&capture_error)?;
    let contents = if agent {
        plugin.append_agent_instructions(&rendered, instructions)?
    } else {
        format!("{rendered}\n\n{instructions}")
    };
    std::fs::write(&path, contents).map_err(capture_error)
}

static DEFAULT_PLUGIN: DefaultAgentPlugin = DefaultAgentPlugin;

const DEFAULT_PLUGIN_NAME: &str = "default";
const DEFAULT_AGENTS_DIRECTORY: &str = ".agents/agents";
const DEFAULT_SKILLS_DIRECTORY: &str = ".agents/skills";

struct DefaultAgentPlugin;

/// Provider-owned configuration and its validation boundary.
pub enum ManagedConfig {
    WorkspaceFile {
        plugin: &'static str,
        path: &'static str,
        default: fn() -> Result<String, DiscussError>,
        validate: fn(&str) -> Result<(), DiscussError>,
    },
    LaunchEnv {
        env_name: &'static str,
        default: fn() -> Result<String, DiscussError>,
        validate: fn(&str) -> Result<(), DiscussError>,
    },
}

impl ManagedConfig {
    pub(super) fn document(&self, document: Option<&str>) -> Result<String, DiscussError> {
        let (default, validate) = match self {
            Self::WorkspaceFile { default, validate, .. } | Self::LaunchEnv { default, validate, .. } => {
                (default, validate)
            }
        };
        let document = match document {
            Some(document) => document.to_owned(),
            None => default()?,
        };
        validate(&document)?;
        Ok(document)
    }
}

pub(super) fn apply_managed_launch_env(
    plugin: &dyn DiscussAgentPlugin,
    server: &mut McpServer,
    document: Option<&str>,
) -> Result<(), DiscussError> {
    if let Some(config @ ManagedConfig::LaunchEnv { .. }) = plugin.managed_config() {
        let value = config.document(document).or_else(|error| match error {
            DiscussError::ManagedConfigValidation { .. } => config.document(None),
            error => Err(error),
        })?;
        if let ManagedConfig::LaunchEnv { env_name, .. } = config
            && let McpServer::Stdio(server) = server
        {
            server.env.retain(|entry| entry.name != env_name);
            server
                .env
                .push(agent_client_protocol::v1::schema::EnvVariable::new(env_name, value));
        }
    }
    Ok(())
}

fn json_object(plugin: &str, document: &str) -> Result<serde_json::Value, DiscussError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|error| DiscussError::ManagedConfigValidation {
            plugin: plugin.to_owned(),
            reason: error.to_string(),
        })?;
    if !value.is_object() {
        return Err(DiscussError::ManagedConfigValidation {
            plugin: plugin.to_owned(),
            reason: "expected a JSON object".to_owned(),
        });
    }
    Ok(value)
}

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

/// Declares whether a Provider's ACP connection may serve multiple sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStrategy {
    /// One ACP process and connection is shared by the Provider's sessions.
    Shared,
    /// Each session owns an independent ACP process and connection.
    Isolated,
}

/// How a Provider makes an installed Agent the primary agent of a session.
#[derive(Clone, Debug, PartialEq)]
pub enum PrimaryAgentSelection {
    /// Select the Provider-advertised session config option whose value is the Agent id.
    ConfigOption(&'static str),
    /// Attach Provider-specific `_meta` to the session open request.
    SessionMeta(serde_json::Map<String, serde_json::Value>),
    /// Append the Agent's instructions as the ACP session system prompt; requires the
    /// Provider to advertise `sessionCapabilities.systemPrompt`.
    SystemPrompt,
    /// Append these arguments to the session's own Provider process launch.
    LaunchArguments(Vec<String>),
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

    fn render_agent(
        &self,
        library: AssetLibrary,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<Vec<RenderedFile>, DiscussError> {
        let agent = resolve_agent(library, agent_id)?;
        Ok(vec![render_agent_file(
            &AgentSelectionView::from_builtin(agent),
            member_settings,
        )?])
    }

    fn workspace_label(&self) -> &'static str {
        DEFAULT_PLUGIN_NAME
    }

    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError> {
        let relative_path =
            Path::new(DEFAULT_AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination()));
        remove_file(DEFAULT_PLUGIN_NAME, root, &relative_path)
    }

    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        Self::render_portable_skill(skill)
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

impl DefaultAgentPlugin {
    pub(super) fn render_portable_skill(skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let relative_path = Path::new(DEFAULT_SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(DEFAULT_PLUGIN_NAME, &relative_path, skill, file)?;
            files.push(RenderedFile {
                relative_path,
                contents,
            });
        }
        Ok(files)
    }
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
    render_skill_contents(
        plugin_name,
        relative_path,
        &skill.id,
        skill.description,
        file.relative_path,
        file.contents,
    )
}

fn render_skill_contents(
    plugin_name: &str,
    relative_path: &Path,
    id: &SkillId,
    description: &str,
    file_path: &str,
    contents: &str,
) -> Result<String, DiscussError> {
    if file_path != "SKILL.md" {
        return Ok(contents.to_string());
    }

    let yaml = yaml_serde::to_string(&SkillFrontmatter {
        name: id.normalized_destination(),
        description,
    })
    .map_err(|source| DiscussError::WorkspaceSerialize {
        plugin: plugin_name.to_string(),
        path: relative_path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;

    Ok(format!("---\n{yaml}---\n\n{contents}"))
}

fn render_agent_file(
    agent: &AgentSelectionView<'_>,
    member_settings: Option<&ConfigSettings>,
) -> Result<RenderedFile, DiscussError> {
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
    Ok(RenderedFile {
        relative_path,
        contents: format!("---\n{yaml}---\n\n{}", agent.body),
    })
}

/// Resolve an installed provider to its plugin. Unknown/custom providers use
/// the provider-neutral ACP implementation and portable Agent renderer.
pub fn for_agent(agent_name: &str) -> &'static dyn DiscussAgentPlugin {
    match agent_name {
        ANTIGRAVITY_PLUGIN_NAME => &ANTIGRAVITY_AGENT_PLUGIN,
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
        ANTIGRAVITY_PLUGIN_NAME => Some(".gemini"),
        CLAUDE_PLUGIN_NAME => Some(".claude"),
        CODEX_PLUGIN_NAME => Some(".codex"),
        GROK_PLUGIN_NAME => Some(".grok"),
        OPENCODE_PLUGIN_NAME => Some(".opencode"),
        _ => None,
    }
}

/// Provider-neutral contract used by the Discuss actor.
pub trait DiscussAgentPlugin: Sync {
    /// Capture standard ACP usage, with provider-specific extensions as overrides.
    fn capture_usage_evidence(&self, event: &UsageCaptureEvent<'_>) -> Option<DiscussUsageEvidence> {
        capture_standard_usage(event)
    }

    /// Return source-verified billing rules, or no declared semantics.
    fn billing_semantics(&self) -> BillingCategorySemantics {
        BillingCategorySemantics::default()
    }

    /// Return the provider's exact models.dev identifier, if known.
    fn models_dev_provider_id(&self) -> Option<&'static str> {
        None
    }
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

    /// Whether custom MCP descriptors are translated into the launch environment.
    fn uses_mcp_launch_env(&self) -> bool {
        false
    }

    /// Return an isolated launch when a Team requires workspace-local assets.
    fn team_launch_server(&self, _server: &McpServer, _workspace: &Path) -> Result<Option<McpServer>, DiscussError> {
        Ok(None)
    }

    /// Apply explicitly supplied session configuration to a private process.
    fn configured_launch_server(
        &self,
        server: &McpServer,
        _workspace: &Path,
        _config: &SessionLaunchConfig,
    ) -> Result<McpServer, DiscussError> {
        Ok(server.clone())
    }

    /// Prepare session metadata and disclose settings that remain inherited.
    fn launch_preparation(
        &self,
        _context: &LaunchContext,
        config: &SessionLaunchConfig,
    ) -> Result<LaunchPreparation, DiscussError> {
        let mut limitations = Vec::new();
        if let Some(rules) = intersect_rules(&config.rule_sources) {
            for rule in rules.rules {
                limitations.push(LaunchLimitation {
                    subject: LaunchLimitationSubject::Permission {
                        operation: rule.operation,
                        requested: rule.effect,
                        actual: LaunchPermissionOutcome::Inherited,
                    },
                    reason: LaunchLimitationReason::InheritedUnverifiedConfiguration,
                });
            }
        }
        for index in 0..config.plugin_references.len() {
            limitations.push(LaunchLimitation {
                subject: LaunchLimitationSubject::PluginReference { index },
                reason: LaunchLimitationReason::UnsupportedPluginReference,
            });
        }
        Ok(LaunchPreparation {
            meta: None,
            limitations,
        })
    }

    /// Select settings after the final provider options are advertised.
    fn launch_option_selections(
        &self,
        _config: &SessionLaunchConfig,
        _options: &[DiscussConfigOption],
    ) -> LaunchSelections {
        LaunchSelections::default()
    }

    /// Choose how this Provider selects an installed Agent as a session's primary agent.
    ///
    /// Unverified Providers use the standard ACP session system prompt.
    fn primary_agent_selection(&self, agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        let _ = agent;
        PrimaryAgentSelection::SystemPrompt
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

    /// Workspace-relative path of the installed agent file.
    fn installed_agent_path(&self, id: &AgentId) -> PathBuf {
        Path::new(DEFAULT_AGENTS_DIRECTORY).join(format!("{}.md", id.normalized_destination()))
    }

    /// Workspace-relative path of the installed skill's instructions file.
    fn installed_skill_path(&self, id: &SkillId) -> PathBuf {
        Path::new(DEFAULT_SKILLS_DIRECTORY)
            .join(id.normalized_destination())
            .join("SKILL.md")
    }

    /// Append instructions to a freshly rendered agent file.
    fn append_agent_instructions(&self, rendered: &str, instructions: &str) -> Result<String, DiscussError> {
        Ok(format!("{rendered}\n\n{instructions}"))
    }

    /// Describe the Provider's managed configuration, if supported.
    fn managed_config(&self) -> Option<ManagedConfig> {
        None
    }

    /// Materialize this Provider's shared configuration in the Agent workspace.
    fn install_config(&self, root: &Path, document: Option<&str>) -> Result<(), DiscussError> {
        match self.managed_config() {
            Some(ManagedConfig::WorkspaceFile {
                plugin,
                path,
                default,
                validate,
            }) => {
                let document = match document {
                    Some(document) => document.to_owned(),
                    None => default()?,
                };
                validate(&document)?;
                write_rendered_files(
                    plugin,
                    root,
                    &[RenderedFile {
                        relative_path: Path::new(path).to_path_buf(),
                        contents: document,
                    }],
                )?;
            }
            Some(ManagedConfig::LaunchEnv {
                env_name: _,
                default,
                validate,
            }) => {
                let document = match document {
                    Some(document) => document.to_owned(),
                    None => default()?,
                };
                validate(&document)?;
            }
            None => {}
        }
        Ok(())
    }

    /// Remove this Provider's shared configuration from the Agent workspace.
    fn remove_config(&self, root: &Path) -> Result<(), DiscussError> {
        if let Some(ManagedConfig::WorkspaceFile { plugin, path, .. }) = self.managed_config() {
            remove_file(plugin, root, Path::new(path))?;
        }
        Ok(())
    }

    /// Render one canonical agent without writing it.
    fn render_agent(
        &self,
        library: AssetLibrary,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<Vec<RenderedFile>, DiscussError>;

    /// Render one complete skill bundle without writing it.
    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError>;

    /// Label used for workspace I/O failures.
    fn workspace_label(&self) -> &'static str;

    /// Remove one canonical Agent from this provider's workspace.
    fn destroy_agent(&self, root: &Path, agent_id: &AgentId) -> Result<(), DiscussError>;

    /// Materialize a Team with actor-resolved settings for its members.
    fn install_team_with_member_overrides(
        &self,
        library: AssetLibrary,
        root: &Path,
        team: &TeamDefinition,
        member_settings: &BTreeMap<AgentId, ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let mut files = Vec::new();
        for id in std::iter::once(&team.leader).chain(&team.followers) {
            let settings = member_settings
                .get(id)
                .filter(|settings| !settings.is_empty());
            files.extend(self.render_agent(library, id, settings)?);
        }
        for id in &team.skills {
            files.extend(self.render_skill(resolve_skill(library, id)?)?);
        }
        write_rendered_files(self.workspace_label(), root, &files)
    }

    /// Materialize one Team member through the Team-only configuration path.
    ///
    /// Empty settings mean Default and must remain config-free.
    fn install_team_member(
        &self,
        library: AssetLibrary,
        root: &Path,
        agent_id: &AgentId,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let settings = member_settings.filter(|settings| !settings.is_empty());
        write_rendered_files(
            self.workspace_label(),
            root,
            &self.render_agent(library, agent_id, settings)?,
        )
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
    fn install_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        write_rendered_files(self.workspace_label(), root, &self.render_skill(skill)?)
    }

    /// Remove one canonical Skill bundle.
    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError>;

    /// Materialize an owned agent using the default workspace layout.
    fn install_custom_agent(
        &self,
        root: &Path,
        agent: &CustomAgentDefinition,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let file = render_agent_file(&AgentSelectionView::from_custom(agent), member_settings)?;
        write_rendered_files(DEFAULT_PLUGIN_NAME, root, &[file])
    }

    /// Materialize an owned skill using the default workspace layout.
    fn install_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let path = Path::new(DEFAULT_SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            let body = if file.relative_path == "SKILL.md" {
                markdown_metadata(&file.contents)?.2
            } else {
                &file.contents
            };
            let contents = render_skill_contents(
                DEFAULT_PLUGIN_NAME,
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
        write_rendered_files(DEFAULT_PLUGIN_NAME, root, &files)
    }

    /// Remove an owned skill using the default workspace layout.
    fn destroy_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let path = Path::new(DEFAULT_SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            remove_file(DEFAULT_PLUGIN_NAME, root, &path)?;
        }
        Ok(())
    }

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

/// Materialize a registered Team's assets under an explicit root (for example an
/// execution workspace), without touching the central Agent workspace or any
/// installation record. Members use default settings.
pub fn install_team_at_root(
    provider_name: &str,
    library: AssetLibrary,
    root: &Path,
    leader_id: &AgentId,
) -> Result<(), DiscussError> {
    let team = resolve_team(library, leader_id)?;
    for_agent(provider_name).install_team_with_member_overrides(library, root, team, &BTreeMap::new())
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

    use agent_client_protocol::v1::schema::{SessionConfigOption, SessionConfigSelectOption};
    use tempfile::tempdir;

    use super::super::library::{AgentId, SkillId, TeamDefinition};
    use super::super::test_library;
    use super::*;

    #[test]
    fn every_plugin_owns_its_primary_agent_selection() {
        let agent = test_library::library().agents_for().next().expect("agent");
        assert_eq!(
            for_agent(ANTIGRAVITY_PLUGIN_NAME).primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::LaunchArguments(vec!["--agent".to_string(), agent.id.to_string()])
        );
        assert_eq!(
            for_agent(CLAUDE_PLUGIN_NAME).primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::ConfigOption("agent")
        );
        assert_eq!(
            for_agent(CODEX_PLUGIN_NAME).primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::SystemPrompt
        );
        assert_eq!(
            for_agent(GROK_PLUGIN_NAME).primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::SessionMeta(serde_json::Map::from_iter([(
                "rules".to_string(),
                serde_json::Value::String(agent.body.to_string()),
            )]))
        );
        assert_eq!(
            for_agent(OPENCODE_PLUGIN_NAME).primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::ConfigOption("mode")
        );
        assert_eq!(
            for_agent("custom-acp").primary_agent_selection(&AgentSelectionView::from_builtin(agent)),
            PrimaryAgentSelection::SystemPrompt
        );
    }

    struct ProviderLayout {
        name: &'static str,
        agents_directory: &'static str,
        agent_extension: &'static str,
        skills_directory: &'static str,
    }

    const PROVIDER_LAYOUTS: [ProviderLayout; 5] = [
        ProviderLayout {
            name: ANTIGRAVITY_PLUGIN_NAME,
            agents_directory: ".agents/agents",
            agent_extension: "md",
            skills_directory: ".agents/skills",
        },
        ProviderLayout {
            name: CLAUDE_PLUGIN_NAME,
            agents_directory: ".claude/agents",
            agent_extension: "md",
            skills_directory: ".claude/skills",
        },
        ProviderLayout {
            name: CODEX_PLUGIN_NAME,
            agents_directory: ".codex/agents",
            agent_extension: "toml",
            skills_directory: ".agents/skills",
        },
        ProviderLayout {
            name: GROK_PLUGIN_NAME,
            agents_directory: ".grok/agents",
            agent_extension: "md",
            skills_directory: ".grok/skills",
        },
        ProviderLayout {
            name: OPENCODE_PLUGIN_NAME,
            agents_directory: ".opencode/agents",
            agent_extension: "md",
            skills_directory: ".opencode/skills",
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
            .install_config(root.path(), None)
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
            .install_team_member(library, directory.path(), &target.id, None)
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
            .install_team_member(library, directory.path(), &target.id, None)
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
            .find(|team| team.leader.as_ref() == "application-customizer")
            .expect("Application customizer team");
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
                .expect("Application customizer team installs");

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

    #[test]
    fn install_team_at_root_materializes_leader_and_followers_under_the_given_root_only() {
        let library = crate::discuss::test_library::library();
        let team = library
            .teams_for()
            .find(|team| !team.followers.is_empty())
            .expect("team with follower");
        let root = tempdir().expect("execution root");
        let central = tempdir().expect("central workspace");

        install_team_at_root("opencode", library, root.path(), &team.leader).expect("team installs at root");

        let leader_path = root
            .path()
            .join(".opencode/agents")
            .join(format!("{}.md", team.leader.normalized_destination()));
        assert!(
            leader_path.is_file(),
            "leader must be materialized in the execution root"
        );
        let follower_path = root
            .path()
            .join(".opencode/agents")
            .join(format!("{}.md", team.followers[0].normalized_destination()));
        assert!(
            follower_path.is_file(),
            "followers must be materialized in the execution root"
        );
        assert!(
            fs::read_dir(central.path())
                .expect("central entries")
                .next()
                .is_none(),
            "the central workspace must not be written"
        );
    }

    #[test]
    fn install_team_at_root_rejects_an_unregistered_leader() {
        let root = tempdir().expect("execution root");
        let missing = AgentId::new("no-such-leader").expect("canonical id");
        assert!(
            install_team_at_root(
                "opencode",
                crate::discuss::test_library::library(),
                root.path(),
                &missing
            )
            .is_err()
        );
    }
}

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::super::library::{
    AgentId, AgentSelectionView, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition, SkillDefinition, SkillId,
};
use super::super::types::{
    DiscussError, Effect, LaunchLimitation, LaunchLimitationReason, LaunchLimitationSubject, Operation,
    SessionLaunchConfig,
};
use super::files::{RenderedFile, write_rendered_files};
use super::launch;
use super::{
    BILLING_INPUT, BILLING_OUTPUT, BILLING_TOTAL, BillingCategorySemantics, ConfigSettings, ConnectionStrategy,
    DiscussAgentPlugin, LaunchContext, LaunchPreparation, ManagedConfig, PrimaryAgentSelection, render_skill_file,
    resolve_agent,
};

pub const CLAUDE_PLUGIN_NAME: &str = "claude-acp";

const AGENTS_DIRECTORY: &str = ".claude/agents";
const SKILLS_DIRECTORY: &str = ".claude/skills";
/// claude-agent-acp's session config option selecting the main-thread custom agent.
const AGENT_CONFIG_ID: &str = "agent";

/// Claude ACP Discuss integration.
pub struct ClaudePlugin;

pub static PLUGIN: ClaudePlugin = ClaudePlugin;

#[derive(Serialize)]
struct AgentFrontmatter<'a> {
    name: &'a str,
    description: &'a str,
    #[serde(flatten)]
    settings: BTreeMap<String, String>,
}

#[derive(Default, Serialize)]
struct Permissions {
    allow: Vec<&'static str>,
    ask: Vec<&'static str>,
    deny: Vec<&'static str>,
}

#[derive(Serialize)]
struct Settings {
    permissions: Permissions,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaudeOptions<'a> {
    settings: &'a Path,
    disallowed_tools: &'a [&'static str],
}

#[derive(Serialize)]
struct ClaudeCode<'a> {
    options: ClaudeOptions<'a>,
}

impl DiscussAgentPlugin for ClaudePlugin {
    fn managed_config(&self) -> Option<ManagedConfig> {
        Some(ManagedConfig::WorkspaceFile {
            plugin: CLAUDE_PLUGIN_NAME,
            path: ".claude/settings.json",
            default: || Ok("{}".to_owned()),
            validate: |document| super::json_object(CLAUDE_PLUGIN_NAME, document).map(|_| ()),
        })
    }
    fn billing_semantics(&self) -> BillingCategorySemantics {
        BillingCategorySemantics {
            guaranteed_categories: BILLING_INPUT | BILLING_OUTPUT | BILLING_TOTAL,
            input_includes_cache: Some(false),
        }
    }

    fn models_dev_provider_id(&self) -> Option<&'static str> {
        Some("anthropic")
    }

    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Shared
    }

    fn materializes_session_config(&self) -> bool {
        true
    }

    fn launch_preparation(
        &self,
        context: &LaunchContext,
        config: &SessionLaunchConfig,
    ) -> Result<LaunchPreparation, DiscussError> {
        launch::validate_supplied_inputs(config)?;
        let limitations = config
            .plugin_references
            .iter()
            .enumerate()
            .map(|(index, _)| LaunchLimitation {
                subject: LaunchLimitationSubject::PluginReference { index },
                reason: LaunchLimitationReason::UnsupportedPluginReference,
            })
            .collect();
        let Some(rules) = launch::intersect_rules(&config.rule_sources) else {
            return Ok(LaunchPreparation {
                meta: None,
                limitations,
            });
        };
        let mut permissions = Permissions::default();
        for rule in rules.rules {
            let tools: &[&str] = match rule.operation {
                Operation::Read => &["Read", "Glob", "Grep"],
                Operation::Write => &["Edit", "Write", "NotebookEdit"],
                Operation::Execute => &["Bash", "PowerShell"],
            };
            match rule.effect {
                Effect::Allow => &mut permissions.allow,
                Effect::Ask => &mut permissions.ask,
                Effect::Deny => &mut permissions.deny,
            }
            .extend_from_slice(tools);
        }
        let settings_path = context.settings_root.join("settings.json");
        let serialize_error = |error: serde_json::Error| DiscussError::WorkspaceSerialize {
            plugin: CLAUDE_PLUGIN_NAME.to_owned(),
            path: settings_path.to_string_lossy().into_owned(),
            format: "json".to_owned(),
            message: error.to_string(),
        };
        let options = ClaudeCode {
            options: ClaudeOptions {
                settings: &settings_path,
                disallowed_tools: &permissions.deny,
            },
        };
        let value = serde_json::to_value(options).map_err(serialize_error)?;
        let contents = serde_json::to_string(&Settings { permissions }).map_err(serialize_error)?;
        std::fs::create_dir_all(&context.settings_root).map_err(|error| DiscussError::WorkspaceIo {
            plugin: CLAUDE_PLUGIN_NAME.to_owned(),
            path: context.settings_root.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
        write_rendered_files(
            CLAUDE_PLUGIN_NAME,
            &context.settings_root,
            &[RenderedFile {
                relative_path: PathBuf::from("settings.json"),
                contents,
            }],
        )?;
        Ok(LaunchPreparation {
            meta: Some([(String::from("claudeCode"), value)].into_iter().collect()),
            limitations,
        })
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.claude-acp"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.claude-acp"
    }

    /// claude-agent-acp lists `.claude/agents` in its `agent` option and runs the main thread as the selection.
    fn primary_agent_selection(&self, _agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        PrimaryAgentSelection::ConfigOption(AGENT_CONFIG_ID)
    }

    fn workspace_label(&self) -> &'static str {
        CLAUDE_PLUGIN_NAME
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
        let relative_path = Path::new(AGENTS_DIRECTORY).join(format!("{}.md", agent_id.normalized_destination()));
        super::remove_file(CLAUDE_PLUGIN_NAME, root, &relative_path)
    }

    fn installed_agent_path(&self, id: &AgentId) -> PathBuf {
        Path::new(AGENTS_DIRECTORY).join(format!("{}.md", id.normalized_destination()))
    }

    fn installed_skill_path(&self, id: &SkillId) -> PathBuf {
        Path::new(SKILLS_DIRECTORY)
            .join(id.normalized_destination())
            .join("SKILL.md")
    }

    fn install_custom_agent(
        &self,
        root: &Path,
        agent: &CustomAgentDefinition,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let file = render_agent(&AgentSelectionView::from_custom(agent), member_settings)?;
        write_rendered_files(CLAUDE_PLUGIN_NAME, root, &[file])
    }

    fn install_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            let body = if file.relative_path == "SKILL.md" {
                super::markdown_metadata(&file.contents)?.2
            } else {
                &file.contents
            };
            let contents = super::render_skill_contents(
                CLAUDE_PLUGIN_NAME,
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
        write_rendered_files(CLAUDE_PLUGIN_NAME, root, &files)
    }

    fn destroy_custom_skill(&self, root: &Path, skill: &CustomSkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(&file.relative_path);
            super::remove_file(CLAUDE_PLUGIN_NAME, root, &path)?;
        }
        Ok(())
    }

    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        let mut files = Vec::new();
        for file in &skill.bundle {
            let relative_path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(CLAUDE_PLUGIN_NAME, &relative_path, skill, file)?;
            files.push(RenderedFile {
                relative_path,
                contents,
            });
        }
        Ok(files)
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        for file in &skill.bundle {
            let relative_path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            super::remove_file(CLAUDE_PLUGIN_NAME, root, &relative_path)?;
        }
        Ok(())
    }
}

fn render_agent(
    agent: &AgentSelectionView<'_>,
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
        plugin: CLAUDE_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        format: "yaml".to_string(),
        message: source.to_string(),
    })?;
    Ok(RenderedFile {
        relative_path: path.to_path_buf(),
        contents: format!("---\n{yaml}---\n\n{body}"),
    })
}

#[cfg(test)]
mod tests {
    use super::super::super::test_library;
    use super::super::super::types::{DiscussSessionId, Rule, RuleSet};
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn session_metadata_and_settings_are_independent() {
        let directory = tempdir().expect("workspace");
        let first = LaunchContext::new(directory.path(), DiscussSessionId::new());
        let second = LaunchContext::new(directory.path(), DiscussSessionId::new());
        let config = |effect| SessionLaunchConfig {
            rule_sources: vec![RuleSet {
                rules: vec![Rule {
                    operation: Operation::Read,
                    effect,
                }],
            }],
            ..Default::default()
        };
        let first_meta = PLUGIN
            .launch_preparation(&first, &config(Effect::Allow))
            .expect("first")
            .meta
            .expect("meta");
        let second_meta = PLUGIN
            .launch_preparation(&second, &config(Effect::Deny))
            .expect("second")
            .meta
            .expect("meta");
        let first_path = first.settings_root.join("settings.json");
        let second_path = second.settings_root.join("settings.json");
        assert_ne!(first_path, second_path);
        assert_eq!(
            first_meta["claudeCode"]["options"]["settings"],
            serde_json::json!(first_path)
        );
        assert_eq!(
            second_meta["claudeCode"]["options"]["settings"],
            serde_json::json!(second_path)
        );
        assert!(
            !first_meta["claudeCode"]["options"]["disallowedTools"]
                .as_array()
                .expect("first denied tools")
                .contains(&serde_json::json!("Read"))
        );
        assert!(
            second_meta["claudeCode"]["options"]["disallowedTools"]
                .as_array()
                .expect("second denied tools")
                .contains(&serde_json::json!("Read"))
        );
        let first_settings: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(first_path).expect("first settings")).expect("json");
        let second_settings: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(second_path).expect("second settings")).expect("json");
        assert!(
            first_settings["permissions"]["allow"]
                .as_array()
                .expect("allow")
                .contains(&serde_json::json!("Read"))
        );
        assert!(
            second_settings["permissions"]["deny"]
                .as_array()
                .expect("deny")
                .contains(&serde_json::json!("Read"))
        );
        assert!(PLUGIN.materializes_session_config());
        assert!(
            PLUGIN
                .materialize_session_config(directory.path(), &[], &config(Effect::Deny))
                .expect("no root config")
                .is_empty()
        );
        assert!(!directory.path().join(".claude/settings.json").exists());
        assert!(!directory.path().join(".mcp.json").exists());
    }

    #[test]
    fn each_effect_maps_every_operation_group() {
        let directory = tempdir().expect("workspace");
        let context = LaunchContext::new(directory.path(), DiscussSessionId::new());
        let tools = serde_json::json!([
            "Read",
            "Glob",
            "Grep",
            "Edit",
            "Write",
            "NotebookEdit",
            "Bash",
            "PowerShell"
        ]);
        for (effect, field) in [(Effect::Allow, "allow"), (Effect::Ask, "ask"), (Effect::Deny, "deny")] {
            let config = SessionLaunchConfig {
                rule_sources: vec![RuleSet {
                    rules: [Operation::Read, Operation::Write, Operation::Execute]
                        .into_iter()
                        .map(|operation| Rule { operation, effect })
                        .collect(),
                }],
                ..Default::default()
            };
            PLUGIN
                .launch_preparation(&context, &config)
                .expect("prepare");
            let settings: serde_json::Value = serde_json::from_str(
                &fs::read_to_string(context.settings_root.join("settings.json")).expect("settings"),
            )
            .expect("json");
            assert_eq!(settings["permissions"][field], tools);
        }
    }

    #[test]
    fn permissions_map_effects_and_denied_tools_to_session_settings() {
        let directory = tempdir().expect("workspace");
        let context = LaunchContext::new(directory.path(), DiscussSessionId::new());
        let config = SessionLaunchConfig {
            rule_sources: vec![RuleSet {
                rules: vec![
                    Rule {
                        operation: Operation::Read,
                        effect: Effect::Allow,
                    },
                    Rule {
                        operation: Operation::Write,
                        effect: Effect::Ask,
                    },
                    Rule {
                        operation: Operation::Execute,
                        effect: Effect::Deny,
                    },
                ],
            }],
            plugin_references: vec!["opaque".into(), "other".into()],
            ..Default::default()
        };
        let result = PLUGIN
            .launch_preparation(&context, &config)
            .expect("prepare");
        let path = context.settings_root.join("settings.json");
        let settings: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("settings")).expect("json");
        assert_eq!(
            settings,
            serde_json::json!({"permissions": {
                "allow": ["Read", "Glob", "Grep"],
                "ask": ["Edit", "Write", "NotebookEdit"],
                "deny": ["Bash", "PowerShell"]
            }})
        );
        let meta = result.meta.expect("metadata");
        assert_eq!(
            meta["claudeCode"]["options"],
            serde_json::json!({
                "settings": path, "disallowedTools": ["Bash", "PowerShell"]
            })
        );
        assert_eq!(result.limitations.len(), 2);
        for (index, limitation) in result.limitations.iter().enumerate() {
            assert_eq!(limitation.subject, LaunchLimitationSubject::PluginReference { index });
            assert_eq!(limitation.reason, LaunchLimitationReason::UnsupportedPluginReference);
        }
        assert!(!directory.path().join("settings.json").exists());
        assert!(!directory.path().join(".claude/settings.json").exists());
        assert_eq!(PLUGIN.connection_strategy(), ConnectionStrategy::Shared);
    }

    #[test]
    fn no_sources_inherit_but_assigned_empty_source_denies_all() {
        let directory = tempdir().expect("workspace");
        let context = LaunchContext::new(directory.path(), DiscussSessionId::new());
        let result = PLUGIN
            .launch_preparation(&context, &SessionLaunchConfig::default())
            .expect("prepare");
        assert!(result.meta.is_none());
        assert!(!context.settings_root.exists());
        let config = SessionLaunchConfig {
            rule_sources: vec![RuleSet::default()],
            ..Default::default()
        };
        let result = PLUGIN
            .launch_preparation(&context, &config)
            .expect("prepare");
        let settings: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(context.settings_root.join("settings.json")).expect("settings"))
                .expect("json");
        let denied = serde_json::json!([
            "Read",
            "Glob",
            "Grep",
            "Edit",
            "Write",
            "NotebookEdit",
            "Bash",
            "PowerShell"
        ]);
        assert_eq!(settings["permissions"]["deny"], denied);
        assert_eq!(
            result.meta.expect("metadata")["claudeCode"]["options"]["disallowedTools"],
            denied
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
        let skill_path = directory.path().join(".claude/skills/keep.md");
        let command_path = directory.path().join(".claude/commands/keep.md");
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

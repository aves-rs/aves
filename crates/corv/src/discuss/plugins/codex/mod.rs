use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

use super::super::DiscussError;
use super::super::library::{AgentId, AgentSelectionView, AssetLibrary, CustomAgentDefinition, SkillDefinition};
use super::super::types::{
    DiscussConfigOption, Effect, LaunchLimitation, LaunchLimitationReason, LaunchLimitationSubject,
    LaunchPermissionOutcome, Operation, SessionLaunchConfig,
};
use super::files::{RenderedFile, write_rendered_files};
use super::launch::{self, LaunchContext, LaunchPreparation, LaunchSelections};
use super::{
    BILLING_CACHED_READ, BILLING_CACHED_WRITE, BILLING_INPUT, BILLING_OUTPUT, BILLING_THOUGHT, BILLING_TOTAL,
    BillingCategorySemantics, ConfigSettings, ConnectionStrategy, DefaultAgentPlugin, DiscussAgentPlugin,
    ManagedConfig, PrimaryAgentSelection, destroy_default_skill, resolve_agent,
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

impl DiscussAgentPlugin for CodexPlugin {
    fn managed_config(&self) -> Option<ManagedConfig> {
        Some(ManagedConfig::LaunchEnv {
            env_name: "CODEX_CONFIG",
            default: || Ok("{}".to_owned()),
            validate: |document| super::json_object(CODEX_PLUGIN_NAME, document).map(|_| ()),
        })
    }
    fn billing_semantics(&self) -> BillingCategorySemantics {
        BillingCategorySemantics {
            guaranteed_categories: BILLING_INPUT
                | BILLING_OUTPUT
                | BILLING_THOUGHT
                | BILLING_CACHED_READ
                | BILLING_CACHED_WRITE
                | BILLING_TOTAL,
            input_includes_cache: Some(false),
        }
    }

    fn models_dev_provider_id(&self) -> Option<&'static str> {
        Some("openai")
    }

    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Shared
    }

    fn launch_preparation(
        &self,
        _context: &LaunchContext,
        config: &SessionLaunchConfig,
    ) -> Result<LaunchPreparation, DiscussError> {
        launch::validate_supplied_inputs(config)?;
        let limitations = (0..config.plugin_references.len())
            .map(|index| LaunchLimitation {
                subject: LaunchLimitationSubject::PluginReference { index },
                reason: LaunchLimitationReason::UnsupportedPluginReference,
            })
            .collect();
        Ok(LaunchPreparation {
            meta: None,
            limitations,
        })
    }

    fn launch_option_selections(
        &self,
        config: &SessionLaunchConfig,
        options: &[DiscussConfigOption],
    ) -> LaunchSelections {
        let Some(rules) = launch::intersect_rules(&config.rule_sources) else {
            return LaunchSelections::default();
        };
        let mode =
            if rules.rules.iter().any(|rule| {
                matches!(rule.operation, Operation::Write | Operation::Execute) && rule.effect == Effect::Allow
            }) {
                "agent"
            } else {
                "read-only"
            };
        let option = options
            .iter()
            .find(|option| option.id == "mode" && option.options.iter().any(|value| value.value == mode));
        let mut selections = LaunchSelections::default();
        if let Some(option) = option {
            selections
                .settings
                .insert(option.id.clone(), mode.to_owned());
        }
        for rule in rules.rules {
            let (actual, reason) = if option.is_none() {
                (
                    LaunchPermissionOutcome::Inherited,
                    Some(LaunchLimitationReason::UnavailableAdvertisedOption),
                )
            } else {
                let actual_effect = match rule.operation {
                    Operation::Read => Effect::Allow,
                    Operation::Execute => Effect::Ask,
                    Operation::Write if mode == "agent" => Effect::Allow,
                    Operation::Write => Effect::Deny,
                };
                let reason = if rule.operation == Operation::Read && rule.effect != Effect::Allow {
                    Some(LaunchLimitationReason::UnsupportedNativeControl)
                } else if rule.effect != actual_effect {
                    Some(LaunchLimitationReason::ModeCoupling)
                } else {
                    None
                };
                (LaunchPermissionOutcome::Mode(mode.to_owned()), reason)
            };
            if let Some(reason) = reason {
                selections.limitations.push(LaunchLimitation {
                    subject: LaunchLimitationSubject::Permission {
                        operation: rule.operation,
                        requested: rule.effect,
                        actual,
                    },
                    reason,
                });
            }
        }
        selections
    }

    fn agent_scope_key(&self) -> &'static str {
        "plugin.codex-acp"
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.default"
    }

    fn installed_agent_path(&self, id: &AgentId) -> PathBuf {
        Path::new(AGENTS_DIRECTORY).join(format!("{}.toml", id.normalized_destination()))
    }

    fn append_agent_instructions(&self, rendered: &str, instructions: &str) -> Result<String, DiscussError> {
        let error = |message: String| DiscussError::WorkspaceSerialize {
            plugin: CODEX_PLUGIN_NAME.to_owned(),
            path: AGENTS_DIRECTORY.to_owned(),
            format: "toml".to_owned(),
            message,
        };
        let mut document: toml::Value = toml::from_str(rendered).map_err(|source| error(source.to_string()))?;
        let Some(toml::Value::String(body)) = document.get_mut("developer_instructions") else {
            return Err(error("developer_instructions must be a string".to_owned()));
        };
        body.push_str("\n\n");
        body.push_str(instructions);
        toml::to_string(&document).map_err(|source| error(source.to_string()))
    }

    /// codex-acp has no main-agent picker; it applies an appended ACP system prompt as developer instructions.
    fn primary_agent_selection(&self, _agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        PrimaryAgentSelection::SystemPrompt
    }

    fn workspace_label(&self) -> &'static str {
        CODEX_PLUGIN_NAME
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

    fn render_skill(&self, skill: &SkillDefinition) -> Result<Vec<RenderedFile>, DiscussError> {
        DefaultAgentPlugin::render_portable_skill(skill)
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

    fn install_custom_agent(
        &self,
        root: &Path,
        agent: &CustomAgentDefinition,
        member_settings: Option<&ConfigSettings>,
    ) -> Result<(), DiscussError> {
        let file = render_agent(&AgentSelectionView::from_custom(agent), member_settings)?;
        write_rendered_files(CODEX_PLUGIN_NAME, root, &[file])
    }

    fn destroy_skill(&self, root: &Path, skill: &SkillDefinition) -> Result<(), DiscussError> {
        destroy_default_skill(root, skill)
    }
}

fn render_agent(
    agent: &AgentSelectionView<'_>,
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
    Ok(RenderedFile {
        relative_path: path,
        contents,
    })
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
    use super::super::super::types::{DiscussConfigOptionValue, DiscussSessionId, Rule, RuleSet};
    use super::*;
    use agent_client_protocol::v1::schema::{McpServer, McpServerStdio};
    use tempfile::tempdir;

    fn config(read: Effect, write: Effect, execute: Effect) -> SessionLaunchConfig {
        SessionLaunchConfig {
            rule_sources: vec![RuleSet {
                rules: vec![
                    Rule {
                        operation: Operation::Read,
                        effect: read,
                    },
                    Rule {
                        operation: Operation::Write,
                        effect: write,
                    },
                    Rule {
                        operation: Operation::Execute,
                        effect: execute,
                    },
                ],
            }],
            ..Default::default()
        }
    }

    fn mode_option(values: &[&str]) -> DiscussConfigOption {
        DiscussConfigOption {
            id: "mode".into(),
            name: "Mode".into(),
            category: "mode".into(),
            value: "agent-full-access".into(),
            options: values
                .iter()
                .map(|value| DiscussConfigOptionValue {
                    value: (*value).into(),
                    label: (*value).into(),
                })
                .collect(),
        }
    }

    #[test]
    fn selects_safe_modes_and_reports_all_effect_differences() {
        let effects = [Effect::Allow, Effect::Ask, Effect::Deny];
        let option = mode_option(&["read-only", "agent", "agent-full-access"]);
        for read in effects {
            for write in effects {
                for execute in effects {
                    let expected = if write == Effect::Allow || execute == Effect::Allow {
                        "agent"
                    } else {
                        "read-only"
                    };
                    let result =
                        PLUGIN.launch_option_selections(&config(read, write, execute), std::slice::from_ref(&option));
                    assert_eq!(result.settings.get("mode").map(String::as_str), Some(expected));
                    let mut limitations = Vec::new();
                    for (operation, requested, actual) in [
                        (Operation::Read, read, Effect::Allow),
                        (
                            Operation::Write,
                            write,
                            if expected == "agent" {
                                Effect::Allow
                            } else {
                                Effect::Deny
                            },
                        ),
                        (Operation::Execute, execute, Effect::Ask),
                    ] {
                        if requested != actual {
                            limitations.push(LaunchLimitation {
                                subject: LaunchLimitationSubject::Permission {
                                    operation,
                                    requested,
                                    actual: LaunchPermissionOutcome::Mode(expected.into()),
                                },
                                reason: if operation == Operation::Read {
                                    LaunchLimitationReason::UnsupportedNativeControl
                                } else {
                                    LaunchLimitationReason::ModeCoupling
                                },
                            });
                        }
                    }
                    assert_eq!(result.limitations, limitations);
                }
            }
        }
    }

    #[test]
    fn missing_mode_or_value_preserves_inherited_mode() {
        let config = config(Effect::Ask, Effect::Allow, Effect::Deny);
        for options in [vec![], vec![mode_option(&["read-only", "agent-full-access"])]] {
            let result = PLUGIN.launch_option_selections(&config, &options);
            assert!(result.settings.is_empty());
            assert_eq!(result.limitations.len(), 3);
            for (limitation, (operation, requested)) in result.limitations.iter().zip([
                (Operation::Read, Effect::Ask),
                (Operation::Write, Effect::Allow),
                (Operation::Execute, Effect::Deny),
            ]) {
                assert_eq!(
                    limitation.subject,
                    LaunchLimitationSubject::Permission {
                        operation,
                        requested,
                        actual: LaunchPermissionOutcome::Inherited
                    }
                );
                assert_eq!(limitation.reason, LaunchLimitationReason::UnavailableAdvertisedOption);
            }
        }
    }

    #[test]
    fn no_sources_preserves_mode_without_limitations() {
        let result =
            PLUGIN.launch_option_selections(&SessionLaunchConfig::default(), &[mode_option(&["agent", "read-only"])]);
        assert!(result.settings.is_empty());
        assert!(result.limitations.is_empty());
    }

    #[test]
    fn preparation_only_reports_plugin_references() {
        let root = Path::new("workspace");
        let context = LaunchContext::new(root, DiscussSessionId::new());
        let mut config = config(Effect::Ask, Effect::Allow, Effect::Deny);
        config.plugin_references = vec!["one".into(), "two".into()];
        let prepared = PLUGIN
            .launch_preparation(&context, &config)
            .expect("preparation");
        assert!(prepared.meta.is_none());
        assert_eq!(prepared.limitations.len(), 2);
        for (index, limitation) in prepared.limitations.iter().enumerate() {
            assert_eq!(limitation.subject, LaunchLimitationSubject::PluginReference { index });
            assert_eq!(limitation.reason, LaunchLimitationReason::UnsupportedPluginReference);
        }
        config.plugin_references.push("one".into());
        assert!(PLUGIN.launch_preparation(&context, &config).is_err());
        assert_eq!(PLUGIN.connection_strategy(), ConnectionStrategy::Shared);
    }

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
            .install_team_member(library, directory.path(), &agent.id, None)
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
            .install_team_member(library, directory.path(), &agent.id, None)
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

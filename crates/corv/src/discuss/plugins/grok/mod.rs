use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::super::error::DiscussError;
use super::super::library::{
    AgentId, AgentRole, AgentSelectionView, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition,
    SkillDefinition, SkillId, TeamDefinition,
};
use super::super::report_types::{DiscussHarnessCostKind, DiscussUsageEvidenceSource};
use super::super::types::{DiscussConfigOption, DiscussConfigOptionValue};
use super::super::types::{DiscussSubagentSurfaceState, DiscussUtilityPayload};
use super::super::usage::{DiscussUsageEvidence, ModelUsage, UsageCaptureEvent, capture_standard_usage};
use super::files::{RenderedFile, write_rendered_files};
use super::subagents::{SubagentObservation, SubagentTracker};
use super::{
    BILLING_CACHED_READ, BILLING_CACHED_WRITE, BILLING_INPUT, BILLING_OUTPUT, BILLING_THOUGHT, BILLING_TOTAL,
    BillingCategorySemantics, ConfigSettings, ConnectionStrategy, DiscussAgentPlugin, ManagedConfig,
    PluginCaptureState, PrimaryAgentSelection, markdown_metadata, render_skill_contents, render_skill_file,
    resolve_agent, resolve_skill,
};

pub const GROK_PLUGIN_NAME: &str = "grok-build";

/// Grok's subagent lifecycle on the parent session: `subagent_spawned` (with `subagent_type`,
/// `description`, `child_session_id`), `subagent_progress` while running, and `subagent_finished`
/// with `status` `completed` | `failed` | `cancelled`.
#[derive(Deserialize)]
struct GrokSubagentUpdate {
    #[serde(rename = "sessionUpdate")]
    kind: GrokSubagentKind,
    subagent_id: Option<String>,
    child_session_id: Option<String>,
    subagent_type: Option<String>,
    description: Option<String>,
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GrokPromptComplete {
    usage: Option<Value>,
    model_id: Option<String>,
}

#[derive(Deserialize)]
enum GrokSubagentKind {
    #[serde(rename = "subagent_spawned")]
    Spawned,
    #[serde(rename = "subagent_progress")]
    Progress,
    #[serde(rename = "subagent_finished")]
    Finished,
}

fn capture_subagent_notification(params: &Value, tracker: &mut SubagentTracker) {
    let Some(update) = params
        .get("update")
        .and_then(|value| serde_json::from_value::<GrokSubagentUpdate>(value.clone()).ok())
    else {
        return;
    };
    let state = match update.kind {
        GrokSubagentKind::Spawned | GrokSubagentKind::Progress => DiscussSubagentSurfaceState::Live,
        GrokSubagentKind::Finished => match update.status.as_deref() {
            Some("completed") => DiscussSubagentSurfaceState::Done,
            _ => DiscussSubagentSurfaceState::Error,
        },
    };
    let Some(capture_id) = update
        .subagent_id
        .as_deref()
        .or(update.child_session_id.as_deref())
    else {
        return;
    };
    tracker.observe(SubagentObservation {
        capture_id,
        child_session_id: update.child_session_id.as_deref(),
        agent_name: update.subagent_type.as_deref(),
        description: update.description.as_deref(),
        state: Some(state),
    });
}

const AGENTS_DIRECTORY: &str = ".grok/agents";
const SKILLS_DIRECTORY: &str = ".grok/skills";

const GROK_SESSION_CONFIG_META_KEY: &str = "x.ai/sessionConfig";
/// `session/new` `_meta` field whose text Grok appends to the session system prompt.
const RULES_META_KEY: &str = "rules";
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
    fn managed_config(&self) -> Option<ManagedConfig> {
        Some(ManagedConfig::WorkspaceFile {
            plugin: GROK_PLUGIN_NAME,
            path: ".grok/config.toml",
            default: || Ok(String::new()),
            validate: validate_managed_config,
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
            input_includes_cache: Some(true),
            output_excludes_thought: false,
        }
    }

    fn models_dev_provider_id(&self) -> Option<&'static str> {
        Some("xai")
    }

    fn capture_usage_evidence(&self, event: &UsageCaptureEvent<'_>) -> Option<DiscussUsageEvidence> {
        if let UsageCaptureEvent::Notification { method, params } = event
            && *method == "x.ai/session/prompt_complete"
        {
            let complete: GrokPromptComplete = serde_json::from_value((*params).clone()).ok()?;
            let Some(usage) = complete.usage.as_ref() else {
                tracing::debug!("Grok prompt completion has no usage");
                return None;
            };
            let evidence = capture_grok_usage(
                usage,
                complete.model_id.as_deref(),
                DiscussUsageEvidenceSource::Complete,
            );
            if evidence.is_none() {
                tracing::debug!("Unrecognized Grok usage payload");
            }
            return evidence;
        }
        let (usage, model_id, source) = match event {
            UsageCaptureEvent::PromptResponse(response) => {
                let Some(meta) = response.meta.as_ref() else {
                    return capture_standard_usage(event);
                };
                let Some(usage) = meta.get("usage") else {
                    return capture_standard_usage(event);
                };
                (
                    usage,
                    meta.get("modelId").and_then(Value::as_str),
                    DiscussUsageEvidenceSource::ResponseMeta,
                )
            }
            _ => return capture_standard_usage(event),
        };
        let evidence = capture_grok_usage(usage, model_id, source);
        if evidence.is_none() {
            tracing::debug!("Unrecognized Grok usage payload");
        }
        evidence
    }

    fn connection_strategy(&self) -> ConnectionStrategy {
        ConnectionStrategy::Isolated
    }

    // Grok Build announces subagents through its ungated `x.ai/session_notification` extension
    // (xai-org/grok-build `extensions/notification.rs`), not through ACP `session/update`.
    // No child-session load guarantee is verified, so child sessions are not loaded.
    fn agent_scope_key(&self) -> &'static str {
        "plugin.grok-build"
    }

    fn capture_extension_notification(
        &self,
        method: &str,
        params: &Value,
        state: &mut PluginCaptureState,
    ) -> Option<DiscussUtilityPayload> {
        let PluginCaptureState::Subagents(tracker) = state else {
            return None;
        };
        if method.trim_start_matches('_') == "x.ai/session_notification" || method == "x.ai/session/update" {
            capture_subagent_notification(params, tracker);
        }
        tracker.next()
    }

    fn skill_scope_key(&self) -> &'static str {
        "plugin.grok-build"
    }

    /// Grok's `_meta.agentProfile` ignores project-only agents, so the leader's instructions go
    /// through `_meta.rules`, which Grok appends to the session system prompt.
    fn primary_agent_selection(&self, agent: &AgentSelectionView<'_>) -> PrimaryAgentSelection {
        PrimaryAgentSelection::SessionMeta(Map::from_iter([(
            RULES_META_KEY.to_string(),
            Value::String(agent.body.to_string()),
        )]))
    }

    fn workspace_label(&self) -> &'static str {
        GROK_PLUGIN_NAME
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
        let path = root
            .join(AGENTS_DIRECTORY)
            .join(format!("{}.md", agent_id.normalized_destination()));
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(capture_write_error(&path, error)),
        }
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
        Path::new(SKILLS_DIRECTORY)
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
        write_rendered_files(GROK_PLUGIN_NAME, root, &[file])
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
                GROK_PLUGIN_NAME,
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
        write_rendered_files(GROK_PLUGIN_NAME, root, &files)
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
            let relative_path = Path::new(SKILLS_DIRECTORY)
                .join(skill.id.normalized_destination())
                .join(file.relative_path);
            let contents = render_skill_file(GROK_PLUGIN_NAME, &relative_path, skill, file)?;
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
            remove_file(root, &relative_path)?;
        }
        Ok(())
    }

    fn extension_config_options(&self, metadata: &Map<String, Value>) -> Option<Vec<DiscussConfigOption>> {
        grok_session_config(metadata).and_then(grok_config_options)
    }
}

fn validate_managed_config(document: &str) -> Result<(), DiscussError> {
    let table: toml::Table =
        document
            .parse()
            .map_err(|error: toml::de::Error| DiscussError::ManagedConfigValidation {
                plugin: GROK_PLUGIN_NAME.to_owned(),
                reason: error.to_string(),
            })?;
    if table.iter().any(|(key, value)| {
        key != "mcp_servers"
            || !matches!(value, toml::Value::Table(servers) if servers.values().all(|entry| matches!(entry, toml::Value::Table(_))))
    }) {
        return Err(DiscussError::ManagedConfigValidation {
            plugin: GROK_PLUGIN_NAME.to_owned(),
            reason: "only the mcp_servers table is allowed".to_owned(),
        });
    }
    Ok(())
}

fn capture_grok_usage(
    usage: &Value,
    model_id: Option<&str>,
    source: DiscussUsageEvidenceSource,
) -> Option<DiscussUsageEvidence> {
    let object = usage.as_object()?;
    let flag = |key| match object.get(key) {
        Some(value) => value.as_bool(),
        None => Some(false),
    };
    let usage_is_incomplete = flag("usageIsIncomplete")?;
    let cost_is_partial = flag("costIsPartial")?;
    let cost = match object.get("costUsdTicks") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let ticks = value.as_i64()?;
            let magnitude = ticks.unsigned_abs();
            let sign = if ticks < 0 { "-" } else { "" };
            Some((
                format!(
                    "{sign}{}.{:010}",
                    magnitude / 10_000_000_000,
                    magnitude % 10_000_000_000
                ),
                "USD".to_string(),
                DiscussHarnessCostKind::PerTurn,
            ))
        }
    };
    let total_model = if [
        "inputTokens",
        "outputTokens",
        "reasoningTokens",
        "cachedReadTokens",
        "cacheCreationTokens",
        "totalTokens",
    ]
    .iter()
    .any(|key| object.contains_key(*key))
    {
        Some(grok_model_usage(usage, model_id)?)
    } else {
        None
    };
    let models = match object.get("modelUsage") {
        Some(value) => {
            let entries = value.as_object()?;
            if entries.is_empty() {
                total_model.into_iter().collect()
            } else {
                entries
                    .iter()
                    .map(|(id, value)| grok_model_usage(value, Some(id)))
                    .collect::<Option<Vec<_>>>()?
            }
        }
        None => total_model.into_iter().collect(),
    };
    if models.is_empty() && cost.is_none() && !usage_is_incomplete && !cost_is_partial {
        return None;
    }
    Some(DiscussUsageEvidence {
        source,
        models,
        context_used_tokens: None,
        context_window_tokens: None,
        cost,
        usage_is_incomplete,
        cost_is_partial,
        turn_correlatable: true,
    })
}

fn grok_model_usage(value: &Value, model_id: Option<&str>) -> Option<ModelUsage> {
    let object = value.as_object()?;
    let keys = [
        "inputTokens",
        "outputTokens",
        "reasoningTokens",
        "cachedReadTokens",
        "cacheCreationTokens",
        "totalTokens",
    ];
    if !keys.iter().any(|key| object.contains_key(*key)) {
        return None;
    }
    let count = |key| match object.get(key) {
        Some(value) => value.as_u64().map(Some),
        None => Some(None),
    };
    Some(ModelUsage {
        model_id: model_id.map(str::to_owned),
        input_tokens: count("inputTokens")?,
        output_tokens: count("outputTokens")?,
        thought_tokens: count("reasoningTokens")?,
        cached_read_tokens: count("cachedReadTokens")?,
        cached_write_tokens: count("cacheCreationTokens")?,
        total_tokens: count("totalTokens")?,
    })
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
        plugin: GROK_PLUGIN_NAME.to_string(),
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
        plugin: GROK_PLUGIN_NAME.to_string(),
        path: path.to_string_lossy().into_owned(),
        message: error.to_string(),
    }
}

fn grok_session_config(metadata: &Map<String, Value>) -> Option<&Value> {
    metadata.get(GROK_SESSION_CONFIG_META_KEY).or_else(|| {
        metadata
            .get("_meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get(GROK_SESSION_CONFIG_META_KEY))
    })
}

fn grok_config_options(value: &Value) -> Option<Vec<DiscussConfigOption>> {
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

    #[test]
    fn subagent_notifications_report_the_lifecycle() {
        let mut state = PluginCaptureState::Subagents(SubagentTracker::default());
        let mut capture = |method: &str, update: serde_json::Value| match PLUGIN.capture_extension_notification(
            method,
            &serde_json::json!({"sessionId": "p", "update": update}),
            &mut state,
        ) {
            Some(DiscussUtilityPayload::Subagent { subagent }) => Some((subagent.agent_name, subagent.surface_state)),
            _ => None,
        };
        let spawned = serde_json::json!({"sessionUpdate": "subagent_spawned", "subagent_id": "c1",
            "child_session_id": "c1", "subagent_type": "explore", "description": "Find auth"});
        assert_eq!(
            capture("_x.ai/session_notification", spawned),
            Some(("explore".to_string(), DiscussSubagentSurfaceState::Live))
        );
        let failed = serde_json::json!({"sessionUpdate": "subagent_finished", "subagent_id": "c1", "status": "failed"});
        assert_eq!(
            capture("x.ai/session_notification", failed),
            Some(("explore".to_string(), DiscussSubagentSurfaceState::Error))
        );
        let other = serde_json::json!({"sessionUpdate": "subagent_spawned", "subagent_id": "c2"});
        assert_eq!(capture("session/request_permission", other), None);
    }
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

    fn session_new_metadata(options: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        Map::from_iter([(
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

    #[test]
    fn default_team_install_preserves_grok_rendering_layout() {
        let library = test_library::library();
        let directory = tempdir().expect("temporary workspace");
        let team = library
            .teams_for()
            .find(|team| !team.skills.is_empty())
            .expect("team with skill");
        let settings = BTreeMap::from([(
            team.leader.clone(),
            ConfigSettings::from([(String::from("model"), String::from("leader-model"))]),
        )]);
        PLUGIN
            .install_team_with_member_overrides(library, directory.path(), team, &settings)
            .expect("team installs");

        for id in std::iter::once(&team.leader).chain(&team.followers) {
            let agent = resolve_agent(library, id).expect("registered agent");
            let path = directory
                .path()
                .join(AGENTS_DIRECTORY)
                .join(format!("{}.md", id.normalized_destination()));
            let contents = fs::read_to_string(path).expect("agent file");
            assert!(contents.starts_with("---\n"));
            assert!(contents.contains(&format!("name: {}\n", id.as_ref())));
            assert!(contents.ends_with(agent.body));
            assert_eq!(contents.contains("model: leader-model\n"), id == &team.leader);
        }
        for id in &team.skills {
            let skill = resolve_skill(library, id).expect("registered skill");
            for file in &skill.bundle {
                let path = Path::new(SKILLS_DIRECTORY)
                    .join(id.normalized_destination())
                    .join(file.relative_path);
                let contents = fs::read_to_string(directory.path().join(&path)).expect("skill file");
                assert_eq!(
                    contents,
                    render_skill_file(GROK_PLUGIN_NAME, &path, skill, file).expect("render skill")
                );
            }
        }
    }
}

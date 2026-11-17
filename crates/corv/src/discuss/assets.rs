use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use turso::Connection;

use super::actor::Actor;
use super::asset_sources::{AssetSource, fetch_asset_source};
use super::database::{retained_team_assets, with_install_compensation_context};
use super::library::{
    AgentId, AssetLibrary, CustomAgentDefinition, CustomSkillDefinition, CustomSkillFile, CustomTeamDefinition,
    ResolvedAgent, ResolvedSkill, ResolvedTeam, SkillId, bump_custom_version, initial_custom_version,
    validate_custom_skill_bundle,
};
use super::messages::{DiscussRequestPayload, DiscussResponse};
use super::plugins;
use super::storage::{
    AssetInstructionsRow, fail_custom_agent_pull, fail_custom_skill_pull, load_asset_instructions,
    upsert_asset_instructions,
};
use super::storage::{
    CustomAssetRow, CustomInstallationKind, CustomInstallationRow, CustomSourceKind, CustomTeamRow, DiscussSessionRow,
    delete_custom_agent, delete_custom_installation, delete_custom_skill, list_custom_agents,
    list_custom_installations, list_custom_skills, list_custom_teams, list_discuss_team_installations,
    load_custom_agent, load_custom_installation, load_custom_skill, load_custom_team, load_discuss_team_installation,
    upsert_custom_agent, upsert_custom_installation, upsert_custom_skill,
};
use super::types::InstalledAssetKind;
use super::types::{
    BuiltinSkillEntry, CustomAgentDetail, CustomAssetMetadata, CustomSkillBundleFile, CustomSkillDetail, DiscussError,
    DiscussMemberOverrides, DiscussSessionSetup, IncompleteTeamClaim, InstalledAssetClaim, InstalledClaimBasis,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// An import awaiting its network result.
pub(super) struct PendingImport {
    pub(super) kind: CustomInstallationKind,
    pub(super) id: String,
    pub(super) url: String,
    pub(super) reply_to: oneshot::Sender<Result<DiscussResponse, DiscussError>>,
    pub(super) abort: AbortHandle,
}

/// Validated identity and source for a custom import.
struct PreparedImport {
    kind: CustomInstallationKind,
    id: String,
    url: String,
}

/// Network result returned to the actor for serialized persistence.
pub(super) struct ImportCompletion {
    pub(super) key: u64,
    pub(super) result: Result<AssetSource, DiscussError>,
}

impl Actor {
    pub(super) fn setup_kind_for(setup: Option<&DiscussSessionSetup>) -> Option<&'static str> {
        match setup {
            Some(DiscussSessionSetup::TeamAtRoot { .. }) => Some("team_at_root"),
            Some(DiscussSessionSetup::Agent { .. }) => Some("agent"),
            _ => None,
        }
    }

    pub(super) fn restored_team_setup(row: &DiscussSessionRow) -> Option<DiscussSessionSetup> {
        let id = row.team_leader_id.clone()?;
        Some(match row.session_setup_kind.as_deref() {
            Some("team_at_root") => DiscussSessionSetup::TeamAtRoot { leader_id: id },
            Some("agent") => DiscussSessionSetup::Agent { agent_id: id },
            _ => DiscussSessionSetup::Team { leader_id: id },
        })
    }

    pub(super) async fn resolve_standalone_agent_setup(
        &self,
        provider: &str,
        id: String,
    ) -> Result<(Option<String>, plugins::ConfigSettings), DiscussError> {
        let agent_id = AgentId::new(id.clone())?;
        if !matches!(self.resolve_agent(&agent_id).await?, ResolvedAgent::Custom(_)) {
            return Err(DiscussError::UnknownCustomAsset);
        }
        let conn = self.persistence.acquire_connection().await?;
        if load_custom_installation(&conn, CustomInstallationKind::Agent, provider, &id)
            .await?
            .is_none()
        {
            return Err(DiscussError::CustomAgentNotInstalled);
        }
        Ok((Some(id), plugins::ConfigSettings::new()))
    }

    pub(super) async fn install_custom_asset(
        &self,
        kind: CustomInstallationKind,
        provider: &str,
        id: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        self.require_custom_provider(provider)?;
        let conn = self.persistence.acquire_connection().await?;
        let asset = self.load_install_asset(&conn, kind, id).await?;
        let existing = load_custom_installation(&conn, kind, provider, id).await?;
        let retained = existing.is_some()
            || self
                .custom_asset_retained(&conn, kind, provider, id)
                .await?;
        let plugin = plugins::for_agent(provider);
        asset.materialize(self, plugin).await?;
        let row = CustomInstallationRow {
            provider_name: provider.to_owned(),
            asset_id: id.to_owned(),
            installed_version: asset.version().to_owned(),
        };
        if let Err(error) = upsert_custom_installation(&conn, kind, &row).await {
            if !retained && let Err(cleanup) = asset.destroy(self, plugin) {
                return Err(with_install_compensation_context(
                    error,
                    id,
                    provider,
                    &self.agents_workspace,
                    &cleanup,
                ));
            }
            return Err(error);
        }
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn uninstall_custom_asset(
        &self,
        kind: CustomInstallationKind,
        provider: &str,
        id: &str,
    ) -> Result<DiscussResponse, DiscussError> {
        self.require_custom_provider(provider)?;
        let conn = self.persistence.acquire_connection().await?;
        let Some(_) = load_custom_installation(&conn, kind, provider, id).await? else {
            return Ok(DiscussResponse::Success);
        };
        let asset = self.load_install_asset(&conn, kind, id).await?;
        let retained = self
            .custom_asset_retained(&conn, kind, provider, id)
            .await?;
        delete_custom_installation(&conn, kind, provider, id).await?;
        if !retained {
            asset.destroy(self, plugins::for_agent(provider))?;
        }
        Ok(DiscussResponse::Success)
    }

    fn require_custom_provider(&self, provider: &str) -> Result<(), DiscussError> {
        if self.agents.contains_key(provider) {
            Ok(())
        } else {
            Err(DiscussError::UnknownAgent {
                agent_key: provider.to_owned(),
            })
        }
    }

    async fn load_install_asset(
        &self,
        conn: &Connection,
        kind: CustomInstallationKind,
        id: &str,
    ) -> Result<InstallAsset, DiscussError> {
        match kind {
            CustomInstallationKind::Agent => {
                let row = load_custom_agent(conn, id)
                    .await?
                    .ok_or(DiscussError::UnknownCustomAsset)?;
                Ok(InstallAsset::Agent(
                    ResolvedAgent::Custom(Arc::new(load_agent(self.cache_root()?, &row)?)),
                    row.version,
                ))
            }
            CustomInstallationKind::Skill => {
                let row = load_custom_skill(conn, id)
                    .await?
                    .ok_or(DiscussError::UnknownCustomAsset)?;
                Ok(InstallAsset::Skill(
                    ResolvedSkill::Custom(Arc::new(load_skill(self.cache_root()?, &row)?)),
                    row.version,
                ))
            }
        }
    }

    async fn custom_asset_retained(
        &self,
        conn: &Connection,
        kind: CustomInstallationKind,
        provider: &str,
        id: &str,
    ) -> Result<bool, DiscussError> {
        let plugin = plugins::for_agent(provider);
        let teams = self.custom_team_compositions(conn).await?;
        let installations = list_discuss_team_installations(conn).await?;
        for installation in &installations {
            self.require_custom_provider(&installation.provider_name)?;
        }
        let (agents, skills) = retained_team_assets(
            self.library,
            &teams,
            &installations,
            "",
            plugin.agent_scope_key(),
            plugin.skill_scope_key(),
        );
        let in_team = match kind {
            CustomInstallationKind::Agent => agents.contains(&AgentId::new(id)?),
            CustomInstallationKind::Skill => skills.contains(&SkillId::new(id)?),
        };
        if in_team {
            return Ok(true);
        }
        for row in list_custom_installations(conn, kind).await? {
            if row.asset_id != id || row.provider_name == provider {
                continue;
            }
            self.require_custom_provider(&row.provider_name)?;
            let other = plugins::for_agent(&row.provider_name);
            let same_scope = match kind {
                CustomInstallationKind::Agent => plugin.agent_scope_key() == other.agent_scope_key(),
                CustomInstallationKind::Skill => plugin.skill_scope_key() == other.skill_scope_key(),
            };
            if same_scope {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) async fn installed_asset_instructions(
        &self,
        provider: &str,
        kind: InstalledAssetKind,
        id: &str,
        instructions: Option<String>,
    ) -> Result<DiscussResponse, DiscussError> {
        self.require_custom_provider(provider)?;
        let plugin = plugins::for_agent(provider);
        let (kind, path) = match kind {
            InstalledAssetKind::Agent => (
                CustomInstallationKind::Agent,
                plugin.installed_agent_path(&AgentId::new(id)?),
            ),
            InstalledAssetKind::Skill => (
                CustomInstallationKind::Skill,
                plugin.installed_skill_path(&SkillId::new(id)?),
            ),
        };
        let conn = self.persistence.acquire_connection().await?;
        let path = path.to_string_lossy().into_owned();
        let Some(instructions) = instructions else {
            let value = load_asset_instructions(&conn, &path)
                .await?
                .map_or_else(String::new, |row| row.instructions);
            return Ok(DiscussResponse::InstalledAssetInstructions { instructions: value });
        };
        let settings = self
            .installed_asset_settings(&conn, provider, kind, id)
            .await?;
        upsert_asset_instructions(
            &conn,
            &AssetInstructionsRow {
                asset_path: path,
                instructions,
                updated_at_ms: chrono::Utc::now().timestamp_millis(),
            },
        )
        .await?;
        if let Some(settings) = settings {
            self.rewrite_installed_asset(plugin, &self.agents_workspace, kind, id, settings.as_ref())
                .await?;
        }
        Ok(DiscussResponse::Success)
    }

    async fn installed_asset_settings(
        &self,
        conn: &Connection,
        provider: &str,
        kind: CustomInstallationKind,
        id: &str,
    ) -> Result<Option<Option<plugins::ConfigSettings>>, DiscussError> {
        let plugin = plugins::for_agent(provider);
        let teams = self.custom_team_compositions(conn).await?;
        for row in list_discuss_team_installations(conn).await? {
            if !self.agents.contains_key(&row.provider_name) {
                continue;
            }
            let other = plugins::for_agent(&row.provider_name);
            let same_scope = match kind {
                CustomInstallationKind::Agent => plugin.agent_scope_key() == other.agent_scope_key(),
                CustomInstallationKind::Skill => plugin.skill_scope_key() == other.skill_scope_key(),
            };
            if !same_scope {
                continue;
            }
            let builtin = self
                .library
                .teams_for()
                .find(|team| team.leader.as_ref() == row.leader_id);
            let custom = teams
                .iter()
                .find(|team| team.leader.as_ref() == row.leader_id);
            let contains = match (kind, builtin, custom) {
                (CustomInstallationKind::Agent, Some(team), _) => {
                    team.leader.as_ref() == id || team.followers.iter().any(|item| item.as_ref() == id)
                }
                (CustomInstallationKind::Skill, Some(team), _) => team.skills.iter().any(|item| item.as_ref() == id),
                (CustomInstallationKind::Agent, None, Some(team)) => {
                    team.leader.as_ref() == id || team.followers.iter().any(|item| item.as_ref() == id)
                }
                (CustomInstallationKind::Skill, None, Some(team)) => team.skills.iter().any(|item| item.as_ref() == id),
                _ => false,
            };
            if contains {
                let overrides: DiscussMemberOverrides = serde_json::from_str(&row.member_overrides_json)?;
                return Ok(Some(overrides.get(id).cloned()));
            }
        }
        for row in list_custom_installations(conn, kind).await? {
            if row.asset_id != id || !self.agents.contains_key(&row.provider_name) {
                continue;
            }
            let other = plugins::for_agent(&row.provider_name);
            let shared = match kind {
                CustomInstallationKind::Agent => plugin.agent_scope_key() == other.agent_scope_key(),
                CustomInstallationKind::Skill => plugin.skill_scope_key() == other.skill_scope_key(),
            };
            if shared {
                return Ok(Some(None));
            }
        }
        Ok(None)
    }

    /// Validate an import before launching its network fetch.
    fn prepare_custom_import(
        &self,
        kind: CustomInstallationKind,
        requested_id: Option<String>,
        url: String,
    ) -> Result<PreparedImport, DiscussError> {
        let id = match requested_id {
            Some(id) => id,
            None => reqwest::Url::parse(&url)
                .map_err(|error| import_error("parse_url", error))?
                .path_segments()
                .and_then(|mut segments| segments.rfind(|part| !part.is_empty()))
                .map(|part| part.strip_suffix(".md").unwrap_or(part).to_owned())
                .ok_or_else(|| import_error("parse_url", "missing asset identity"))?,
        };
        match kind {
            CustomInstallationKind::Agent => validate_agent_id(self.library, &AgentId::new(&id)?)?,
            CustomInstallationKind::Skill => validate_skill_id(self.library, &SkillId::new(&id)?)?,
        }
        reqwest::Url::parse(&url).map_err(|error| import_error("parse_url", error))?;
        Ok(PreparedImport { kind, id, url })
    }

    /// Start the network-only part of an import without blocking the actor.
    pub(super) fn start_custom_import(
        &mut self,
        payload: &DiscussRequestPayload,
        reply_to: oneshot::Sender<Result<DiscussResponse, DiscussError>>,
    ) {
        let (kind, id, url) = match payload {
            DiscussRequestPayload::ImportCustomAgentFromUrl { agent_id, url } => {
                (CustomInstallationKind::Agent, agent_id.clone(), url.clone())
            }
            DiscussRequestPayload::ImportCustomSkillFromUrl { skill_id, url } => {
                (CustomInstallationKind::Skill, skill_id.clone(), url.clone())
            }
            _ => unreachable!("only import requests are dispatched here"),
        };
        let PreparedImport { kind, id, url } = match self.prepare_custom_import(kind, id, url) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = reply_to.send(Err(error));
                return;
            }
        };
        let key = self.next_import_key;
        self.next_import_key = self.next_import_key.wrapping_add(1);
        let sender = self.import_completion_tx.clone();
        let fetch_url = url.clone();
        let task = tokio::spawn(async move {
            let result = fetch_asset_source(&fetch_url).await;
            let _ = sender.send(ImportCompletion { key, result }).await;
        });
        self.pending_imports.insert(
            key,
            PendingImport {
                kind,
                id,
                url,
                reply_to,
                abort: task.abort_handle(),
            },
        );
    }

    /// Persist a fetched import and resolve its original caller.
    pub(super) async fn finish_custom_import(&mut self, completion: ImportCompletion) {
        if let Some(pending) = self.pending_imports.remove(&completion.key) {
            let result = self
                .complete_custom_import(pending.kind, pending.id, pending.url, completion.result)
                .await;
            let _ = pending.reply_to.send(result);
        }
    }

    /// Abort imports and resolve their callers during shutdown.
    pub(super) fn cancel_custom_imports(&mut self) {
        for (_, pending) in self.pending_imports.drain() {
            pending.abort.abort();
            let _ = pending.reply_to.send(Err(DiscussError::ShuttingDown));
        }
    }

    /// Apply a fetch result on the actor, preserving cached assets on failure.
    pub(super) async fn complete_custom_import(
        &self,
        kind: CustomInstallationKind,
        id: String,
        url: String,
        source: Result<AssetSource, DiscussError>,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let previous = match kind {
            CustomInstallationKind::Agent => load_custom_agent(&conn, &id).await?,
            CustomInstallationKind::Skill => load_custom_skill(&conn, &id).await?,
        };
        let mut stale = None;
        let result = async {
            let parsed = reqwest::Url::parse(&url).map_err(|error| import_error("parse_url", error))?;
            let source = source?;
            let source_kind = if parsed.host_str() == Some("github.com") {
                if matches!(&source, AssetSource::Directory(_)) {
                    CustomSourceKind::GithubDirectory
                } else {
                    CustomSourceKind::GithubFile
                }
            } else {
                CustomSourceKind::UrlMarkdown
            };
            let now = chrono::Utc::now().timestamp_millis();
            match (kind, source) {
                (CustomInstallationKind::Agent, AssetSource::Document(markdown)) => {
                    self.replace_imported_agent(&conn, &id, &url, source_kind, now, &markdown)
                        .await
                }
                (CustomInstallationKind::Skill, source) => {
                    let bundle = match source {
                        AssetSource::Document(contents) => vec![CustomSkillFile {
                            relative_path: "SKILL.md".to_owned(),
                            contents,
                        }],
                        AssetSource::Directory(bundle) => bundle,
                    };
                    validate_custom_skill_bundle(&bundle).map_err(|error| import_error("skill paths", error))?;
                    if let Some(row) = previous.as_ref() {
                        stale = stale_skill_files(self.cache_root()?, row, &bundle)?;
                    }
                    let (name, description, _) = markdown_metadata(&bundle[0].contents)?;
                    let row = CustomAssetRow {
                        id: id.clone(),
                        name,
                        job_title: None,
                        description,
                        version: String::new(),
                        source_kind,
                        source_url: Some(url.clone()),
                        last_pull_at_ms: Some(now),
                        last_pull_ok: Some(true),
                        pull_error: None,
                        updated_at_ms: now,
                    };
                    replace_skill(
                        &conn,
                        self.cache_root()?,
                        self.library,
                        &SkillId::new(&id)?,
                        &bundle,
                        row,
                    )
                    .await
                }
                (CustomInstallationKind::Agent, AssetSource::Directory(_)) => {
                    Err(import_error("source", "agent must be a Markdown document"))
                }
            }
        }
        .await;
        let assigned = match result {
            Ok(version) => version,
            Err(error) => {
                let now = chrono::Utc::now().timestamp_millis();
                match kind {
                    CustomInstallationKind::Agent => {
                        fail_custom_agent_pull(&conn, &id, &error.to_string(), now).await?
                    }
                    CustomInstallationKind::Skill => {
                        fail_custom_skill_pull(&conn, &id, &error.to_string(), now).await?
                    }
                }
                return Err(error);
            }
        };
        drop(conn);
        if previous
            .as_ref()
            .is_none_or(|row| row.version != assigned.to_string())
        {
            self.refresh_custom_installs(kind, &id, stale.as_ref())
                .await?;
        }
        match kind {
            CustomInstallationKind::Agent => self.handle_get_custom_agent(&id).await,
            CustomInstallationKind::Skill => self.handle_get_custom_skill(&id).await,
        }
    }

    /// Validate and replace one fetched agent document.
    async fn replace_imported_agent(
        &self,
        conn: &Connection,
        id: &str,
        url: &str,
        source_kind: CustomSourceKind,
        now: i64,
        markdown: &str,
    ) -> Result<Version, DiscussError> {
        let (name, description, _) = markdown_metadata(markdown)?;
        let row = CustomAssetRow {
            id: id.to_owned(),
            name: name.clone(),
            job_title: Some(name),
            description,
            version: String::new(),
            source_kind,
            source_url: Some(url.to_owned()),
            last_pull_at_ms: Some(now),
            last_pull_ok: Some(true),
            pull_error: None,
            updated_at_ms: now,
        };
        replace_agent(
            conn,
            self.cache_root()?,
            self.library,
            &AgentId::new(id)?,
            markdown,
            row,
        )
        .await
    }

    pub(super) fn handle_list_builtin_skills(&self) -> Result<DiscussResponse, DiscussError> {
        let entries = self
            .library
            .skills_for()
            .map(|skill| BuiltinSkillEntry {
                skill_id: skill.id.to_string(),
                name: skill.name.to_owned(),
                description: skill.description.to_owned(),
                version: skill.version.to_string(),
            })
            .collect();
        Ok(DiscussResponse::ListBuiltinSkills { entries })
    }

    pub(super) async fn handle_list_provider_installed_assets(&self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let agent_ids: BTreeSet<String> = self
            .library
            .agents_for()
            .map(|agent| agent.id.to_string())
            .chain(
                list_custom_agents(&conn)
                    .await?
                    .into_iter()
                    .map(|row| row.id),
            )
            .collect();
        let skill_ids: BTreeSet<String> = self
            .library
            .skills_for()
            .map(|skill| skill.id.to_string())
            .chain(
                list_custom_skills(&conn)
                    .await?
                    .into_iter()
                    .map(|row| row.id),
            )
            .collect();
        let custom_teams: BTreeMap<_, _> = list_custom_teams(&conn)
            .await?
            .into_iter()
            .map(|team| (team.leader_id.clone(), team))
            .collect();
        let mut agents = Vec::new();
        let mut skills = Vec::new();
        let mut incomplete_teams = Vec::new();
        for (kind, ids, claims) in [
            (CustomInstallationKind::Agent, &agent_ids, &mut agents),
            (CustomInstallationKind::Skill, &skill_ids, &mut skills),
        ] {
            for row in list_custom_installations(&conn, kind).await? {
                let present = ids.contains(&row.asset_id);
                claims.push(self.inventory_claim(
                    row.provider_name,
                    row.asset_id,
                    kind,
                    None,
                    Some(row.installed_version),
                    present,
                ));
            }
        }
        for row in list_discuss_team_installations(&conn).await? {
            let builtin = self
                .library
                .teams_for()
                .find(|team| team.leader.as_ref() == row.leader_id);
            let (members, team_skills): (Vec<String>, Vec<String>) = if let Some(team) = builtin {
                (
                    std::iter::once(team.leader.to_string())
                        .chain(team.followers.iter().map(ToString::to_string))
                        .collect(),
                    team.skills.iter().map(ToString::to_string).collect(),
                )
            } else if let Some(team) = custom_teams.get(&row.leader_id) {
                let followers: Vec<String> = serde_json::from_str(&team.followers_json)?;
                let team_skills: Vec<String> = serde_json::from_str(&team.skills_json)?;
                (
                    std::iter::once(team.leader_id.clone())
                        .chain(followers)
                        .collect(),
                    team_skills,
                )
            } else {
                incomplete_teams.push(IncompleteTeamClaim {
                    provider_name: row.provider_name.clone(),
                    team_leader_id: row.leader_id.clone(),
                });
                (serde_json::from_str(&row.members_json)?, Vec::new())
            };
            for id in members {
                let present = agent_ids.contains(&id);
                agents.push(self.inventory_claim(
                    row.provider_name.clone(),
                    id,
                    CustomInstallationKind::Agent,
                    Some(row.leader_id.clone()),
                    None,
                    present,
                ));
            }
            for id in team_skills {
                let present = skill_ids.contains(&id);
                skills.push(self.inventory_claim(
                    row.provider_name.clone(),
                    id,
                    CustomInstallationKind::Skill,
                    Some(row.leader_id.clone()),
                    None,
                    present,
                ));
            }
        }
        for claims in [&mut agents, &mut skills] {
            claims.sort_by(|a, b| {
                (&a.provider_name, &a.asset_id, a.basis, &a.team_leader_id).cmp(&(
                    &b.provider_name,
                    &b.asset_id,
                    b.basis,
                    &b.team_leader_id,
                ))
            });
        }
        incomplete_teams
            .sort_by(|a, b| (&a.provider_name, &a.team_leader_id).cmp(&(&b.provider_name, &b.team_leader_id)));
        Ok(DiscussResponse::ProviderInstalledAssets {
            agents,
            skills,
            incomplete_teams,
        })
    }

    fn inventory_claim(
        &self,
        provider_name: String,
        asset_id: String,
        kind: CustomInstallationKind,
        team_leader_id: Option<String>,
        installed_version: Option<String>,
        definition_present: bool,
    ) -> InstalledAssetClaim {
        let provider_installed = self.agents.contains_key(&provider_name);
        let scope_key = if provider_installed {
            let plugin = plugins::for_agent(&provider_name);
            Some(
                match kind {
                    CustomInstallationKind::Agent => plugin.agent_scope_key(),
                    CustomInstallationKind::Skill => plugin.skill_scope_key(),
                }
                .to_owned(),
            )
        } else {
            None
        };
        let basis = if team_leader_id.is_some() {
            InstalledClaimBasis::Team
        } else {
            InstalledClaimBasis::Standalone
        };
        InstalledAssetClaim {
            provider_name,
            asset_id,
            basis,
            team_leader_id,
            installed_version,
            scope_key,
            provider_installed,
            definition_present,
        }
    }

    pub(super) async fn handle_list_custom_agents(&self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        Ok(DiscussResponse::ListCustomAgents {
            entries: list_custom_agents(&conn)
                .await?
                .into_iter()
                .map(metadata)
                .collect(),
        })
    }

    pub(super) async fn handle_list_custom_skills(&self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        Ok(DiscussResponse::ListCustomSkills {
            entries: list_custom_skills(&conn)
                .await?
                .into_iter()
                .map(metadata)
                .collect(),
        })
    }

    pub(super) async fn handle_get_custom_agent(&self, id: &str) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let row = load_custom_agent(&conn, id)
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        let agent = load_agent(self.cache_root()?, &row)?;
        Ok(DiscussResponse::GetCustomAgent {
            agent: CustomAgentDetail {
                metadata: metadata(row),
                body: agent.body,
            },
        })
    }

    pub(super) async fn handle_get_custom_skill(&self, id: &str) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let row = load_custom_skill(&conn, id)
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        let skill = load_skill(self.cache_root()?, &row)?;
        let files = skill
            .bundle
            .into_iter()
            .map(|file| CustomSkillBundleFile {
                relative_path: file.relative_path,
                contents: file.contents,
            })
            .collect();
        Ok(DiscussResponse::GetCustomSkill {
            skill: CustomSkillDetail {
                metadata: metadata(row),
                files,
            },
        })
    }

    pub(super) async fn handle_create_custom_agent(
        &self,
        id: Option<String>,
        name: String,
        job_title: String,
        description: String,
        body: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let id = AgentId::new(id.unwrap_or_else(|| derived_id(&name)))?;
        validate_agent_id(self.library, &id)?;
        let conn = self.persistence.acquire_connection().await?;
        if load_custom_agent(&conn, id.as_ref()).await?.is_some() {
            return Err(DiscussError::CustomAssetIdCollision);
        }
        let frontmatter = yaml_serde::to_string(&Frontmatter {
            name: name.clone(),
            description: description.clone(),
        })
        .map_err(|error| import_error("frontmatter", error))?;
        let markdown = format!("---\n{frontmatter}---\n\n{body}");
        let row = local_row(id.to_string(), name, Some(job_title), description);
        replace_agent(&conn, self.cache_root()?, self.library, &id, &markdown, row).await?;
        drop(conn);
        self.handle_get_custom_agent(id.as_ref()).await
    }

    pub(super) async fn handle_create_custom_skill(
        &self,
        id: Option<String>,
        name: String,
        description: String,
        files: Vec<CustomSkillBundleFile>,
    ) -> Result<DiscussResponse, DiscussError> {
        let id = SkillId::new(id.unwrap_or_else(|| derived_id(&name)))?;
        validate_skill_id(self.library, &id)?;
        let conn = self.persistence.acquire_connection().await?;
        if load_custom_skill(&conn, id.as_ref()).await?.is_some() {
            return Err(DiscussError::CustomAssetIdCollision);
        }
        let bundle: Vec<_> = files
            .into_iter()
            .map(|file| CustomSkillFile {
                relative_path: file.relative_path,
                contents: file.contents,
            })
            .collect();
        let row = local_row(id.to_string(), name, None, description);
        replace_skill(&conn, self.cache_root()?, self.library, &id, &bundle, row).await?;
        drop(conn);
        self.handle_get_custom_skill(id.as_ref()).await
    }

    /// An imported agent's next successful pull overwrites local edits.
    pub(super) async fn handle_update_custom_agent(
        &self,
        id: String,
        name: String,
        job_title: String,
        description: String,
        body: String,
    ) -> Result<DiscussResponse, DiscussError> {
        let id = AgentId::new(id)?;
        let conn = self.persistence.acquire_connection().await?;
        let mut row = load_custom_agent(&conn, id.as_ref())
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        let frontmatter = yaml_serde::to_string(&Frontmatter {
            name: name.clone(),
            description: description.clone(),
        })
        .map_err(|error| import_error("frontmatter", error))?;
        let markdown = format!("---\n{frontmatter}---\n\n{body}");
        row.name = name;
        row.job_title = Some(job_title);
        row.description = description;
        row.updated_at_ms = chrono::Utc::now().timestamp_millis();
        replace_agent(&conn, self.cache_root()?, self.library, &id, &markdown, row).await?;
        drop(conn);
        self.refresh_custom_installs(CustomInstallationKind::Agent, id.as_ref(), None)
            .await?;
        self.handle_get_custom_agent(id.as_ref()).await
    }

    /// An imported skill's next successful pull overwrites local edits.
    pub(super) async fn handle_update_custom_skill(
        &self,
        id: String,
        name: String,
        description: String,
        files: Vec<CustomSkillBundleFile>,
    ) -> Result<DiscussResponse, DiscussError> {
        let id = SkillId::new(id)?;
        let conn = self.persistence.acquire_connection().await?;
        let mut row = load_custom_skill(&conn, id.as_ref())
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        let previous = row.clone();
        row.name = name;
        row.description = description;
        row.updated_at_ms = chrono::Utc::now().timestamp_millis();
        let bundle: Vec<_> = files
            .into_iter()
            .map(|file| CustomSkillFile {
                relative_path: file.relative_path,
                contents: file.contents,
            })
            .collect();
        let stale = stale_skill_files(self.cache_root()?, &previous, &bundle)?;
        replace_skill(&conn, self.cache_root()?, self.library, &id, &bundle, row).await?;
        drop(conn);
        self.refresh_custom_installs(CustomInstallationKind::Skill, id.as_ref(), stale.as_ref())
            .await?;
        self.handle_get_custom_skill(id.as_ref()).await
    }

    pub(super) async fn handle_delete_custom_agent(&self, id: &str) -> Result<DiscussResponse, DiscussError> {
        let id = AgentId::new(id)?;
        let conn = self.persistence.acquire_connection().await?;
        load_custom_agent(&conn, id.as_ref())
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        self.ensure_unreferenced(&conn, CustomInstallationKind::Agent, id.as_ref())
            .await?;
        let path = agent_path(self.cache_root()?, &id);
        let root = path
            .parent()
            .ok_or_else(|| import_error("agent cache", "missing parent"))?
            .to_path_buf();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| import_error("agent cache", "invalid file name"))?
            .to_owned();
        let mut old = BTreeMap::new();
        match fs::read(&path) {
            Ok(bytes) => {
                old.insert(name, bytes);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(import_error("read agent", error)),
        }
        swap_and_update(root, old, BTreeMap::new(), || delete_custom_agent(&conn, id.as_ref())).await?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_delete_custom_skill(&self, id: &str) -> Result<DiscussResponse, DiscussError> {
        let id = SkillId::new(id)?;
        let conn = self.persistence.acquire_connection().await?;
        load_custom_skill(&conn, id.as_ref())
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        self.ensure_unreferenced(&conn, CustomInstallationKind::Skill, id.as_ref())
            .await?;
        let root = skill_path(self.cache_root()?, &id);
        let old = read_tree(&root)?;
        swap_and_update(root, old, BTreeMap::new(), || delete_custom_skill(&conn, id.as_ref())).await?;
        Ok(DiscussResponse::Success)
    }

    async fn ensure_unreferenced(
        &self,
        conn: &Connection,
        kind: CustomInstallationKind,
        id: &str,
    ) -> Result<(), DiscussError> {
        let mut references: Vec<String> = list_custom_installations(conn, kind)
            .await?
            .into_iter()
            .filter(|row| row.asset_id == id)
            .map(|row| format!("provider:{}", row.provider_name))
            .collect();
        for team in list_custom_teams(conn).await? {
            if team_contains(&team, kind, id)? {
                references.push(format!("team:{}", team.leader_id));
            }
        }
        if references.is_empty() {
            Ok(())
        } else {
            Err(DiscussError::CustomAssetReferenced { references })
        }
    }

    async fn refresh_custom_installs(
        &self,
        kind: CustomInstallationKind,
        id: &str,
        stale: Option<&CustomSkillDefinition>,
    ) -> Result<(), DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let version = match kind {
            CustomInstallationKind::Agent => load_custom_agent(&conn, id).await?,
            CustomInstallationKind::Skill => load_custom_skill(&conn, id).await?,
        }
        .ok_or(DiscussError::UnknownCustomAsset)?
        .version;
        for mut row in list_custom_installations(&conn, kind)
            .await?
            .into_iter()
            .filter(|row| row.asset_id == id && row.installed_version != version)
        {
            self.rewrite_custom_asset(kind, id, &row.provider_name, None, stale)
                .await?;
            row.installed_version = version.clone();
            upsert_custom_installation(&conn, kind, &row).await?;
        }
        for team in list_custom_teams(&conn).await? {
            if !team_contains(&team, kind, id)? {
                continue;
            }
            if let Some(installation) = load_discuss_team_installation(&conn, &team.leader_id).await? {
                let overrides: DiscussMemberOverrides = serde_json::from_str(&installation.member_overrides_json)?;
                let settings = plugins::member_settings_for_team(overrides)?;
                let agent_id = AgentId::new(id)?;
                self.rewrite_custom_asset(kind, id, &installation.provider_name, settings.get(&agent_id), stale)
                    .await?;
            }
        }
        Ok(())
    }

    async fn rewrite_custom_asset(
        &self,
        kind: CustomInstallationKind,
        id: &str,
        provider: &str,
        settings: Option<&plugins::ConfigSettings>,
        stale: Option<&CustomSkillDefinition>,
    ) -> Result<(), DiscussError> {
        if !self.agents.contains_key(provider) {
            return Err(DiscussError::UnknownAgent {
                agent_key: provider.to_owned(),
            });
        }
        let plugin = plugins::for_agent(provider);
        if matches!(kind, CustomInstallationKind::Skill)
            && let Some(skill) = stale.filter(|skill| !skill.bundle.is_empty())
        {
            plugin.destroy_custom_skill(&self.agents_workspace, skill)?;
        }
        self.rewrite_installed_asset(plugin, &self.agents_workspace, kind, id, settings)
            .await
    }

    fn cache_root(&self) -> Result<&Path, DiscussError> {
        self.database_path
            .parent()
            .ok_or_else(|| import_error("cache root", "database path has no parent"))
    }

    pub(super) async fn resolve_agent(&self, id: &AgentId) -> Result<ResolvedAgent, DiscussError> {
        if let Ok(agent) = plugins::resolve_agent(self.library, id) {
            return Ok(ResolvedAgent::Builtin(agent));
        }
        let conn = self.persistence.acquire_connection().await?;
        let Some(row) = load_custom_agent(&conn, id.as_ref()).await? else {
            return plugins::resolve_agent(self.library, id).map(ResolvedAgent::Builtin);
        };
        let root = self
            .database_path
            .parent()
            .ok_or_else(|| import_error("cache root", "database path has no parent"))?;
        Ok(ResolvedAgent::Custom(Arc::new(load_agent(root, &row)?)))
    }

    pub(super) async fn resolve_skill(&self, id: &SkillId) -> Result<ResolvedSkill, DiscussError> {
        if let Ok(skill) = plugins::resolve_skill(self.library, id) {
            return Ok(ResolvedSkill::Builtin(skill));
        }
        let conn = self.persistence.acquire_connection().await?;
        let Some(row) = load_custom_skill(&conn, id.as_ref()).await? else {
            return plugins::resolve_skill(self.library, id).map(ResolvedSkill::Builtin);
        };
        let root = self
            .database_path
            .parent()
            .ok_or_else(|| import_error("cache root", "database path has no parent"))?;
        Ok(ResolvedSkill::Custom(Arc::new(load_skill(root, &row)?)))
    }

    pub(super) async fn resolve_team(&self, id: &AgentId) -> Result<ResolvedTeam, DiscussError> {
        if let Ok(team) = plugins::resolve_team(self.library, id) {
            return Ok(ResolvedTeam::Builtin(team));
        }
        let conn = self.persistence.acquire_connection().await?;
        let Some(row) = load_custom_team(&conn, id.as_ref()).await? else {
            return plugins::resolve_team(self.library, id).map(ResolvedTeam::Builtin);
        };
        let followers: Vec<String> = serde_json::from_str(&row.followers_json)?;
        let skills: Vec<String> = serde_json::from_str(&row.skills_json)?;
        Ok(ResolvedTeam::Custom(Arc::new(CustomTeamDefinition {
            leader: id.clone(),
            followers: followers
                .into_iter()
                .map(AgentId::try_from)
                .collect::<Result<_, _>>()?,
            skills: skills
                .into_iter()
                .map(SkillId::try_from)
                .collect::<Result<_, _>>()?,
            version: version(&row.version)?,
        })))
    }
}

enum InstallAsset {
    Agent(ResolvedAgent, String),
    Skill(ResolvedSkill, String),
}

impl InstallAsset {
    fn version(&self) -> &str {
        match self {
            Self::Agent(_, version) | Self::Skill(_, version) => version,
        }
    }

    async fn materialize(&self, actor: &Actor, plugin: &dyn plugins::DiscussAgentPlugin) -> Result<(), DiscussError> {
        match self {
            Self::Agent(agent, _) => {
                actor
                    .materialize_member(plugin, &actor.agents_workspace, agent, None)
                    .await
            }
            Self::Skill(skill, _) => {
                actor
                    .materialize_skill(plugin, &actor.agents_workspace, skill)
                    .await
            }
        }
    }

    fn destroy(&self, actor: &Actor, plugin: &dyn plugins::DiscussAgentPlugin) -> Result<(), DiscussError> {
        match self {
            Self::Agent(agent, _) => actor.destroy_member(plugin, &actor.agents_workspace, agent),
            Self::Skill(skill, _) => actor.destroy_skill(plugin, &actor.agents_workspace, skill),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Frontmatter {
    name: String,
    description: String,
}

fn derived_id(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join("-")
        .to_lowercase()
}

fn team_contains(team: &CustomTeamRow, kind: CustomInstallationKind, id: &str) -> Result<bool, DiscussError> {
    let values: Vec<String> = match kind {
        CustomInstallationKind::Agent => serde_json::from_str(&team.followers_json)?,
        CustomInstallationKind::Skill => serde_json::from_str(&team.skills_json)?,
    };
    Ok(matches!(kind, CustomInstallationKind::Agent) && team.leader_id == id || values.iter().any(|value| value == id))
}

fn local_row(id: String, name: String, job_title: Option<String>, description: String) -> CustomAssetRow {
    CustomAssetRow {
        id,
        name,
        job_title,
        description,
        version: String::new(),
        source_kind: CustomSourceKind::LocalMarkdown,
        source_url: None,
        last_pull_at_ms: None,
        last_pull_ok: None,
        pull_error: None,
        updated_at_ms: chrono::Utc::now().timestamp_millis(),
    }
}

fn metadata(row: CustomAssetRow) -> CustomAssetMetadata {
    let source_kind = match row.source_kind {
        CustomSourceKind::LocalMarkdown => "local_markdown",
        CustomSourceKind::UrlMarkdown => "url_markdown",
        CustomSourceKind::GithubFile => "github_file",
        CustomSourceKind::GithubDirectory => "github_directory",
    };
    CustomAssetMetadata {
        id: row.id,
        name: row.name,
        job_title: row.job_title,
        description: row.description,
        version: row.version,
        source_kind: source_kind.to_owned(),
        source_url: row.source_url,
        last_pull_at_ms: row.last_pull_at_ms,
        last_pull_ok: row.last_pull_ok,
        pull_error: row.pull_error,
    }
}

fn import_error(stage: &str, cause: impl std::fmt::Display) -> DiscussError {
    DiscussError::AssetImport {
        stage: stage.to_owned(),
        cause: cause.to_string(),
    }
}

fn parse_markdown(markdown: &str) -> Result<(Frontmatter, &str), DiscussError> {
    let rest = markdown
        .strip_prefix("---\n")
        .ok_or_else(|| import_error("frontmatter", "missing opening delimiter"))?;
    let (yaml, body) = rest
        .split_once("\n---\n")
        .ok_or_else(|| import_error("frontmatter", "missing closing delimiter"))?;
    let frontmatter: Frontmatter = yaml_serde::from_str(yaml).map_err(|error| import_error("frontmatter", error))?;
    if frontmatter.name.trim().is_empty() || frontmatter.description.trim().is_empty() {
        return Err(import_error("frontmatter", "name and description must be nonempty"));
    }
    Ok((frontmatter, body.trim_start_matches('\n')))
}

/// Extract validated metadata and body from authored or imported Markdown.
pub(super) fn markdown_metadata(markdown: &str) -> Result<(String, String, &str), DiscussError> {
    let (frontmatter, body) = parse_markdown(markdown)?;
    Ok((frontmatter.name, frontmatter.description, body))
}

/// Reject an agent identity already owned by the built-in catalog.
pub(super) fn validate_agent_id(library: AssetLibrary, id: &AgentId) -> Result<(), DiscussError> {
    if library.agents_for().any(|agent| &agent.id == id) {
        return Err(DiscussError::CustomAssetIdCollision);
    }
    Ok(())
}

/// Reject a skill identity already owned by the built-in catalog.
pub(super) fn validate_skill_id(library: AssetLibrary, id: &SkillId) -> Result<(), DiscussError> {
    if library.skills_for().any(|skill| &skill.id == id) {
        return Err(DiscussError::CustomAssetIdCollision);
    }
    Ok(())
}

fn version(value: &str) -> Result<Version, DiscussError> {
    Version::parse(value).map_err(|error| import_error("version", error))
}

fn agent_path(config_dir: &Path, id: &AgentId) -> PathBuf {
    config_dir
        .join("discuss_cache")
        .join("agents")
        .join(format!("{id}.md"))
}

fn skill_path(config_dir: &Path, id: &SkillId) -> PathBuf {
    config_dir
        .join("discuss_cache")
        .join("skills")
        .join(id.as_ref())
}

/// Read an agent definition exclusively from its cached Markdown and row.
pub(super) fn load_agent(config_dir: &Path, row: &CustomAssetRow) -> Result<CustomAgentDefinition, DiscussError> {
    let id = AgentId::new(&row.id).map_err(|error| import_error("agent id", error))?;
    let markdown =
        fs::read_to_string(agent_path(config_dir, &id)).map_err(|error| import_error("read agent", error))?;
    let (_, body) = parse_markdown(&markdown)?;
    Ok(CustomAgentDefinition {
        id,
        name: row.name.clone(),
        job_title: row
            .job_title
            .clone()
            .ok_or_else(|| import_error("agent row", "missing job title"))?,
        description: row.description.clone(),
        body: body.to_owned(),
        version: version(&row.version)?,
    })
}

/// Identify files removed from a previously cached skill bundle.
fn stale_skill_files(
    config_dir: &Path,
    row: &CustomAssetRow,
    bundle: &[CustomSkillFile],
) -> Result<Option<CustomSkillDefinition>, DiscussError> {
    let id = SkillId::new(&row.id)?;
    match fs::symlink_metadata(skill_path(config_dir, &id)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(import_error("read cache", error)),
        Ok(_) => (),
    }
    let mut previous = load_skill(config_dir, row)?;
    previous.bundle.retain(|old| {
        !bundle
            .iter()
            .any(|new| new.relative_path == old.relative_path)
    });
    Ok(Some(previous))
}

/// Read a skill definition exclusively from its cached bundle and row.
pub(super) fn load_skill(config_dir: &Path, row: &CustomAssetRow) -> Result<CustomSkillDefinition, DiscussError> {
    let id = SkillId::new(&row.id).map_err(|error| import_error("skill id", error))?;
    let files = read_tree(&skill_path(config_dir, &id))?;
    let mut bundle = Vec::new();
    let skill = files
        .get("SKILL.md")
        .ok_or_else(|| import_error("read skill", "SKILL.md is missing"))?;
    bundle.push(CustomSkillFile {
        relative_path: "SKILL.md".into(),
        contents: String::from_utf8(skill.clone()).map_err(|error| import_error("read skill", error))?,
    });
    for (path, contents) in files.into_iter().filter(|(path, _)| path != "SKILL.md") {
        bundle.push(CustomSkillFile {
            relative_path: path,
            contents: String::from_utf8(contents).map_err(|error| import_error("read skill", error))?,
        });
    }
    validate_custom_skill_bundle(&bundle).map_err(|error| import_error("skill paths", error))?;
    parse_markdown(&bundle[0].contents)?;
    Ok(CustomSkillDefinition {
        id,
        name: row.name.clone(),
        description: row.description.clone(),
        bundle,
        version: version(&row.version)?,
    })
}

fn read_tree(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, DiscussError> {
    let mut files = BTreeMap::new();
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => (),
        Ok(_) => return Err(import_error("read cache", "cache root is not a directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(import_error("read cache", error)),
    }
    collect_tree(root, root, &mut files)?;
    Ok(files)
}

fn collect_tree(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) -> Result<(), DiscussError> {
    for entry in fs::read_dir(dir).map_err(|error| import_error("read cache", error))? {
        let entry = entry.map_err(|error| import_error("read cache", error))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| import_error("read cache", error))?;
        if kind.is_dir() {
            collect_tree(root, &path, files)?;
        } else if kind.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| import_error("read cache", error))?;
            let relative = relative
                .to_str()
                .ok_or_else(|| import_error("read cache", "non-UTF-8 path"))?;
            files.insert(
                relative.to_owned(),
                fs::read(&path).map_err(|error| import_error("read cache", error))?,
            );
        } else {
            return Err(import_error("read cache", "non-file cache entry"));
        }
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), DiscussError> {
    let parent = path
        .parent()
        .ok_or_else(|| import_error("write cache", "missing parent"))?;
    fs::create_dir_all(parent).map_err(|error| import_error("write cache", error))?;
    let temp = parent.join(format!(
        ".discuss-{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|error| import_error("write cache", error))?;
    let result = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        fs::rename(&temp, path)?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) => match fs::remove_file(&temp) {
            Ok(()) => Err(import_error("write cache", error)),
            Err(cleanup) => Err(import_error(
                "write cache",
                format!("{error}; temporary file cleanup failed: {cleanup}"),
            )),
        },
    }
}

struct SwapGuard {
    root: PathBuf,
    old: BTreeMap<String, Vec<u8>>,
    new: BTreeMap<String, Vec<u8>>,
    created: Vec<String>,
    active: bool,
}
impl SwapGuard {
    fn apply(&mut self) -> Result<(), DiscussError> {
        for (path, bytes) in &self.new {
            atomic_write(&self.root.join(path), bytes)?;
            if !self.old.contains_key(path) {
                self.created.push(path.clone());
            }
        }
        for path in self.old.keys().filter(|path| !self.new.contains_key(*path)) {
            fs::remove_file(self.root.join(path)).map_err(|error| import_error("remove cache file", error))?;
        }
        Ok(())
    }

    fn restore(&mut self) -> Result<(), DiscussError> {
        self.active = false;
        let mut errors = Vec::new();
        for path in &self.created {
            match fs::remove_file(self.root.join(path)) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => errors.push(error.to_string()),
            }
        }
        for (path, bytes) in &self.old {
            if let Err(error) = atomic_write(&self.root.join(path), bytes) {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(import_error("restore cache", errors.join("; ")))
        }
    }
}
impl Drop for SwapGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.restore();
        }
    }
}

async fn swap_and_update<F, Fut>(
    root: PathBuf,
    old: BTreeMap<String, Vec<u8>>,
    new: BTreeMap<String, Vec<u8>>,
    update: F,
) -> Result<(), DiscussError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), DiscussError>>,
{
    let mut guard = SwapGuard {
        root,
        old,
        new,
        created: Vec::new(),
        active: true,
    };
    let result = match guard.apply() {
        Ok(()) => update().await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        let restored = guard.restore();
        return Err(import_error(
            "swap",
            format!(
                "{error}; restore: {}",
                match restored {
                    Ok(()) => "succeeded".to_owned(),
                    Err(error) => error.to_string(),
                }
            ),
        ));
    }
    guard.active = false;
    Ok(())
}

fn next_version(
    previous: Option<&CustomAssetRow>,
    row: &CustomAssetRow,
    old: &BTreeMap<String, Vec<u8>>,
    new: &BTreeMap<String, Vec<u8>>,
) -> Result<Version, DiscussError> {
    let Some(previous) = previous else {
        return Ok(initial_custom_version());
    };
    let current = version(&previous.version)?;
    let metadata_changed = previous.name != row.name
        || previous.job_title != row.job_title
        || previous.description != row.description
        || previous.source_kind != row.source_kind
        || previous.source_url != row.source_url;
    if old == new && !metadata_changed {
        return Ok(current);
    }
    bump_custom_version(&current).ok_or_else(|| import_error("version", "patch version exhausted"))
}

/// Replace an agent's cached Markdown and persist its automatically assigned version.
pub(super) async fn replace_agent(
    conn: &Connection,
    config_dir: &Path,
    library: AssetLibrary,
    id: &AgentId,
    markdown: &str,
    mut row: CustomAssetRow,
) -> Result<Version, DiscussError> {
    validate_agent_id(library, id)?;
    parse_markdown(markdown)?;
    if row.id != id.as_ref() {
        return Err(import_error("agent row", "identity mismatch"));
    }
    if row.job_title.is_none() {
        return Err(import_error("agent row", "missing job title"));
    }
    let previous = load_custom_agent(conn, id.as_ref()).await?;
    let path = agent_path(config_dir, id);
    let root = path
        .parent()
        .ok_or_else(|| import_error("agent cache", "missing parent"))?
        .to_path_buf();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| import_error("agent cache", "invalid file name"))?
        .to_owned();
    let mut old = BTreeMap::new();
    match fs::read(&path) {
        Ok(bytes) => {
            old.insert(name.clone(), bytes);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(import_error("read agent", error)),
    }
    let mut new = BTreeMap::new();
    new.insert(name, markdown.as_bytes().to_vec());
    let assigned = next_version(previous.as_ref(), &row, &old, &new)?;
    row.version = assigned.to_string();
    swap_and_update(root, old, new, || upsert_custom_agent(conn, &row)).await?;
    Ok(assigned)
}

/// Replace a skill bundle and persist its automatically assigned version.
pub(super) async fn replace_skill(
    conn: &Connection,
    config_dir: &Path,
    library: AssetLibrary,
    id: &SkillId,
    bundle: &[CustomSkillFile],
    mut row: CustomAssetRow,
) -> Result<Version, DiscussError> {
    validate_skill_id(library, id)?;
    validate_custom_skill_bundle(bundle).map_err(|error| import_error("skill paths", error))?;
    parse_markdown(&bundle[0].contents)?;
    if row.id != id.as_ref() {
        return Err(import_error("skill row", "identity mismatch"));
    }
    row.job_title = None;
    let previous = load_custom_skill(conn, id.as_ref()).await?;
    let mut names = BTreeSet::new();
    let mut files = BTreeMap::new();
    for file in bundle {
        if !names.insert(&file.relative_path) {
            return Err(import_error("skill paths", "duplicate path"));
        }
        files.insert(file.relative_path.clone(), file.contents.as_bytes().to_vec());
    }
    let root = skill_path(config_dir, id);
    let old = read_tree(&root)?;
    let assigned = next_version(previous.as_ref(), &row, &old, &files)?;
    row.version = assigned.to_string();
    swap_and_update(root, old, files, || upsert_custom_skill(conn, &row)).await?;
    Ok(assigned)
}

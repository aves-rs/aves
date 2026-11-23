use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use super::actor::{Actor, AgentVersionParseError, team_versions_match};
use super::catalog_types::{AgentThemeColor, TeamCatalogEntry, TeamCatalogMember, TeamDefinitionKind};
use super::database::*;
use super::error::DiscussError;
use super::launch_types::SessionLaunchConfig;
use super::library::{
    AgentId, CustomTeamDefinition, ResolvedAgent, ResolvedSkill, ResolvedTeam, SkillId, TeamDefinition,
    bump_custom_version, initial_custom_version,
};
use super::messages::DiscussResponse;
use super::plugins;
use super::storage::{
    CustomInstallationKind, CustomInstallationRow, CustomTeamRow, DiscussTeamInstallationRow, delete_custom_team,
    delete_discuss_team_installation, list_custom_installations, list_custom_teams, list_discuss_team_installations,
    load_asset_instructions, load_custom_team, load_discuss_team_installation, upsert_custom_installation,
    upsert_custom_team, upsert_discuss_team_installation,
};
use super::types::{CustomMcpScope, DiscussMemberOverrides};

impl Actor {
    async fn validate_custom_team_members(
        &self,
        leader: &AgentId,
        followers: &[String],
        skills: &[String],
    ) -> Result<(Vec<AgentId>, Vec<SkillId>), DiscussError> {
        self.resolve_agent(leader).await?;
        let mut agents = Vec::new();
        for id in followers {
            let id = AgentId::try_from(id.clone())?;
            self.resolve_agent(&id).await?;
            agents.push(id);
        }
        let mut resolved_skills = Vec::new();
        for id in skills {
            let id = SkillId::try_from(id.clone())?;
            self.resolve_skill(&id).await?;
            resolved_skills.push(id);
        }
        Ok((agents, resolved_skills))
    }

    pub(super) async fn handle_create_custom_team(
        &self,
        leader_id: String,
        display_name: String,
        followers: Vec<String>,
        skills: Vec<String>,
    ) -> Result<DiscussResponse, DiscussError> {
        let leader = AgentId::try_from(leader_id.clone())?;
        let conn = self.persistence.acquire_connection().await?;
        if self.library.teams_for().any(|team| team.leader == leader)
            || load_custom_team(&conn, &leader_id).await?.is_some()
        {
            return Err(DiscussError::TeamLeaderInUse);
        }
        self.validate_custom_team_members(&leader, &followers, &skills)
            .await?;
        upsert_custom_team(
            &conn,
            &CustomTeamRow {
                leader_id,
                display_name: Some(display_name),
                followers_json: serde_json::to_string(&followers)?,
                skills_json: serde_json::to_string(&skills)?,
                version: initial_custom_version().to_string(),
                updated_at_ms: chrono::Utc::now().timestamp_millis(),
            },
        )
        .await?;
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_update_custom_team(
        &self,
        leader_id: String,
        display_name: String,
        followers: Vec<String>,
        skills: Vec<String>,
    ) -> Result<DiscussResponse, DiscussError> {
        let leader = AgentId::try_from(leader_id.clone())?;
        if self.library.teams_for().any(|team| team.leader == leader) {
            return Err(DiscussError::TeamLeaderInUse);
        }
        let conn = self.persistence.acquire_connection().await?;
        let old_row = load_custom_team(&conn, &leader_id)
            .await?
            .ok_or(DiscussError::UnknownCustomAsset)?;
        let ResolvedTeam::Custom(old_team) = self.resolve_team(&leader).await? else {
            return Err(DiscussError::UnknownCustomAsset);
        };
        let (followers_ids, skills_ids) = self
            .validate_custom_team_members(&leader, &followers, &skills)
            .await?;
        let changed = old_team.followers != followers_ids || old_team.skills != skills_ids;
        let version = if changed {
            bump_custom_version(&old_team.version).ok_or_else(|| DiscussError::Validation {
                message: "custom Team version exhausted".to_string(),
            })?
        } else {
            old_team.version.clone()
        };
        let team = CustomTeamDefinition {
            leader,
            followers: followers_ids,
            skills: skills_ids,
            version,
        };
        let row = CustomTeamRow {
            leader_id: leader_id.clone(),
            display_name: Some(display_name),
            followers_json: serde_json::to_string(&followers)?,
            skills_json: serde_json::to_string(&skills)?,
            version: team.version.to_string(),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let installation = load_discuss_team_installation(&conn, &leader_id).await?;
        upsert_custom_team(&conn, &row).await?;
        if let Some(installation) = installation {
            let mut overrides = serde_json::from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)?;
            overrides.retain(|id, _| id == &leader_id || followers.contains(id));
            let color = installation
                .theme_color_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?;
            if let Err(error) = self
                .write_custom_team_installation(
                    &conn,
                    &team,
                    &installation.provider_name,
                    overrides,
                    color,
                    Some(&installation),
                )
                .await
            {
                upsert_custom_team(&conn, &old_row).await?;
                return Err(error);
            }
            let plugin = plugins::for_agent(&installation.provider_name);
            let installations = list_discuss_team_installations(&conn).await?;
            let custom_teams = self.custom_team_compositions(&conn).await?;
            let (_, mut retained_skills) = retained_team_assets(
                self.library,
                &custom_teams,
                &installations,
                &leader_id,
                plugin.agent_scope_key(),
                plugin.skill_scope_key(),
            );
            retained_skills.extend(team.skills.iter().cloned());
            for id in &old_team.skills {
                if !retained_skills.contains(id) {
                    self.destroy_skill(plugin, &self.agents_workspace, &self.resolve_skill(id).await?)?;
                }
            }
        }
        Ok(DiscussResponse::Success)
    }

    pub(super) async fn handle_delete_custom_team(&self, leader_id: String) -> Result<DiscussResponse, DiscussError> {
        let leader = AgentId::try_from(leader_id.clone())?;
        if self.library.teams_for().any(|team| team.leader == leader) {
            return Err(DiscussError::TeamLeaderInUse);
        }
        let conn = self.persistence.acquire_connection().await?;
        if load_custom_team(&conn, &leader_id).await?.is_none() {
            return Err(DiscussError::UnknownCustomAsset);
        }
        if let Some(installation) = load_discuss_team_installation(&conn, &leader_id).await? {
            return Err(DiscussError::CustomAssetReferenced {
                references: vec![installation.provider_name],
            });
        }
        super::mcp::clear_mcp_scope(&conn, CustomMcpScope::Team, &leader_id).await?;
        delete_custom_team(&conn, &leader_id).await?;
        Ok(DiscussResponse::Success)
    }

    /// Refresh stale standalone custom installations without blocking startup on failures.
    pub(super) async fn upgrade_custom_installations(&self) {
        let conn = match self.persistence.acquire_connection().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(?error, "skipping custom installation upgrades: storage unavailable");
                return;
            }
        };
        for kind in [CustomInstallationKind::Agent, CustomInstallationKind::Skill] {
            let installations = match list_custom_installations(&conn, kind).await {
                Ok(installations) => installations,
                Err(error) => {
                    tracing::warn!(?error, "could not list custom installations for upgrade");
                    continue;
                }
            };
            for row in installations {
                if !self.agents.contains_key(&row.provider_name) {
                    continue;
                }
                if let Err(error) = self
                    .upgrade_custom_installation(&conn, kind, row.clone())
                    .await
                {
                    tracing::warn!(?error, provider_name = %row.provider_name, asset_id = %row.asset_id, "custom installation upgrade failed");
                }
            }
        }
    }

    async fn upgrade_custom_installation(
        &self,
        conn: &turso::Connection,
        kind: CustomInstallationKind,
        mut row: CustomInstallationRow,
    ) -> Result<(), DiscussError> {
        let plugin = plugins::for_agent(&row.provider_name);
        let version = match kind {
            CustomInstallationKind::Agent => {
                let id = AgentId::try_from(row.asset_id.clone())?;
                let agent = self.resolve_agent(&id).await?;
                let ResolvedAgent::Custom(definition) = &agent else {
                    return Ok(());
                };
                let version = definition.version.to_string();
                if row.installed_version == version {
                    return Ok(());
                }
                self.materialize_member(plugin, &self.agents_workspace, &agent, None)
                    .await?;
                version
            }
            CustomInstallationKind::Skill => {
                let id = SkillId::try_from(row.asset_id.clone())?;
                let skill = self.resolve_skill(&id).await?;
                let ResolvedSkill::Custom(definition) = &skill else {
                    return Ok(());
                };
                let version = definition.version.to_string();
                if row.installed_version == version {
                    return Ok(());
                }
                self.materialize_skill(plugin, &self.agents_workspace, &skill)
                    .await?;
                version
            }
        };
        row.installed_version = version;
        upsert_custom_installation(conn, kind, &row).await
    }

    async fn write_custom_team_installation(
        &self,
        conn: &turso::Connection,
        team: &CustomTeamDefinition,
        provider_name: &str,
        overrides: DiscussMemberOverrides,
        color: Option<AgentThemeColor>,
        existing: Option<&DiscussTeamInstallationRow>,
    ) -> Result<Vec<super::types::CleanupWarning>, DiscussError> {
        let members = std::iter::once(team.leader.to_string())
            .chain(team.followers.iter().map(ToString::to_string))
            .collect::<Vec<_>>();
        let overrides = validate_member_overrides_for_ids(conn, provider_name, members.clone(), overrides).await?;
        let settings = plugins::member_settings_for_team(overrides.clone())?;
        let overrides_json = serde_json::to_string(&overrides)?;
        let members_json = serde_json::to_string(&members)?;
        let color_json = color
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
        let installations = list_discuss_team_installations(conn).await?;
        let custom_teams = self.custom_team_compositions(conn).await?;
        let plugin = plugins::for_agent(provider_name);
        let (retain_agents, retain_skills) = retained_team_assets(
            self.library,
            &custom_teams,
            &installations,
            team.leader.as_ref(),
            plugin.agent_scope_key(),
            plugin.skill_scope_key(),
        );
        self.materialize_custom_team(plugin, &self.agents_workspace, team, &settings, false)
            .await?;
        if let Err(error) = upsert_discuss_team_installation(
            conn,
            team.leader.as_ref(),
            provider_name,
            &team.version.to_string(),
            &overrides_json,
            &members_json,
            color_json.as_deref(),
        )
        .await
        {
            if existing.is_none() || existing.is_some_and(|row| row.provider_name != provider_name) {
                if let Err(cleanup) = self
                    .destroy_custom_team(plugin, team, &retain_agents, &retain_skills)
                    .await
                {
                    return Err(with_install_compensation_context(
                        error,
                        team.leader.as_ref(),
                        provider_name,
                        &self.agents_workspace,
                        &cleanup,
                    ));
                }
            } else if let Some(old) = existing {
                let previous = serde_json::from_str::<DiscussMemberOverrides>(&old.member_overrides_json)
                    .map_err(DiscussError::from)
                    .and_then(plugins::member_settings_for_team);
                let restored = match previous {
                    Ok(previous) => {
                        self.materialize_custom_team(plugin, &self.agents_workspace, team, &previous, false)
                            .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(restore_error) = restored {
                    return Err(with_install_compensation_context(
                        error,
                        team.leader.as_ref(),
                        provider_name,
                        &self.agents_workspace,
                        &restore_error,
                    ));
                }
            }
            return Err(error);
        }
        let mut warnings = Vec::new();
        if let Some(old) = existing {
            let old_plugin = plugins::for_agent(&old.provider_name);
            let (mut keep_agents, mut keep_skills) = retained_team_assets(
                self.library,
                &custom_teams,
                &installations,
                team.leader.as_ref(),
                old_plugin.agent_scope_key(),
                old_plugin.skill_scope_key(),
            );
            if old_plugin.agent_scope_key() == plugin.agent_scope_key() {
                keep_agents.insert(team.leader.clone());
                keep_agents.extend(team.followers.iter().cloned());
            }
            if old_plugin.skill_scope_key() == plugin.skill_scope_key() {
                keep_skills.extend(team.skills.iter().cloned());
            }
            let old_members: Vec<String> = serde_json::from_str(&old.members_json).unwrap_or_default();
            for id in old_members {
                let Ok(id) = AgentId::try_from(id) else { continue };
                if !keep_agents.contains(&id)
                    && !members.iter().any(|member| member == id.as_ref())
                    && let Err(error) = old_plugin.destroy_agent(&self.agents_workspace, &id)
                {
                    warnings.push(cleanup_warning(&old.provider_name, team.leader.as_ref(), &error));
                }
            }
            if old.provider_name != provider_name
                && let Err(error) = self
                    .destroy_custom_team(old_plugin, team, &keep_agents, &keep_skills)
                    .await
            {
                warnings.push(cleanup_warning(&old.provider_name, team.leader.as_ref(), &error));
            }
        }
        Ok(warnings)
    }

    pub(super) async fn materialize_custom_team(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        team: &CustomTeamDefinition,
        settings: &BTreeMap<AgentId, plugins::ConfigSettings>,
        custom_only: bool,
    ) -> Result<(), DiscussError> {
        for id in std::iter::once(&team.leader).chain(&team.followers) {
            let agent = self.resolve_agent(id).await?;
            if !custom_only || matches!(agent, ResolvedAgent::Custom(_)) {
                self.materialize_member(plugin, root, &agent, settings.get(id))
                    .await?;
            }
        }
        for id in &team.skills {
            let skill = self.resolve_skill(id).await?;
            if !custom_only || matches!(skill, ResolvedSkill::Custom(_)) {
                self.materialize_skill(plugin, root, &skill).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn destroy_custom_team(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        team: &CustomTeamDefinition,
        retained_agents: &BTreeSet<AgentId>,
        retained_skills: &BTreeSet<SkillId>,
    ) -> Result<(), DiscussError> {
        for id in std::iter::once(&team.leader).chain(&team.followers) {
            if !retained_agents.contains(id) {
                self.destroy_member(plugin, &self.agents_workspace, &self.resolve_agent(id).await?)?;
            }
        }
        for id in &team.skills {
            if !retained_skills.contains(id) {
                self.destroy_skill(plugin, &self.agents_workspace, &self.resolve_skill(id).await?)?;
            }
        }
        Ok(())
    }

    pub(super) async fn custom_team_compositions(
        &self,
        conn: &turso::Connection,
    ) -> Result<Vec<CustomTeamDefinition>, DiscussError> {
        let mut teams = Vec::new();
        for row in list_custom_teams(conn).await? {
            let id = AgentId::try_from(row.leader_id)?;
            if let ResolvedTeam::Custom(team) = self.resolve_team(&id).await? {
                teams.push((*team).clone());
            }
        }
        Ok(teams)
    }

    pub(super) async fn materialize_custom_team_at_root(
        &self,
        provider_name: &str,
        root: &Path,
        leader_id: &str,
        session_config: Option<&SessionLaunchConfig>,
    ) -> Result<(), DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let installation = load_discuss_team_installation(&conn, leader_id).await?;
        drop(conn);
        let overrides = installation
            .map(|row| serde_json::from_str::<DiscussMemberOverrides>(&row.member_overrides_json))
            .transpose()?
            .unwrap_or_default();
        let mut settings = plugins::member_settings_for_team(overrides)?;
        let plugin = plugins::for_agent(provider_name);
        let leader =
            AgentId::try_from(leader_id.to_owned()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
                asset_id: leader_id.to_owned(),
                violation: error.to_string(),
            })?;
        let resolved = self.resolve_team(&leader).await?;
        if let Some(config) = session_config {
            let members: Vec<_> = match &resolved {
                ResolvedTeam::Builtin(team) => std::iter::once(&team.leader)
                    .chain(&team.followers)
                    .cloned()
                    .collect(),
                ResolvedTeam::Custom(team) => std::iter::once(&team.leader)
                    .chain(&team.followers)
                    .cloned()
                    .collect(),
            };
            for id in members {
                let merged =
                    plugin.session_member_settings(settings.get(&id).unwrap_or(&Default::default()), config)?;
                settings.insert(id, merged);
            }
        }
        if let Some(team) = self
            .library
            .teams_for()
            .find(|team| team.leader.as_ref() == leader_id)
        {
            return self
                .install_builtin_team(plugins::for_agent(provider_name), root, team, &settings)
                .await;
        }
        let ResolvedTeam::Custom(team) = resolved else {
            return Ok(());
        };
        self.materialize_custom_team(plugins::for_agent(provider_name), root, &team, &settings, false)
            .await
    }

    pub(super) async fn materialize_member(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        agent: &ResolvedAgent,
        member_settings: Option<&plugins::ConfigSettings>,
    ) -> Result<(), DiscussError> {
        match agent {
            ResolvedAgent::Builtin(agent) => plugin.install_team_member(self.library, root, &agent.id, member_settings),
            ResolvedAgent::Custom(agent) => plugin.install_custom_agent(root, agent, member_settings),
        }?;
        let id = match agent {
            ResolvedAgent::Builtin(agent) => &agent.id,
            ResolvedAgent::Custom(agent) => &agent.id,
        };
        self.apply_asset_instructions(plugin, root, CustomInstallationKind::Agent, id.as_ref())
            .await
    }

    pub(super) async fn materialize_skill(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        skill: &ResolvedSkill,
    ) -> Result<(), DiscussError> {
        match skill {
            ResolvedSkill::Builtin(skill) => plugin.install_skill(root, skill),
            ResolvedSkill::Custom(skill) => plugin.install_custom_skill(root, skill),
        }?;
        let id = match skill {
            ResolvedSkill::Builtin(skill) => &skill.id,
            ResolvedSkill::Custom(skill) => &skill.id,
        };
        self.apply_asset_instructions(plugin, root, CustomInstallationKind::Skill, id.as_ref())
            .await
    }

    async fn apply_asset_instructions(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        kind: CustomInstallationKind,
        id: &str,
    ) -> Result<(), DiscussError> {
        let path = match kind {
            CustomInstallationKind::Agent => plugin.installed_agent_path(&AgentId::new(id)?),
            CustomInstallationKind::Skill => plugin.installed_skill_path(&SkillId::new(id)?),
        };
        let conn = self.persistence.acquire_connection().await?;
        if let Some(row) = load_asset_instructions(&conn, &path.to_string_lossy()).await?
            && !row.instructions.is_empty()
        {
            plugins::append_installed_instructions(
                plugin,
                root,
                &path,
                &row.instructions,
                matches!(kind, CustomInstallationKind::Agent),
            )?;
        }
        Ok(())
    }

    pub(super) async fn rewrite_installed_asset(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        kind: CustomInstallationKind,
        id: &str,
        settings: Option<&plugins::ConfigSettings>,
    ) -> Result<(), DiscussError> {
        match kind {
            CustomInstallationKind::Agent => {
                self.materialize_member(plugin, root, &self.resolve_agent(&AgentId::new(id)?).await?, settings)
                    .await
            }
            CustomInstallationKind::Skill => {
                self.materialize_skill(plugin, root, &self.resolve_skill(&SkillId::new(id)?).await?)
                    .await
            }
        }
    }

    async fn install_builtin_team(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        team: &TeamDefinition,
        settings: &BTreeMap<AgentId, plugins::ConfigSettings>,
    ) -> Result<(), DiscussError> {
        plugin.install_team_with_member_overrides(self.library, root, team, settings)?;
        for id in std::iter::once(&team.leader).chain(&team.followers) {
            self.apply_asset_instructions(plugin, root, CustomInstallationKind::Agent, id.as_ref())
                .await?;
        }
        for id in &team.skills {
            self.apply_asset_instructions(plugin, root, CustomInstallationKind::Skill, id.as_ref())
                .await?;
        }
        Ok(())
    }

    pub(super) fn destroy_member(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        agent: &ResolvedAgent,
    ) -> Result<(), DiscussError> {
        let id = match agent {
            ResolvedAgent::Builtin(agent) => &agent.id,
            ResolvedAgent::Custom(agent) => &agent.id,
        };
        plugin.destroy_agent(root, id)
    }

    pub(super) fn destroy_skill(
        &self,
        plugin: &dyn plugins::DiscussAgentPlugin,
        root: &Path,
        skill: &ResolvedSkill,
    ) -> Result<(), DiscussError> {
        match skill {
            ResolvedSkill::Builtin(skill) => plugin.destroy_skill(root, skill),
            ResolvedSkill::Custom(skill) => plugin.destroy_custom_skill(root, skill),
        }
    }

    /// Re-materialize installed Phanes Teams whose registry version changed.
    ///
    /// Boot must remain available when a Provider or workspace is temporarily
    /// unavailable, so every upgrade failure is logged and processing continues.
    pub(super) async fn upgrade_installed_teams(&self) {
        let Ok(conn) = self.persistence.acquire_connection().await else {
            tracing::warn!(
                target: "application_acp",
                "skipping Discuss Team version upgrades because installation storage could not be opened"
            );
            return;
        };
        let installations = match list_discuss_team_installations(&conn).await {
            Ok(installations) => installations,
            Err(error) => {
                tracing::warn!(
                    target: "application_acp",
                    ?error,
                    "skipping Discuss Team version upgrades because installations could not be loaded"
                );
                return;
            }
        };
        let custom_teams = match self.custom_team_compositions(&conn).await {
            Ok(teams) => teams,
            Err(error) => {
                tracing::warn!(?error, "skipping Team upgrades: custom compositions unavailable");
                return;
            }
        };

        for installation in &installations {
            self.upgrade_team_installation(&conn, &installations, &custom_teams, installation)
                .await;
        }
    }

    async fn upgrade_team_installation(
        &self,
        conn: &turso::Connection,
        installations: &[DiscussTeamInstallationRow],
        custom_teams: &[CustomTeamDefinition],
        installation: &DiscussTeamInstallationRow,
    ) {
        if let Some(team) = custom_teams
            .iter()
            .find(|team| team.leader.as_ref() == installation.leader_id)
        {
            let matches = match team_versions_match(&installation.installed_version, &team.version) {
                Ok(matches) => matches,
                Err(error) => {
                    tracing::warn!(?error, leader_id = %installation.leader_id, "skipping invalid custom Team version");
                    return;
                }
            };
            if !self.agents.contains_key(&installation.provider_name) {
                return;
            }
            let settings = match serde_json::from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)
                .map_err(DiscussError::from)
                .and_then(plugins::member_settings_for_team)
            {
                Ok(settings) => settings,
                Err(error) => {
                    tracing::warn!(?error, "skipping invalid custom Team settings");
                    return;
                }
            };
            let plugin = plugins::for_agent(&installation.provider_name);
            if let Err(error) = self
                .materialize_custom_team(plugin, &self.agents_workspace, team, &settings, matches)
                .await
            {
                tracing::warn!(?error, leader_id = %installation.leader_id, "custom Team refresh failed");
                return;
            }
            if !matches {
                let members = std::iter::once(team.leader.to_string())
                    .chain(team.followers.iter().map(ToString::to_string))
                    .collect::<Vec<_>>();
                let (keep, _) = retained_team_assets(
                    self.library,
                    custom_teams,
                    installations,
                    &installation.leader_id,
                    plugin.agent_scope_key(),
                    plugin.skill_scope_key(),
                );
                for old in serde_json::from_str::<Vec<String>>(&installation.members_json).unwrap_or_default() {
                    if !members.contains(&old)
                        && let Ok(id) = AgentId::try_from(old)
                        && !keep.contains(&id)
                        && let Err(error) = plugin.destroy_agent(&self.agents_workspace, &id)
                    {
                        tracing::warn!(?error, "dropped Team member cleanup failed");
                    }
                }
                match serde_json::to_string(&members) {
                    Ok(json) => {
                        if let Err(error) = upsert_discuss_team_installation(
                            conn,
                            &installation.leader_id,
                            &installation.provider_name,
                            &team.version.to_string(),
                            &installation.member_overrides_json,
                            &json,
                            installation.theme_color_json.as_deref(),
                        )
                        .await
                        {
                            tracing::warn!(?error, "custom Team version update failed");
                        }
                    }
                    Err(error) => tracing::warn!(?error, "custom Team member serialization failed"),
                }
            }
            return;
        }
        let Some(definition) = self
            .library
            .teams_for()
            .find(|definition| definition.leader.as_ref() == installation.leader_id.as_str())
        else {
            tracing::warn!(target: "application_acp", leader_id = %installation.leader_id, "skipping Discuss Team version upgrade because its Phanes definition is unavailable");
            return;
        };
        match team_versions_match(&installation.installed_version, &definition.version) {
            Ok(true) => return,
            Ok(false) => {}
            Err(AgentVersionParseError::Installed(error)) => {
                tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, installed_version = %installation.installed_version, current_version = %definition.version, "skipping Discuss Team version upgrade because the persisted version is invalid");
                return;
            }
        }
        if !self.agents.contains_key(&installation.provider_name) {
            tracing::warn!(target: "application_acp", leader_id = %installation.leader_id, provider_name = %installation.provider_name, installed_version = %installation.installed_version, current_version = %definition.version, "skipping Discuss Team version upgrade because its Provider is no longer installed");
            return;
        }

        let plugin = plugins::for_agent(&installation.provider_name);
        let current_members = team_member_ids(definition);
        self.cleanup_dropped_team_members(
            installations,
            custom_teams,
            installation,
            definition,
            &current_members,
            plugin,
        );
        let member_settings = match serde_json::from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)
            .map_err(DiscussError::from)
            .and_then(plugins::member_settings_for_team)
        {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, "skipping Discuss Team version upgrade because member settings could not be restored");
                return;
            }
        };
        if let Err(error) = self
            .install_builtin_team(plugin, &self.agents_workspace, definition, &member_settings)
            .await
        {
            tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, installed_version = %installation.installed_version, current_version = %definition.version, "Discuss Team version upgrade failed; retaining the persisted version");
            return;
        }
        self.persist_team_upgrade(conn, installation, definition, &current_members)
            .await;
    }

    fn cleanup_dropped_team_members(
        &self,
        installations: &[DiscussTeamInstallationRow],
        custom_teams: &[CustomTeamDefinition],
        installation: &DiscussTeamInstallationRow,
        definition: &TeamDefinition,
        current_members: &[String],
        plugin: &dyn plugins::DiscussAgentPlugin,
    ) {
        let old_members = serde_json::from_str::<Vec<String>>(&installation.members_json).unwrap_or_default();
        let (retain_agents, _) = retained_team_assets(
            self.library,
            custom_teams,
            installations,
            &installation.leader_id,
            plugin.agent_scope_key(),
            plugin.skill_scope_key(),
        );
        for dropped_id in old_members
            .into_iter()
            .filter(|id| !current_members.iter().any(|current| current == id))
        {
            let Ok(dropped_id) = AgentId::try_from(dropped_id.clone()) else {
                tracing::warn!(target: "application_acp", agent_id = %dropped_id, "skipping invalid dropped Team member during upgrade");
                continue;
            };
            if !retain_agents.contains(&dropped_id)
                && let Err(error) = plugin.destroy_agent(&self.agents_workspace, &dropped_id)
            {
                tracing::warn!(target: "application_acp", ?error, agent_id = %dropped_id, team = %definition.leader, "could not clean up dropped Team member during upgrade");
            }
        }
    }

    async fn persist_team_upgrade(
        &self,
        conn: &turso::Connection,
        installation: &DiscussTeamInstallationRow,
        definition: &TeamDefinition,
        current_members: &[String],
    ) {
        let current_members_json = match serde_json::to_string(current_members) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, "Discuss Team materialized but member snapshot could not be serialized");
                return;
            }
        };
        if let Err(error) = upsert_discuss_team_installation(
            conn,
            &installation.leader_id,
            &installation.provider_name,
            &definition.version.to_string(),
            &installation.member_overrides_json,
            &current_members_json,
            installation.theme_color_json.as_deref(),
        )
        .await
        {
            tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, installed_version = %installation.installed_version, current_version = %definition.version, "Discuss Team materialized but persisted version update failed");
        }
    }

    pub(super) async fn handle_list_team_catalog(&self) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let installations = list_discuss_team_installations(&conn).await?;
        let mut installations_by_id = installations
            .into_iter()
            .map(|row| (row.leader_id.clone(), row))
            .collect::<HashMap<_, _>>();
        let mut entries = Vec::new();

        for team in self.library.teams_for() {
            let leader_id = team.leader.to_string();
            let installation = installations_by_id.remove(&leader_id);
            let leader = self
                .library
                .agents_for()
                .find(|agent| agent.id == team.leader);
            let members = std::iter::once(team.leader.clone())
                .chain(team.followers.iter().cloned())
                .map(|agent_id| {
                    self.library
                        .agents_for()
                        .find(|agent| agent.id == agent_id)
                        .map(|agent| TeamCatalogMember {
                            agent_id: agent_id.to_string(),
                            name: agent.name.to_string(),
                            job_title: agent.job_title.to_string(),
                            body: agent.body.to_string(),
                        })
                })
                .collect::<Option<Vec<_>>>();
            let skill_names = team
                .skills
                .iter()
                .map(|skill_id| {
                    self.library
                        .skills_for()
                        .find(|skill| skill.id == *skill_id)
                        .map(|skill| skill.name.to_string())
                })
                .collect::<Option<Vec<_>>>();
            entries.push(TeamCatalogEntry {
                leader_id,
                version: installation
                    .as_ref()
                    .map_or_else(|| team.version.to_string(), |row| row.installed_version.clone()),
                name: leader.map(|agent| agent.name.to_string()),
                job_title: leader.map(|agent| agent.job_title.to_string()),
                description: leader.map(|agent| agent.description.to_string()),
                installed: installation.is_some(),
                provider_name: installation.as_ref().map(|row| row.provider_name.clone()),
                member_overrides: installation
                    .as_ref()
                    .map(|row| serde_json::from_str(&row.member_overrides_json))
                    .transpose()?,
                theme_color: installation
                    .as_ref()
                    .and_then(|row| row.theme_color_json.as_deref())
                    .map(serde_json::from_str)
                    .transpose()?,
                definition_available: true,
                definition_kind: TeamDefinitionKind::Builtin,
                members,
                skill_names,
                skill_ids: Some(team.skills.iter().map(ToString::to_string).collect()),
            });
        }

        for row in list_custom_teams(&conn).await? {
            let leader_id = row.leader_id.clone();
            let leader = AgentId::try_from(leader_id.clone())?;
            let ResolvedTeam::Custom(team) = self.resolve_team(&leader).await? else {
                continue;
            };
            let installation = installations_by_id.remove(&leader_id);
            let mut members = Vec::new();
            for id in std::iter::once(&team.leader).chain(&team.followers) {
                let agent = self.resolve_agent(id).await?;
                let (name, job_title, body) = match agent {
                    ResolvedAgent::Builtin(agent) => (
                        agent.name.to_string(),
                        agent.job_title.to_string(),
                        agent.body.to_string(),
                    ),
                    ResolvedAgent::Custom(agent) => (agent.name.clone(), agent.job_title.clone(), agent.body.clone()),
                };
                members.push(TeamCatalogMember {
                    agent_id: id.to_string(),
                    name,
                    job_title,
                    body,
                });
            }
            let mut skill_names = Vec::new();
            for id in &team.skills {
                skill_names.push(match self.resolve_skill(id).await? {
                    ResolvedSkill::Builtin(skill) => skill.name.to_string(),
                    ResolvedSkill::Custom(skill) => skill.name.clone(),
                });
            }
            let leader_agent = self.resolve_agent(&team.leader).await?;
            let (job_title, description) = match leader_agent {
                ResolvedAgent::Builtin(agent) => (agent.job_title.to_string(), agent.description.to_string()),
                ResolvedAgent::Custom(agent) => (agent.job_title.clone(), agent.description.clone()),
            };
            entries.push(TeamCatalogEntry {
                leader_id,
                version: team.version.to_string(),
                name: row.display_name,
                job_title: Some(job_title),
                description: Some(description),
                installed: installation.is_some(),
                provider_name: installation.as_ref().map(|row| row.provider_name.clone()),
                member_overrides: installation
                    .as_ref()
                    .map(|row| serde_json::from_str(&row.member_overrides_json))
                    .transpose()?,
                theme_color: installation
                    .as_ref()
                    .and_then(|row| row.theme_color_json.as_deref())
                    .map(serde_json::from_str)
                    .transpose()?,
                definition_available: true,
                definition_kind: TeamDefinitionKind::Custom,
                members: Some(members),
                skill_names: Some(skill_names),
                skill_ids: Some(team.skills.iter().map(ToString::to_string).collect()),
            });
        }

        for installation in installations_by_id.into_values() {
            entries.push(TeamCatalogEntry {
                leader_id: installation.leader_id,
                version: installation.installed_version,
                name: None,
                job_title: None,
                description: None,
                installed: true,
                provider_name: Some(installation.provider_name),
                member_overrides: Some(serde_json::from_str(&installation.member_overrides_json)?),
                theme_color: installation
                    .theme_color_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?,
                definition_available: false,
                definition_kind: TeamDefinitionKind::Unavailable,
                members: None,
                skill_names: None,
                skill_ids: None,
            });
        }

        Ok(DiscussResponse::ListTeamCatalog { entries })
    }

    pub(super) async fn handle_install_team(
        &self,
        leader_id: String,
        provider_name: String,
        member_overrides: DiscussMemberOverrides,
        theme_color: Option<AgentThemeColor>,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.agents.contains_key(&provider_name) {
            return Err(DiscussError::WorkspaceInvalidAssetId {
                asset_id: provider_name,
                violation: "provider is not installed".to_string(),
            });
        }
        let leader = AgentId::try_from(leader_id.clone()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
            asset_id: leader_id.clone(),
            violation: error.to_string(),
        })?;
        let team = match self.resolve_team(&leader).await? {
            ResolvedTeam::Builtin(team) => team,
            ResolvedTeam::Custom(team) => {
                let conn = self.persistence.acquire_connection().await?;
                let existing = load_discuss_team_installation(&conn, &leader_id).await?;
                let warnings = self
                    .write_custom_team_installation(
                        &conn,
                        &team,
                        &provider_name,
                        member_overrides,
                        theme_color,
                        existing.as_ref(),
                    )
                    .await?;
                return Ok(DiscussResponse::InstallTeam { warnings });
            }
        };
        let plugin = plugins::for_agent(&provider_name);

        let conn = self.persistence.acquire_connection().await?;
        let member_overrides = validate_member_overrides(&conn, &provider_name, team, member_overrides).await?;
        let member_overrides_json = serde_json::to_string(&member_overrides)?;
        let members_json = serde_json::to_string(&team_member_ids(team))?;
        let theme_color_json = theme_color
            .map(|color| serde_json::to_string(&color))
            .transpose()?;

        let existing_installations = { list_discuss_team_installations(&conn).await? };
        let custom_teams = self.custom_team_compositions(&conn).await?;
        let (retain_agents, retain_skills) = retained_team_assets(
            self.library,
            &custom_teams,
            &existing_installations,
            &leader_id,
            plugin.agent_scope_key(),
            plugin.skill_scope_key(),
        );

        let member_settings = plugins::member_settings_for_team(member_overrides.clone())?;
        self.install_builtin_team(plugin, &self.agents_workspace, team, &member_settings)
            .await?;

        let persistence_result = async {
            let conn = self.persistence.acquire_connection().await?;
            upsert_discuss_team_installation(
                &conn,
                &leader_id,
                &provider_name,
                &team.version.to_string(),
                &member_overrides_json,
                &members_json,
                theme_color_json.as_deref(),
            )
            .await
        }
        .await;

        match persistence_result {
            Ok(()) => Ok(DiscussResponse::InstallTeam { warnings: Vec::new() }),
            Err(error) => match plugin.destroy_team(
                self.library,
                &self.agents_workspace,
                team,
                &retain_agents,
                &retain_skills,
            ) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(with_install_compensation_context(
                    error,
                    &leader_id,
                    &provider_name,
                    &self.agents_workspace,
                    &cleanup_error,
                )),
            },
        }
    }

    pub(super) async fn handle_edit_team(
        &self,
        leader_id: String,
        provider_name: String,
        member_overrides: DiscussMemberOverrides,
        theme_color: Option<AgentThemeColor>,
    ) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let Some(installation) = load_discuss_team_installation(&conn, &leader_id).await? else {
            return Err(DiscussError::WorkspaceInvalidAssetId {
                asset_id: leader_id,
                violation: "team installation is not present".to_string(),
            });
        };
        let leader = AgentId::try_from(leader_id.clone()).map_err(|error| DiscussError::WorkspaceInvalidAssetId {
            asset_id: leader_id.clone(),
            violation: error.to_string(),
        })?;

        if let ResolvedTeam::Custom(team) = self.resolve_team(&leader).await? {
            if !self.agents.contains_key(&provider_name) {
                return Err(DiscussError::WorkspaceInvalidAssetId {
                    asset_id: provider_name,
                    violation: "provider is not installed".to_string(),
                });
            }
            let warnings = self
                .write_custom_team_installation(
                    &conn,
                    &team,
                    &provider_name,
                    member_overrides,
                    theme_color,
                    Some(&installation),
                )
                .await?;
            return Ok(DiscussResponse::EditTeam { warnings });
        }

        let team = self
            .library
            .teams_for()
            .find(|team| team.leader == leader)
            .ok_or_else(|| DiscussError::WorkspaceInvalidAssetId {
                asset_id: leader_id.clone(),
                violation: "team definition is not registered and is uninstall-only".to_string(),
            })?;

        let member_overrides = validate_member_overrides(&conn, &provider_name, team, member_overrides).await?;
        let member_overrides_json = serde_json::to_string(&member_overrides)?;
        let members_json = serde_json::to_string(&team_member_ids(team))?;
        let theme_color_json = theme_color
            .map(|color| serde_json::to_string(&color))
            .transpose()?;
        if installation.provider_name == provider_name {
            return self
                .edit_team_on_provider(
                    &conn,
                    &installation,
                    team,
                    &leader_id,
                    &member_overrides,
                    &member_overrides_json,
                    &members_json,
                    theme_color_json.as_deref(),
                )
                .await;
        }

        if !self.agents.contains_key(&provider_name) {
            return Err(DiscussError::WorkspaceInvalidAssetId {
                asset_id: provider_name,
                violation: "provider is not installed".to_string(),
            });
        }

        self.move_team_to_provider(
            &conn,
            &installation,
            team,
            &leader_id,
            &provider_name,
            &member_overrides,
            &member_overrides_json,
            &members_json,
            theme_color_json.as_deref(),
        )
        .await
    }

    // Keep the persisted Team edit inputs explicit; grouping them would obscure the unchanged workflow.
    #[allow(clippy::too_many_arguments)]
    async fn edit_team_on_provider(
        &self,
        conn: &turso::Connection,
        installation: &DiscussTeamInstallationRow,
        team: &TeamDefinition,
        leader_id: &str,
        member_overrides: &DiscussMemberOverrides,
        member_overrides_json: &str,
        members_json: &str,
        theme_color_json: Option<&str>,
    ) -> Result<DiscussResponse, DiscussError> {
        let member_settings = plugins::member_settings_for_team(member_overrides.clone())?;
        let previous_member_settings =
            serde_json::from_str::<DiscussMemberOverrides>(&installation.member_overrides_json)
                .map_err(DiscussError::from)
                .and_then(plugins::member_settings_for_team)?;
        let plugin = plugins::for_agent(&installation.provider_name);
        self.install_builtin_team(plugin, &self.agents_workspace, team, &member_settings)
            .await?;
        let persistence_result = upsert_discuss_team_installation(
            conn,
            leader_id,
            &installation.provider_name,
            &team.version.to_string(),
            member_overrides_json,
            members_json,
            theme_color_json,
        )
        .await;
        match persistence_result {
            Ok(()) => Ok(DiscussResponse::EditTeam { warnings: Vec::new() }),
            Err(error) => {
                match self
                    .install_builtin_team(plugin, &self.agents_workspace, team, &previous_member_settings)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(restore_error) => Err(with_install_compensation_context(
                        error,
                        leader_id,
                        &installation.provider_name,
                        &self.agents_workspace,
                        &restore_error,
                    )),
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn move_team_to_provider(
        &self,
        conn: &turso::Connection,
        installation: &DiscussTeamInstallationRow,
        team: &TeamDefinition,
        leader_id: &str,
        provider_name: &str,
        member_overrides: &DiscussMemberOverrides,
        member_overrides_json: &str,
        members_json: &str,
        theme_color_json: Option<&str>,
    ) -> Result<DiscussResponse, DiscussError> {
        let destination_plugin = plugins::for_agent(provider_name);
        let all_installations = list_discuss_team_installations(conn).await?;
        let custom_teams = self.custom_team_compositions(conn).await?;
        let (destination_retain_agents, destination_retain_skills) = retained_team_assets(
            self.library,
            &custom_teams,
            &all_installations,
            leader_id,
            destination_plugin.agent_scope_key(),
            destination_plugin.skill_scope_key(),
        );
        let member_settings = plugins::member_settings_for_team(member_overrides.clone())?;
        self.install_builtin_team(destination_plugin, &self.agents_workspace, team, &member_settings)
            .await?;

        let persistence_result = upsert_discuss_team_installation(
            conn,
            leader_id,
            provider_name,
            &team.version.to_string(),
            member_overrides_json,
            members_json,
            theme_color_json,
        )
        .await;
        if let Err(error) = persistence_result {
            return match destination_plugin.destroy_team(
                self.library,
                &self.agents_workspace,
                team,
                &destination_retain_agents,
                &destination_retain_skills,
            ) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(with_install_compensation_context(
                    error,
                    leader_id,
                    provider_name,
                    &self.agents_workspace,
                    &cleanup_error,
                )),
            };
        }

        let old_plugin = plugins::for_agent(&installation.provider_name);
        let installations = list_discuss_team_installations(conn).await?;
        let (mut retain_agents, mut retain_skills) = retained_team_assets(
            self.library,
            &custom_teams,
            &installations,
            leader_id,
            old_plugin.agent_scope_key(),
            old_plugin.skill_scope_key(),
        );
        if old_plugin.agent_scope_key() == destination_plugin.agent_scope_key() {
            retain_agents.insert(team.leader.clone());
            retain_agents.extend(team.followers.iter().cloned());
        }
        if old_plugin.skill_scope_key() == destination_plugin.skill_scope_key() {
            retain_skills.extend(team.skills.iter().cloned());
        }
        let warnings = match old_plugin.destroy_team(
            self.library,
            &self.agents_workspace,
            team,
            &retain_agents,
            &retain_skills,
        ) {
            Ok(()) => Vec::new(),
            Err(error) => vec![cleanup_warning(&installation.provider_name, leader_id, &error)],
        };
        Ok(DiscussResponse::EditTeam { warnings })
    }

    pub(super) async fn handle_uninstall_team(&mut self, leader_id: String) -> Result<DiscussResponse, DiscussError> {
        let conn = self.persistence.acquire_connection().await?;
        let Some(installation) = load_discuss_team_installation(&conn, &leader_id).await? else {
            return Ok(DiscussResponse::UninstallTeam { warnings: Vec::new() });
        };
        let installations = list_discuss_team_installations(&conn).await?;
        let custom_teams = self.custom_team_compositions(&conn).await?;
        let plugin = plugins::for_agent(&installation.provider_name);
        let custom_team = custom_teams
            .iter()
            .find(|team| team.leader.as_ref() == leader_id);
        let cleanup_result = if let Some(team) = custom_team {
            let (retain_agents, retain_skills) = retained_team_assets(
                self.library,
                &custom_teams,
                &installations,
                &leader_id,
                plugin.agent_scope_key(),
                plugin.skill_scope_key(),
            );
            self.destroy_custom_team(plugin, team, &retain_agents, &retain_skills)
                .await
        } else {
            match AgentId::try_from(leader_id.clone()) {
                Ok(leader_id) => match self
                    .library
                    .teams_for()
                    .find(|team| team.leader == leader_id)
                {
                    Some(team) => {
                        let (retain_agents, retain_skills) = retained_team_assets(
                            self.library,
                            &custom_teams,
                            &installations,
                            leader_id.as_ref(),
                            plugin.agent_scope_key(),
                            plugin.skill_scope_key(),
                        );
                        plugin.destroy_team(
                            self.library,
                            &self.agents_workspace,
                            team,
                            &retain_agents,
                            &retain_skills,
                        )
                    }
                    None => Ok(()),
                },
                Err(error) => Err(DiscussError::WorkspaceInvalidAssetId {
                    asset_id: leader_id.clone(),
                    violation: error.to_string(),
                }),
            }
        };

        let warning = cleanup_result
            .as_ref()
            .err()
            .map(|error| cleanup_warning(&installation.provider_name, &leader_id, error));
        let delete_result = delete_discuss_team_installation(&conn, &leader_id).await;
        if let Err(error) = delete_result {
            return match cleanup_result {
                Err(cleanup_error) => Err(with_install_compensation_context(
                    error,
                    &leader_id,
                    &installation.provider_name,
                    &self.agents_workspace,
                    &cleanup_error,
                )),
                Ok(()) => Err(error),
            };
        }
        Ok(DiscussResponse::UninstallTeam {
            warnings: warning.into_iter().collect(),
        })
    }
}

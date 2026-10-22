use std::collections::HashMap;

use super::actor::{Actor, AgentVersionParseError, team_versions_match};
use super::database::*;
use super::library::{AgentId, TeamDefinition};
use super::messages::DiscussResponse;
use super::plugins;
use super::storage::{
    DiscussTeamInstallationRow, delete_discuss_team_installation, list_discuss_team_installations,
    load_discuss_team_installation, upsert_discuss_team_installation,
};
use super::types::{AgentThemeColor, DiscussError, DiscussMemberOverrides, TeamCatalogEntry, TeamCatalogMember};

impl Actor {
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

        for installation in &installations {
            self.upgrade_team_installation(&conn, &installations, installation)
                .await;
        }
    }

    async fn upgrade_team_installation(
        &self,
        conn: &turso::Connection,
        installations: &[DiscussTeamInstallationRow],
        installation: &DiscussTeamInstallationRow,
    ) {
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
        self.cleanup_dropped_team_members(installations, installation, definition, &current_members, plugin);
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
        if let Err(error) = plugin.install_team_with_member_overrides(
            self.library,
            &self.agents_workspace,
            definition,
            &member_settings,
        ) {
            tracing::warn!(target: "application_acp", ?error, leader_id = %installation.leader_id, provider_name = %installation.provider_name, installed_version = %installation.installed_version, current_version = %definition.version, "Discuss Team version upgrade failed; retaining the persisted version");
            return;
        }
        self.persist_team_upgrade(conn, installation, definition, &current_members)
            .await;
    }

    fn cleanup_dropped_team_members(
        &self,
        installations: &[DiscussTeamInstallationRow],
        installation: &DiscussTeamInstallationRow,
        definition: &TeamDefinition,
        current_members: &[String],
        plugin: &dyn plugins::DiscussAgentPlugin,
    ) {
        let old_members = serde_json::from_str::<Vec<String>>(&installation.members_json).unwrap_or_default();
        let (retain_agents, _) = retained_team_assets(
            self.library,
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
                members,
                skill_names,
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
                members: None,
                skill_names: None,
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
        let team = plugins::resolve_team(self.library, &leader)?;
        let plugin = plugins::for_agent(&provider_name);

        let conn = self.persistence.acquire_connection().await?;
        let member_overrides = validate_member_overrides(&conn, &provider_name, team, member_overrides).await?;
        let member_overrides_json = serde_json::to_string(&member_overrides)?;
        let members_json = serde_json::to_string(&team_member_ids(team))?;
        let theme_color_json = theme_color
            .map(|color| serde_json::to_string(&color))
            .transpose()?;

        let existing_installations = { list_discuss_team_installations(&conn).await? };
        let (retain_agents, retain_skills) = retained_team_assets(
            self.library,
            &existing_installations,
            &leader_id,
            plugin.agent_scope_key(),
            plugin.skill_scope_key(),
        );

        let member_settings = plugins::member_settings_for_team(member_overrides.clone())?;
        plugin.install_team_with_member_overrides(self.library, &self.agents_workspace, team, &member_settings)?;

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
        plugin.install_team_with_member_overrides(self.library, &self.agents_workspace, team, &member_settings)?;
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
                match plugin.install_team_with_member_overrides(
                    self.library,
                    &self.agents_workspace,
                    team,
                    &previous_member_settings,
                ) {
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
        let (destination_retain_agents, destination_retain_skills) = retained_team_assets(
            self.library,
            &all_installations,
            leader_id,
            destination_plugin.agent_scope_key(),
            destination_plugin.skill_scope_key(),
        );
        let member_settings = plugins::member_settings_for_team(member_overrides.clone())?;
        destination_plugin.install_team_with_member_overrides(
            self.library,
            &self.agents_workspace,
            team,
            &member_settings,
        )?;

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
        let plugin = plugins::for_agent(&installation.provider_name);
        let cleanup_result = match AgentId::try_from(leader_id.clone()) {
            Ok(leader_id) => match self
                .library
                .teams_for()
                .find(|team| team.leader == leader_id)
            {
                Some(team) => {
                    let (retain_agents, retain_skills) = retained_team_assets(
                        self.library,
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

    #[cfg(test)]
    pub(super) async fn upgrade_installed_agents(&self) {
        self.upgrade_installed_teams().await;
    }
}

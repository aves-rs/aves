use super::error::DiscussError;
use super::plugins::ConfigSettings;
use super::types::DiscussConfigOption;

/// Serialize provider-refreshed config options into the persisted JSON blob for
/// a load refresh (R3), returning `None` when the provider response carried no
/// options so the merge-safe refresh preserves cached options instead of
/// clearing them.
pub(super) fn provider_config_options_json(
    config_options: &Option<Vec<DiscussConfigOption>>,
) -> Result<Option<String>, DiscussError> {
    match config_options {
        Some(options) => Ok(Some(serde_json::to_string(options)?)),
        None => Ok(None),
    }
}

pub(super) fn config_options_from_json(json: &str) -> Result<Vec<DiscussConfigOption>, DiscussError> {
    serde_json::from_str(json).map_err(DiscussError::from)
}

pub(super) fn config_settings_from_json(json: &str) -> Result<ConfigSettings, DiscussError> {
    serde_json::from_str(json).map_err(DiscussError::from)
}

pub(super) fn config_settings_from_value(value: &serde_json::Value) -> ConfigSettings {
    value
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(id, value)| value.as_str().map(|value| (id.clone(), value.to_string())))
        .collect()
}

pub(super) fn merged_config_preferences(
    agent_defaults: &ConfigSettings,
    session_settings: &ConfigSettings,
) -> ConfigSettings {
    let mut preferences = agent_defaults.clone();
    preferences.extend(
        session_settings
            .iter()
            .map(|(config_id, value)| (config_id.clone(), value.clone())),
    );
    preferences
}

/// Finds the Provider-advertised value selecting an Agent through the plugin-chosen config option.
pub(super) fn primary_agent_option_selection(
    provider_options: &[DiscussConfigOption],
    config_id: &str,
    agent_id: &str,
) -> Option<ConfigPreferenceSelection> {
    provider_options
        .iter()
        .find(|option| option.id == config_id)
        .filter(|option| {
            option
                .options
                .iter()
                .any(|candidate| candidate.value == agent_id)
        })
        .map(|option| ConfigPreferenceSelection {
            config_id: option.id.clone(),
            value: agent_id.to_string(),
        })
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ConfigPreferenceSelection {
    pub(super) config_id: String,
    pub(super) value: String,
}

pub(super) struct ConfigPreferenceReconciler<'a> {
    preferences: &'a ConfigSettings,
    provider_options: Vec<DiscussConfigOption>,
    attempted: std::collections::HashSet<String>,
}

impl<'a> ConfigPreferenceReconciler<'a> {
    pub(super) fn new(preferences: &'a ConfigSettings, provider_options: Vec<DiscussConfigOption>) -> Self {
        Self {
            preferences,
            provider_options,
            attempted: std::collections::HashSet::new(),
        }
    }

    pub(super) fn next(&mut self) -> Option<ConfigPreferenceSelection> {
        let selection = next_config_preference(self.preferences, &self.provider_options, &self.attempted)?;
        self.attempted.insert(selection.config_id.clone());
        Some(selection)
    }

    pub(super) fn adopt_provider_options(&mut self, provider_options: Vec<DiscussConfigOption>) {
        self.provider_options = provider_options;
    }

    pub(super) fn finish(self) -> Vec<DiscussConfigOption> {
        self.provider_options
    }
}

pub(super) fn next_config_preference(
    preferences: &ConfigSettings,
    provider_options: &[DiscussConfigOption],
    attempted: &std::collections::HashSet<String>,
) -> Option<ConfigPreferenceSelection> {
    provider_options.iter().find_map(|option| {
        if attempted.contains(&option.id) {
            return None;
        }
        let value = preferences.get(&option.id)?;
        if option.value == *value {
            return None;
        }
        config_option_supports_value(option, value).then(|| ConfigPreferenceSelection {
            config_id: option.id.clone(),
            value: value.clone(),
        })
    })
}

fn config_option_supports_value(option: &DiscussConfigOption, requested: &str) -> bool {
    option.options.iter().any(|entry| entry.value == requested)
}

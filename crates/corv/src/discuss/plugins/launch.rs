//! Provider-neutral launch preparation contracts.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use agent_client_protocol::v1::schema::Meta;

use super::super::types::{
    DiscussError, DiscussSessionId, Effect, LaunchLimitation, Operation, Rule, RuleSet, SessionLaunchConfig,
};
use super::ConfigSettings;

pub(crate) struct LaunchContext {
    pub(crate) settings_root: PathBuf,
}

impl LaunchContext {
    pub(crate) fn new(agents_workspace: &Path, session_id: DiscussSessionId) -> Self {
        Self {
            settings_root: agents_workspace
                .join("sessions")
                .join(session_id.to_string()),
        }
    }
}

#[derive(Default)]
pub(crate) struct LaunchPreparation {
    pub(crate) meta: Option<Meta>,
    pub(crate) limitations: Vec<LaunchLimitation>,
}

#[derive(Default)]
pub(crate) struct LaunchSelections {
    pub(crate) settings: ConfigSettings,
    pub(crate) limitations: Vec<LaunchLimitation>,
}

/// Intersect sources, treating an omitted operation as denied.
pub(crate) fn intersect_rules(sources: &[RuleSet]) -> Option<RuleSet> {
    if sources.is_empty() {
        return None;
    }
    let rules = [Operation::Read, Operation::Write, Operation::Execute]
        .into_iter()
        .map(|operation| {
            let effect = sources
                .iter()
                .map(|source| {
                    source
                        .rules
                        .iter()
                        .filter(|rule| rule.operation == operation)
                        .map(|rule| rule.effect)
                        .max()
                        .unwrap_or(Effect::Deny)
                })
                .max()
                .unwrap_or(Effect::Deny);
            Rule { operation, effect }
        })
        .collect();
    Some(RuleSet { rules })
}

/// Reject duplicate caller-supplied plugin references.
pub(crate) fn validate_supplied_inputs(config: &SessionLaunchConfig) -> Result<(), DiscussError> {
    let mut references = BTreeSet::new();
    for reference in &config.plugin_references {
        if !references.insert(reference) {
            return Err(DiscussError::Validation {
                message: "Duplicate plugin reference".to_owned(),
            });
        }
    }
    Ok(())
}

/// Combine only disjoint or identical top-level metadata entries.
pub(crate) fn merge_metadata(first: Option<Meta>, second: Option<Meta>) -> Result<Option<Meta>, DiscussError> {
    let Some(mut first) = first else { return Ok(second) };
    if let Some(second) = second {
        for (key, value) in second {
            if let Some(existing) = first.get(&key) {
                if existing != &value {
                    return Err(DiscussError::Validation {
                        message: format!("Conflicting session metadata key: {key}"),
                    });
                }
            } else {
                first.insert(key, value);
            }
        }
    }
    Ok(Some(first))
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{LaunchLimitationReason, LaunchLimitationSubject, LaunchPermissionOutcome};
    use super::super::{DEFAULT_PLUGIN, DiscussAgentPlugin};
    use super::*;
    use serde_json::json;

    #[test]
    fn intersection_handles_absence_empty_and_mixed_sources() {
        assert_eq!(intersect_rules(&[]), None);
        assert_eq!(
            intersect_rules(&[RuleSet::default()])
                .unwrap()
                .rules
                .iter()
                .map(|r| r.effect)
                .collect::<Vec<_>>(),
            vec![Effect::Deny; 3]
        );
        let source = |effect| RuleSet {
            rules: vec![
                Rule {
                    operation: Operation::Read,
                    effect,
                },
                Rule {
                    operation: Operation::Write,
                    effect: Effect::Allow,
                },
            ],
        };
        assert_eq!(
            intersect_rules(&[source(Effect::Allow), source(Effect::Ask)])
                .unwrap()
                .rules
                .iter()
                .map(|r| r.effect)
                .collect::<Vec<_>>(),
            vec![Effect::Ask, Effect::Allow, Effect::Deny]
        );
    }

    #[test]
    fn supplied_duplicates_are_rejected() {
        assert!(
            validate_supplied_inputs(&SessionLaunchConfig {
                plugin_references: vec!["x".into(), "x".into()],
                ..Default::default()
            })
            .is_err()
        );
        assert!(validate_supplied_inputs(&SessionLaunchConfig::default()).is_ok());
    }

    #[test]
    fn metadata_merge_checks_top_level_conflicts() {
        let map = |key: &str, value| Meta::from_iter([(key.to_owned(), value)]);
        assert_eq!(
            merge_metadata(Some(map("a", json!(1))), Some(map("b", json!(2))))
                .unwrap()
                .unwrap()
                .len(),
            2
        );
        assert!(merge_metadata(Some(map("a", json!(1))), Some(map("a", json!(1)))).is_ok());
        assert!(merge_metadata(Some(map("a", json!(1))), Some(map("a", json!(2)))).is_err());
    }

    #[test]
    fn default_preparation_discloses_inherited_permissions_and_plugins() {
        let root = Path::new("workspace");
        let context = LaunchContext::new(root, DiscussSessionId::new());
        let config = SessionLaunchConfig {
            rule_sources: vec![RuleSet {
                rules: vec![Rule {
                    operation: Operation::Read,
                    effect: Effect::Allow,
                }],
            }],
            plugin_references: vec!["one".into(), "two".into()],
            ..Default::default()
        };
        let prepared = DEFAULT_PLUGIN
            .launch_preparation(&context, &config)
            .expect("preparation");
        assert!(prepared.meta.is_none());
        assert_eq!(prepared.limitations.len(), 5);
        for (limitation, (operation, requested)) in prepared.limitations[..3].iter().zip([
            (Operation::Read, Effect::Allow),
            (Operation::Write, Effect::Deny),
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
            assert_eq!(
                limitation.reason,
                LaunchLimitationReason::InheritedUnverifiedConfiguration
            );
        }
        for (index, limitation) in prepared.limitations[3..].iter().enumerate() {
            assert_eq!(limitation.subject, LaunchLimitationSubject::PluginReference { index });
            assert_eq!(limitation.reason, LaunchLimitationReason::UnsupportedPluginReference);
        }
        assert!(
            DEFAULT_PLUGIN
                .launch_preparation(&context, &SessionLaunchConfig::default())
                .unwrap()
                .limitations
                .is_empty()
        );
    }
}

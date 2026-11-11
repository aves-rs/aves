//! Translation of resolved, harness-neutral operation rules to OpenCode V2.

use crate::types::{Effect, Operation, RuleSet};
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct OpenCodeRule {
    action: &'static str,
    resource: &'static str,
    effect: Effect,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct OpenCodePolicy {
    action: &'static str,
    resource: &'static str,
    effect: Effect,
}

/// Configuration fragment; the caller must enforce safe per-session placement.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct OpenCodePermissions {
    pub permissions: Vec<OpenCodeRule>,
    pub policies: Vec<OpenCodePolicy>,
}

/// Indicates an absent source or a source missing one or more operations.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct UnavailablePolicy;

/// A deny-by-default fragment and its resolution status, kept together.
pub(crate) struct Translation {
    pub config: OpenCodePermissions,
    pub resolution: Result<(), UnavailablePolicy>,
}

/// Intersect all resolved sources. No sources or missing operations deny.
pub(crate) fn translate(sources: &[RuleSet]) -> Translation {
    let effect = |operation| {
        sources
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
            .unwrap_or(Effect::Deny)
    };
    let read = effect(Operation::Read);
    let write = effect(Operation::Write);
    let execute = effect(Operation::Execute);
    let mut permissions = vec![OpenCodeRule {
        action: "*",
        resource: "*",
        effect: Effect::Deny,
    }];
    for (action, effect) in [("read", read), ("edit", write), ("shell", execute)] {
        permissions.push(OpenCodeRule {
            action,
            resource: "*",
            effect,
        });
    }
    // Code Mode can reach tools independently of shell, so it is never granted.
    permissions.push(OpenCodeRule {
        action: "execute",
        resource: "*",
        effect: Effect::Deny,
    });
    let mut policies = Vec::new();
    for (resource, effect) in [
        ("read:*", read),
        ("edit:*", write),
        ("shell:*", execute),
        ("execute:*", Effect::Deny),
    ] {
        if effect == Effect::Deny {
            policies.push(OpenCodePolicy {
                action: "tool.use",
                resource,
                effect,
            });
        }
    }
    let complete = !sources.is_empty()
        && sources.iter().all(|source| {
            [Operation::Read, Operation::Write, Operation::Execute]
                .iter()
                .all(|operation| source.rules.iter().any(|rule| rule.operation == *operation))
        });
    Translation {
        config: OpenCodePermissions { permissions, policies },
        resolution: if complete { Ok(()) } else { Err(UnavailablePolicy) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Rule;

    #[test]
    fn empty_and_intersected_sets_fail_closed() {
        let unrestricted = RuleSet {
            rules: vec![Rule {
                operation: Operation::Write,
                effect: Effect::Allow,
            }],
        };
        let readonly = RuleSet {
            rules: vec![Rule {
                operation: Operation::Write,
                effect: Effect::Deny,
            }],
        };
        let empty = translate(&[]);
        assert!(empty.config.policies.iter().any(|p| p.resource == "edit:*"));
        assert!(empty.resolution.is_err());
        assert_eq!(
            translate(&[unrestricted, readonly]).config.permissions[2].effect,
            Effect::Deny
        );
    }

    #[test]
    fn serializes_native_v2_shape() {
        let fragment = serde_json::to_value(translate(&[]).config).unwrap();
        assert_eq!(fragment["permissions"][0]["effect"], "deny");
        assert_eq!(fragment["policies"][0]["action"], "tool.use");
    }
}

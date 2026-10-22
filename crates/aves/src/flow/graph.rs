use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use specta::Type;

use super::ast::{FlowNode, NodeDataSpace};
use super::registration::FlowNodeRegistry;
use super::types::{FlowGraphLoadError, FlowNodeId, FlowRegistrationError};

/// Bonsai-supported policy variants for a parallel controller.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub enum ParallelPolicy {
    /// Continue until all children finish.
    WhileAll,
    /// Succeed when every child succeeds.
    WhenAll,
    /// Succeed when any child succeeds.
    WhenAny,
    /// Finish on the first terminal child and fence the rest.
    Race,
}

/// A first-class Bonsai controller in the persisted graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum GraphNode {
    /// Continue after the child completes, retaining the child's recorded outcome.
    AlwaysSucceed {
        /// The behavior whose failure does not stop its parent sequence.
        child: Box<Self>,
    },
    /// Ordered child execution.
    Sequence { children: Vec<Self> },
    /// Ordered success/failure fallback execution.
    Selector { children: Vec<Self> },
    /// Explicit fallback spelling retained in the persisted AST.
    Fallback { children: Vec<Self> },
    /// Parallel execution under one Bonsai policy.
    Parallel {
        /// Supported Bonsai parallel policy.
        policy: ParallelPolicy,
        /// Child graph nodes.
        children: Vec<Self>,
    },
    /// A typed Flow leaf node.
    Leaf {
        /// Leaf payload with flattened typed configuration.
        node: Box<FlowNode>,
    },
}

/// Versioned JSON AST persisted as the authoritative Flow definition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlowGraphAst {
    /// AST schema version.
    pub version: u32,
    /// Root controller or leaf.
    pub root: GraphNode,
}

impl FlowGraphAst {
    /// Decode and semantically validate a persisted graph before execution.
    pub(super) fn load_json(value: &str) -> Result<Self, FlowGraphLoadError> {
        let graph: Self = serde_json::from_str(value)?;
        graph.validate()?;
        Ok(graph)
    }

    /// Validate Flow-owned graph structure without constructing a runtime cursor.
    pub fn validate(&self) -> Result<(), FlowGraphLoadError> {
        if self.version == 0 {
            return Err(invalid("version", "must be greater than zero"));
        }
        let mut ids = HashSet::new();
        validate_node(&self.root, "root", &mut ids)
    }

    pub(super) fn validate_with_registry(&self, registry: &FlowNodeRegistry) -> Result<(), FlowGraphLoadError> {
        self.validate()?;
        let mut result = Ok(());
        self.root.for_each_leaf(&mut |node| {
            if result.is_err() {
                return;
            }
            result = registry
                .get(&node.node_kind)
                .map_err(|error| match error {
                    FlowRegistrationError::Unregistered(kind) => FlowGraphLoadError::UnregisteredNodeKind(kind),
                    error => invalid("root", &error.to_string()),
                })
                .and_then(|registration| {
                    registration
                        .decode_configuration(node.configuration.clone())
                        .map(|_| ())
                        .map_err(|error| invalid("root", &error.to_string()))
                });
        });
        result
    }
}

/// Validated, load-ready graph model. Its root remains the sole source of node
/// configuration; no owner-specific side table is created during loading.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowGraph {
    ast: FlowGraphAst,
}

impl FlowGraph {
    /// Decode a persisted JSON AST and reject it before it can be executed.
    pub(super) fn load_json(value: &str) -> Result<Self, FlowGraphLoadError> {
        Ok(Self {
            ast: FlowGraphAst::load_json(value)?,
        })
    }

    /// Returns the validated persisted AST.
    pub(super) fn ast(&self) -> &FlowGraphAst {
        &self.ast
    }
}

impl GraphNode {
    /// Visits every leaf in depth-first, left-to-right order.
    pub(super) fn for_each_leaf<'a>(&'a self, visitor: &mut impl FnMut(&'a FlowNode)) {
        match self {
            GraphNode::AlwaysSucceed { child } => child.for_each_leaf(visitor),
            GraphNode::Leaf { node } => visitor(node),
            GraphNode::Sequence { children }
            | GraphNode::Selector { children }
            | GraphNode::Fallback { children }
            | GraphNode::Parallel { children, .. } => {
                for child in children {
                    child.for_each_leaf(visitor);
                }
            }
        }
    }
}

pub(super) fn contains_node(root: &GraphNode, node_id: FlowNodeId) -> bool {
    match root {
        GraphNode::AlwaysSucceed { child } => contains_node(child, node_id),
        GraphNode::Leaf { node } => node.id() == node_id,
        GraphNode::Sequence { children }
        | GraphNode::Selector { children }
        | GraphNode::Fallback { children }
        | GraphNode::Parallel { children, .. } => children.iter().any(|child| contains_node(child, node_id)),
    }
}

pub(super) fn replace_node(root: &mut GraphNode, replacement: &FlowNode) -> bool {
    match root {
        GraphNode::AlwaysSucceed { child } => replace_node(child, replacement),
        GraphNode::Leaf { node } if node.id() == replacement.id() => {
            node.as_mut().clone_from(replacement);
            true
        }
        GraphNode::Leaf { .. } => false,
        GraphNode::Sequence { children }
        | GraphNode::Selector { children }
        | GraphNode::Fallback { children }
        | GraphNode::Parallel { children, .. } => children
            .iter_mut()
            .any(|child| replace_node(child, replacement)),
    }
}

pub(super) fn remove_node(root: &mut GraphNode, node_id: FlowNodeId) -> bool {
    let children = match root {
        GraphNode::AlwaysSucceed { child } => {
            if matches!(child.as_ref(), GraphNode::Leaf { node } if node.id() == node_id) {
                *root = GraphNode::Sequence { children: Vec::new() };
                return true;
            }
            return remove_node(child, node_id);
        }
        GraphNode::Leaf { .. } => return false,
        GraphNode::Sequence { children }
        | GraphNode::Selector { children }
        | GraphNode::Fallback { children }
        | GraphNode::Parallel { children, .. } => children,
    };
    if let Some(index) = children
        .iter()
        .position(|child| matches!(child, GraphNode::Leaf { node } if node.id() == node_id))
    {
        children.remove(index);
        return true;
    }
    children.iter_mut().any(|child| remove_node(child, node_id))
}

fn validate_node(node: &GraphNode, path: &str, ids: &mut HashSet<FlowNodeId>) -> Result<(), FlowGraphLoadError> {
    match node {
        GraphNode::AlwaysSucceed { child } => validate_node(child, &format!("{path}.child"), ids)?,
        GraphNode::Sequence { children } | GraphNode::Selector { children } | GraphNode::Fallback { children } => {
            if children.is_empty() {
                return Err(invalid(path, "controller must contain at least one child"));
            }
            for (index, child) in children.iter().enumerate() {
                validate_node(child, &format!("{path}.children[{index}]"), ids)?;
            }
        }
        GraphNode::Parallel { children, .. } => {
            if children.len() < 2 {
                return Err(invalid(path, "parallel controller must contain at least two children"));
            }
            for (index, child) in children.iter().enumerate() {
                validate_node(child, &format!("{path}.children[{index}]"), ids)?;
            }
        }
        GraphNode::Leaf { node } => validate_leaf(node, path, ids)?,
    }
    Ok(())
}

fn validate_leaf(node: &FlowNode, path: &str, ids: &mut HashSet<FlowNodeId>) -> Result<(), FlowGraphLoadError> {
    if node.id().0.is_nil() {
        return Err(invalid(path, "node id must not be nil"));
    }
    if node.label().trim().is_empty() {
        return Err(invalid(path, "node label must not be empty"));
    }
    if !ids.insert(node.id()) {
        return Err(invalid(path, "node id must be unique within the graph"));
    }

    let constraints = node.constraints();
    if constraints.runtime.max_ticks == Some(0) || constraints.runtime.max_duration_ms == Some(0) {
        return Err(invalid(path, "runtime limits must be greater than zero"));
    }
    validate_data_space(node.data_space(), path)?;
    let (inputs, outputs) = node.bindings();
    for binding in inputs.iter().chain(outputs) {
        if binding.name.trim().is_empty() || binding.key.trim().is_empty() {
            return Err(invalid(path, "blackboard binding names and keys must not be empty"));
        }
    }
    if constraints
        .capabilities
        .iter()
        .any(|capability| capability.name.trim().is_empty())
    {
        return Err(invalid(path, "capability names must not be empty"));
    }
    Ok(())
}

fn validate_data_space(data_space: &NodeDataSpace, path: &str) -> Result<(), FlowGraphLoadError> {
    for data_path in data_space
        .code_paths
        .iter()
        .chain(data_space.state_paths.iter())
    {
        if data_path
            .path
            .split(['\\', '/'])
            .any(|part| part == ".." || part.is_empty())
        {
            return Err(invalid(
                path,
                "content-addressed paths must be relative and traversal-free",
            ));
        }
    }
    Ok(())
}

fn invalid(path: &str, reason: &str) -> FlowGraphLoadError {
    FlowGraphLoadError::Invalid {
        path: path.to_string(),
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(node_kind: &str, id: &str, config: &str) -> String {
        format!(
            r#"{{"node_kind":"{node_kind}","id":"{id}","label":"node","constraints":{{"runtime":{{}},"capabilities":[]}},"data_space":{{"code_paths":[],"state_paths":[]}},"inputs":[],"outputs":[],"configuration":{{{config}}}}}"#
        )
    }

    fn leaf(node_kind: &str, id: &str, config: &str) -> String {
        format!(
            r#"{{"version":1,"root":{{"type":"Leaf","node":{}}}}}"#,
            node(node_kind, id, config)
        )
    }

    #[test]
    fn accepts_each_initial_leaf_kind() {
        let id = "11111111-1111-1111-1111-111111111111";
        for (kind, config) in [
            (
                "ScriptNode",
                r#""source_root":null,"entry_point":"run","fuel_limit":0,"timeout_secs":0"#,
            ),
            ("OrderNode", r#""command":"submit""#),
            (
                "AgentNode",
                r#""providerName":"provider","agentName":"agent","prompt":"act","maxTurns":null,"maxToolCalls":null,"maxDuration":null,"configOverrides":{},"sluiceMcpEnabled":false,"workspace":null"#,
            ),
        ] {
            assert!(
                FlowGraphAst::load_json(&leaf(kind, id, config)).is_ok(),
                "{kind} should load"
            );
        }
    }

    #[test]
    fn rejects_legacy_node_and_empty_controller() {
        let unknown = r#"{"version":1,"root":{"type":"Leaf","node":{"node_type":"OrderNode"}}}"#;
        assert!(matches!(
            FlowGraphAst::load_json(unknown),
            Err(FlowGraphLoadError::Decode(_))
        ));

        let empty = r#"{"version":1,"root":{"type":"Sequence","children":[]}}"#;
        assert!(matches!(
            FlowGraphAst::load_json(empty),
            Err(FlowGraphLoadError::Invalid { path, reason })
                if path == "root" && reason == "controller must contain at least one child"
        ));
    }

    #[test]
    fn rejects_duplicate_ids_and_traversal_paths() {
        let id = "11111111-1111-1111-1111-111111111111";
        let graph = format!(
            r#"{{"version":1,"root":{{"type":"Parallel","policy":"WhenAll","children":[{{"type":"Leaf","node":{}}},{{"type":"Leaf","node":{}}}]}}}}"#,
            node("OrderNode", id, r#""command":"one""#),
            node("OrderNode", id, r#""command":"two""#)
        );
        assert!(
            matches!(FlowGraphAst::load_json(&graph), Err(FlowGraphLoadError::Invalid { reason, .. }) if reason.contains("unique"))
        );

        let traversal = format!(
            r#"{{"version":1,"root":{{"type":"Leaf","node":{{"node_kind":"OrderNode","id":"{id}","label":"node","constraints":{{"runtime":{{}},"capabilities":[]}},"data_space":{{"code_paths":[{{"root":{{"digest":"{}"}},"path":"../secret"}}],"state_paths":[]}},"inputs":[],"outputs":[],"configuration":{{"command":"submit"}}}}}}}}"#,
            "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f"
        );
        assert!(matches!(
            FlowGraphAst::load_json(&traversal),
            Err(FlowGraphLoadError::Invalid { reason, .. }) if reason.contains("traversal-free")
        ));
    }
}

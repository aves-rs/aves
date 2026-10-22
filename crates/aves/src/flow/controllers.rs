use super::graph::{FlowGraph, GraphNode, ParallelPolicy};
use super::registration::{CancellationDisposition, FlowNodeRegistry, SideEffectDisposition};
use super::types::ControllerError;

/// Validates the controller policies before a Flow is compiled for execution.
pub(super) fn validate_parallel_composition(
    graph: &FlowGraph,
    registry: &FlowNodeRegistry,
) -> Result<(), ControllerError> {
    validate_node(&graph.ast().root, registry)
}

fn validate_node(node: &GraphNode, registry: &FlowNodeRegistry) -> Result<(), ControllerError> {
    match node {
        GraphNode::AlwaysSucceed { child } => validate_node(child, registry),
        GraphNode::Sequence { children } | GraphNode::Selector { children } | GraphNode::Fallback { children } => {
            children
                .iter()
                .try_for_each(|child| validate_node(child, registry))
        }
        GraphNode::Parallel { policy, children } => {
            if matches!(policy, ParallelPolicy::WhenAny | ParallelPolicy::Race) {
                // Resolve each child's registrations before its discard-safety check.
                for child in children {
                    let mut registration_error = None;
                    child.for_each_leaf(&mut |node| {
                        if registration_error.is_none() {
                            registration_error = registry
                                .get(&node.node_kind)
                                .map(|_| ())
                                .map_err(ControllerError::from)
                                .err();
                        }
                    });
                    if let Some(error) = registration_error {
                        return Err(error);
                    }

                    let mut unsafe_kind = None;
                    child.for_each_leaf(&mut |node| {
                        let kind = node.node_kind.clone();
                        let descriptor = &registry
                            .get(&kind)
                            .expect("this child's registrations were validated above")
                            .descriptor;
                        if unsafe_kind.is_none()
                            && descriptor.cancellation == CancellationDisposition::NonCancellable
                            && descriptor.side_effects == SideEffectDisposition::SideEffecting
                        {
                            unsafe_kind = Some(kind);
                        }
                    });
                    if let Some(kind) = unsafe_kind {
                        return Err(ControllerError::UnsafeDiscard { kind });
                    }
                }
                return children
                    .iter()
                    .try_for_each(|child| validate_node(child, registry));
            }
            children
                .iter()
                .try_for_each(|child| validate_node(child, registry))
        }
        GraphNode::Leaf { node } => {
            registry.get(&node.node_kind)?;
            Ok(())
        }
    }
}

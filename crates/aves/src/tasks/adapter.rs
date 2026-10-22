use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::bus::BusIngress;
use crate::flow::{
    BlackboardBinding, BoundOutputs, CancellationDisposition, ContentAddressedPath, ContentAddressedRoot,
    FlowAdapterError, FlowRegistrationError, NodeKind, NodeKindDescriptor, NodeKindRegistration, NodeLifecycleContext,
    NodeLifecycleDispatchError, NodeLifecycleResult, ScriptNodeConfig, SideEffectDisposition, WorkspacePreparation,
    checked_relative,
};

use super::domain::TasksLifecycleSudoService;
use super::messages::{CancelScriptExecutionInput, StartScriptInput, TickScriptInput};
use super::types::{ScriptNodeOutputs, ScriptNodeTickResult};

pub(super) struct ScriptNodeOwnerState {
    lifecycle: Arc<dyn TasksLifecycleSudoService>,
    node_bus: Option<BusIngress>,
}

pub(super) fn script_node_registration(
    lifecycle: Arc<dyn TasksLifecycleSudoService>,
    node_bus: Option<BusIngress>,
) -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: script_node_kind(),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::Pure,
    };
    let owner_state = Arc::new(ScriptNodeOwnerState { lifecycle, node_bus });
    NodeKindRegistration::typed::<ScriptNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.entry_point.trim().is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "entry point must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        |context, configuration| async move {
            let (lifecycle, node_bus) = script_owner_state(&context)?;
            let correlation = context.correlation;
            let (source, filename) = match context.inputs.get("source").and_then(Value::as_str) {
                Some(source) => (source.to_owned(), configuration.entry_point.clone()),
                None => source_from_workspace(
                    &context.execution_root,
                    &configuration.entry_point,
                    configuration.source_root.as_ref(),
                    &context.node.data_space.code_paths,
                )
                .map_err(dispatch_adapter_error)?,
            };
            let (fuel_limit, timeout_secs) = configuration.resolved_limits();
            lifecycle
                .start_script(StartScriptInput {
                    correlation: correlation.clone(),
                    source,
                    filename,
                    input: context.inputs.as_value(),
                    fuel_limit,
                    timeout_secs,
                    workspace_path: context.execution_root,
                    node_token: context.node_token,
                    node_bus,
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            Ok(NodeLifecycleResult::Running)
        },
        |context, _configuration| async move {
            let (lifecycle, _) = script_owner_state(&context)?;
            let correlation = context.correlation;
            let output = lifecycle
                .tick_script(TickScriptInput {
                    correlation,
                    input: context.inputs.as_value(),
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            match output.result {
                ScriptNodeTickResult::Running => Ok(NodeLifecycleResult::Running),
                ScriptNodeTickResult::Succeeded { completion } => {
                    let state = json!({"outputs": completion.outputs, "logs": completion.logs});
                    if !completion.errors.is_empty() {
                        return Ok(NodeLifecycleResult::Failed {
                            message: completion.errors.join("\n"),
                            state,
                        });
                    }
                    match project_script_outputs(&context.node.outputs, completion.outputs) {
                        Ok(outputs) => Ok(NodeLifecycleResult::Succeeded { outputs, state }),
                        Err(error) => Ok(NodeLifecycleResult::Failed {
                            message: error.to_string(),
                            state,
                        }),
                    }
                }
                ScriptNodeTickResult::Failed { failure } => Ok(NodeLifecycleResult::Failed {
                    message: failure.error.to_string(),
                    state: json!({"outputs": failure.outputs, "logs": failure.logs}),
                }),
            }
        },
        |context, _configuration| async move {
            let (lifecycle, _) = script_owner_state(&context)?;
            lifecycle
                .cancel_script_execution(CancelScriptExecutionInput {
                    node_execution_id: context.correlation.node_execution_id,
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            Ok(NodeLifecycleResult::Failed {
                message: String::from("ScriptNode execution cancelled"),
                state: Value::Null,
            })
        },
        Some(owner_state),
        WorkspacePreparation::new().node_content_roots::<ScriptNodeConfig>(|configuration| {
            configuration.source_root.clone().into_iter().collect()
        }),
    )
}

fn script_node_kind() -> NodeKind {
    NodeKind::parse("ScriptNode").expect("static node kind is valid")
}

fn script_owner_state(
    context: &NodeLifecycleContext,
) -> Result<(Arc<dyn TasksLifecycleSudoService>, Option<BusIngress>), NodeLifecycleDispatchError> {
    context
        .owner_state
        .as_ref()
        .and_then(|state| state.downcast_ref::<ScriptNodeOwnerState>())
        .map(|state| (state.lifecycle.clone(), state.node_bus.clone()))
        .ok_or_else(|| {
            NodeLifecycleDispatchError::Owner(Box::new(std::io::Error::other("ScriptNode owner state unavailable")))
        })
}

fn source_from_workspace(
    workspace: &Path,
    entry_point: &str,
    root: Option<&ContentAddressedRoot>,
    paths: &[ContentAddressedPath],
) -> Result<(String, String), FlowAdapterError> {
    let source_path = if let Some(root) = root {
        let mut matching_paths = paths.iter().filter(|path| path.root == *root);
        let selected = matching_paths
            .next()
            .ok_or_else(|| FlowAdapterError::InvalidOwnerInput {
                kind: script_node_kind(),
                message: "ScriptNode source root requires a configured code path".to_owned(),
            })?;
        if matching_paths.next().is_some() {
            return Err(FlowAdapterError::InvalidOwnerInput {
                kind: script_node_kind(),
                message: "ScriptNode source root requires exactly one configured code path".to_owned(),
            });
        }
        selected
            .path
            .strip_prefix("code/")
            .unwrap_or(&selected.path)
            .to_owned()
    } else {
        checked_relative(Path::new(entry_point))
            .map_err(|source| FlowAdapterError::Owner {
                kind: script_node_kind(),
                source: Box::new(source),
            })?
            .to_string_lossy()
            .into_owned()
    };
    let source = fs::read_to_string(if root.is_some() {
        workspace.join("code").join(&source_path)
    } else {
        workspace.join(&source_path)
    })
    .map_err(|source| FlowAdapterError::Owner {
        kind: script_node_kind(),
        source: Box::new(source),
    })?;
    Ok((source, source_path))
}

fn project_script_outputs(
    bindings: &[BlackboardBinding],
    outputs: ScriptNodeOutputs,
) -> Result<BoundOutputs, FlowAdapterError> {
    let mut values = BTreeMap::new();
    for binding in bindings {
        let value = outputs
            .get(&binding.name)
            .cloned()
            .ok_or_else(|| FlowAdapterError::InvalidOwnerInput {
                kind: script_node_kind(),
                message: format!("ScriptNode did not emit configured output {}", binding.name),
            })?;
        values.insert(binding.name.clone(), value);
    }
    Ok(BoundOutputs::new(values))
}

fn dispatch_adapter_error(error: FlowAdapterError) -> NodeLifecycleDispatchError {
    match error {
        FlowAdapterError::Owner { source, .. } => NodeLifecycleDispatchError::Owner(source),
        other => NodeLifecycleDispatchError::Owner(Box::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::flow::BlackboardBinding;

    use super::*;

    #[test]
    fn projects_named_script_outputs_to_matching_blackboard_bindings() {
        let bindings = vec![
            BlackboardBinding {
                name: String::from("summary"),
                key: String::from("run_summary"),
            },
            BlackboardBinding {
                name: String::from("metrics"),
                key: String::from("run_metrics"),
            },
        ];
        let outputs = super::super::types::ScriptNodeOutputs::new(BTreeMap::from([
            (String::from("summary"), serde_json::json!("complete")),
            (String::from("metrics"), serde_json::json!({"count": 2})),
            (String::from("unbound"), serde_json::json!(true)),
        ]));

        let projected = project_script_outputs(&bindings, outputs).unwrap();

        assert_eq!(
            projected,
            BoundOutputs::new(BTreeMap::from([
                (String::from("summary"), serde_json::json!("complete")),
                (String::from("metrics"), serde_json::json!({"count": 2})),
            ]))
        );
    }

    #[test]
    fn rejects_missing_configured_script_output() {
        let bindings = vec![BlackboardBinding {
            name: String::from("summary"),
            key: String::from("run_summary"),
        }];

        let error = project_script_outputs(&bindings, super::super::types::ScriptNodeOutputs::default()).unwrap_err();

        assert!(matches!(
            error,
            FlowAdapterError::InvalidOwnerInput { kind, .. } if kind == script_node_kind()
        ));
    }
}

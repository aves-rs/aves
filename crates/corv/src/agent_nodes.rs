use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use serde_json::{to_string, to_value};
use tokio::{
    sync::{Mutex, oneshot::error::TryRecvError},
    time::timeout,
};

use aves::auth::Jwt;
use aves::bus::BusMcpEndpoint;
use aves::flow::{
    AgentNodeConfig, BoundInputs, BoundOutputs, CancellationDisposition, FlowAdapterError, FlowExecutionId,
    FlowRegistrationError, LifecycleCorrelation, NodeExecutionId, NodeKind, NodeKindDescriptor, NodeKindRegistration,
    NodeLifecycleContext, NodeLifecycleDispatchError, NodeLifecycleResult, SideEffectDisposition, WorkspacePreparation,
    WorkspacePreparationError, WorkspaceSeed,
};
use aves::runtime::ServiceReader;

use super::actor::Actor;
use super::domain::{DiscussAgentNodeLifecycleService, DiscussStreamingService};
use super::messages::{
    CancelAgentExecutionInput, CancelSessionInput, DiscussResponse, StartAgentInput, TickAgentInput,
};
use super::types::{
    AgentNodeTerminalResult, AgentNodeTickResult, DiscussConfigValues, DiscussError, SluiceSessionId, TaskSessionStart,
    WorkspaceCopySourcePort, resolve_agent_node_limits,
};

impl Actor {
    // The argument list mirrors the wire request payload fields.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start_agent(
        &mut self,
        correlation: LifecycleCorrelation,
        provider_name: String,
        agent_name: String,
        config_overrides: DiscussConfigValues,
        sluice_mcp_enabled: bool,
        prompt: String,
        session_cwd: PathBuf,
        max_turns: Option<i64>,
        max_tool_calls: Option<i64>,
        max_duration: Option<i64>,
        node_token: Option<Jwt>,
        flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
    ) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let id = correlation.node_execution_id;
        if self.agent_completions.contains_key(&id) {
            return Err(DiscussError::AgentNodeIdempotencyConflict { node_execution_id: id });
        }
        let node_token = node_token.ok_or(DiscussError::AgentNodeTokenRequired)?;
        let limits = resolve_agent_node_limits(max_turns, max_tool_calls, max_duration)?;
        let started = self
            .start_task_session(TaskSessionStart {
                provider_name,
                agent_name,
                config_overrides,
                sluice_mcp_enabled,
                prompt,
                session_cwd: session_cwd.clone(),
                limits,
                node_token,
                flow_mcp_endpoints,
            })
            .await?;
        self.agent_completions.insert(id, started.terminal);
        Ok(DiscussResponse::AgentNodeStarted {
            correlation,
            session_id: started.session_id,
            limits,
            session_cwd,
        })
    }

    pub(super) fn tick_agent(&mut self, correlation: LifecycleCorrelation) -> Result<DiscussResponse, DiscussError> {
        if !self.server_running {
            return Err(DiscussError::ShuttingDown);
        }
        let id = correlation.node_execution_id;
        let terminal_rx = self
            .agent_completions
            .get_mut(&id)
            .ok_or(DiscussError::AgentNodeCompletionNotFound { node_execution_id: id })?;
        let result = match terminal_rx.try_recv() {
            Ok(terminal) => {
                self.agent_completions.remove(&id);
                terminal.into()
            }
            Err(TryRecvError::Empty) => AgentNodeTickResult::Running,
            Err(TryRecvError::Closed) => {
                self.agent_completions.remove(&id);
                return Err(DiscussError::AgentNodeCompletionDropped);
            }
        };
        Ok(DiscussResponse::AgentNodeTicked { correlation, result })
    }

    pub(super) fn cancel_agent(&mut self, node_execution_id: NodeExecutionId) {
        self.agent_completions.remove(&node_execution_id);
    }
}

struct AgentNodeOwnerState {
    lifecycle: Arc<dyn DiscussAgentNodeLifecycleService>,
    streaming: ServiceReader<dyn DiscussStreamingService>,
    copy_source_port: ServiceReader<WorkspaceCopySourcePort>,
    sessions: Mutex<HashMap<NodeExecutionId, (FlowExecutionId, SluiceSessionId)>>,
}

pub(super) fn agent_node_registration(
    lifecycle: Arc<dyn DiscussAgentNodeLifecycleService>,
    streaming: ServiceReader<dyn DiscussStreamingService>,
    copy_source_port: ServiceReader<WorkspaceCopySourcePort>,
) -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("AgentNode").expect("static node kind is valid"),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::Pure,
    };
    let owner_state = Arc::new(AgentNodeOwnerState {
        lifecycle,
        streaming,
        copy_source_port: copy_source_port.clone(),
        sessions: Mutex::new(HashMap::new()),
    });
    NodeKindRegistration::typed::<AgentNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.prompt.trim().is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "prompt must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        |context, configuration| async move {
            let state = agent_owner_state(&context)?;
            let correlation = context.correlation.clone();
            let execution_root = context.execution_root.clone();
            let node_token = context.node_token.clone();
            let flow_mcp_endpoints = if configuration.sluice_mcp_enabled {
                Some(
                    context
                        .mcp_ports
                        .endpoints(correlation.flow_execution_id)
                        .await
                        .map_err(dispatch_owner_error)?,
                )
            } else {
                None
            };
            let input = agent_node_start_input(
                correlation.clone(),
                configuration,
                execution_root,
                node_token,
                flow_mcp_endpoints,
            )
            .map_err(dispatch_adapter_error)?;
            let input = StartAgentInput {
                prompt: agent_node_prompt(input.prompt, &context.inputs).map_err(dispatch_adapter_error)?,
                ..input
            };
            let output = state
                .lifecycle
                .start_agent(input)
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            state.sessions.lock().await.insert(
                correlation.node_execution_id,
                (correlation.flow_execution_id, output.session_id),
            );
            Ok(NodeLifecycleResult::Running)
        },
        |context, _configuration| async move {
            let state = agent_owner_state(&context)?;
            let correlation = context.correlation.clone();
            let output = state
                .lifecycle
                .tick_agent(TickAgentInput {
                    correlation: correlation.clone(),
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            let terminal = match output.result {
                AgentNodeTickResult::Running => return Ok(NodeLifecycleResult::Running),
                AgentNodeTickResult::Succeeded { output } => AgentNodeTerminalResult::Succeeded { output },
                AgentNodeTickResult::Failed { error } => AgentNodeTerminalResult::Failed { error },
            };
            state
                .sessions
                .lock()
                .await
                .remove(&correlation.node_execution_id);
            let state_value = to_value(&terminal).map_err(dispatch_owner_error)?;
            match terminal {
                AgentNodeTerminalResult::Succeeded { output } => {
                    let output_object = output.as_object();
                    let values = context
                        .node
                        .outputs
                        .iter()
                        .map(|binding| {
                            let value = output_object
                                .and_then(|object| object.get(&binding.name))
                                .cloned()
                                .unwrap_or_else(|| output.clone());
                            (binding.name.clone(), value)
                        })
                        .collect();
                    Ok(NodeLifecycleResult::Succeeded {
                        outputs: BoundOutputs::new(values),
                        state: state_value,
                    })
                }
                AgentNodeTerminalResult::Failed { error } => Ok(NodeLifecycleResult::Failed {
                    message: error.to_string(),
                    state: state_value,
                }),
            }
        },
        |context, _configuration| async move {
            let state = agent_owner_state(&context)?;
            let correlation = context.correlation.clone();
            let session_id = state
                .sessions
                .lock()
                .await
                .remove(&correlation.node_execution_id)
                .map(|(_, session)| session);
            if let Some(session_id) = session_id {
                match timeout(
                    Duration::from_secs(1),
                    state
                        .streaming
                        .get()
                        .expect("Discuss streaming must be published before Flow execution")
                        .cancel_session(CancelSessionInput {
                            sluice_session_id: session_id,
                        }),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => tracing::warn!(%session_id, %error, "Flow session cancellation failed"),
                    Err(_) => tracing::warn!(%session_id, "Flow session cancellation timed out"),
                }
            }
            if let Err(error) = state
                .lifecycle
                .cancel_agent_execution(CancelAgentExecutionInput {
                    node_execution_id: correlation.node_execution_id,
                })
                .await
            {
                tracing::warn!(?error, "could not clear cancelled AgentNode completion");
            }
            let error = match session_id {
                Some(sluice_session_id) => DiscussError::SessionCancelled { sluice_session_id },
                None => DiscussError::AgentNodeCompletionNotFound {
                    node_execution_id: correlation.node_execution_id,
                },
            };
            let message = error.to_string();
            let state_value = to_value(&AgentNodeTerminalResult::Failed { error }).map_err(dispatch_owner_error)?;
            Ok(NodeLifecycleResult::Failed {
                message,
                state: state_value,
            })
        },
        Some(owner_state.clone()),
        WorkspacePreparation::new()
            .node_content_roots::<AgentNodeConfig>(|configuration| {
                configuration.workspace.clone().into_iter().collect()
            })
            .graph_seeds::<AgentNodeConfig, _, _>(move |configurations| {
                let copy_source_port = owner_state.copy_source_port.clone();
                async move {
                    let provider_names: BTreeSet<String> = configurations
                        .into_iter()
                        .map(|configuration| configuration.provider_name)
                        .collect();
                    if provider_names.is_empty() {
                        return Ok(Vec::new());
                    }
                    let port = copy_source_port
                        .get()
                        .ok_or(WorkspacePreparationError::ServiceUnavailable)?;
                    let sources =
                        port.resolve(provider_names)
                            .await
                            .map_err(|error| WorkspacePreparationError::Resolution {
                                message: error.to_string(),
                                source: Box::new(error),
                            })?;
                    let relative_paths: BTreeSet<String> = sources
                        .sources_by_provider
                        .into_values()
                        .flatten()
                        .collect();
                    if relative_paths.is_empty() {
                        return Ok(Vec::new());
                    }
                    Ok(vec![WorkspaceSeed {
                        source_root: sources.agents_workspace,
                        relative_paths,
                    }])
                }
            }),
    )
}

fn agent_owner_state(context: &NodeLifecycleContext) -> Result<&AgentNodeOwnerState, NodeLifecycleDispatchError> {
    context
        .owner_state
        .as_ref()
        .and_then(|state| state.downcast_ref::<AgentNodeOwnerState>())
        .ok_or_else(|| {
            NodeLifecycleDispatchError::Owner(Box::new(std::io::Error::other("AgentNode owner state unavailable")))
        })
}

fn agent_node_start_input(
    correlation: LifecycleCorrelation,
    configuration: AgentNodeConfig,
    execution_root: PathBuf,
    node_token: Option<Jwt>,
    flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
) -> Result<StartAgentInput, FlowAdapterError> {
    let to_limit = |value: Option<u64>| value.map(i64::try_from).transpose().map_err(owner_error);
    Ok(StartAgentInput {
        correlation,
        provider_name: configuration.provider_name,
        agent_name: configuration.agent_name,
        config_overrides: configuration.config_overrides,
        sluice_mcp_enabled: configuration.sluice_mcp_enabled,
        prompt: configuration.prompt,
        session_cwd: agent_node_session_cwd(execution_root),
        max_turns: to_limit(configuration.max_turns)?,
        max_tool_calls: to_limit(configuration.max_tool_calls)?,
        max_duration: to_limit(configuration.max_duration_secs)?,
        node_token,
        flow_mcp_endpoints,
    })
}

fn agent_node_session_cwd(execution_root: PathBuf) -> PathBuf {
    execution_root
}

fn agent_node_prompt(prompt: String, inputs: &BoundInputs) -> Result<String, FlowAdapterError> {
    if inputs == &BoundInputs::default() {
        return Ok(prompt);
    }
    let inputs_json = to_string(&inputs.as_value()).map_err(owner_error)?;
    Ok(format!(
        "{prompt}\n\n--- Sluice task inputs ---\n```json\n{inputs_json}\n```\n--- End Sluice task inputs ---"
    ))
}

fn dispatch_owner_error(source: impl Error + Send + Sync + 'static) -> NodeLifecycleDispatchError {
    NodeLifecycleDispatchError::Owner(Box::new(source))
}

fn dispatch_adapter_error(error: FlowAdapterError) -> NodeLifecycleDispatchError {
    match error {
        FlowAdapterError::Owner { source, .. } => NodeLifecycleDispatchError::Owner(source),
        other => dispatch_owner_error(other),
    }
}

fn owner_error(source: impl Error + Send + Sync + 'static) -> FlowAdapterError {
    FlowAdapterError::Owner {
        kind: NodeKind::parse("AgentNode").expect("static node kind is valid"),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_node_start_uses_the_common_execution_root_as_session_cwd() {
        let execution_root = PathBuf::from("/tmp/flow-execution");
        let session_cwd = agent_node_session_cwd(execution_root.clone());
        assert_eq!(session_cwd, execution_root);
    }

    #[test]
    fn agent_node_prompt_leaves_empty_inputs_unchanged() {
        let inputs = BoundInputs::default();
        assert_eq!(
            agent_node_prompt("configured prompt".into(), &inputs).unwrap(),
            "configured prompt"
        );
    }

    #[test]
    fn agent_node_prompt_serializes_nonempty_inputs_deterministically() {
        let inputs: BoundInputs = serde_json::from_value(
            serde_json::json!({"values": {"zeta": [1, false, null], "alpha": {"nested": "value"}}}),
        )
        .unwrap();
        assert_eq!(
            agent_node_prompt("configured prompt".into(), &inputs).unwrap(),
            "configured prompt\n\n--- Sluice task inputs ---\n```json\n{\"alpha\":{\"nested\":\"value\"},\"zeta\":[1,false,null]}\n```\n--- End Sluice task inputs ---"
        );
    }
}

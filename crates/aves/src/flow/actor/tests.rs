use async_trait::async_trait;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::auth::{
    AuthSudoService, AvesAuthError, Jwt,
    messages::{
        CreateInvitationInput, DelegatedJwtIssuedOutput, FirstContactInput, FirstContactedOutput, InvitationOutput,
        IssueJwtInput, IssuePeerJwtInput, IssueRootJwtInput, JwtIssuedOutput, ListPairingCandidatesInput,
        ListPeersInput, PairPeerInput, PairingCandidatesOutput, PeerJwtIssuedOutput, PeersOutput, RevokeJtiInput,
        RevokePeerInput, SuccessOutput, VerifiedOutput, VerifyJwtInput,
    },
};
use crate::bus::BusRpcContext;
use crate::migrations::flows::MIGRATIONS as FLOWS_MIGRATIONS;
use crate::runtime::ServiceReader;
use crate::tasks::{
    CancelScriptExecutionInput, ScriptNodeCompletion, ScriptNodeOutputs, ScriptNodeTickResult, ScriptNodeTickedOutput,
    StartRuntimeInput, StartScriptInput, StartedOutput, StopRuntimeInput, SuccessOutput as TasksSuccessOutput,
    TasksError, TasksLifecycleSudoService, TickScriptInput,
};
use crate::utils::database::{MigrationRunner, connect_database};

use super::super::database::FlowStore;
use super::super::files::{checked_relative, seed_workspace_seeds};
use super::super::fixtures::NativeDocsFixture;
use super::super::registration::NodeRegistrationSupply;
use super::super::types::FlowMobilityReadiness;
use super::super::{
    BoundOutputs, CancellationDisposition, FlowAuthorityState, FlowBootOptions, FlowExecutionSnapshot,
    FlowInvocationKind, FlowRegistrationError, FlowTimestamp, NodeKind, NodeKindDescriptor, NodeKindRegistration,
    NodeLifecycleContext, NodeLifecycleDispatchError, NodeLifecycleResult, OwnerLifecycleResult, SideEffectDisposition,
    WorkspaceError, WorkspacePreparation, WorkspaceSeed,
};
use super::*;

#[tokio::test]
async fn boot_opens_root_database_and_applies_all_registered_migrations() {
    let directory = tempfile::tempdir().unwrap();
    let options = FlowBootOptions::open(directory.path().to_path_buf())
        .await
        .unwrap();
    assert!(directory.path().join("flows.db").is_file());
    assert_eq!(options.data_root, directory.path().join("flows"));
    assert!(!directory.path().join("flow").exists());
    assert!(!options.data_root.join("flows.db").exists());
    let connection = connect_database(&options.database).await.unwrap();
    let mut rows = connection
        .query("SELECT COUNT(*) FROM __flow_migrations", ())
        .await
        .unwrap();
    let count = rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
    assert_eq!(count as usize, FLOWS_MIGRATIONS.len());
    connection
        .query("SELECT namespace_id FROM flow_execution_documents LIMIT 0", ())
        .await
        .unwrap();
    assert_eq!(
        MigrationRunner::new("__flow_migrations", FLOWS_MIGRATIONS)
            .apply(&options.database)
            .await
            .unwrap(),
        0
    );
}

#[test]
fn readiness_preserves_every_durable_commit_gate() {
    let readiness = FlowMobilityReadiness {
        flow_id: FlowId(uuid::Uuid::nil()),
        snapshot_generation: Some(7),
        history_complete: true,
        documents_complete: true,
        blobs_complete: true,
        inactive: true,
        failure: None,
    };

    let encoded = serde_json::to_value(&readiness).unwrap();
    assert_eq!(encoded["snapshotGeneration"], 7);
    assert_eq!(encoded["historyComplete"], true);
    assert_eq!(encoded["documentsComplete"], true);
    assert_eq!(encoded["blobsComplete"], true);
    assert_eq!(encoded["inactive"], true);
    assert!(encoded["failure"].is_null());
}

fn copy_sources(root: &Path, paths: &[&str]) -> Vec<WorkspaceSeed> {
    vec![WorkspaceSeed {
        source_root: root.to_path_buf(),
        relative_paths: paths
            .iter()
            .map(|path| (*path).to_owned())
            .collect::<BTreeSet<_>>(),
    }]
}

#[test]
fn provider_sources_union_overwrites_inputs_and_skips_missing_entries() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("agents");
    let target = directory.path().join("execution");
    std::fs::create_dir_all(source.join(".agents/agents")).unwrap();
    std::fs::create_dir_all(target.join(".agents/agents")).unwrap();
    std::fs::write(source.join(".agents/agents/persona.md"), "selected").unwrap();
    std::fs::write(target.join(".agents/agents/persona.md"), "input").unwrap();
    let sources = copy_sources(&source, &[".agents", "missing"]);

    seed_workspace_seeds(&sources, &target).unwrap();

    assert_eq!(
        std::fs::read_to_string(target.join(".agents/agents/persona.md")).unwrap(),
        "selected"
    );
    assert!(!target.join("missing").exists());
}

#[test]
fn invalid_provider_paths_never_copy_the_workspace_root() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("agents");
    let target = directory.path().join("execution");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(source.join("secret"), "unselected").unwrap();

    for invalid in ["", ".", "./", "../escape", "/absolute"] {
        let sources = copy_sources(&source, &[invalid]);
        assert!(matches!(
            seed_workspace_seeds(&sources, &target),
            Err(WorkspaceError::PathEscapesRoot { .. })
        ));
        assert!(!target.join("secret").exists());
    }
}

#[test]
fn destination_write_failure_is_a_typed_workspace_error() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("agents");
    let target = directory.path().join("execution");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(source.join("selected"), "provider content").unwrap();
    std::fs::create_dir(target.join("selected")).unwrap();

    let error = seed_workspace_seeds(&copy_sources(&source, &["selected"]), &target).unwrap_err();

    assert!(matches!(error, WorkspaceError::Io { path, .. } if path == target.join("selected")));
}

#[cfg(unix)]
#[test]
fn source_links_at_selected_intermediate_leaf_and_nested_entries_are_skipped() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("agents");
    let target = directory.path().join("execution");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(source.join("managed")).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret"), "outside").unwrap();
    std::fs::write(source.join("managed/normal"), "inside").unwrap();
    symlink(&outside, source.join("link")).unwrap();
    symlink(outside.join("secret"), source.join("managed/nested-link")).unwrap();

    seed_workspace_seeds(&copy_sources(&source, &["link/secret", "link", "managed"]), &target).unwrap();

    assert_eq!(
        std::fs::read_to_string(target.join("managed/normal")).unwrap(),
        "inside"
    );
    assert!(!target.join("link").exists());
    assert!(!target.join("managed/nested-link").exists());
}

#[cfg(unix)]
#[test]
fn destination_leaf_link_is_replaced_but_intermediate_link_is_rejected() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("agents");
    let target = directory.path().join("execution");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(source.join("nested")).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(source.join("selected"), "safe").unwrap();
    std::fs::write(source.join("nested/file"), "safe").unwrap();
    std::fs::write(outside.join("file"), "original").unwrap();
    symlink(outside.join("file"), target.join("selected")).unwrap();

    seed_workspace_seeds(&copy_sources(&source, &["selected"]), &target).unwrap();
    assert_eq!(std::fs::read_to_string(target.join("selected")).unwrap(), "safe");
    assert_eq!(std::fs::read_to_string(outside.join("file")).unwrap(), "original");
    symlink(&outside, target.join("nested")).unwrap();

    assert!(matches!(
        seed_workspace_seeds(&copy_sources(&source, &["nested/file"]), &target),
        Err(WorkspaceError::SymbolicLink { .. })
    ));
    assert_eq!(std::fs::read_to_string(outside.join("file")).unwrap(), "original");
}

struct TestAuth;

#[async_trait]
impl AuthSudoService for TestAuth {
    async fn issue_jwt(&self, _input: IssueJwtInput) -> Result<DelegatedJwtIssuedOutput, AvesAuthError> {
        Ok(DelegatedJwtIssuedOutput {
            token: Jwt::from_opaque("test-node-token".to_owned()),
            jti: Uuid::new_v4(),
        })
    }

    async fn issue_peer_jwt(&self, _input: IssuePeerJwtInput) -> Result<PeerJwtIssuedOutput, AvesAuthError> {
        Ok(PeerJwtIssuedOutput {
            token: Jwt::from_opaque("test-peer-token".to_owned()),
        })
    }

    async fn issue_root_jwt(&self, _input: IssueRootJwtInput) -> Result<JwtIssuedOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }

    async fn verify_jwt(&self, _input: VerifyJwtInput) -> Result<VerifiedOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }

    async fn revoke_jti(&self, _input: RevokeJtiInput) -> Result<SuccessOutput, AvesAuthError> {
        Ok(SuccessOutput {})
    }

    async fn pair_peer(&self, _input: PairPeerInput) -> Result<SuccessOutput, AvesAuthError> {
        Ok(SuccessOutput {})
    }

    async fn revoke_peer(&self, _input: RevokePeerInput) -> Result<SuccessOutput, AvesAuthError> {
        Ok(SuccessOutput {})
    }

    async fn list_peers(&self, _input: ListPeersInput) -> Result<PeersOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }

    async fn list_pairing_candidates(
        &self,
        _input: ListPairingCandidatesInput,
    ) -> Result<PairingCandidatesOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }

    async fn create_invitation(&self, _input: CreateInvitationInput) -> Result<InvitationOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }

    async fn first_contact(&self, _input: FirstContactInput) -> Result<FirstContactedOutput, AvesAuthError> {
        panic!("not used by Flow actor tests")
    }
}

struct TestTasks {
    starts: AtomicUsize,
    fail_tick: bool,
    last_source: std::sync::Mutex<Option<String>>,
}

struct SequenceTasks {
    starts: AtomicUsize,
    observed_file: std::sync::Mutex<Option<String>>,
}

fn script_registration(tasks: Arc<dyn TasksLifecycleSudoService>) -> NodeKindRegistration {
    use super::super::ast::ScriptNodeConfig;
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("ScriptNode").unwrap(),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    let start_tasks = tasks.clone();
    let start = move |context: NodeLifecycleContext, configuration: ScriptNodeConfig| {
        let tasks = start_tasks.clone();
        async move {
            let correlation = context.correlation;
            let inputs = context.inputs.as_value();
            let (source, filename) = match inputs.get("source").and_then(serde_json::Value::as_str) {
                Some(source) => (source.to_owned(), configuration.entry_point.clone()),
                None => {
                    let (relative, rooted) = if configuration.source_root.is_some() {
                        (configuration.entry_point.clone(), true)
                    } else {
                        (
                            checked_relative(std::path::Path::new(&configuration.entry_point))
                                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?
                                .to_string_lossy()
                                .into_owned(),
                            false,
                        )
                    };
                    let path = if rooted {
                        context.execution_root.join("code").join(&relative)
                    } else {
                        context.execution_root.join(&relative)
                    };
                    (
                        std::fs::read_to_string(path)
                            .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?,
                        relative,
                    )
                }
            };
            let (fuel_limit, timeout_secs) = configuration.resolved_limits();
            tasks
                .start_script(StartScriptInput {
                    correlation,
                    source,
                    filename,
                    input: inputs,
                    fuel_limit,
                    timeout_secs,
                    workspace_path: context.execution_root,
                    node_token: context.node_token,
                    node_bus: None,
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            Ok(NodeLifecycleResult::Running)
        }
    };
    let tick_tasks = tasks.clone();
    let tick = move |context: NodeLifecycleContext, _config: ScriptNodeConfig| {
        let tasks = tick_tasks.clone();
        async move {
            let correlation = context.correlation;
            let output = tasks
                .tick_script(TickScriptInput {
                    correlation,
                    input: context.inputs.as_value(),
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            match output.result {
                ScriptNodeTickResult::Running => Ok(NodeLifecycleResult::Running),
                ScriptNodeTickResult::Failed { failure } => Ok(NodeLifecycleResult::Failed {
                    message: failure.error.to_string(),
                    state: serde_json::json!({"outputs": failure.outputs, "logs": failure.logs}),
                }),
                ScriptNodeTickResult::Succeeded { completion } => {
                    let outputs = serde_json::to_value(&completion.outputs)
                        .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
                    let state = serde_json::json!({"outputs": outputs, "logs": completion.logs});
                    if !completion.errors.is_empty() {
                        return Ok(NodeLifecycleResult::Failed {
                            message: completion.errors.join("\n"),
                            state,
                        });
                    }
                    let mut values = BTreeMap::new();
                    for binding in context.node.outputs {
                        let value = outputs.get(&binding.name).cloned().ok_or_else(|| {
                            NodeLifecycleDispatchError::Owner(Box::new(std::io::Error::other(format!(
                                "ScriptNode did not emit configured output {}",
                                binding.name
                            ))))
                        })?;
                        values.insert(binding.name, value);
                    }
                    Ok(NodeLifecycleResult::Succeeded {
                        outputs: BoundOutputs::new(values),
                        state,
                    })
                }
            }
        }
    };
    let cancel_tasks = tasks;
    let cancel = move |context: NodeLifecycleContext, _: ScriptNodeConfig| {
        let tasks = cancel_tasks.clone();
        async move {
            tasks
                .cancel_script_execution(CancelScriptExecutionInput {
                    node_execution_id: context.correlation.node_execution_id,
                })
                .await
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            Ok(NodeLifecycleResult::Failed {
                message: "ScriptNode execution cancelled".to_owned(),
                state: serde_json::Value::Null,
            })
        }
    };
    NodeKindRegistration::typed::<ScriptNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.entry_point.is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "entry point must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        start,
        tick,
        cancel,
        None,
        WorkspacePreparation::new().node_content_roots::<ScriptNodeConfig>(|configuration| {
            configuration.source_root.clone().into_iter().collect()
        }),
    )
}

#[async_trait]
impl TasksLifecycleSudoService for TestTasks {
    async fn start_script(&self, input: StartScriptInput) -> Result<StartedOutput, TasksError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        *self.last_source.lock().unwrap() = Some(input.source);
        Ok(StartedOutput {
            correlation: input.correlation,
        })
    }

    async fn tick_script(&self, input: TickScriptInput) -> Result<ScriptNodeTickedOutput, TasksError> {
        Ok(ScriptNodeTickedOutput {
            correlation: input.correlation.clone(),
            result: ScriptNodeTickResult::Succeeded {
                completion: ScriptNodeCompletion {
                    correlation: input.correlation.clone(),
                    outputs: ScriptNodeOutputs::default(),
                    errors: if self.fail_tick {
                        vec![String::from("test failure")]
                    } else {
                        Vec::new()
                    },
                    logs: serde_json::json!([]),
                },
            },
        })
    }

    async fn cancel_script_execution(
        &self,
        _input: CancelScriptExecutionInput,
    ) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }

    async fn start_runtime(&self, _input: StartRuntimeInput) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }

    async fn stop_runtime(&self, _input: StopRuntimeInput) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }
}

#[async_trait]
impl TasksLifecycleSudoService for SequenceTasks {
    async fn start_script(&self, input: StartScriptInput) -> Result<StartedOutput, TasksError> {
        let start_number = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        let shared_file = input.workspace_path.join("first-node.txt");
        if start_number == 1 {
            std::fs::write(&shared_file, "written by first node").map_err(|source| TasksError::Execution {
                message: source.to_string(),
            })?;
        } else if start_number == 2 {
            let contents = std::fs::read_to_string(&shared_file).map_err(|source| TasksError::Execution {
                message: source.to_string(),
            })?;
            *self.observed_file.lock().unwrap() = Some(contents);
        }
        Ok(StartedOutput {
            correlation: input.correlation,
        })
    }

    async fn tick_script(&self, input: TickScriptInput) -> Result<ScriptNodeTickedOutput, TasksError> {
        Ok(ScriptNodeTickedOutput {
            correlation: input.correlation.clone(),
            result: ScriptNodeTickResult::Succeeded {
                completion: ScriptNodeCompletion {
                    correlation: input.correlation.clone(),
                    outputs: ScriptNodeOutputs::default(),
                    errors: Vec::new(),
                    logs: serde_json::json!([]),
                },
            },
        })
    }

    async fn cancel_script_execution(
        &self,
        _input: CancelScriptExecutionInput,
    ) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }

    async fn start_runtime(&self, _input: StartRuntimeInput) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }

    async fn stop_runtime(&self, _input: StopRuntimeInput) -> Result<TasksSuccessOutput, TasksError> {
        Ok(TasksSuccessOutput {})
    }
}

struct TestWorkspaceSource;

struct RejectingMobilityService;

impl super::super::mobility::FlowMobilityService for RejectingMobilityService {
    fn import_snapshot(
        &self,
        _destination: super::super::types::FlowMobilityDestination,
        _snapshot: super::super::mobility::FlowMobilitySnapshot,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<super::super::mobility::FlowMobilityCommitAcknowledgement, FlowError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async {
            Err(FlowError::MobilityPeer {
                message: "mobility service is not available in actor tests".to_owned(),
            })
        })
    }

    fn promote_replica(
        &self,
        _destination: super::super::types::FlowMobilityDestination,
        _acknowledgement: super::super::mobility::FlowYieldAcknowledgement,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<super::super::mobility::FlowActivationAcknowledgement, FlowError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async {
            Err(FlowError::MobilityPeer {
                message: "mobility service is not available in actor tests".to_owned(),
            })
        })
    }
}

impl super::super::types::FlowWorkspaceSourceService for TestWorkspaceSource {
    fn resolve_known_directory(
        &self,
        _relative_identity: &str,
    ) -> Result<PathBuf, super::super::types::FlowWorkspaceSourceError> {
        Err(super::super::types::FlowWorkspaceSourceError::BackendUnavailable)
    }
}

impl Actor {
    async fn shutdown(&mut self) -> Result<FlowResponse, FlowError> {
        self.shutdown_runtime().await
    }
}

struct TestAgent {
    starts: AtomicUsize,
    observed_workspaces: std::sync::Mutex<Vec<(PathBuf, String)>>,
}

fn agent_registration(agent: Arc<TestAgent>, copy_sources: Vec<WorkspaceSeed>) -> NodeKindRegistration {
    use super::super::ast::AgentNodeConfig;
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("AgentNode").unwrap(),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    let start_agent = agent.clone();
    let start = move |context: NodeLifecycleContext, _: AgentNodeConfig| {
        let agent = start_agent.clone();
        async move {
            agent.starts.fetch_add(1, Ordering::SeqCst);
            let contents = std::fs::read_to_string(context.execution_root.join("known.txt"))
                .map_err(|error| NodeLifecycleDispatchError::Owner(Box::new(error)))?;
            agent
                .observed_workspaces
                .lock()
                .unwrap()
                .push((context.execution_root, contents));
            Ok(NodeLifecycleResult::Running)
        }
    };
    let tick = |_: NodeLifecycleContext, _: AgentNodeConfig| async {
        Ok(NodeLifecycleResult::Succeeded {
            outputs: BoundOutputs::new(Default::default()),
            state: serde_json::Value::Null,
        })
    };
    let cancel = |_: NodeLifecycleContext, _: AgentNodeConfig| async {
        Ok(NodeLifecycleResult::Succeeded {
            outputs: BoundOutputs::new(Default::default()),
            state: serde_json::Value::Null,
        })
    };
    NodeKindRegistration::typed::<AgentNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.prompt.is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "prompt must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        start,
        tick,
        cancel,
        None,
        WorkspacePreparation::new()
            .node_content_roots::<AgentNodeConfig>(|configuration| {
                configuration.workspace.clone().into_iter().collect()
            })
            .graph_seeds::<AgentNodeConfig, _, _>(move |_| {
                let copy_sources = copy_sources.clone();
                async move { Ok(copy_sources) }
            }),
    )
}

fn unused_agent_registration() -> NodeKindRegistration {
    use super::super::ast::AgentNodeConfig;
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("AgentNode").unwrap(),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    let start = |_: NodeLifecycleContext, _: AgentNodeConfig| async { panic!("not used by ScriptNode tests") };
    let tick = |_: NodeLifecycleContext, _: AgentNodeConfig| async { panic!("not used by ScriptNode tests") };
    let cancel = |_: NodeLifecycleContext, _: AgentNodeConfig| async { panic!("not used by ScriptNode tests") };
    NodeKindRegistration::typed::<AgentNodeConfig, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration.prompt.is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "prompt must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        start,
        tick,
        cancel,
        None,
        WorkspacePreparation::default(),
    )
}

fn unused_order_registration() -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("OrderNode").unwrap(),
        cancellation: CancellationDisposition::NonCancellable,
        side_effects: SideEffectDisposition::SideEffecting,
    };
    let start = |_: NodeLifecycleContext, _: serde_json::Value| async { panic!("not used by ScriptNode tests") };
    let tick = |_: NodeLifecycleContext, _: serde_json::Value| async { panic!("not used by ScriptNode tests") };
    let cancel = |_: NodeLifecycleContext, _: serde_json::Value| async {
        Ok(NodeLifecycleResult::Failed {
            message: "OrderNode cannot be cancelled after submission".to_owned(),
            state: serde_json::Value::Null,
        })
    };
    NodeKindRegistration::typed::<serde_json::Value, _, _, _, _, _, _, _>(
        descriptor,
        |configuration| {
            if configuration
                .get("command")
                .and_then(serde_json::Value::as_str)
                .map_or(true, str::is_empty)
            {
                Err(FlowRegistrationError::Configuration {
                    message: "command must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        start,
        tick,
        cancel,
        None,
        WorkspacePreparation::default(),
    )
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct EchoConfig {
    message: String,
}

fn echo_registration(recorder: Arc<std::sync::Mutex<Vec<String>>>) -> NodeKindRegistration {
    let descriptor = NodeKindDescriptor {
        kind: NodeKind::parse("EchoNode").unwrap(),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::Pure,
    };
    let start_recorder = recorder.clone();
    let start = move |context: NodeLifecycleContext, config: EchoConfig| {
        let recorder = start_recorder.clone();
        async move {
            recorder.lock().unwrap().push("start".to_owned());
            assert!(context.node_token.is_some());
            assert_eq!(context.inputs.get("greeting"), Some(&serde_json::json!("hello")));
            assert_eq!(config.message, "hi");
            Ok(NodeLifecycleResult::Running)
        }
    };
    let tick_recorder = recorder.clone();
    let tick = move |context: NodeLifecycleContext, config: EchoConfig| {
        let recorder = tick_recorder.clone();
        async move {
            recorder.lock().unwrap().push("tick".to_owned());
            assert!(context.node_token.is_none());
            Ok(NodeLifecycleResult::Succeeded {
                outputs: BoundOutputs::new(BTreeMap::from([("echo".to_owned(), serde_json::json!(config.message))])),
                state: serde_json::json!("done"),
            })
        }
    };
    let cancel_recorder = recorder;
    let cancel = move |_: NodeLifecycleContext, _: EchoConfig| {
        let recorder = cancel_recorder.clone();
        async move {
            recorder.lock().unwrap().push("cancel".to_owned());
            Ok(NodeLifecycleResult::Failed {
                message: "cancelled".to_owned(),
                state: serde_json::Value::Null,
            })
        }
    };
    NodeKindRegistration::typed::<EchoConfig, _, _, _, _, _, _, _>(
        descriptor,
        |config| {
            if config.message.trim().is_empty() {
                Err(FlowRegistrationError::Configuration {
                    message: "message must not be empty".to_owned(),
                })
            } else {
                Ok(())
            }
        },
        start,
        tick,
        cancel,
        None,
        WorkspacePreparation::default(),
    )
}

fn echo_definition(flow_id: FlowId) -> FlowDefinition {
    echo_definition_with_message(flow_id, "hi")
}

fn echo_definition_with_message(flow_id: FlowId, message: &str) -> FlowDefinition {
    let graph = serde_json::json!({
        "version": 1,
        "root": {"type": "Leaf", "node": {
            "node_kind": "EchoNode", "id": Uuid::new_v4(), "label": "echo",
            "constraints": {"runtime": {}, "capabilities": []},
            "data_space": {"code_paths": [], "state_paths": []},
            "configuration": {"message": message},
            "inputs": [{"name": "greeting", "key": "greeting"}],
            "outputs": [{"name": "echo", "key": "echo"}]
        }}
    });
    FlowDefinition {
        flow_id,
        name: "echo-flow".to_owned(),
        revision: 1,
        graph: serde_json::from_value(graph).unwrap(),
        blackboard_defaults: serde_json::json!({"greeting": "hello"}),
        editor_metadata: serde_json::json!({}),
        automatic_enabled: true,
        workspace_mappings: Vec::new(),
    }
}

async fn echo_actor(
    path: &std::path::Path,
    recorder: Arc<std::sync::Mutex<Vec<String>>>,
    include_script: bool,
) -> Actor {
    let options = FlowBootOptions::open(path.to_path_buf()).await.unwrap();
    let (_, rx) = mpsc::channel(16);
    let (broadcast_tx, _) = mpsc::channel(16);
    let native_fixture = NativeDocsFixture::open(path).await;
    let mut node_registrations = options.node_registrations;
    node_registrations.push(echo_registration(recorder));
    if include_script {
        let tasks: Arc<dyn TasksLifecycleSudoService> = Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        });
        node_registrations.push(script_registration(tasks));
    }
    Actor::new(
        rx,
        ActorDependencies {
            trigger_registry: TriggerRegistry::new(),
            broadcast_tx,
            auth_service: ServiceReader::from_published_test_service(Arc::new(TestAuth)),
            mcp_bus_service: None,
            data_root: options.data_root,
            database: options.database,
            node_registrations,
            node_registration_supply: NodeRegistrationSupply::default(),
            bus_context: BusRpcContext::unavailable(),
            workspace_source_service: ServiceReader::from_published_test_service(Arc::new(TestWorkspaceSource)),
            sync_services: native_fixture.sync_services(),
            mobility_service: Arc::new(RejectingMobilityService),
            retention_completed_runs: 10,
            native_fixture: Some(native_fixture),
        },
    )
    .await
    .unwrap()
}

fn retention_candidates(executions: &[FlowExecutionSnapshot], retention: usize) -> Vec<FlowExecutionId> {
    let mut ordered = executions.to_vec();
    ordered.sort_by(|left, right| {
        right
            .completed_at
            .as_ref()
            .map(FlowTimestamp::as_datetime)
            .cmp(&left.completed_at.as_ref().map(FlowTimestamp::as_datetime))
    });
    ordered
        .into_iter()
        .skip(retention)
        .map(|snapshot| snapshot.execution_id)
        .collect()
}

#[test]
fn terminal_retention_retry_is_throttled() {
    let now = tokio::time::Instant::now();
    let mut next_retry = now + TERMINAL_RETENTION_RETRY_INTERVAL;

    assert!(!terminal_retention_retry_due(now, &mut next_retry));
    assert!(terminal_retention_retry_due(
        now + TERMINAL_RETENTION_RETRY_INTERVAL,
        &mut next_retry
    ));
    assert!(!terminal_retention_retry_due(
        now + TERMINAL_RETENTION_RETRY_INTERVAL + Duration::from_secs(1),
        &mut next_retry
    ));
}

fn definition(flow_id: FlowId, revision: i64) -> FlowDefinition {
    let node_id = Uuid::new_v4();
    let graph = serde_json::json!({
        "version": 1,
        "root": {"type": "Leaf", "node": {
            "node_kind": "ScriptNode", "id": node_id, "label": "script",
            "constraints": {"runtime": {}, "capabilities": []},
            "data_space": {"code_paths": [], "state_paths": []},
            "inputs": [{"name": "source", "key": "source"}], "outputs": [],
            "configuration": {
                "source_root": null, "entry_point": "script.ts", "fuel_limit": 1, "timeout_secs": 1
            }
        }}
    });
    FlowDefinition {
        flow_id,
        name: "flow".to_owned(),
        revision,
        graph: serde_json::from_value(graph).unwrap(),
        blackboard_defaults: serde_json::json!({"source":"export function run() {}"}),
        editor_metadata: serde_json::json!({}),
        automatic_enabled: true,
        workspace_mappings: Vec::new(),
    }
}

fn unregistered_kind_definition(flow_id: FlowId) -> FlowDefinition {
    let graph = serde_json::json!({
        "version": 1,
        "root": {"type": "Leaf", "node": {
            "node_kind": "GhostNode", "id": Uuid::new_v4(), "label": "ghost",
            "constraints": {"runtime": {}, "capabilities": []},
            "data_space": {"code_paths": [], "state_paths": []},
            "configuration": {}, "inputs": [], "outputs": []
        }}
    });
    FlowDefinition {
        flow_id,
        name: "ghost-flow".to_owned(),
        revision: 1,
        graph: serde_json::from_value(graph).unwrap(),
        blackboard_defaults: serde_json::json!({}),
        editor_metadata: serde_json::json!({}),
        automatic_enabled: true,
        workspace_mappings: Vec::new(),
    }
}

fn sequence_definition(flow_id: FlowId) -> FlowDefinition {
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let graph = serde_json::json!({
        "version": 1,
        "root": {
            "type": "Sequence",
            "children": [
                {
                    "type": "Leaf",
                    "node": {
                        "node_kind": "ScriptNode",
                        "id": first_id,
                        "label": "first",
                        "constraints": { "runtime": {}, "capabilities": [] },
                        "data_space": { "code_paths": [], "state_paths": [] },
                        "inputs": [{ "name": "source", "key": "source" }],
                        "outputs": [],
                        "configuration": {
                            "source_root": null,
                            "entry_point": "first.ts",
                            "fuel_limit": 1,
                            "timeout_secs": 1
                        }
                    }
                },
                {
                    "type": "Leaf",
                    "node": {
                        "node_kind": "ScriptNode",
                        "id": second_id,
                        "label": "second",
                        "constraints": { "runtime": {}, "capabilities": [] },
                        "data_space": { "code_paths": [], "state_paths": [] },
                        "inputs": [{ "name": "source", "key": "source" }],
                        "outputs": [],
                        "configuration": {
                            "source_root": null,
                            "entry_point": "second.ts",
                            "fuel_limit": 1,
                            "timeout_secs": 1
                        }
                    }
                }
            ]
        }
    });
    FlowDefinition {
        flow_id,
        name: "sequence-flow".to_owned(),
        revision: 1,
        graph: serde_json::from_value(graph).unwrap(),
        blackboard_defaults: serde_json::json!({"source":"export function run() {}"}),
        editor_metadata: serde_json::json!({}),
        automatic_enabled: true,
        workspace_mappings: Vec::new(),
    }
}

fn agent_definition(flow_id: FlowId, root: super::super::ast::ContentAddressedRoot) -> FlowDefinition {
    let graph = serde_json::json!({
        "version": 1,
        "root": {
            "type": "Leaf",
            "node": {
                "node_kind": "AgentNode",
                "id": Uuid::new_v4(),
                "label": "agent",
                "constraints": { "runtime": {}, "capabilities": [] },
                "data_space": { "code_paths": [], "state_paths": [] },
                "inputs": [],
                "outputs": [],
                "configuration": {
                    "providerName": "test-provider",
                    "agentName": "test-agent",
                    "prompt": "Inspect the workspace.",
                    "maxTurns": null,
                    "maxToolCalls": null,
                    "maxDuration": null,
                    "configOverrides": {},
                    "sluiceMcpEnabled": false,
                    "workspace": root
                }
            }
        }
    });
    FlowDefinition {
        flow_id,
        name: "agent-flow".to_owned(),
        revision: 1,
        graph: serde_json::from_value(graph).unwrap(),
        blackboard_defaults: serde_json::json!({}),
        editor_metadata: serde_json::json!({}),
        automatic_enabled: true,
        workspace_mappings: Vec::new(),
    }
}

async fn actor_for(path: &std::path::Path, tasks: Arc<TestTasks>) -> (Actor, mpsc::Sender<FlowRequest>) {
    actor_for_with_agent_and_retention(path, tasks, None, Vec::new(), 10).await
}

async fn actor_for_with_agent(
    path: &std::path::Path,
    tasks: Arc<dyn TasksLifecycleSudoService>,
    agent: Arc<TestAgent>,
) -> (Actor, mpsc::Sender<FlowRequest>) {
    actor_for_with_agent_and_retention(
        path,
        tasks,
        Some(agent),
        copy_sources(&path.join("agents"), &[".agents"]),
        10,
    )
    .await
}

async fn actor_for_with_agent_and_retention(
    path: &std::path::Path,
    tasks: Arc<dyn TasksLifecycleSudoService>,
    agent: Option<Arc<TestAgent>>,
    agent_copy_sources: Vec<WorkspaceSeed>,
    retention_completed_runs: u32,
) -> (Actor, mpsc::Sender<FlowRequest>) {
    let options = super::super::types::FlowBootOptions::open(path.to_path_buf())
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(16);
    let (broadcast_tx, _) = mpsc::channel(16);
    let native_fixture = NativeDocsFixture::open(path).await;
    let sync_services = native_fixture.sync_services();
    let mobility_service: Arc<dyn super::super::mobility::FlowMobilityService> = Arc::new(RejectingMobilityService);
    let actor = Actor::new(
        rx,
        ActorDependencies {
            trigger_registry: TriggerRegistry::new(),
            broadcast_tx,
            auth_service: ServiceReader::from_published_test_service(Arc::new(TestAuth)),
            mcp_bus_service: None,
            data_root: options.data_root,
            database: options.database,
            node_registrations: {
                let mut registrations = options.node_registrations;
                registrations.push(script_registration(tasks));
                registrations.push(
                    agent
                        .map(|agent| agent_registration(agent, agent_copy_sources))
                        .unwrap_or_else(unused_agent_registration),
                );
                registrations.push(unused_order_registration());
                registrations
            },
            node_registration_supply: NodeRegistrationSupply::default(),
            bus_context: BusRpcContext::unavailable(),
            workspace_source_service: ServiceReader::from_published_test_service(Arc::new(TestWorkspaceSource)),
            sync_services,
            mobility_service,
            retention_completed_runs,
            native_fixture: Some(native_fixture),
        },
    )
    .await
    .unwrap();
    (actor, tx)
}

#[tokio::test]
async fn retention_update_is_applied_while_idle() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _) = actor_for(
        dir.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;

    let execution_id = FlowExecutionId(Uuid::new_v4());
    actor.terminal_executions.push(FlowExecutionSnapshot {
        execution_id,
        flow_id: FlowId(Uuid::new_v4()),
        revision: 1,
        status: FlowExecutionStatus::Succeeded,
        started_at: FlowTimestamp::now(),
        completed_at: Some(FlowTimestamp::now()),
        error: None,
    });
    let (reply_to, reply_rx) = oneshot::channel();

    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::UpdateRetention {
                retention_completed_runs: 0,
            },
            reply_to,
        })
        .await;

    assert!(matches!(reply_rx.await.unwrap(), Ok(FlowResponse::RetentionUpdated)));
    assert_eq!(actor.retention_completed_runs, 0);
    assert!(actor.terminal_executions.is_empty());
    assert!(actor.execution.is_empty());
}

async fn start_flow(actor: &mut Actor, flow_id: FlowId) -> FlowExecutionId {
    let (reply_to, reply_rx) = oneshot::channel();
    let mut reply_rx = reply_rx;
    let background_task_count = actor.background_tasks.len();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::Start {
                flow_id,
                pulse: serde_json::Value::Null,
                automatic: false,
                invocation_kind: super::super::types::FlowInvocationKind::Manual,
            },
            reply_to,
        })
        .await;
    match reply_rx.try_recv() {
        Ok(Err(error)) => panic!("flow start failed before owner completion: {error:?}"),
        Ok(Ok(response)) => panic!("unexpected immediate flow start response: {response:?}"),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
            panic!("flow start reply channel closed before owner completion")
        }
        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
    }
    await_new_owner_dispatches(actor, background_task_count).await;
    let completion = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        actor
            .completion_rx
            .recv()
            .await
            .expect("Flow owner completion channel must remain open")
    })
    .await
    .expect("Flow owner start must complete promptly");
    actor.handle_dispatch_result(completion).await.unwrap();
    let FlowResponse::Started { execution_id } = reply_rx.await.unwrap().unwrap() else {
        panic!("start response")
    };
    execution_id
}

async fn await_new_owner_dispatches(actor: &mut Actor, background_task_count: usize) {
    let dispatch_tasks = actor.background_tasks.split_off(background_task_count);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        for task in dispatch_tasks {
            task.await
                .expect("Flow owner dispatch task must complete without JoinError");
        }
    })
    .await
    .expect("Flow owner dispatch must complete promptly");
}

fn due_trigger(flow_id: FlowId, revision: i64) -> FlowTriggerRecord {
    FlowTriggerRecord {
        trigger_id: Uuid::new_v4(),
        flow_id,
        revision,
        adapter_kind: "cron".to_owned(),
        adapter_config: super::super::cron::CronTriggerConfig {
            cron: super::super::cron::CronExpression::parse("* * * * *").unwrap(),
        }
        .into(),
        state: serde_json::json!({}),
        enabled: true,
        next_fire_at: Some(FlowTimestamp::from_datetime(
            chrono::Utc::now() - chrono::Duration::minutes(1),
        )),
        last_fired_at: None,
    }
}

async fn assert_trigger_cursor_advanced(store: FlowStore, trigger: FlowTriggerRecord) {
    let trigger_id = trigger.trigger_id;
    let fired_at = trigger.last_fired_at.as_ref().unwrap();
    assert!(trigger.next_fire_at.as_ref().unwrap().as_datetime() > fired_at.as_datetime());
    let persisted = store.load_triggers().await.unwrap();
    assert_eq!(persisted.get(&trigger_id), Some(&trigger));
}

async fn await_trigger_start(actor: &mut Actor, background_task_count: usize, trigger_errors: Vec<(Uuid, FlowError)>) {
    if let Some((trigger_id, error)) = trigger_errors.into_iter().next() {
        panic!("scheduled Flow start failed for trigger {trigger_id}: {error:?}");
    }
    await_new_owner_dispatches(actor, background_task_count).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let completion = actor
                .completion_rx
                .recv()
                .await
                .expect("Flow owner completion channel must remain open");
            if completion.kind == FlowDispatchKind::Start {
                return;
            }
        }
    })
    .await
    .expect("scheduled Flow owner start must complete promptly");
}

#[tokio::test]
async fn imported_workspace_source_reaches_normal_script_owner_and_snapshots_roots() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("script.ts"), "export const known = 'workspace';").unwrap();
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;

    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::ImportWorkspace {
                source_path: source.to_string_lossy().into_owned(),
            },
            reply_to,
        })
        .await;
    let FlowResponse::WorkspaceImported { root } = reply_rx.await.unwrap().unwrap() else {
        panic!("workspace import response")
    };
    let flow_id = FlowId(Uuid::new_v4());
    let mut flow = definition(flow_id, 1);
    let super::super::graph::GraphNode::Leaf { node } = &mut flow.graph.root else {
        panic!("script leaf")
    };
    let node = node.as_mut();
    if node.node_kind != NodeKind::parse("ScriptNode").unwrap() {
        panic!("script node")
    }
    let mut configuration: super::super::ast::ScriptNodeConfig =
        serde_json::from_value(node.configuration.clone()).unwrap();
    configuration.source_root = Some(root.clone());
    node.inputs.clear();
    node.configuration = serde_json::to_value(configuration).unwrap();
    node.data_space.code_paths = vec![super::super::ast::ContentAddressedPath {
        root: root.clone(),
        path: "script.ts".to_owned(),
    }];
    flow.blackboard_defaults = serde_json::json!({});
    actor.create_flow(flow).await.unwrap();

    let execution_id = start_flow(&mut actor, flow_id).await;
    assert_eq!(
        tasks.last_source.lock().unwrap().as_deref(),
        Some("export const known = 'workspace';")
    );
    let snapshot = actor
        .snapshot_store
        .load_all()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(snapshot.code_roots, vec![root]);
    assert!(snapshot.state_roots.is_empty());
    assert!(
        directory
            .path()
            .join("flows")
            .join("executions")
            .join(execution_id.to_string())
            .join("code")
            .exists()
    );

    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();
    let execution_root = directory
        .path()
        .join("flows")
        .join("executions")
        .join(execution_id.to_string());
    assert!(
        execution_root.exists(),
        "retained terminal execution root should remain inspectable"
    );
    assert_eq!(
        std::fs::read_to_string(execution_root.join("code").join("script.ts")).unwrap(),
        "export const known = 'workspace';"
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn imported_workspace_materializes_agent_owner_cwd_with_known_file() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let agents = directory.path().join("agents");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(agents.join(".agents")).unwrap();
    std::fs::write(agents.join(".agents/persona.md"), "provider asset").unwrap();
    std::fs::write(source.join("known.txt"), "agent workspace contents").unwrap();
    let agent = Arc::new(TestAgent {
        starts: AtomicUsize::new(0),
        observed_workspaces: std::sync::Mutex::new(Vec::new()),
    });
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for_with_agent(directory.path(), tasks, agent.clone()).await;
    actor.running = true;

    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::ImportWorkspace {
                source_path: source.to_string_lossy().into_owned(),
            },
            reply_to,
        })
        .await;
    let FlowResponse::WorkspaceImported { root } = reply_rx.await.unwrap().unwrap() else {
        panic!("workspace import response");
    };
    let flow_id = FlowId(Uuid::new_v4());
    actor
        .create_flow(agent_definition(flow_id, root.clone()))
        .await
        .unwrap();

    let execution_id = start_flow(&mut actor, flow_id).await;

    assert_eq!(agent.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        agent.observed_workspaces.lock().unwrap().as_slice(),
        &[(
            directory
                .path()
                .join("flows")
                .join("executions")
                .join(execution_id.to_string()),
            "agent workspace contents".to_owned(),
        )]
    );
    let snapshot = actor
        .snapshot_store
        .load_all()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(snapshot.code_roots, vec![root]);
    assert!(snapshot.state_roots.is_empty());
    assert!(
        snapshot
            .workspace_entries
            .contains_key(".agents/persona.md")
    );
    assert_eq!(
        std::fs::read_to_string(
            actor
                .workspace
                .execution_root(execution_id)
                .join(".agents/persona.md")
        )
        .unwrap(),
        "provider asset"
    );

    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();
    std::fs::write(agents.join(".agents/persona.md"), "changed after checkpoint").unwrap();
    let checkpoint = snapshot
        .workspace_checkpoint
        .as_ref()
        .expect("initial workspace checkpoint");
    actor
        .workspace
        .materialize_clean(
            checkpoint,
            &actor.workspace.execution_root(execution_id),
            "test/recovery",
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(
            actor
                .workspace
                .execution_root(execution_id)
                .join(".agents/persona.md")
        )
        .unwrap(),
        "provider asset"
    );
    assert!(
        directory
            .path()
            .join("flows")
            .join("executions")
            .join(execution_id.to_string())
            .exists()
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn due_cron_starts_idle_flow_and_advances_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    actor.set_flow_enabled(flow_id, true).await.unwrap();
    let trigger = due_trigger(flow_id, 1);
    actor.create_trigger(trigger.clone()).await.unwrap();

    let background_task_count = actor.background_tasks.len();
    let trigger_errors = actor.fire_due_triggers().await;
    await_trigger_start(&mut actor, background_task_count, trigger_errors).await;

    assert_eq!(actor.execution.len(), 1);
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    assert_trigger_cursor_advanced(
        FlowStore::new(actor.store.database()),
        actor.triggers.get(&trigger.trigger_id).cloned().unwrap(),
    )
    .await;
}

#[tokio::test]
async fn due_cron_skips_active_flow_and_advances_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    actor.set_flow_enabled(flow_id, true).await.unwrap();
    start_flow(&mut actor, flow_id).await;
    let trigger = due_trigger(flow_id, 1);
    actor.create_trigger(trigger.clone()).await.unwrap();

    let trigger_errors = actor.fire_due_triggers().await;
    assert!(trigger_errors.is_empty());
    assert_eq!(actor.execution.len(), 1);
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    assert_trigger_cursor_advanced(
        FlowStore::new(actor.store.database()),
        actor.triggers.get(&trigger.trigger_id).cloned().unwrap(),
    )
    .await;
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn due_cron_starts_a_different_flow_while_another_is_active() {
    let directory = tempfile::tempdir().unwrap();
    let active_flow_id = FlowId(Uuid::new_v4());
    let scheduled_flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    actor
        .create_flow(definition(active_flow_id, 1))
        .await
        .unwrap();
    actor
        .create_flow(definition(scheduled_flow_id, 1))
        .await
        .unwrap();
    actor
        .set_flow_enabled(scheduled_flow_id, true)
        .await
        .unwrap();
    start_flow(&mut actor, active_flow_id).await;
    let trigger = due_trigger(scheduled_flow_id, 1);
    actor.create_trigger(trigger.clone()).await.unwrap();

    let background_task_count = actor.background_tasks.len();
    let trigger_errors = actor.fire_due_triggers().await;
    await_trigger_start(&mut actor, background_task_count, trigger_errors).await;

    assert_eq!(actor.execution.len(), 2);
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 2);
    assert_trigger_cursor_advanced(
        FlowStore::new(actor.store.database()),
        actor.triggers.get(&trigger.trigger_id).cloned().unwrap(),
    )
    .await;
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn delete_write_failure_keeps_catalog_and_actor_trigger_state() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let trigger = FlowTriggerRecord {
        trigger_id: Uuid::new_v4(),
        flow_id,
        revision: 1,
        adapter_kind: "cron".to_owned(),
        adapter_config: super::super::cron::CronTriggerConfig {
            cron: super::super::cron::CronExpression::parse("0 * * * *").unwrap(),
        }
        .into(),
        state: serde_json::json!({}),
        enabled: true,
        next_fire_at: None,
        last_fired_at: None,
    };
    let canonical_trigger = match actor.create_trigger(trigger.clone()).await.unwrap() {
        FlowResponse::FlowTriggerCreated { trigger } => trigger,
        response => panic!("unexpected trigger creation response: {response:?}"),
    };
    let connection = connect_database(&actor.store.database()).await.unwrap();
    connection
        .execute("DROP TABLE flow_triggers", ())
        .await
        .unwrap();

    assert!(matches!(
        actor.delete_flow(flow_id).await,
        Err(FlowError::Persistence(_))
    ));
    assert!(actor.definitions.contains_key(&flow_id));
    assert_eq!(actor.triggers.get(&trigger.trigger_id), Some(&canonical_trigger));
    let mut rows = connection
        .query(
            "SELECT deleted_at FROM flows WHERE flow_id = ?1",
            (flow_id.0.to_string(),),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<Option<String>>(0)
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn write_failure_never_mutates_actor_or_reports_success() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    let connection = connect_database(&actor.store.database()).await.unwrap();
    connection.execute("DROP TABLE flows", ()).await.unwrap();
    assert!(matches!(
        actor.create_flow(definition(flow_id, 1)).await,
        Err(FlowError::Persistence(_))
    ));
    assert!(!actor.definitions.contains_key(&flow_id));
}

#[tokio::test]
async fn node_crud_revisions_graph_and_node_rows_transactionally() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    let original = definition(flow_id, 1);
    actor.create_flow(original).await.unwrap();
    let extra_definition = definition(FlowId(Uuid::new_v4()), 1);
    let super::super::graph::GraphNode::Leaf { node } = extra_definition.graph.root else {
        panic!("fixture leaf")
    };
    let extra_id = node.id();
    actor.create_node(flow_id, *node).await.unwrap();

    let mut replacement_definition = definition(FlowId(Uuid::new_v4()), 1);
    let super::super::graph::GraphNode::Leaf { node: replacement } = &mut replacement_definition.graph.root else {
        panic!("fixture leaf")
    };
    replacement.id = extra_id;
    replacement.label = "replacement".to_owned();
    actor
        .update_node(flow_id, (**replacement).clone())
        .await
        .unwrap();
    actor.delete_node(flow_id, extra_id).await.unwrap();

    let current = actor.definitions.get(&flow_id).unwrap();
    assert_eq!(current.revision, 4);
    let connection = connect_database(&actor.store.database()).await.unwrap();
    let mut rows = connection
        .query(
            "SELECT COUNT(*) FROM flow_nodes WHERE flow_id=?1 AND revision=?2",
            (flow_id.0.to_string(), current.revision),
        )
        .await
        .unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 1);
    let mut rows = connection
        .query(
            "SELECT COUNT(*) FROM flow_definition_revisions WHERE flow_id=?1",
            (flow_id.0.to_string(),),
        )
        .await
        .unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 4);
}

#[tokio::test]
async fn inactive_committed_replica_rejects_edits_scheduling_and_execution() {
    let directory = tempfile::tempdir().expect("temporary Flow database");
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks).await;
    let flow = definition(flow_id, 1);

    actor.create_flow(flow.clone()).await.expect("create Flow");
    actor
        .store
        .set_authority_state(flow_id, FlowAuthorityState::InactiveCommitted)
        .await
        .expect("persist inactive imported state");
    actor
        .mobility
        .set_authority_state(flow_id, FlowAuthorityState::InactiveCommitted);
    actor.running = true;

    assert!(matches!(
        actor.update_flow(definition(flow_id, 2)).await,
        Err(FlowError::InactiveReplica { flow_id: rejected }) if rejected == flow_id
    ));
    assert!(matches!(
        actor.create_trigger(due_trigger(flow_id, 1)).await,
        Err(FlowError::InactiveReplica { flow_id: rejected }) if rejected == flow_id
    ));
    assert!(matches!(
        actor
            .begin_start(
                flow_id,
                None,
                serde_json::Value::Null,
                false,
                FlowInvocationKind::Manual,
            )
            .await,
        Err(FlowError::InactiveReplica { flow_id: rejected }) if rejected == flow_id
    ));
}

#[tokio::test]
async fn owner_dispatch_persists_pending_then_correlated_success() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;
    assert!(actor.execution.contains_key(&execution_id));
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        tasks.last_source.lock().unwrap().as_deref(),
        Some("export function run() {}")
    );
    let terminal = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(terminal).await.unwrap();
    assert!(!actor.execution.contains_key(&execution_id));
    assert_eq!(
        actor.terminal_executions.last().unwrap().status,
        FlowExecutionStatus::Succeeded
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminal_completion_retries_persistence_without_redispatching_owner_work() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;
    let completion = actor.completion_rx.recv().await.unwrap();

    actor.snapshot_store.fail_save_after_successes(0);
    assert!(actor.handle_dispatch_result(completion).await.is_err());
    assert!(actor.execution.contains_key(&execution_id));
    assert!(actor.terminal_executions.is_empty());
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    let checkpoint = actor.snapshot_store.load_all().await.unwrap();
    assert_eq!(checkpoint[0].execution.status, FlowExecutionStatus::Active);
    assert_eq!(checkpoint[0].scratchpad.mailbox.len(), 0);
    assert!(
        actor
            .execution
            .get(&execution_id)
            .is_some_and(|state| !state.pending_terminal_persistence.is_empty())
    );
    let prepared = actor
        .execution
        .get(&execution_id)
        .and_then(|state| state.prepared_checkpoint_commit.as_ref())
        .expect("failed terminal save retains prepared commit")
        .snapshot
        .clone();

    actor.retry_pending_execution_work().await;

    assert!(!actor.execution.contains_key(&execution_id));
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        actor.terminal_executions.last().unwrap().status,
        FlowExecutionStatus::Succeeded
    );
    let persisted = actor.snapshot_store.load_all().await.unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].execution.status, FlowExecutionStatus::Succeeded);
    assert_eq!(persisted[0].execution.execution_id, prepared.execution.execution_id);
    assert_eq!(persisted[0].execution.flow_id, prepared.execution.flow_id);
    assert_eq!(persisted[0].execution.revision, prepared.execution.revision);
    assert_eq!(persisted[0].execution.status, prepared.execution.status);
    assert_eq!(persisted[0].execution.started_at, prepared.execution.started_at);
    assert_eq!(persisted[0].execution.completed_at, prepared.execution.completed_at);
    assert_eq!(persisted[0].execution.error, prepared.execution.error);
    assert_eq!(persisted[0].blackboard, prepared.blackboard);
    assert_eq!(persisted[0].code_roots, prepared.code_roots);
    assert_eq!(persisted[0].state_roots, prepared.state_roots);
    assert_eq!(persisted[0].workspace_checkpoint, prepared.workspace_checkpoint);
    assert_eq!(persisted[0].workspace_entries, prepared.workspace_entries);
    assert_eq!(persisted[0].reconstruction, prepared.reconstruction);
    assert_eq!(persisted[0].scratchpad, prepared.scratchpad);
    assert_eq!(persisted[0].nodes, prepared.nodes);
    assert!(persisted[0].scratchpad.mailbox.is_empty());
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn sequence_successor_preserves_shared_workspace_through_terminal_retry() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let tasks = Arc::new(SequenceTasks {
        starts: AtomicUsize::new(0),
        observed_file: std::sync::Mutex::new(None),
    });
    let (mut actor, _) =
        actor_for_with_agent_and_retention(directory.path(), tasks.clone(), None, Vec::new(), 10).await;
    actor.running = true;
    actor
        .create_flow(sequence_definition(flow_id))
        .await
        .unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;

    let first_tick = tokio::time::timeout(Duration::from_secs(1), actor.completion_rx.recv())
        .await
        .unwrap_or_else(|_| panic!("first Tick completion timed out for execution {execution_id:?}"))
        .expect("first Tick completion channel closed");
    let first_tick_correlation = first_tick.correlation.clone();
    actor.snapshot_store.fail_checkpoint_save_after_successes(0);
    assert!(actor.handle_dispatch_result(first_tick).await.is_err());
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 1);
    assert!(actor.execution.contains_key(&execution_id));
    assert_eq!(
        actor
            .execution
            .get(&execution_id)
            .and_then(|state| state.prepared_checkpoint_commit.as_ref())
            .expect("failed checkpoint save retains prepared successor")
            .dispatches
            .len(),
        1
    );
    actor.retry_pending_execution_work().await;
    let second_start = tokio::time::timeout(Duration::from_secs(1), actor.completion_rx.recv())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "successor Start completion timed out for execution {execution_id:?}, after first node correlation {first_tick_correlation:?}"
            )
        })
        .expect("successor Start completion channel closed");
    assert_eq!(second_start.kind, FlowDispatchKind::Start);
    assert_eq!(second_start.correlation.flow_execution_id, execution_id);
    assert_ne!(
        second_start.correlation.node_execution_id,
        first_tick_correlation.node_execution_id
    );
    let second_start_correlation = second_start.correlation.clone();
    actor.handle_dispatch_result(second_start).await.unwrap();
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        tasks.observed_file.lock().unwrap().as_deref(),
        Some("written by first node")
    );

    let second_tick = tokio::time::timeout(Duration::from_secs(1), actor.completion_rx.recv())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "successor Tick completion timed out for execution {execution_id:?}, correlation {second_start_correlation:?}"
            )
        })
        .expect("successor Tick completion channel closed");
    assert_eq!(second_tick.kind, FlowDispatchKind::Tick);
    assert_eq!(second_tick.correlation, second_start_correlation);

    actor.handle_dispatch_result(second_tick).await.unwrap();
    assert!(!actor.execution.contains_key(&execution_id));
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 2);
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_after_definition_update_keeps_original_revision_and_start_time() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;
    let started_at = actor
        .execution
        .get(&execution_id)
        .unwrap()
        .active
        .started_at();
    actor.update_flow(definition(flow_id, 2)).await.unwrap();
    actor.abort(execution_id).await.unwrap();
    assert!(actor.execution.contains_key(&execution_id));
    assert!(
        directory
            .path()
            .join("flows")
            .join("executions")
            .join(execution_id.to_string())
            .exists()
    );
    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();
    let terminal = actor.terminal_executions.last().unwrap();
    assert_eq!(terminal.revision, 1);
    assert_eq!(terminal.started_at, started_at);
    assert_eq!(terminal.status, FlowExecutionStatus::Aborted);
    let connection = connect_database(&actor.store.database()).await.unwrap();
    let mut rows = connection
        .query(
            "SELECT revision, started_at, observer_state_json FROM flow_executions WHERE execution_id=?1",
            (execution_id.0.to_string(),),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let observer: FlowExecutionSnapshot = serde_json::from_str(&row.get::<String>(2).unwrap()).unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    let persisted_started_at = chrono::DateTime::parse_from_rfc3339(&row.get::<String>(1).unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(persisted_started_at, *started_at.as_datetime());
    assert_eq!(observer.execution_id, terminal.execution_id);
    assert_eq!(observer.flow_id, terminal.flow_id);
    assert_eq!(observer.revision, terminal.revision);
    assert_eq!(observer.status, terminal.status);
    assert_eq!(observer.started_at, terminal.started_at);
    assert_eq!(observer.completed_at, terminal.completed_at);
    assert_eq!(observer.error, terminal.error);
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_correlation_is_fenced_and_script_completion_errors_terminalize() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: true,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    actor.running = true;
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;
    let stale = super::super::messages::LifecycleCorrelation {
        node_kind: NodeKind::parse("ScriptNode").unwrap(),
        flow_id,
        flow_execution_id: execution_id,
        flow_node_id: super::super::types::FlowNodeId(Uuid::new_v4()),
        node_execution_id: super::super::types::NodeExecutionId(Uuid::new_v4()),
    };
    actor
        .handle_dispatch_result(DispatchCompletion {
            correlation: stale.clone(),
            node_kind: NodeKind::parse("ScriptNode").unwrap(),
            token_expired: false,
            kind: FlowDispatchKind::Tick,
            result: Ok(OwnerLifecycleResult::Succeeded {
                correlation: stale,
                outputs: super::super::blackboard::BoundOutputs::new(Default::default()),
                state: serde_json::Value::Null,
            }),
        })
        .await
        .unwrap();
    assert!(actor.execution.contains_key(&execution_id));

    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();
    assert_eq!(
        actor.terminal_executions.last().unwrap().status,
        FlowExecutionStatus::Failed
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn script_adapter_error_terminalizes_with_null_state() {
    let directory = tempfile::tempdir().unwrap();
    let flow_id = FlowId(Uuid::new_v4());
    let (mut actor, _) = actor_for(
        directory.path(),
        Arc::new(TestTasks {
            starts: AtomicUsize::new(0),
            fail_tick: false,
            last_source: std::sync::Mutex::new(None),
        }),
    )
    .await;
    actor.running = true;
    let mut flow = definition(flow_id, 1);
    let mut graph = serde_json::to_value(&flow.graph).unwrap();
    graph["root"]["node"]["outputs"] = json!([{"name":"required","key":"result"}]);
    flow.graph = serde_json::from_value(graph).unwrap();
    actor.create_flow(flow).await.unwrap();

    let execution_id = start_flow(&mut actor, flow_id).await;
    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();

    let terminal = actor.terminal_executions.last().unwrap();
    assert_eq!(terminal.execution_id, execution_id);
    assert_eq!(terminal.status, FlowExecutionStatus::Failed);
    let snapshot = actor
        .snapshot_store
        .load_all()
        .await
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.execution.execution_id == execution_id)
        .unwrap();
    let node = snapshot.nodes.iter().next().unwrap();
    assert_eq!(node.status, super::super::snapshot::NodeExecutionStatus::Failed);
    assert_eq!(node.state, serde_json::Value::Null);
    assert!(node.error.as_deref().unwrap().contains("required"));
    assert!(!actor.execution.contains_key(&execution_id));
    actor.shutdown().await.unwrap();
}

#[test]
fn retention_candidates_keep_newest_terminal_runs_and_zero_prunes_all() {
    let flow_id = FlowId(Uuid::new_v4());
    let snapshots = (0..3)
        .map(|offset| FlowExecutionSnapshot {
            execution_id: FlowExecutionId(Uuid::new_v4()),
            flow_id,
            revision: 1,
            status: FlowExecutionStatus::Succeeded,
            started_at: FlowTimestamp::from_datetime(chrono::Utc::now() - chrono::Duration::minutes(offset + 1)),
            completed_at: Some(FlowTimestamp::from_datetime(
                chrono::Utc::now() - chrono::Duration::minutes(offset),
            )),
            error: None,
        })
        .collect::<Vec<_>>();

    assert_eq!(retention_candidates(&snapshots, 1).len(), 2);
    assert_eq!(retention_candidates(&snapshots, 0).len(), 3);
}

#[test]
fn unregistered_graph_kind_is_typed_and_registered_bad_configuration_stays_invalid() {
    let ghost = serde_json::json!({
        "version": 1,
        "root": {"type": "Leaf", "node": {
            "node_kind": "GhostNode", "id": Uuid::new_v4(), "label": "ghost",
            "constraints": {"runtime": {}, "capabilities": []},
            "data_space": {"code_paths": [], "state_paths": []},
            "configuration": {}, "inputs": [], "outputs": []
        }}
    });
    let ghost: super::super::graph::FlowGraphAst = serde_json::from_value(ghost).unwrap();
    let registry = super::super::registration::FlowNodeRegistry::new();
    assert!(matches!(
        ghost.validate_with_registry(&registry),
        Err(super::super::types::FlowGraphLoadError::UnregisteredNodeKind(kind))
            if kind == NodeKind::parse("GhostNode").unwrap()
    ));
    let flow_error = FlowError::from(super::super::types::FlowGraphLoadError::UnregisteredNodeKind(
        NodeKind::parse("GhostNode").unwrap(),
    ));
    assert!(matches!(flow_error, FlowError::UnregisteredNodeKind(_)));

    let invalid = serde_json::json!({
        "version": 1,
        "root": {"type": "Leaf", "node": {
            "node_kind": "ScriptNode", "id": Uuid::new_v4(), "label": "script",
            "constraints": {"runtime": {}, "capabilities": []},
            "data_space": {"code_paths": [], "state_paths": []},
            "configuration": {"unexpected": true}, "inputs": [], "outputs": []
        }}
    });
    let invalid: super::super::graph::FlowGraphAst = serde_json::from_value(invalid).unwrap();
    let tasks: Arc<dyn TasksLifecycleSudoService> = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let mut registered = super::super::registration::FlowNodeRegistry::new();
    registered.register(script_registration(tasks)).unwrap();
    assert!(matches!(
        invalid.validate_with_registry(&registered),
        Err(super::super::types::FlowGraphLoadError::Invalid { .. })
    ));
}

#[tokio::test]
async fn start_rejects_unregistered_kind_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let tasks = Arc::new(TestTasks {
        starts: AtomicUsize::new(0),
        fail_tick: false,
        last_source: std::sync::Mutex::new(None),
    });
    let (mut actor, _) = actor_for(directory.path(), tasks.clone()).await;
    actor.running = true;
    let flow_id = FlowId(Uuid::new_v4());
    actor
        .create_flow(unregistered_kind_definition(flow_id))
        .await
        .unwrap();

    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::Start {
                flow_id,
                pulse: serde_json::Value::Null,
                automatic: false,
                invocation_kind: FlowInvocationKind::Manual,
            },
            reply_to,
        })
        .await;

    assert!(matches!(
        reply_rx.await.unwrap(),
        Err(FlowError::UnregisteredNodeKind(kind)) if kind == NodeKind::parse("GhostNode").unwrap()
    ));
    assert_eq!(tasks.starts.load(Ordering::SeqCst), 0);
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn echo_node_full_lifecycle_through_generic_registry() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut actor = echo_actor(directory.path(), recorder.clone(), false).await;
    actor.running = true;
    let flow_id = FlowId(Uuid::new_v4());
    actor.create_flow(echo_definition(flow_id)).await.unwrap();

    let execution_id = start_flow(&mut actor, flow_id).await;
    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();

    assert_eq!(*recorder.lock().unwrap(), ["start", "tick"]);
    assert!(!actor.execution.contains_key(&execution_id));
    assert_eq!(
        actor.terminal_executions.last().unwrap().status,
        FlowExecutionStatus::Succeeded
    );
    let snapshots = actor.snapshot_store.load_all().await.unwrap();
    let snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.execution.execution_id == execution_id)
        .unwrap();
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(
        snapshot.nodes[0].inputs.get("greeting"),
        Some(&serde_json::json!("hello"))
    );
    assert_eq!(
        serde_json::to_value(&snapshot.nodes[0].outputs).unwrap()["values"]["echo"],
        serde_json::json!("hi")
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn echo_node_abort_dispatches_cancel() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut actor = echo_actor(directory.path(), recorder.clone(), true).await;
    actor.running = true;
    let flow_id = FlowId(Uuid::new_v4());
    actor.create_flow(echo_definition(flow_id)).await.unwrap();
    let execution_id = start_flow(&mut actor, flow_id).await;
    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::Abort { execution_id },
            reply_to,
        })
        .await;
    let _ = reply_rx.await.unwrap().unwrap();
    let completion = actor.completion_rx.recv().await.unwrap();
    actor.handle_dispatch_result(completion).await.unwrap();

    assert!(recorder.lock().unwrap().contains(&"cancel".to_owned()));
    assert_eq!(
        actor.terminal_executions.last().unwrap().status,
        FlowExecutionStatus::Aborted
    );
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn echo_definition_save_reload_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let mut actor = echo_actor(directory.path(), Arc::new(std::sync::Mutex::new(Vec::new())), false).await;
    let flow_id = FlowId(Uuid::new_v4());
    actor.create_flow(echo_definition(flow_id)).await.unwrap();

    let reloaded = actor
        .store
        .load_definition_revision(flow_id, 1)
        .await
        .unwrap();
    let super::super::graph::GraphNode::Leaf { node } = reloaded.graph.root else {
        panic!("expected EchoNode leaf")
    };
    assert_eq!(node.node_kind.as_str(), "EchoNode");
    assert_eq!(node.configuration["message"], "hi");
    assert_eq!(node.inputs.len(), 1);
    assert_eq!(node.outputs.len(), 1);
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_builtin_special_casing_for_unregistered_script() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut actor = echo_actor(directory.path(), recorder.clone(), false).await;
    actor.running = true;
    let flow_id = FlowId(Uuid::new_v4());
    actor.create_flow(definition(flow_id, 1)).await.unwrap();
    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::Start {
                flow_id,
                pulse: serde_json::Value::Null,
                automatic: false,
                invocation_kind: FlowInvocationKind::Manual,
            },
            reply_to,
        })
        .await;

    assert!(
        matches!(reply_rx.await.unwrap(), Err(FlowError::UnregisteredNodeKind(kind)) if kind.as_str() == "ScriptNode")
    );
    assert!(recorder.lock().unwrap().is_empty());
    actor.shutdown().await.unwrap();
}

#[tokio::test]
async fn echo_invalid_config_rejected_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut actor = echo_actor(directory.path(), recorder.clone(), false).await;
    actor.running = true;
    let flow_id = FlowId(Uuid::new_v4());

    actor
        .create_flow(echo_definition_with_message(flow_id, "  "))
        .await
        .unwrap();
    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(FlowRequest {
            payload: FlowRequestPayload::Start {
                flow_id,
                pulse: serde_json::Value::Null,
                automatic: false,
                invocation_kind: FlowInvocationKind::Manual,
            },
            reply_to,
        })
        .await;

    assert!(matches!(
        reply_rx.await.unwrap(),
        Err(FlowError::Graph(
            super::super::types::FlowGraphLoadError::Invalid { .. }
        ))
    ));
    assert!(recorder.lock().unwrap().is_empty());
    actor.shutdown().await.unwrap();
}

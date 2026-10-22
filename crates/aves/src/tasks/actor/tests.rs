use std::collections::BTreeMap;
use std::path::PathBuf;

use tempfile::tempdir;
use uuid::Uuid;

use crate::auth::Jwt;
use crate::bus::BusIngress;
use crate::flow::{FlowExecutionId, FlowId, FlowNodeId, LifecycleCorrelation, NodeExecutionId, NodeKind};

use super::super::client::generated_guest_rpc_factory;
use super::super::messages::TasksRequestPayload;
use super::super::types::{ScriptNodeCompletion, ScriptNodeOutputs};
use super::*;

fn correlation() -> LifecycleCorrelation {
    LifecycleCorrelation {
        node_kind: NodeKind::parse("ScriptNode").expect("static node kind is valid"),
        flow_id: FlowId(Uuid::new_v4()),
        flow_node_id: FlowNodeId(Uuid::new_v4()),
        flow_execution_id: FlowExecutionId(Uuid::new_v4()),
        node_execution_id: NodeExecutionId(Uuid::new_v4()),
    }
}

fn test_bus() -> BusIngress {
    let (sender, _receiver) = mpsc::channel(1);
    sender
}

fn test_actor(receiver: mpsc::Receiver<TasksRequest>) -> Actor {
    Actor::new(
        receiver,
        ScriptSandbox::new().unwrap(),
        generated_guest_rpc_factory::<TasksError>(Vec::new()),
    )
}

#[tokio::test]
async fn start_script_applies_finite_defaults_and_rejects_input_replay() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let workspace = tempdir().unwrap();
    let workspace_path = workspace.path().to_path_buf();
    let (runtime_reply_tx, runtime_reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, runtime_reply_tx, &completion_tx)
        .await;
    assert!(matches!(runtime_reply_rx.await.unwrap(), Ok(TasksResponse::Success)));
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let correlation = correlation();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: correlation.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"x":1}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path: workspace_path.clone(),
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));
    assert_eq!(
        actor
            .pending
            .get(&correlation.node_execution_id)
            .map(|pending| (pending.fuel, pending.timeout)),
        Some((1_000_000, 30))
    );
    assert_eq!(
        actor
            .pending
            .get(&correlation.node_execution_id)
            .map(|pending| pending.workspace_path.as_path()),
        Some(workspace_path.as_path())
    );

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: correlation.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"x":1}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path: workspace_path.clone(),
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Err(TasksError::IdempotencyConflict { .. })
    ));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: serde_json::json!({"x":2}),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Err(TasksError::IdempotencyConflict { .. })
    ));
    actor.running.insert(
        correlation.node_execution_id,
        Running {
            correlation: correlation.clone(),
            input: serde_json::json!({"x":1}),
            abort: ScriptAbortHandle::new(),
        },
    );
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: serde_json::json!({"x":1}),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Ok(TasksResponse::ScriptNodeTicked {
            result: ScriptNodeTickResult::Running,
            ..
        })
    ));
}
#[tokio::test]
async fn runtime_lifecycle_rejects_work_while_stopped_and_discards_pending_work() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let workspace = tempdir().unwrap();
    let workspace_path = workspace.path().to_path_buf();
    let correlation = correlation();
    let start_payload = || TasksRequestPayload::StartScript {
        correlation: correlation.clone(),
        source: "const value = 1;".into(),
        filename: "script.ts".into(),
        input: serde_json::json!({"x":1}),
        fuel_limit: 1_000_000,
        timeout_secs: 30,
        workspace_path: workspace_path.clone(),
        node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
        node_bus: Some(test_bus()),
    };

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(start_payload(), reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Err(TasksError::RuntimeStopped)));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(start_payload(), reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));
    assert!(actor.pending.contains_key(&correlation.node_execution_id));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StopRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));
    assert!(actor.pending.is_empty());
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: serde_json::json!({"x":1}),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Err(TasksError::RuntimeStopped)));
}

#[tokio::test]
async fn tick_requires_an_in_process_start() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let correlation = correlation();

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: serde_json::json!({"x": 1}),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Err(TasksError::ScriptNotStarted { node_execution_id }) if node_execution_id == correlation.node_execution_id
    ));
}
#[tokio::test]
async fn execution_capacity_is_four_and_finite_limits_remain_pending_on_rejection() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let workspace = tempdir().unwrap();
    let workspace_path = workspace.path().to_path_buf();
    let mut permits = Vec::new();
    for _ in 0..MAX_CONCURRENT_EXECUTIONS {
        permits.push(actor.execution_slots.clone().try_acquire_owned().unwrap());
    }
    assert_eq!(MAX_CONCURRENT_EXECUTIONS, 4);

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));
    let correlation = correlation();
    let input = serde_json::json!({"x":1});
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: correlation.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: input.clone(),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path,
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: input.clone(),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Ok(TasksResponse::ScriptNodeTicked {
            result: ScriptNodeTickResult::Running,
            ..
        })
    ));
    let pending = actor.pending.get(&correlation.node_execution_id).unwrap();
    assert_eq!((pending.fuel, pending.timeout), (1_000_000, 30));
    drop(permits);
}
#[tokio::test]
async fn pre_execution_start_failures_leave_no_pending_work() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let workspace = tempdir().unwrap();
    let workspace_path = workspace.path().to_path_buf();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));

    let compilation_failure = correlation();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: compilation_failure.clone(),
                source: "const =".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"value": 7}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path: workspace_path.clone(),
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Err(TasksError::Compilation { .. })));
    assert!(
        !actor
            .pending
            .contains_key(&compilation_failure.node_execution_id)
    );

    let token_failure = correlation();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: token_failure.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"value": 7}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path: workspace_path.clone(),
                node_token: None,
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Err(TasksError::MissingNodeToken)));
    assert!(!actor.pending.contains_key(&token_failure.node_execution_id));

    let retry = correlation();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: retry.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"value": 7}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path: workspace_path.clone(),
                node_token: None,
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Err(TasksError::MissingNodeToken)));
    assert!(!actor.pending.contains_key(&retry.node_execution_id));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: retry.clone(),
                source: "const value = 1;".into(),
                filename: "script.ts".into(),
                input: serde_json::json!({"value": 7}),
                fuel_limit: 0,
                timeout_secs: 0,
                workspace_path,
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));
    assert!(actor.pending.contains_key(&retry.node_execution_id));
}
#[cfg(feature = "wasm-tests")]
#[tokio::test]
async fn restarted_script_node_starts_and_executes_without_durable_replay() {
    let (_tx, rx) = mpsc::channel(1);
    let mut actor = test_actor(rx);
    let (completion_tx, _completion_rx) = mpsc::channel(1);
    let workspace = tempdir().unwrap();
    let workspace_path = workspace.path().to_path_buf();
    let correlation = correlation();
    let input = serde_json::json!({"value": 7});

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: correlation.clone(),
                source: "const interrupted = true;".into(),
                filename: "script.ts".into(),
                input: input.clone(),
                fuel_limit: 100_000_000,
                timeout_secs: 30,
                workspace_path: workspace_path.clone(),
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));
    drop(actor);

    let (_tx, rx) = mpsc::channel(1);
    let mut restarted = test_actor(rx);
    let (restarted_completion_tx, mut restarted_completion_rx) = mpsc::channel(1);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    restarted
        .handle_message(TasksRequestPayload::StartRuntime, reply_tx, &restarted_completion_tx)
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Success)));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    restarted
        .handle_message(
            TasksRequestPayload::StartScript {
                correlation: correlation.clone(),
                source: "__sluice_outputs.restarted = true;".into(),
                filename: "script.ts".into(),
                input: input.clone(),
                fuel_limit: 100_000_000,
                timeout_secs: 30,
                workspace_path,
                node_token: Some(Jwt::from_opaque("test-node-token".to_owned())),
                node_bus: Some(test_bus()),
            },
            reply_tx,
            &restarted_completion_tx,
        )
        .await;
    assert!(matches!(reply_rx.await.unwrap(), Ok(TasksResponse::Started { .. })));
    assert!(
        restarted
            .pending
            .contains_key(&correlation.node_execution_id)
    );

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    restarted
        .handle_message(
            TasksRequestPayload::TickScript {
                correlation: correlation.clone(),
                input: input.clone(),
            },
            reply_tx,
            &restarted_completion_tx,
        )
        .await;
    assert!(matches!(
        reply_rx.await.unwrap(),
        Ok(TasksResponse::ScriptNodeTicked {
            result: ScriptNodeTickResult::Running,
            ..
        })
    ));
    let completion = restarted_completion_rx.recv().await.unwrap();
    restarted.complete(completion).await;
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    restarted
        .handle_message(
            TasksRequestPayload::TickScript { correlation, input },
            reply_tx,
            &restarted_completion_tx,
        )
        .await;
    let response = reply_rx.await.unwrap();
    assert!(
        matches!(
            &response,
            Ok(TasksResponse::ScriptNodeTicked {
                result: ScriptNodeTickResult::Succeeded { completion },
                ..
            }) if completion.outputs.get("restarted") == Some(&serde_json::json!(true))
        ),
        "expected a completed restarted ScriptNode output, got {response:?}"
    );
}

fn lf_correlation() -> LifecycleCorrelation {
    LifecycleCorrelation {
        node_kind: NodeKind::parse("ScriptNode").expect("static node kind is valid"),
        flow_id: FlowId(Uuid::new_v4()),
        flow_node_id: FlowNodeId(Uuid::new_v4()),
        flow_execution_id: FlowExecutionId(Uuid::new_v4()),
        node_execution_id: NodeExecutionId(Uuid::new_v4()),
    }
}

#[test]
fn lifecycle_messages_roundtrip_the_full_correlation() {
    let request = TasksRequestPayload::StartScript {
        correlation: lf_correlation(),
        source: "export function run() {}".into(),
        filename: "script.ts".into(),
        input: serde_json::json!({"x":1}),
        fuel_limit: 0,
        timeout_secs: 0,
        workspace_path: PathBuf::from("/tmp/flow-workspace"),
        node_token: None,
        node_bus: None,
    };
    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: TasksRequestPayload = serde_json::from_str(&encoded).unwrap();
    assert!(
        matches!(decoded, TasksRequestPayload::StartScript { correlation, .. } if !correlation.node_execution_id.0.is_nil())
    );
}

#[test]
fn script_node_completion_round_trips_the_generic_shape() {
    let completion = ScriptNodeCompletion {
        correlation: LifecycleCorrelation {
            node_kind: NodeKind::parse("ScriptNode").expect("static node kind is valid"),
            flow_id: FlowId(Uuid::new_v4()),
            flow_node_id: FlowNodeId(Uuid::new_v4()),
            flow_execution_id: FlowExecutionId(Uuid::new_v4()),
            node_execution_id: NodeExecutionId(Uuid::new_v4()),
        },
        outputs: ScriptNodeOutputs::new(BTreeMap::from([
            (String::from("summary"), serde_json::json!("complete")),
            (String::from("metrics"), serde_json::json!({"count": 2})),
        ])),
        errors: vec![String::from("script warning")],
        logs: serde_json::json!({"log": ["completed"]}),
    };
    let encoded = serde_json::to_value(&completion).unwrap();
    assert!(encoded.get("outputs").is_some());
    assert_eq!(encoded["outputs"]["summary"], serde_json::json!("complete"));
    assert_eq!(encoded["outputs"]["metrics"], serde_json::json!({"count": 2}));
    assert!(encoded.get("errors").is_some());
    assert!(encoded.get("logs").is_some());
    assert!(encoded.get("decisions").is_none());
    assert!(encoded.get("global_errors").is_none());
    assert!(encoded.get("triggered_at").is_none());
    assert_eq!(
        serde_json::from_value::<ScriptNodeCompletion>(encoded)
            .unwrap()
            .outputs,
        completion.outputs
    );
}

use super::*;
use async_trait::async_trait;
use aves::flow::{FlowExecutionId, FlowId, FlowNodeId, NodeKind};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::time::Instant;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::super::types::{
    BoolAnswer, BoolNodeConfig, BoolQuestion, ChoiceAnswer, ChoiceNodeConfig, ChoiceQuestion, ScoreAnswer,
    ScoreNodeConfig, ScoreQuestion,
};

#[derive(Clone)]
enum Step {
    Answer,
    Mismatch,
    Error(DecideError),
    Wait,
}

struct Script {
    steps: Mutex<VecDeque<Step>>,
    seen: Mutex<Vec<(Option<Url>, Duration, bool, Instant)>>,
    released: Semaphore,
    dropped: AtomicUsize,
}

static SCRIPTS: LazyLock<Mutex<HashMap<String, Arc<Script>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Mock;
static MOCK: Mock = Mock;

struct DropGuard(Arc<Script>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DecidePlugin for Mock {
    async fn ask(
        &self,
        request: &DecideAskRequest,
        credential: Option<&DecideCredential>,
    ) -> Result<DecideOutcome, DecideError> {
        let script = SCRIPTS.lock().unwrap().get(&request.model).unwrap().clone();
        script.seen.lock().unwrap().push((
            request.endpoint.clone(),
            request.timeout,
            credential.is_some(),
            Instant::now(),
        ));
        let step = script
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Step::Answer);
        match step {
            Step::Error(error) => Err(error),
            Step::Mismatch => Ok(outcome(&request.model, &DecideQuestion::Bool(bool_question()))),
            Step::Wait => {
                let _guard = DropGuard(script.clone());
                script.released.acquire().await.unwrap().forget();
                Ok(outcome(&request.model, &request.question))
            }
            Step::Answer => Ok(outcome(&request.model, &request.question)),
        }
    }
}

fn lookup(name: &str) -> Result<&'static dyn DecidePlugin, DecideError> {
    if name == "mock" {
        Ok(&MOCK)
    } else {
        Err(DecideError::UnknownProvider { provider: name.into() })
    }
}

fn script(steps: Vec<Step>) -> (String, Arc<Script>) {
    let model = format!("mock-{}", NEXT.fetch_add(1, Ordering::SeqCst));
    let state = Arc::new(Script {
        steps: Mutex::new(steps.into()),
        seen: Mutex::new(Vec::new()),
        released: Semaphore::new(0),
        dropped: AtomicUsize::new(0),
    });
    SCRIPTS.lock().unwrap().insert(model.clone(), state.clone());
    (model, state)
}

fn bool_question() -> BoolQuestion {
    BoolQuestion {
        instructions: None,
        true_description: None,
        false_description: None,
    }
}
fn choice_question() -> ChoiceQuestion {
    ChoiceQuestion {
        instructions: None,
        criteria: BTreeMap::from([("yes".into(), None)]),
    }
}
fn score_question() -> ScoreQuestion {
    ScoreQuestion {
        instructions: None,
        criteria: vec![DecideContent::Text("low".into())],
    }
}
fn outcome(model: &str, question: &DecideQuestion) -> DecideOutcome {
    let answer = match question {
        DecideQuestion::Bool(_) => DecideAnswer::Bool(BoolAnswer { probability: 0.7 }),
        DecideQuestion::Choice(_) => DecideAnswer::Choice(ChoiceAnswer {
            choice: "yes".into(),
            confidence: 0.8,
            probabilities: BTreeMap::from([("yes".into(), 1.0)]),
        }),
        DecideQuestion::Score(_) => DecideAnswer::Score(ScoreAnswer {
            score: 0.0,
            confidence: 0.9,
            legend: BTreeMap::from([("0".into(), DecideContent::Text("low".into()))]),
            probabilities: BTreeMap::from([("0".into(), 1.0)]),
        }),
    };
    DecideOutcome {
        model: model.into(),
        answer,
    }
}

fn ask(
    model: &str,
    variant: Variant,
    credential_id: Option<SpectaUuid>,
    endpoint: Option<&str>,
    timeout_secs: Option<u64>,
) -> DecideRequestPayload {
    let provider = "mock".into();
    let model = model.into();
    let state = DecideContent::Text("state".into());
    let endpoint = endpoint.map(str::to_owned);
    match variant {
        Variant::Bool => DecideRequestPayload::AskBool {
            provider,
            model,
            state,
            question: bool_question(),
            credential_id,
            endpoint,
            timeout_secs,
        },
        Variant::Choice => DecideRequestPayload::AskChoice {
            provider,
            model,
            state,
            question: choice_question(),
            credential_id,
            endpoint,
            timeout_secs,
        },
        Variant::Score => DecideRequestPayload::AskScore {
            provider,
            model,
            state,
            question: score_question(),
            credential_id,
            endpoint,
            timeout_secs,
        },
    }
}

fn config(model: &str, variant: Variant) -> DecideNodeConfig {
    let provider = "mock".into();
    let model = model.into();
    let state = DecideContent::Text("state".into());
    match variant {
        Variant::Bool => DecideNodeConfig::Bool(BoolNodeConfig {
            provider,
            model,
            state,
            question: bool_question(),
            credential_id: None,
            endpoint: None,
            timeout_secs: 0,
        }),
        Variant::Choice => DecideNodeConfig::Choice(ChoiceNodeConfig {
            provider,
            model,
            state,
            question: choice_question(),
            credential_id: None,
            endpoint: None,
            timeout_secs: 0,
        }),
        Variant::Score => DecideNodeConfig::Score(ScoreNodeConfig {
            provider,
            model,
            state,
            question: score_question(),
            credential_id: None,
            endpoint: None,
            timeout_secs: 0,
        }),
    }
}

fn correlation(variant: Variant) -> LifecycleCorrelation {
    LifecycleCorrelation {
        node_kind: NodeKind::parse(variant.kind()).unwrap(),
        flow_id: FlowId(Uuid::new_v4()),
        flow_node_id: FlowNodeId(Uuid::new_v4()),
        flow_execution_id: FlowExecutionId(Uuid::new_v4()),
        node_execution_id: NodeExecutionId(Uuid::new_v4()),
    }
}
fn credential(endpoint: &str, key: &str) -> DecideCredential {
    DecideCredential {
        api_key: Zeroizing::new(key.into()),
        endpoint: Url::parse(endpoint).unwrap(),
    }
}
fn actor() -> mpsc::Sender<DecideRequest> {
    let (tx, rx) = mpsc::channel(128);
    let (reply_to, _reply) = oneshot::channel();
    assert!(
        tx.try_send(DecideRequest {
            payload: DecideRequestPayload::StartServer,
            reply_to,
        })
        .is_ok()
    );
    tokio::spawn(run_actor(Actor::new(rx, lookup)));
    tx
}
async fn dispatch(
    tx: &mpsc::Sender<DecideRequest>,
    payload: DecideRequestPayload,
) -> oneshot::Receiver<Result<DecideResponse, DecideError>> {
    let (reply_to, receiver) = oneshot::channel();
    tx.send(DecideRequest { payload, reply_to })
        .await
        .expect("actor alive");
    receiver
}
async fn send(tx: &mpsc::Sender<DecideRequest>, payload: DecideRequestPayload) -> Result<DecideResponse, DecideError> {
    dispatch(tx, payload).await.await.expect("reply delivered")
}
async fn wait_hits(script: &Script, count: usize) {
    for _ in 0..1000 {
        if script.seen.lock().unwrap().len() >= count {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("mock did not reach {count} calls");
}
async fn tick(
    tx: &mpsc::Sender<DecideRequest>,
    correlation: &LifecycleCorrelation,
) -> Result<DecideResponse, DecideError> {
    send(
        tx,
        DecideRequestPayload::TickNode {
            correlation: correlation.clone(),
        },
    )
    .await
}
async fn start(
    tx: &mpsc::Sender<DecideRequest>,
    correlation: &LifecycleCorrelation,
    config: DecideNodeConfig,
) -> Result<DecideResponse, DecideError> {
    send(
        tx,
        DecideRequestPayload::StartNode {
            correlation: correlation.clone(),
            config,
        },
    )
    .await
}

#[tokio::test]
async fn asks_all_types_and_validates_timeouts() {
    let tx = actor();
    let (model, script) = script(vec![]);
    for (variant, timeout) in [
        (Variant::Bool, None),
        (Variant::Choice, Some(0)),
        (Variant::Score, Some(3600)),
    ] {
        let response = send(&tx, ask(&model, variant, None, None, timeout))
            .await
            .unwrap();
        assert!(matches!(
            (variant, response),
            (Variant::Bool, DecideResponse::Bool { .. })
                | (Variant::Choice, DecideResponse::Choice { .. })
                | (Variant::Score, DecideResponse::Score { .. })
        ));
    }
    let seen = script.seen.lock().unwrap();
    assert_eq!(seen.iter().map(|s| s.1.as_secs()).collect::<Vec<_>>(), [300, 300, 3600]);
    drop(seen);
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, None, None, Some(3601))).await,
        Err(DecideError::TimeoutInvalid {
            value: 3601,
            maximum: 3600
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn retries_only_retryable_and_limits_total_attempts() {
    let tx = actor();
    let retry = DecideError::ProviderRetryable {
        retry_after_seconds: None,
        message: "retry".into(),
    };
    let (model, state) = script(vec![
        Step::Error(retry.clone()),
        Step::Error(retry.clone()),
        Step::Error(retry),
    ]);
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, None, None, None)).await,
        Err(DecideError::ProviderRetryable { .. })
    ));
    assert_eq!(state.seen.lock().unwrap().len(), 3);
    for error in [
        DecideError::ProviderRequestFailed {
            message: "failed".into(),
        },
        DecideError::ProviderResponseInvalid {
            message: "invalid".into(),
        },
    ] {
        let code = error.rpc_code();
        let (model, state) = script(vec![Step::Error(error)]);
        assert_eq!(
            send(&tx, ask(&model, Variant::Bool, None, None, None))
                .await
                .unwrap_err()
                .rpc_code(),
            code
        );
        assert_eq!(state.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_is_honored_and_capped() {
    let tx = actor();
    for seconds in [30, 300] {
        let (model, state) = script(vec![
            Step::Error(DecideError::ProviderRetryable {
                retry_after_seconds: Some(seconds),
                message: "retry".into(),
            }),
            Step::Answer,
        ]);
        let reply = dispatch(&tx, ask(&model, Variant::Bool, None, None, None)).await;
        wait_hits(&state, 1).await;
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(seconds.min(60) - 1)).await;
        tokio::task::yield_now().await;
        assert_eq!(state.seen.lock().unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(reply.await.unwrap(), Ok(DecideResponse::Bool { .. })));
        let seen = state.seen.lock().unwrap();
        let elapsed = seen[1].3.duration_since(seen[0].3);
        assert!(elapsed >= Duration::from_secs(seconds.min(60)));
        assert!(elapsed <= Duration::from_secs(120));
    }
}

#[tokio::test]
async fn capacity_is_bounded_and_caller_drop_cancels() {
    let tx = actor();
    let (model, state) = script(vec![Step::Wait; 32]);
    let mut replies = Vec::new();
    for _ in 0..32 {
        replies.push(dispatch(&tx, ask(&model, Variant::Bool, None, None, None)).await);
    }
    wait_hits(&state, 32).await;
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, None, None, None)).await,
        Err(DecideError::Busy)
    ));
    let corr = correlation(Variant::Bool);
    assert!(matches!(
        start(&tx, &corr, config(&model, Variant::Bool)).await,
        Err(DecideError::Busy)
    ));
    drop(replies.pop());
    for _ in 0..1000 {
        if state.dropped.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.dropped.load(Ordering::SeqCst), 1);
    drop(replies);
}

#[tokio::test]
async fn credentials_endpoint_binding_and_keyless_overrides() {
    let tx = actor();
    let (model, state) = script(vec![]);
    let id = SpectaUuid(Uuid::new_v4());
    for cred in [
        credential("https://example.test", "  "),
        credential("http://example.test", "key"),
    ] {
        assert!(matches!(
            send(
                &tx,
                DecideRequestPayload::SupplyCredential {
                    credential_id: id,
                    credential: cred
                }
            )
            .await,
            Err(DecideError::Validation { .. })
        ));
    }
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, Some(id), None, None)).await,
        Err(DecideError::CredentialUnavailable { .. })
    ));
    assert!(matches!(
        send(
            &tx,
            DecideRequestPayload::SupplyCredential {
                credential_id: id,
                credential: credential("http://127.0.0.1:8080", "key")
            }
        )
        .await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(
            &tx,
            DecideRequestPayload::SupplyCredential {
                credential_id: id,
                credential: credential("https://example.test/", "key")
            }
        )
        .await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(
            &tx,
            ask(&model, Variant::Bool, Some(id), Some("https://other.test/"), None)
        )
        .await,
        Err(DecideError::EndpointUnauthorized)
    ));
    assert!(state.seen.lock().unwrap().is_empty());
    assert!(
        send(
            &tx,
            ask(&model, Variant::Bool, Some(id), Some("HTTPS://EXAMPLE.TEST:443"), None)
        )
        .await
        .is_ok()
    );
    assert!(
        send(
            &tx,
            ask(&model, Variant::Bool, None, Some("http://127.0.0.1:8080"), None)
        )
        .await
        .is_ok()
    );
    assert!(
        send(&tx, ask(&model, Variant::Bool, None, None, None))
            .await
            .is_ok()
    );
    let seen = state.seen.lock().unwrap();
    assert_eq!(seen[0].0, Some(Url::parse("https://example.test/").unwrap()));
    assert!(seen[0].2);
    assert_eq!(seen[1].0, Some(Url::parse("http://127.0.0.1:8080").unwrap()));
    assert!(!seen[1].2);
    assert!(seen[2].0.is_none());
    assert!(!seen[2].2);
    drop(seen);
    assert!(matches!(
        send(&tx, DecideRequestPayload::RemoveCredential { credential_id: id }).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, Some(id), None, None)).await,
        Err(DecideError::CredentialUnavailable { .. })
    ));
}

#[tokio::test]
async fn node_start_uses_same_credential_endpoint_policy() {
    let tx = actor();
    let (model, state) = script(vec![Step::Wait]);
    let id = SpectaUuid(Uuid::new_v4());
    assert!(
        send(
            &tx,
            DecideRequestPayload::SupplyCredential {
                credential_id: id,
                credential: credential("https://example.test/", "key")
            }
        )
        .await
        .is_ok()
    );
    let corr = correlation(Variant::Bool);
    let mut node = match config(&model, Variant::Bool) {
        DecideNodeConfig::Bool(node) => node,
        _ => unreachable!(),
    };
    node.credential_id = Some(id);
    node.endpoint = Some("https://other.test/".into());
    assert!(matches!(
        start(&tx, &corr, DecideNodeConfig::Bool(node.clone())).await,
        Err(DecideError::EndpointUnauthorized)
    ));
    assert!(state.seen.lock().unwrap().is_empty());
    node.endpoint = Some("HTTPS://EXAMPLE.TEST:443".into());
    assert!(
        start(&tx, &corr, DecideNodeConfig::Bool(node))
            .await
            .is_ok()
    );
    wait_hits(&state, 1).await;
    assert_eq!(
        state.seen.lock().unwrap()[0].0,
        Some(Url::parse("https://example.test/").unwrap())
    );
    assert!(
        send(&tx, DecideRequestPayload::RemoveCredential { credential_id: id })
            .await
            .is_ok()
    );
    state.released.add_permits(1);
    loop {
        match tick(&tx, &corr).await.unwrap() {
            DecideResponse::NodeTicked {
                result: DecideNodeTickResult::Running,
                ..
            } => tokio::task::yield_now().await,
            DecideResponse::NodeTicked {
                result: DecideNodeTickResult::Succeeded { .. },
                ..
            } => break,
            other => panic!("unexpected response: {other:?}"),
        }
    }
}

#[tokio::test]
async fn invalid_question_provider_and_answer_are_terminal() {
    let tx = actor();
    let (model, state) = script(vec![Step::Mismatch]);
    let mut request = ask(&model, Variant::Choice, None, None, None);
    if let DecideRequestPayload::AskChoice { question, .. } = &mut request {
        question.criteria.clear();
    }
    assert!(matches!(
        send(&tx, request).await,
        Err(DecideError::InvalidQuestion { .. })
    ));
    let mut request = ask(&model, Variant::Bool, None, None, None);
    if let DecideRequestPayload::AskBool { provider, .. } = &mut request {
        *provider = "unknown".into();
    }
    assert!(matches!(
        send(&tx, request).await,
        Err(DecideError::UnknownProvider { .. })
    ));
    assert!(state.seen.lock().unwrap().is_empty());
    assert!(matches!(
        send(&tx, ask(&model, Variant::Choice, None, None, None)).await,
        Err(DecideError::ProviderResponseInvalid { .. })
    ));
    assert_eq!(state.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn shutdown_and_channel_close_cancel_pending_requests() {
    let tx = actor();
    let (model, state) = script(vec![Step::Wait]);
    let id = SpectaUuid(Uuid::new_v4());
    assert!(matches!(
        send(
            &tx,
            DecideRequestPayload::SupplyCredential {
                credential_id: id,
                credential: credential("https://example.test", "key"),
            }
        )
        .await,
        Ok(DecideResponse::Success)
    ));
    let pending = dispatch(&tx, ask(&model, Variant::Bool, None, None, None)).await;
    wait_hits(&state, 1).await;
    assert!(matches!(
        send(&tx, DecideRequestPayload::StopServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(pending.await.unwrap(), Err(DecideError::ShuttingDown)));
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, None, None, None)).await,
        Err(DecideError::ShuttingDown)
    ));
    let other_id = SpectaUuid(Uuid::new_v4());
    assert!(matches!(
        send(
            &tx,
            DecideRequestPayload::SupplyCredential {
                credential_id: other_id,
                credential: credential("https://example.test", "key"),
            }
        )
        .await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(
            &tx,
            DecideRequestPayload::RemoveCredential {
                credential_id: other_id
            }
        )
        .await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        start(&tx, &correlation(Variant::Bool), config(&model, Variant::Bool)).await,
        Err(DecideError::ShuttingDown)
    ));
    assert!(matches!(
        send(&tx, DecideRequestPayload::StopServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(&tx, DecideRequestPayload::StartServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(&tx, DecideRequestPayload::StartServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, Some(id), None, None)).await,
        Ok(DecideResponse::Bool { .. })
    ));
    assert!(state.seen.lock().unwrap()[1].2);
    let tx = actor();
    let (model, state) = script(vec![Step::Wait]);
    let pending = dispatch(&tx, ask(&model, Variant::Bool, None, None, None)).await;
    wait_hits(&state, 1).await;
    drop(tx);
    assert!(matches!(pending.await.unwrap(), Err(DecideError::ShuttingDown)));
}

#[tokio::test]
async fn starts_stopped_and_stop_cancels_nodes() {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(run_actor(Actor::new(rx, lookup)));
    let (model, state) = script(vec![Step::Wait]);
    let corr = correlation(Variant::Bool);
    assert!(matches!(
        send(&tx, ask(&model, Variant::Bool, None, None, None)).await,
        Err(DecideError::ShuttingDown)
    ));
    assert!(matches!(
        start(&tx, &corr, config(&model, Variant::Bool)).await,
        Err(DecideError::ShuttingDown)
    ));
    assert!(matches!(
        send(&tx, DecideRequestPayload::StartServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        start(&tx, &corr, config(&model, Variant::Bool)).await,
        Ok(DecideResponse::NodeStarted { .. })
    ));
    wait_hits(&state, 1).await;
    assert!(matches!(
        send(&tx, DecideRequestPayload::StopServer).await,
        Ok(DecideResponse::Success)
    ));
    assert!(matches!(
        tick(&tx, &corr).await,
        Err(DecideError::NodeCompletionNotFound { .. })
    ));
    for _ in 0..1000 {
        if state.dropped.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn nodes_run_and_project_each_kind() {
    let tx = actor();
    for variant in [Variant::Bool, Variant::Choice, Variant::Score] {
        let (model, state) = script(vec![Step::Wait]);
        let corr = correlation(variant);
        assert!(matches!(
            start(&tx, &corr, config(&model, variant)).await,
            Ok(DecideResponse::NodeStarted { .. })
        ));
        wait_hits(&state, 1).await;
        assert!(matches!(
            tick(&tx, &corr).await,
            Ok(DecideResponse::NodeTicked {
                result: DecideNodeTickResult::Running,
                ..
            })
        ));
        state.released.add_permits(1);
        let output = loop {
            match tick(&tx, &corr).await.unwrap() {
                DecideResponse::NodeTicked {
                    result: DecideNodeTickResult::Succeeded { output },
                    ..
                } => break output,
                DecideResponse::NodeTicked {
                    result: DecideNodeTickResult::Running,
                    ..
                } => tokio::task::yield_now().await,
                other => panic!("unexpected response: {other:?}"),
            }
        };
        let expected = match variant {
            Variant::Bool => serde_json::json!({"probability":0.7,"model":model}),
            Variant::Choice => {
                serde_json::json!({"choice":"yes","confidence":0.8,"probabilities":{"yes":1.0},"model":model})
            }
            Variant::Score => {
                serde_json::json!({"score":0.0,"confidence":0.9,"legend":{"0":"low"},"probabilities":{"0":1.0},"model":model})
            }
        };
        assert_eq!(output, expected);
        assert!(matches!(
            tick(&tx, &corr).await,
            Err(DecideError::NodeCompletionNotFound { .. })
        ));
    }
}

#[tokio::test]
async fn nodes_validate_correlations_idempotency_and_cancellation() {
    let tx = actor();
    let (model, state) = script(vec![Step::Wait]);
    let corr = correlation(Variant::Bool);
    assert!(matches!(
        start(&tx, &corr, config(&model, Variant::Choice)).await,
        Err(DecideError::Validation { .. })
    ));
    assert!(
        start(&tx, &corr, config(&model, Variant::Bool))
            .await
            .is_ok()
    );
    wait_hits(&state, 1).await;
    assert!(matches!(
        start(&tx, &corr, config(&model, Variant::Bool)).await,
        Err(DecideError::NodeIdempotencyConflict { .. })
    ));
    let mut wrong = corr.clone();
    wrong.flow_id = FlowId(Uuid::new_v4());
    assert!(matches!(tick(&tx, &wrong).await, Err(DecideError::Validation { .. })));
    for _ in 0..2 {
        assert!(matches!(
            send(
                &tx,
                DecideRequestPayload::CancelNode {
                    node_execution_id: corr.node_execution_id
                }
            )
            .await,
            Ok(DecideResponse::Success)
        ));
    }
    assert!(matches!(
        tick(&tx, &corr).await,
        Err(DecideError::NodeCompletionNotFound { .. })
    ));
    for _ in 0..1000 {
        if state.dropped.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn nodes_report_mismatch_errors_and_dropped_completion() {
    let tx = actor();
    let (model, state) = script(vec![Step::Mismatch]);
    let corr = correlation(Variant::Choice);
    assert!(
        start(&tx, &corr, config(&model, Variant::Choice))
            .await
            .is_ok()
    );
    wait_hits(&state, 1).await;
    loop {
        match tick(&tx, &corr).await.unwrap() {
            DecideResponse::NodeTicked {
                result: DecideNodeTickResult::Running,
                ..
            } => tokio::task::yield_now().await,
            DecideResponse::NodeTicked {
                result:
                    DecideNodeTickResult::Failed {
                        error: DecideError::ProviderResponseInvalid { .. },
                    },
                ..
            } => break,
            other => panic!("unexpected response: {other:?}"),
        }
    }
    let (model, state) = script(vec![Step::Error(DecideError::ProviderRequestFailed {
        message: "failed".into(),
    })]);
    let corr = correlation(Variant::Bool);
    assert!(
        start(&tx, &corr, config(&model, Variant::Bool))
            .await
            .is_ok()
    );
    wait_hits(&state, 1).await;
    loop {
        match tick(&tx, &corr).await.unwrap() {
            DecideResponse::NodeTicked {
                result: DecideNodeTickResult::Running,
                ..
            } => tokio::task::yield_now().await,
            DecideResponse::NodeTicked {
                result:
                    DecideNodeTickResult::Failed {
                        error: DecideError::ProviderRequestFailed { .. },
                    },
                ..
            } => break,
            other => panic!("unexpected response: {other:?}"),
        }
    }
    // Install a closed completion through the actor's private state; no production seam is widened.
    let mut actor = Actor::new(mpsc::channel(1).1, lookup);
    let corr = correlation(Variant::Bool);
    let (sender, receiver) = oneshot::channel();
    drop(sender);
    actor.completions.insert(
        corr.node_execution_id,
        CompletionEntry {
            receiver,
            token: CancellationToken::new(),
            correlation: corr.clone(),
            variant: Variant::Bool,
        },
    );
    assert!(matches!(actor.tick(corr), Err(DecideError::NodeCompletionDropped)));
    assert!(actor.completions.is_empty());
}

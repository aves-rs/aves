use std::{collections::BTreeMap, sync::Arc};

use super::actor::{DECIDE_BOOL_NODE_KIND, DECIDE_CHOICE_NODE_KIND, DECIDE_SCORE_NODE_KIND};
use super::domain::DecideNodeLifecycleService;
use super::messages::{CancelNodeInput, StartNodeInput, TickNodeInput};
use super::types::{
    BoolNodeConfig, ChoiceNodeConfig, DecideContent, DecideError, DecideNodeConfig, DecideNodeTickResult,
    ScoreNodeConfig,
};
use aves::flow::{
    BoundOutputs, CancellationDisposition, FlowRegistrationError, LifecycleCorrelation, NodeExecutionId, NodeKind,
    NodeKindDescriptor, NodeKindRegistration, NodeLifecycleContext, NodeLifecycleDispatchError, NodeLifecycleResult,
    SideEffectDisposition, WorkspacePreparation,
};
use serde_json::Value;

struct DecideNodeOwnerState {
    lifecycle: Arc<dyn DecideNodeLifecycleService>,
}

#[cfg(test)]
mod tests {
    use super::super::messages::{NodeStartedOutput, NodeTickedOutput, SuccessOutput};
    use super::*;
    use aves::flow::{FlowExecutionId, FlowId, FlowNodeId};
    use serde_json::{Value, json};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeService {
        starts: Mutex<Vec<(LifecycleCorrelation, DecideNodeConfig)>>,
        cancelled: Mutex<Vec<NodeExecutionId>>,
        start_error: Mutex<Option<DecideError>>,
        tick_result: Mutex<Option<DecideNodeTickResult>>,
    }

    #[async_trait::async_trait]
    impl DecideNodeLifecycleService for FakeService {
        async fn start_node(&self, input: StartNodeInput) -> Result<NodeStartedOutput, DecideError> {
            if let Some(error) = self.start_error.lock().unwrap().take() {
                return Err(error);
            }
            self.starts
                .lock()
                .unwrap()
                .push((input.correlation.clone(), input.config));
            Ok(NodeStartedOutput {
                correlation: input.correlation,
            })
        }

        async fn tick_node(&self, input: TickNodeInput) -> Result<NodeTickedOutput, DecideError> {
            let result = self
                .tick_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or(DecideNodeTickResult::Running);
            Ok(NodeTickedOutput {
                correlation: input.correlation,
                result,
            })
        }

        async fn cancel_node(&self, input: CancelNodeInput) -> Result<SuccessOutput, DecideError> {
            self.cancelled.lock().unwrap().push(input.node_execution_id);
            Ok(SuccessOutput {})
        }
    }

    #[test]
    fn registrations_decode_and_validate_each_kind() {
        let registrations = decide_node_registrations(Arc::new(FakeService::default()));
        let questions = [
            json!({}),
            json!({"criteria": {"one": null}}),
            json!({"criteria": ["low"]}),
        ];
        for (registration, question) in registrations.iter().zip(questions) {
            let config =
                json!({"credentialId": uuid::Uuid::nil(), "model": "model", "state": "state", "question": question});
            assert!(registration.decode_configuration(config.clone()).is_ok());
            let mut invalid = config.clone();
            invalid["unexpected"] = json!(true);
            assert!(registration.decode_configuration(invalid).is_err());
            for field in ["model"] {
                let mut invalid = config.clone();
                invalid[field] = json!("  ");
                assert!(registration.decode_configuration(invalid).is_err());
            }
            for field in ["provider", "endpoint"] {
                let mut invalid = config.clone();
                invalid[field] = json!("unexpected");
                assert!(registration.decode_configuration(invalid).is_err());
            }
            let mut missing = config.clone();
            missing.as_object_mut().unwrap().remove("credentialId");
            assert!(registration.decode_configuration(missing).is_err());
        }
        for criteria in [
            json!({}),
            json!({" ": null}),
            Value::Object((0..256).map(|i| (i.to_string(), Value::Null)).collect()),
        ] {
            assert!(
                registrations[1]
                    .decode_configuration(
                        json!({"credentialId": uuid::Uuid::nil(), "model":"m", "state":"s", "question":{"criteria": criteria}})
                    )
                    .is_err()
            );
        }
        assert!(
            registrations[2]
                .decode_configuration(
                    json!({"credentialId": uuid::Uuid::nil(), "model":"m", "state":"s", "question":{"criteria": vec!["level"; 11]}})
                )
                .is_err()
        );
    }

    #[test]
    fn failure_terminal_preserves_typed_error() {
        let error = DecideError::NodeCompletionDropped;
        let expected = serde_json::to_value(&error).unwrap();
        assert!(matches!(failed(error), Ok(NodeLifecycleResult::Failed { state, .. }) if state == expected));
    }

    #[test]
    fn owner_dispatch_error_preserves_typed_source() {
        let dispatch = owner_error(DecideError::ShuttingDown);
        assert!(matches!(dispatch, NodeLifecycleDispatchError::Owner(source)
            if matches!(source.downcast_ref::<DecideError>(), Some(DecideError::ShuttingDown))));
    }

    fn correlation(kind: &str) -> LifecycleCorrelation {
        LifecycleCorrelation {
            node_kind: NodeKind::parse(kind).unwrap(),
            flow_id: FlowId(uuid::Uuid::new_v4()),
            flow_node_id: FlowNodeId(uuid::Uuid::new_v4()),
            flow_execution_id: FlowExecutionId(uuid::Uuid::new_v4()),
            node_execution_id: NodeExecutionId(uuid::Uuid::new_v4()),
        }
    }

    fn config(kind: &str) -> DecideNodeConfig {
        let question = match kind {
            DECIDE_BOOL_NODE_KIND => json!({}),
            DECIDE_CHOICE_NODE_KIND => json!({"criteria": {"one": null}}),
            DECIDE_SCORE_NODE_KIND => json!({"criteria": ["low"]}),
            _ => unreachable!(),
        };
        let value = json!({"credentialId": uuid::Uuid::nil(), "model": "m", "state": "original", "question": question});
        match kind {
            DECIDE_BOOL_NODE_KIND => DecideNodeConfig::Bool(serde_json::from_value(value).unwrap()),
            DECIDE_CHOICE_NODE_KIND => DecideNodeConfig::Choice(serde_json::from_value(value).unwrap()),
            DECIDE_SCORE_NODE_KIND => DecideNodeConfig::Score(serde_json::from_value(value).unwrap()),
            _ => unreachable!(),
        }
    }

    fn assert_typed_validation(result: Result<NodeLifecycleResult, NodeLifecycleDispatchError>) {
        assert!(matches!(result, Err(NodeLifecycleDispatchError::Owner(source))
            if matches!(source.downcast_ref::<DecideError>(), Some(DecideError::Validation { .. }))));
    }

    async fn exercise_lifecycle(kind: &str, field: &str, output: Value) {
        let service = FakeService::default();
        let correlation = correlation(kind);
        let outputs = vec![field.to_owned(), "model".to_owned()];
        let inputs = BTreeMap::from([("state".to_owned(), json!({"override": true}))]);
        assert!(matches!(
            start_with(&service, correlation.clone(), &inputs, &outputs, config(kind)).await,
            Ok(NodeLifecycleResult::Running)
        ));
        {
            let starts = service.starts.lock().unwrap();
            assert_eq!(starts.len(), 1);
            assert_eq!(starts[0].0, correlation);
            let state = match (&starts[0].1, kind) {
                (DecideNodeConfig::Bool(config), DECIDE_BOOL_NODE_KIND) => &config.state,
                (DecideNodeConfig::Choice(config), DECIDE_CHOICE_NODE_KIND) => &config.state,
                (DecideNodeConfig::Score(config), DECIDE_SCORE_NODE_KIND) => &config.state,
                _ => panic!("wrong configuration variant"),
            };
            assert_eq!(serde_json::to_value(state).unwrap(), json!({"override": true}));
        }
        for bad_inputs in [
            BTreeMap::from([("state".to_owned(), json!(42))]),
            BTreeMap::from([("unexpected".to_owned(), json!("value"))]),
        ] {
            assert_typed_validation(
                start_with(&service, correlation.clone(), &bad_inputs, &outputs, config(kind)).await,
            );
        }
        assert_typed_validation(
            start_with(
                &service,
                correlation.clone(),
                &BTreeMap::new(),
                &["unexpected".into()],
                config(kind),
            )
            .await,
        );
        assert_eq!(service.starts.lock().unwrap().len(), 1);
        *service.start_error.lock().unwrap() = Some(DecideError::ShuttingDown);
        assert!(
            matches!(start_with(&service, correlation.clone(), &BTreeMap::new(), &outputs, config(kind)).await,
            Err(NodeLifecycleDispatchError::Owner(source)) if matches!(source.downcast_ref::<DecideError>(), Some(DecideError::ShuttingDown)))
        );

        assert!(matches!(
            tick_with(&service, correlation.clone(), &outputs).await,
            Ok(NodeLifecycleResult::Running)
        ));
        *service.tick_result.lock().unwrap() = Some(DecideNodeTickResult::Succeeded { output: output.clone() });
        let expected = BoundOutputs::new(BTreeMap::from([
            (field.to_owned(), output[field].clone()),
            ("model".to_owned(), output["model"].clone()),
        ]));
        assert!(matches!(tick_with(&service, correlation.clone(), &outputs).await,
            Ok(NodeLifecycleResult::Succeeded { outputs, state }) if outputs == expected && state == output));
        let error = DecideError::NodeCompletionDropped;
        let expected_error = serde_json::to_value(&error).unwrap();
        *service.tick_result.lock().unwrap() = Some(DecideNodeTickResult::Failed { error });
        assert!(matches!(tick_with(&service, correlation.clone(), &outputs).await,
            Ok(NodeLifecycleResult::Failed { state, .. }) if state == expected_error));
        let expected_cancel = serde_json::to_value(DecideError::NodeCancelled {
            node_execution_id: correlation.node_execution_id,
        })
        .unwrap();
        assert!(matches!(cancel_with(&service, correlation.node_execution_id).await,
            Ok(NodeLifecycleResult::Failed { state, .. }) if state == expected_cancel));
        assert_eq!(*service.cancelled.lock().unwrap(), vec![correlation.node_execution_id]);
    }

    #[tokio::test]
    async fn bool_lifecycle_helpers() {
        exercise_lifecycle(
            DECIDE_BOOL_NODE_KIND,
            "probability",
            json!({"probability": 0.6, "model": "m", "extra": true}),
        )
        .await;
    }

    #[tokio::test]
    async fn choice_lifecycle_helpers() {
        exercise_lifecycle(
            DECIDE_CHOICE_NODE_KIND,
            "choice",
            json!({"choice": "one", "model": "m", "confidence": 0.8}),
        )
        .await;
    }

    #[tokio::test]
    async fn score_lifecycle_helpers() {
        exercise_lifecycle(
            DECIDE_SCORE_NODE_KIND,
            "score",
            json!({"score": 0.5, "model": "m", "legend": {}}),
        )
        .await;
    }
}

fn owner(context: &NodeLifecycleContext) -> Result<&DecideNodeOwnerState, NodeLifecycleDispatchError> {
    context
        .owner_state
        .as_ref()
        .and_then(|state| state.downcast_ref())
        .ok_or_else(|| {
            owner_error(DecideError::Validation {
                message: "Decide node owner state unavailable".into(),
            })
        })
}

fn owner_error(error: DecideError) -> NodeLifecycleDispatchError {
    NodeLifecycleDispatchError::Owner(Box::new(error))
}

fn validate_names(model: &str) -> Result<(), FlowRegistrationError> {
    if model.trim().is_empty() {
        return Err(FlowRegistrationError::Configuration {
            message: "model must not be empty".into(),
        });
    }
    Ok(())
}

fn validate_question(result: Result<(), DecideError>) -> Result<(), FlowRegistrationError> {
    result.map_err(|error| FlowRegistrationError::Configuration {
        message: error.to_string(),
    })
}

async fn start_with(
    service: &dyn DecideNodeLifecycleService,
    correlation: LifecycleCorrelation,
    inputs: &BTreeMap<String, Value>,
    outputs: &[String],
    mut config: DecideNodeConfig,
) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    let (state, allowed): (&mut DecideContent, &[&str]) = match &mut config {
        DecideNodeConfig::Bool(config) => (&mut config.state, &["probability", "model"]),
        DecideNodeConfig::Choice(config) => (&mut config.state, &["choice", "confidence", "probabilities", "model"]),
        DecideNodeConfig::Score(config) => (
            &mut config.state,
            &["score", "confidence", "legend", "probabilities", "model"],
        ),
    };
    for (name, value) in inputs {
        if name != "state" {
            return Err(owner_error(DecideError::Validation {
                message: format!("unknown input binding: {name}"),
            }));
        }
        *state = serde_json::from_value(value.clone()).map_err(|error| owner_error(DecideError::from(error)))?;
    }
    for name in outputs {
        if !allowed.contains(&name.as_str()) {
            return Err(owner_error(DecideError::Validation {
                message: format!("unknown output binding: {name}"),
            }));
        }
    }
    service
        .start_node(StartNodeInput { correlation, config })
        .await
        .map_err(owner_error)?;
    Ok(NodeLifecycleResult::Running)
}

async fn start(
    context: NodeLifecycleContext,
    config: DecideNodeConfig,
) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    let inputs = context
        .inputs
        .as_value()
        .as_object()
        .expect("bound inputs are an object")
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let outputs = output_names(&context);
    start_with(
        owner(&context)?.lifecycle.as_ref(),
        context.correlation.clone(),
        &inputs,
        &outputs,
        config,
    )
    .await
}

fn output_names(context: &NodeLifecycleContext) -> Vec<String> {
    context
        .node
        .outputs
        .iter()
        .map(|binding| binding.name.clone())
        .collect()
}

fn failed(error: DecideError) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    let message = error.to_string();
    let state = serde_json::to_value(&error).map_err(|error| owner_error(error.into()))?;
    Ok(NodeLifecycleResult::Failed { message, state })
}

async fn tick(context: NodeLifecycleContext) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    tick_with(
        owner(&context)?.lifecycle.as_ref(),
        context.correlation.clone(),
        &output_names(&context),
    )
    .await
}

async fn tick_with(
    service: &dyn DecideNodeLifecycleService,
    correlation: LifecycleCorrelation,
    outputs: &[String],
) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    let result = service
        .tick_node(TickNodeInput { correlation })
        .await
        .map_err(owner_error)?
        .result;
    match result {
        DecideNodeTickResult::Running => Ok(NodeLifecycleResult::Running),
        DecideNodeTickResult::Failed { error } => failed(error),
        DecideNodeTickResult::Succeeded { output } => {
            let object = output.as_object().ok_or_else(|| {
                owner_error(DecideError::ProviderResponseInvalid {
                    message: "node output must be an object".into(),
                })
            })?;
            let values = outputs
                .iter()
                .map(|name| {
                    let value = object.get(name).cloned().ok_or_else(|| {
                        owner_error(DecideError::ProviderResponseInvalid {
                            message: format!("missing node output: {name}"),
                        })
                    })?;
                    Ok((name.clone(), value))
                })
                .collect::<Result<_, NodeLifecycleDispatchError>>()?;
            Ok(NodeLifecycleResult::Succeeded {
                outputs: BoundOutputs::new(values),
                state: output,
            })
        }
    }
}

async fn cancel(context: NodeLifecycleContext) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    cancel_with(
        owner(&context)?.lifecycle.as_ref(),
        context.correlation.node_execution_id,
    )
    .await
}

async fn cancel_with(
    service: &dyn DecideNodeLifecycleService,
    node_execution_id: NodeExecutionId,
) -> Result<NodeLifecycleResult, NodeLifecycleDispatchError> {
    service
        .cancel_node(CancelNodeInput { node_execution_id })
        .await
        .map_err(owner_error)?;
    failed(DecideError::NodeCancelled { node_execution_id })
}

fn descriptor(kind: &str) -> NodeKindDescriptor {
    NodeKindDescriptor {
        kind: NodeKind::parse(kind).expect("static node kind is valid"),
        cancellation: CancellationDisposition::Cancellable,
        side_effects: SideEffectDisposition::Pure,
    }
}

pub(super) fn decide_node_registrations(service: Arc<dyn DecideNodeLifecycleService>) -> Vec<NodeKindRegistration> {
    let owner = Arc::new(DecideNodeOwnerState { lifecycle: service });
    vec![
        NodeKindRegistration::typed::<BoolNodeConfig, _, _, _, _, _, _, _>(
            descriptor(DECIDE_BOOL_NODE_KIND),
            |config| {
                validate_names(&config.model)?;
                validate_question(config.question.validate())
            },
            |context, config| start(context, DecideNodeConfig::Bool(config)),
            |context, _| tick(context),
            |context, _| cancel(context),
            Some(owner.clone()),
            WorkspacePreparation::new(),
        ),
        NodeKindRegistration::typed::<ChoiceNodeConfig, _, _, _, _, _, _, _>(
            descriptor(DECIDE_CHOICE_NODE_KIND),
            |config| {
                validate_names(&config.model)?;
                validate_question(config.question.validate())
            },
            |context, config| start(context, DecideNodeConfig::Choice(config)),
            |context, _| tick(context),
            |context, _| cancel(context),
            Some(owner.clone()),
            WorkspacePreparation::new(),
        ),
        NodeKindRegistration::typed::<ScoreNodeConfig, _, _, _, _, _, _, _>(
            descriptor(DECIDE_SCORE_NODE_KIND),
            |config| {
                validate_names(&config.model)?;
                validate_question(config.question.validate())
            },
            |context, config| start(context, DecideNodeConfig::Score(config)),
            |context, _| tick(context),
            |context, _| cancel(context),
            Some(owner),
            WorkspacePreparation::new(),
        ),
    ]
}

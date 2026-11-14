//! Volatile decision execution and credential custody.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use aves::{
    flow::{LifecycleCorrelation, NodeExecutionId},
    utils::{
        retry::{RetryBackoff, retry_with_backoff},
        uuid::SpectaUuid,
    },
};
use reqwest::Url;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    messages::{DecideRequest, DecideRequestPayload, DecideResponse},
    plugins::DecidePlugin,
    types::{
        DecideAnswer, DecideAskRequest, DecideContent, DecideCredential, DecideError, DecideNodeConfig,
        DecideNodeTickResult, DecideOutcome, DecideQuestion, canonical_endpoint, validate_credentialed_endpoint,
        validate_keyless_endpoint,
    },
};

pub(super) const DECIDE_BOOL_NODE_KIND: &str = "DecideBoolNode";
pub(super) const DECIDE_CHOICE_NODE_KIND: &str = "DecideChoiceNode";
pub(super) const DECIDE_SCORE_NODE_KIND: &str = "DecideScoreNode";
pub(super) type ProviderLookup = fn(&str) -> Result<&'static dyn DecidePlugin, DecideError>;

#[derive(Clone, Copy)]
enum Variant {
    Bool,
    Choice,
    Score,
}

impl Variant {
    fn kind(self) -> &'static str {
        match self {
            Self::Bool => DECIDE_BOOL_NODE_KIND,
            Self::Choice => DECIDE_CHOICE_NODE_KIND,
            Self::Score => DECIDE_SCORE_NODE_KIND,
        }
    }
    fn response(self, outcome: DecideOutcome) -> Result<DecideResponse, DecideError> {
        match (self, outcome.answer) {
            (Self::Bool, DecideAnswer::Bool(answer)) => Ok(DecideResponse::Bool {
                answer,
                model: outcome.model,
            }),
            (Self::Choice, DecideAnswer::Choice(answer)) => Ok(DecideResponse::Choice {
                answer,
                model: outcome.model,
            }),
            (Self::Score, DecideAnswer::Score(answer)) => Ok(DecideResponse::Score {
                answer,
                model: outcome.model,
            }),
            _ => Err(DecideError::ProviderResponseInvalid {
                message: "Answer kind does not match question".into(),
            }),
        }
    }
    fn output(self, outcome: DecideOutcome) -> Result<serde_json::Value, DecideError> {
        match (self, outcome.answer) {
            (Self::Bool, DecideAnswer::Bool(answer)) => {
                Ok(serde_json::json!({"probability": answer.probability, "model": outcome.model}))
            }
            (Self::Choice, DecideAnswer::Choice(answer)) => Ok(
                serde_json::json!({"choice": answer.choice, "confidence": answer.confidence, "probabilities": answer.probabilities, "model": outcome.model}),
            ),
            (Self::Score, DecideAnswer::Score(answer)) => Ok(
                serde_json::json!({"score": answer.score, "confidence": answer.confidence, "legend": answer.legend, "probabilities": answer.probabilities, "model": outcome.model}),
            ),
            _ => Err(DecideError::ProviderResponseInvalid {
                message: "Answer kind does not match question".into(),
            }),
        }
    }
}

struct CompletionEntry {
    receiver: oneshot::Receiver<Result<DecideOutcome, DecideError>>,
    token: CancellationToken,
    correlation: LifecycleCorrelation,
    variant: Variant,
}

struct Input {
    provider: String,
    model: String,
    state: DecideContent,
    question: DecideQuestion,
    credential_id: Option<SpectaUuid>,
    endpoint: Option<String>,
    timeout_secs: Option<u64>,
}

impl Input {
    fn new(
        question: DecideQuestion,
        provider: String,
        model: String,
        state: DecideContent,
        credential_id: Option<SpectaUuid>,
        endpoint: Option<String>,
        timeout_secs: Option<u64>,
    ) -> Self {
        Self {
            provider,
            model,
            state,
            question,
            credential_id,
            endpoint,
            timeout_secs,
        }
    }

    fn variant(&self) -> Variant {
        match &self.question {
            DecideQuestion::Bool(_) => Variant::Bool,
            DecideQuestion::Choice(_) => Variant::Choice,
            DecideQuestion::Score(_) => Variant::Score,
        }
    }
}

struct QuestionContext {
    running: bool,
    lookup: ProviderLookup,
    credentials: HashMap<SpectaUuid, DecideCredential>,
    permits: Arc<Semaphore>,
    token: CancellationToken,
}

struct Prepared {
    plugin: &'static dyn DecidePlugin,
    request: DecideAskRequest,
    credential: Option<DecideCredential>,
    permit: OwnedSemaphorePermit,
}

pub(super) struct Actor {
    receiver: mpsc::Receiver<DecideRequest>,
    lookup: ProviderLookup,
    credentials: HashMap<SpectaUuid, DecideCredential>,
    completions: HashMap<NodeExecutionId, CompletionEntry>,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    running: bool,
}

impl Actor {
    pub(super) fn new(receiver: mpsc::Receiver<DecideRequest>, lookup: ProviderLookup) -> Self {
        Self {
            receiver,
            lookup,
            credentials: HashMap::new(),
            completions: HashMap::new(),
            permits: Arc::new(Semaphore::new(32)),
            shutdown: CancellationToken::new(),
            running: false,
        }
    }

    fn cleanup(&mut self) {
        let _ = self.stop_server();
        self.credentials.clear();
    }

    fn start_server(&mut self) -> Result<DecideResponse, DecideError> {
        self.running = true;
        if self.shutdown.is_cancelled() {
            self.shutdown = CancellationToken::new();
        }
        Ok(DecideResponse::Success)
    }

    fn stop_server(&mut self) -> Result<DecideResponse, DecideError> {
        self.running = false;
        self.shutdown.cancel();
        self.completions.clear();
        Ok(DecideResponse::Success)
    }

    fn gate(&self) -> Result<(), DecideError> {
        if !self.running {
            Err(DecideError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    fn question_context(&self) -> QuestionContext {
        QuestionContext {
            running: self.running,
            lookup: self.lookup,
            credentials: self.credentials.clone(),
            permits: self.permits.clone(),
            token: self.shutdown.child_token(),
        }
    }
}

impl QuestionContext {
    fn prepare(&self, input: Input) -> Result<Prepared, DecideError> {
        if !self.running {
            return Err(DecideError::ShuttingDown);
        }
        match &input.question {
            DecideQuestion::Bool(q) => q.validate()?,
            DecideQuestion::Choice(q) => q.validate()?,
            DecideQuestion::Score(q) => q.validate()?,
        }
        if input.provider.trim().is_empty() || input.model.trim().is_empty() {
            return Err(DecideError::Validation {
                message: "Provider and model must not be empty".into(),
            });
        }
        let timeout = match input.timeout_secs.unwrap_or(0) {
            0 => 300,
            value if value > 3600 => return Err(DecideError::TimeoutInvalid { value, maximum: 3600 }),
            value => value,
        };
        let plugin = (self.lookup)(&input.provider)?;
        let credential = input
            .credential_id
            .map(|id| {
                self.credentials
                    .get(&id)
                    .cloned()
                    .ok_or(DecideError::CredentialUnavailable { credential_id: id })
            })
            .transpose()?;
        let endpoint = resolve_endpoint(credential.as_ref(), input.endpoint.as_deref())?;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| DecideError::Busy)?;
        Ok(Prepared {
            plugin,
            request: DecideAskRequest {
                model: input.model,
                endpoint,
                timeout: Duration::from_secs(timeout),
                state: input.state,
                question: input.question,
            },
            credential,
            permit,
        })
    }
}

impl Actor {
    async fn handle_question(
        context: QuestionContext,
        payload: DecideRequestPayload,
        mut reply: oneshot::Sender<Result<DecideResponse, DecideError>>,
    ) {
        use DecideRequestPayload::*;
        let input = match payload {
            AskBool {
                provider,
                model,
                state,
                question,
                credential_id,
                endpoint,
                timeout_secs,
            } => Input::new(
                DecideQuestion::Bool(question),
                provider,
                model,
                state,
                credential_id,
                endpoint,
                timeout_secs,
            ),
            AskChoice {
                provider,
                model,
                state,
                question,
                credential_id,
                endpoint,
                timeout_secs,
            } => Input::new(
                DecideQuestion::Choice(question),
                provider,
                model,
                state,
                credential_id,
                endpoint,
                timeout_secs,
            ),
            AskScore {
                provider,
                model,
                state,
                question,
                credential_id,
                endpoint,
                timeout_secs,
            } => Input::new(
                DecideQuestion::Score(question),
                provider,
                model,
                state,
                credential_id,
                endpoint,
                timeout_secs,
            ),
            _ => {
                let _ = reply.send(Err(DecideError::Validation {
                    message: "Not a question request".into(),
                }));
                return;
            }
        };
        let variant = input.variant();
        let prepared = match context.prepare(input) {
            Ok(value) => value,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        tokio::select! {
            result = infer(prepared) => { let _ = reply.send(result.and_then(|outcome| variant.response(outcome))); }
            _ = context.token.cancelled() => { let _ = reply.send(Err(DecideError::ShuttingDown)); }
            _ = reply.closed() => {}
        }
    }

    fn start(
        &mut self,
        correlation: LifecycleCorrelation,
        config: DecideNodeConfig,
    ) -> Result<DecideResponse, DecideError> {
        self.gate()?;
        if self
            .completions
            .contains_key(&correlation.node_execution_id)
        {
            return Err(DecideError::NodeIdempotencyConflict {
                node_execution_id: correlation.node_execution_id,
            });
        }
        let input = match config {
            DecideNodeConfig::Bool(c) => Input::new(
                DecideQuestion::Bool(c.question),
                c.provider,
                c.model,
                c.state,
                c.credential_id,
                c.endpoint,
                Some(c.timeout_secs),
            ),
            DecideNodeConfig::Choice(c) => Input::new(
                DecideQuestion::Choice(c.question),
                c.provider,
                c.model,
                c.state,
                c.credential_id,
                c.endpoint,
                Some(c.timeout_secs),
            ),
            DecideNodeConfig::Score(c) => Input::new(
                DecideQuestion::Score(c.question),
                c.provider,
                c.model,
                c.state,
                c.credential_id,
                c.endpoint,
                Some(c.timeout_secs),
            ),
        };
        let variant = input.variant();
        if correlation.node_kind.as_str() != variant.kind() {
            return Err(DecideError::Validation {
                message: "Node kind does not match configuration".into(),
            });
        }
        let context = self.question_context();
        let prepared = context.prepare(input)?;
        let token = context.token;
        let task_token = token.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            tokio::select! {
                result = infer(prepared) => { let _ = sender.send(result); }
                _ = task_token.cancelled() => {}
            }
        });
        self.completions.insert(
            correlation.node_execution_id,
            CompletionEntry {
                receiver,
                token,
                correlation: correlation.clone(),
                variant,
            },
        );
        Ok(DecideResponse::NodeStarted { correlation })
    }

    fn tick(&mut self, correlation: LifecycleCorrelation) -> Result<DecideResponse, DecideError> {
        let id = correlation.node_execution_id;
        let entry = self
            .completions
            .get_mut(&id)
            .ok_or(DecideError::NodeCompletionNotFound { node_execution_id: id })?;
        if entry.correlation != correlation {
            return Err(DecideError::Validation {
                message: "Node correlation mismatch".into(),
            });
        }
        let result = match entry.receiver.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {
                return Ok(DecideResponse::NodeTicked {
                    correlation,
                    result: DecideNodeTickResult::Running,
                });
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.completions.remove(&id);
                return Err(DecideError::NodeCompletionDropped);
            }
            Ok(Ok(outcome)) => match entry.variant.output(outcome) {
                Ok(output) => DecideNodeTickResult::Succeeded { output },
                Err(error) => DecideNodeTickResult::Failed { error },
            },
            Ok(Err(error)) => DecideNodeTickResult::Failed { error },
        };
        self.completions.remove(&id);
        Ok(DecideResponse::NodeTicked { correlation, result })
    }

    async fn handle_message(&mut self, request: DecideRequest) {
        let DecideRequest {
            payload,
            reply_to: reply,
        } = request;
        use DecideRequestPayload::*;
        let result = match payload {
            StartServer => self.start_server(),
            StopServer => self.stop_server(),
            question @ (AskBool { .. } | AskChoice { .. } | AskScore { .. }) => {
                let context = self.question_context();
                tokio::spawn(Self::handle_question(context, question, reply));
                return;
            }
            SupplyCredential {
                credential_id,
                credential,
            } => (|| {
                if credential.api_key.trim().is_empty() {
                    return Err(DecideError::Validation {
                        message: "Credential key must not be empty".into(),
                    });
                }
                validate_credentialed_endpoint(&credential.endpoint)?;
                self.credentials.insert(credential_id, credential);
                Ok(DecideResponse::Success)
            })(),
            RemoveCredential { credential_id } => {
                self.credentials.remove(&credential_id);
                Ok(DecideResponse::Success)
            }
            StartNode { correlation, config } => self.start(correlation, config),
            TickNode { correlation } => self.tick(correlation),
            CancelNode { node_execution_id } => {
                if let Some(entry) = self.completions.remove(&node_execution_id) {
                    entry.token.cancel();
                } else {
                    tracing::warn!("Cancel requested for unknown Decide node execution");
                }
                Ok(DecideResponse::Success)
            }
        };
        let _ = reply.send(result);
    }
}

/// Runs the sequential actor loop; channel closure terminates the actor.
pub(super) async fn run_actor(mut actor: Actor) {
    while let Some(request) = actor.receiver.recv().await {
        actor.handle_message(request).await;
    }
    actor.cleanup();
}

fn resolve_endpoint(
    credential: Option<&DecideCredential>,
    override_url: Option<&str>,
) -> Result<Option<Url>, DecideError> {
    if let Some(credential) = credential {
        if let Some(raw) = override_url {
            let parsed = Url::parse(raw).map_err(|_| DecideError::EndpointUnauthorized)?;
            if canonical_endpoint(&parsed) != canonical_endpoint(&credential.endpoint) {
                return Err(DecideError::EndpointUnauthorized);
            }
        }
        Ok(Some(credential.endpoint.clone()))
    } else {
        override_url.map(validate_keyless_endpoint).transpose()
    }
}

async fn infer(prepared: Prepared) -> Result<DecideOutcome, DecideError> {
    let Prepared {
        plugin,
        request,
        credential,
        permit: _permit,
    } = prepared;
    let retry_after = Mutex::new(None::<u64>);
    retry_with_backoff(
        3,
        RetryBackoff::new(Duration::from_secs(10), Duration::from_secs(60)),
        || {
            let delay = retry_after.lock().expect("retry state").take();
            let request = &request;
            let credential = credential.as_ref();
            async move {
                if let Some(seconds) = delay {
                    tokio::time::sleep(Duration::from_secs(seconds)).await;
                }
                plugin.ask(request, credential).await
            }
        },
        |error| {
            if let DecideError::ProviderRetryable {
                retry_after_seconds, ..
            } = error
            {
                *retry_after.lock().expect("retry state") = retry_after_seconds.map(|seconds| seconds.min(60));
                true
            } else {
                false
            }
        },
    )
    .await
}

#[cfg(test)]
mod tests;

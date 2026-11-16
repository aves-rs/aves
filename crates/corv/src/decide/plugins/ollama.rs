//! Ollama System One provider mapping.

use async_trait::async_trait;
use reqwest::Url;
use serde_json::{Map, Value, json};
use std::sync::LazyLock;

use super::super::types::{
    DecideAskRequest, DecideContent, DecideCredential, DecideError, DecideOutcome, DecideQuestion,
};
use super::{DecidePlugin, post_json, systemone};

pub(super) const PROVIDER_ID: &str = "ollama";
const DEFAULT_BASE: &str = "http://localhost:11434";
static LOCAL_BASE: LazyLock<Url> = LazyLock::new(|| Url::parse(DEFAULT_BASE).expect("static local URL is valid"));
pub(super) static PLUGIN: OllamaPlugin = OllamaPlugin;

pub(super) struct OllamaPlugin;

fn invalid(message: &'static str) -> DecideError {
    DecideError::InvalidQuestion {
        message: message.into(),
    }
}

fn content_text(content: &DecideContent) -> Result<String, DecideError> {
    match content {
        DecideContent::Text(text) if text.trim().is_empty() => Err(invalid("Text content must not be blank")),
        DecideContent::Text(text) => Ok(text.clone()),
        _ => serde_json::to_string(content).map_err(|_| invalid("Content serialization failed")),
    }
}

fn instructions(content: &Option<DecideContent>) -> Result<String, DecideError> {
    content
        .as_ref()
        .ok_or_else(|| invalid("Instructions are required"))
        .and_then(content_text)
}

fn criterion(content: &DecideContent) -> Result<&str, DecideError> {
    match content {
        DecideContent::Text(text) => Ok(text),
        _ => Err(invalid("Criteria must be text")),
    }
}

fn wire_request(request: &DecideAskRequest) -> Result<Value, DecideError> {
    let state = content_text(&request.state)?;
    let question = match &request.question {
        DecideQuestion::Bool(question) => {
            let mut value = json!({"type": "noul", "instructions": instructions(&question.instructions)?});
            let mut criteria = Map::new();
            for (key, description) in [
                ("true", &question.true_description),
                ("false", &question.false_description),
            ] {
                if let Some(description) = description {
                    criteria.insert(key.into(), json!(criterion(description)?));
                }
            }
            if !criteria.is_empty() {
                value["criteria"] = Value::Object(criteria);
            }
            value
        }
        DecideQuestion::Choice(question) => {
            question.validate()?;
            if !(2..=255).contains(&question.criteria.len()) {
                return Err(invalid("Choice requires 2 to 255 criteria"));
            }
            let criteria: Result<Map<String, Value>, DecideError> = question
                .criteria
                .iter()
                .map(|(name, description)| {
                    Ok((
                        name.clone(),
                        match description {
                            Some(value) => json!(criterion(value)?),
                            None => Value::Null,
                        },
                    ))
                })
                .collect();
            json!({"type": "choice", "instructions": instructions(&question.instructions)?, "criteria": criteria?})
        }
        DecideQuestion::Score(question) => {
            if !(2..=10).contains(&question.criteria.len()) {
                return Err(invalid("Score requires 2 to 10 levels"));
            }
            let criteria: Result<Vec<&str>, DecideError> = question.criteria.iter().map(criterion).collect();
            json!({"type": "score", "instructions": instructions(&question.instructions)?, "criteria": criteria?})
        }
    };
    Ok(systemone::request_envelope(
        &request.model,
        Value::String(state),
        question,
    ))
}

#[async_trait]
impl DecidePlugin for OllamaPlugin {
    async fn ask(
        &self,
        request: &DecideAskRequest,
        credential: Option<&DecideCredential>,
    ) -> Result<DecideOutcome, DecideError> {
        let base = match (&request.endpoint, credential) {
            (Some(base), _) => base,
            (None, Some(_)) => {
                return Err(DecideError::Validation {
                    message: "Credential endpoint was not resolved".into(),
                });
            }
            (None, None) => &LOCAL_BASE,
        };
        let body = wire_request(request)?;
        let response = post_json(
            base,
            credential
                .and_then(|value| value.api_key.as_ref())
                .map(|key| key.as_str()),
            &body,
            request.timeout,
            |status| matches!(status, 429 | 502 | 503),
        )
        .await?;
        systemone::decode(&response, &request.question)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{BoolQuestion, ChoiceQuestion, DecideAnswer, ScoreQuestion};
    use super::super::test_server;
    use super::*;
    use axum08::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use zeroize::Zeroizing;

    fn text(value: &str) -> DecideContent {
        DecideContent::Text(value.into())
    }
    fn object() -> DecideContent {
        DecideContent::Object(BTreeMap::from([("x".into(), json!(1))]))
    }
    fn array() -> DecideContent {
        DecideContent::Array(vec![json!(1), json!(2)])
    }
    fn bool_question() -> DecideQuestion {
        DecideQuestion::Bool(BoolQuestion {
            instructions: Some(text("decide")),
            true_description: None,
            false_description: None,
        })
    }
    fn choice_question() -> DecideQuestion {
        DecideQuestion::Choice(ChoiceQuestion {
            instructions: Some(text("decide")),
            criteria: BTreeMap::from([("a".into(), None), ("b".into(), Some(text("B")))]),
        })
    }
    fn score_question() -> DecideQuestion {
        DecideQuestion::Score(ScoreQuestion {
            instructions: Some(text("decide")),
            criteria: vec![text("low"), text("high")],
        })
    }
    fn request(question: DecideQuestion) -> DecideAskRequest {
        DecideAskRequest {
            model: "clef".into(),
            endpoint: None,
            timeout: Duration::from_secs(2),
            state: text("state"),
            question,
        }
    }
    fn rejected(request: &DecideAskRequest) {
        assert!(matches!(
            wire_request(request),
            Err(DecideError::InvalidQuestion { .. })
        ));
    }

    #[test]
    fn mapping_and_validation() {
        let mut r = request(bool_question());
        assert_eq!(
            wire_request(&r).unwrap(),
            json!({"model":"clef","state":"state","questions":{"q":{"type":"noul","instructions":"decide"}}})
        );
        if let DecideQuestion::Bool(q) = &mut r.question {
            q.true_description = Some(text("yes"));
        }
        assert_eq!(
            wire_request(&r).unwrap()["questions"]["q"]["criteria"],
            json!({"true":"yes"})
        );
        if let DecideQuestion::Bool(q) = &mut r.question {
            q.true_description = Some(object());
        }
        rejected(&r);
        if let DecideQuestion::Bool(q) = &mut r.question {
            q.true_description = None;
            q.instructions = None;
        }
        rejected(&r);
        if let DecideQuestion::Bool(q) = &mut r.question {
            q.instructions = Some(text("  "));
        }
        rejected(&r);
        for structured in [object(), array()] {
            if let DecideQuestion::Bool(q) = &mut r.question {
                q.instructions = Some(structured.clone());
            }
            assert_eq!(
                wire_request(&r).unwrap()["questions"]["q"]["instructions"],
                json!(serde_json::to_string(&structured).unwrap())
            );
        }
        r.state = text(" ");
        rejected(&r);
        r.state = object();
        assert_eq!(wire_request(&r).unwrap()["state"], json!("{\"x\":1}"));
        let mut r = request(choice_question());
        assert_eq!(
            wire_request(&r).unwrap()["questions"]["q"],
            json!({"type":"choice","instructions":"decide","criteria":{"a":null,"b":"B"}})
        );
        if let DecideQuestion::Choice(q) = &mut r.question {
            q.criteria.insert("a".into(), Some(array()));
        }
        rejected(&r);
        if let DecideQuestion::Choice(q) = &mut r.question {
            q.criteria.remove("a");
        }
        rejected(&r);
        let mut r = request(score_question());
        assert_eq!(
            wire_request(&r).unwrap()["questions"]["q"],
            json!({"type":"score","instructions":"decide","criteria":["low","high"]})
        );
        if let DecideQuestion::Score(q) = &mut r.question {
            q.criteria[0] = object();
        }
        rejected(&r);
        if let DecideQuestion::Score(q) = &mut r.question {
            q.criteria.pop();
        }
        rejected(&r);
    }

    #[test]
    fn decodes_all_answer_fields_without_io() {
        let choice = DecideQuestion::Choice(ChoiceQuestion {
            instructions: Some(text("decide")),
            criteria: BTreeMap::from([("a".into(), None), ("b".into(), Some(text("B"))), ("c".into(), None)]),
        });
        let score = DecideQuestion::Score(ScoreQuestion {
            instructions: Some(text("decide")),
            criteria: vec![text("low"), text("medium"), text("high")],
        });
        let cases = [
            (bool_question(), json!({"type":"noul","noul":0.73})),
            (
                choice,
                json!({"type":"choice","choice":"b","confidence":0.42,"probabilities":{"a":0.7,"b":0.2,"c":0.1}}),
            ),
            (
                score,
                json!({"type":"score","score":1.37,"confidence":0.46,"legend":{"0":"low","1":"medium","2":"high"},"probabilities":{"0":0.15,"1":0.65,"2":0.2}}),
            ),
        ];
        for (question, answer) in cases {
            wire_request(&request(question.clone())).unwrap();
            let outcome = systemone::decode(&json!({"model":"actual","answers":{"q":answer}}), &question).unwrap();
            assert_eq!(outcome.model, "actual");
            match (question, outcome.answer) {
                (DecideQuestion::Bool(_), DecideAnswer::Bool(answer)) => assert_eq!(answer.probability, 0.73),
                (DecideQuestion::Choice(_), DecideAnswer::Choice(answer)) => {
                    assert_eq!(answer.choice, "b");
                    assert_eq!(answer.confidence, 0.42);
                    assert_eq!(
                        answer.probabilities,
                        BTreeMap::from([("a".into(), 0.7), ("b".into(), 0.2), ("c".into(), 0.1)])
                    );
                }
                (DecideQuestion::Score(_), DecideAnswer::Score(answer)) => {
                    assert_eq!(answer.score, 1.37);
                    assert_eq!(answer.confidence, 0.46);
                    assert_eq!(
                        answer.legend,
                        BTreeMap::from([
                            ("0".into(), text("low")),
                            ("1".into(), text("medium")),
                            ("2".into(), text("high"))
                        ])
                    );
                    assert_eq!(
                        answer.probabilities,
                        BTreeMap::from([("0".into(), 0.15), ("1".into(), 0.65), ("2".into(), 0.2)])
                    );
                }
                _ => panic!("answer type does not match question"),
            }
        }
    }

    #[derive(Clone)]
    struct Mock {
        hits: Arc<AtomicUsize>,
        auth: Option<&'static str>,
        status: StatusCode,
    }

    async fn handler(
        State(mock): State<Mock>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        mock.hits.fetch_add(1, Ordering::SeqCst);
        assert_eq!(headers.get("authorization").map(|h| h.to_str().unwrap()), mock.auth);
        if !mock.status.is_success() {
            return (mock.status, Json(json!({"error":"SENTINEL"})));
        }
        let answer = match body["questions"]["q"]["type"].as_str().unwrap() {
            "noul" => json!({"type":"noul","noul":0.7}),
            "choice" => json!({"type":"choice","choice":"a","confidence":0.42,"probabilities":{"a":0.7,"b":0.3}}),
            "score" => {
                json!({"type":"score","score":0.3,"confidence":0.42,"probabilities":{"0":0.7,"1":0.3},"legend":{"0":"low","1":"high"}})
            }
            _ => unreachable!(),
        };
        (mock.status, Json(json!({"model":body["model"],"answers":{"q":answer}})))
    }

    async fn server(mock: Mock) -> test_server::TestServer {
        test_server::start(
            Router::new()
                .route("/v1/systemone", post(handler))
                .with_state(mock),
        )
        .await
    }

    #[tokio::test]
    async fn mock_auth_decoding_and_preflight() {
        for keyed in [false, true] {
            let hits = Arc::new(AtomicUsize::new(0));
            let server = server(Mock {
                hits: hits.clone(),
                auth: keyed.then_some("Bearer test-key"),
                status: StatusCode::OK,
            })
            .await;
            let credential = DecideCredential {
                provider: PROVIDER_ID.into(),
                api_key: keyed.then(|| Zeroizing::new("test-key".into())),
                endpoint: server.base.clone(),
            };
            for question in [bool_question(), choice_question(), score_question()] {
                let mut r = request(question);
                r.endpoint = Some(server.base.clone());
                let outcome = PLUGIN.ask(&r, Some(&credential)).await.unwrap();
                assert_eq!(outcome.model, "clef");
                match outcome.answer {
                    DecideAnswer::Bool(answer) => assert_eq!(answer.probability, 0.7),
                    DecideAnswer::Choice(answer) => assert_eq!(answer.confidence, 0.42),
                    DecideAnswer::Score(answer) => assert_eq!(answer.confidence, 0.42),
                }
            }
            assert_eq!(hits.load(Ordering::SeqCst), 3);
            let mut invalids = Vec::new();
            let mut r = request(bool_question());
            if let DecideQuestion::Bool(q) = &mut r.question {
                q.instructions = None;
            }
            invalids.push(r.clone());
            if let DecideQuestion::Bool(q) = &mut r.question {
                q.instructions = Some(text(" "));
            }
            invalids.push(r.clone());
            if let DecideQuestion::Bool(q) = &mut r.question {
                q.instructions = Some(text("ok"));
                q.true_description = Some(object());
            }
            invalids.push(r.clone());
            if let DecideQuestion::Bool(q) = &mut r.question {
                q.true_description = None;
            }
            r.state = text(" ");
            invalids.push(r);
            let mut r = request(choice_question());
            if let DecideQuestion::Choice(q) = &mut r.question {
                q.criteria.insert("a".into(), Some(array()));
            }
            invalids.push(r.clone());
            if let DecideQuestion::Choice(q) = &mut r.question {
                q.criteria.remove("a");
            }
            invalids.push(r);
            let mut r = request(score_question());
            if let DecideQuestion::Score(q) = &mut r.question {
                q.criteria[0] = object();
            }
            invalids.push(r.clone());
            if let DecideQuestion::Score(q) = &mut r.question {
                q.criteria.pop();
            }
            invalids.push(r);
            for mut invalid in invalids {
                invalid.endpoint = Some(server.base.clone());
                assert!(matches!(
                    PLUGIN.ask(&invalid, keyed.then_some(&credential)).await,
                    Err(DecideError::InvalidQuestion { .. })
                ));
            }
            if keyed {
                assert!(matches!(
                    PLUGIN
                        .ask(&request(bool_question()), Some(&credential))
                        .await,
                    Err(DecideError::Validation { .. })
                ));
            }
            assert_eq!(hits.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test]
    async fn mock_status_classification_never_exposes_body() {
        for code in [429, 502, 503, 500, 400, 401, 418, 302] {
            let hits = Arc::new(AtomicUsize::new(0));
            let server = server(Mock {
                hits: hits.clone(),
                auth: None,
                status: StatusCode::from_u16(code).unwrap(),
            })
            .await;
            let mut r = request(bool_question());
            r.endpoint = Some(server.base.clone());
            let error = PLUGIN.ask(&r, None).await.unwrap_err();
            assert!(!format!("{error:?}").contains("SENTINEL"));
            if matches!(code, 429 | 502 | 503) {
                assert!(matches!(error, DecideError::ProviderRetryable { .. }));
            } else {
                assert!(matches!(error, DecideError::ProviderRequestFailed { .. }));
            }
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }
    }
}

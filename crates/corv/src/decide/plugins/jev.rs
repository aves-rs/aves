//! TypeSafe System One provider.

use async_trait::async_trait;
use serde_json::{Value, json};

use super::super::types::{DecideAskRequest, DecideCredential, DecideError, DecideOutcome, DecideQuestion};
use super::{DecidePlugin, post_json, systemone};

pub(super) const PROVIDER_ID: &str = "jev";
pub(super) static PLUGIN: JevPlugin = JevPlugin;

pub(super) struct JevPlugin;

fn question_json(question: &DecideQuestion) -> Result<Value, DecideError> {
    let (mut value, instructions) = match question {
        DecideQuestion::Bool(question) => {
            let mut value = json!({"type": "noul"});
            if question.true_description.is_some() || question.false_description.is_some() {
                value["criteria"] = json!({"true": question.true_description, "false": question.false_description});
            }
            (value, &question.instructions)
        }
        DecideQuestion::Choice(question) => {
            question.validate()?;
            (
                json!({"type": "choice", "criteria": question.criteria}),
                &question.instructions,
            )
        }
        DecideQuestion::Score(question) => {
            question.validate()?;
            (
                json!({"type": "score", "criteria": question.criteria}),
                &question.instructions,
            )
        }
    };
    if let Some(instructions) = instructions {
        value["instructions"] = json!(instructions);
    }
    Ok(value)
}

fn retryable(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

#[async_trait]
impl DecidePlugin for JevPlugin {
    async fn ask(
        &self,
        request: &DecideAskRequest,
        credential: Option<&DecideCredential>,
    ) -> Result<DecideOutcome, DecideError> {
        let key = credential
            .and_then(|value| value.api_key.as_ref())
            .ok_or(DecideError::AuthenticationRequired)?;
        let endpoint = request
            .endpoint
            .as_ref()
            .ok_or_else(|| DecideError::Validation {
                message: "credentialed request requires a resolved endpoint".into(),
            })?;
        let question = question_json(&request.question)?;
        let body = systemone::request_envelope(&request.model, json!(request.state), question);
        let response = post_json(endpoint, Some(key.as_str()), &body, request.timeout, retryable).await?;
        systemone::decode(&response, &request.question)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum08::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode, header},
        routing::post,
    };
    use serde_json::{Value, json};
    use zeroize::Zeroizing;

    use super::super::super::types::{BoolQuestion, ChoiceQuestion, DecideAnswer, DecideContent, ScoreQuestion};
    use super::super::test_server;
    use super::*;

    fn boolean() -> DecideQuestion {
        DecideQuestion::Bool(BoolQuestion {
            instructions: None,
            true_description: None,
            false_description: None,
        })
    }

    fn request(endpoint: Option<reqwest::Url>, question: DecideQuestion) -> DecideAskRequest {
        DecideAskRequest {
            model: "requested".into(),
            endpoint,
            timeout: Duration::from_secs(2),
            state: DecideContent::Object(BTreeMap::from([("nested".into(), json!([1, 2]))])),
            question,
        }
    }

    fn credential(base: &reqwest::Url) -> DecideCredential {
        DecideCredential {
            provider: PROVIDER_ID.into(),
            api_key: Some(Zeroizing::new("test-key".into())),
            endpoint: base.clone(),
        }
    }

    #[test]
    fn maps_questions_and_preserves_rich_values() {
        assert_eq!(question_json(&boolean()).unwrap(), json!({"type":"noul"}));
        let bool_question = DecideQuestion::Bool(BoolQuestion {
            instructions: Some(DecideContent::Text("question".into())),
            true_description: Some(DecideContent::Text("yes".into())),
            false_description: None,
        });
        assert_eq!(
            question_json(&bool_question).unwrap(),
            json!({"type":"noul","instructions":"question","criteria":{"true":"yes","false":null}})
        );
        let choice = DecideQuestion::Choice(ChoiceQuestion {
            instructions: Some(DecideContent::Object(BTreeMap::from([("hint".into(), json!([1]))]))),
            criteria: BTreeMap::from([("a".into(), None)]),
        });
        assert_eq!(
            question_json(&choice).unwrap(),
            json!({"type":"choice","instructions":{"hint":[1]},"criteria":{"a":null}})
        );
        let score = DecideQuestion::Score(ScoreQuestion {
            instructions: None,
            criteria: vec![
                DecideContent::Object(BTreeMap::from([("x".into(), json!(1))])),
                DecideContent::Array(vec![json!("high")]),
            ],
        });
        assert_eq!(
            question_json(&score).unwrap(),
            json!({"type":"score","criteria":[{"x":1},["high"]]})
        );
        let envelope = systemone::request_envelope(
            "m",
            json!(request(None, boolean()).state),
            question_json(&boolean()).unwrap(),
        );
        assert_eq!(envelope["state"], json!({"nested":[1,2]}));
    }

    #[test]
    fn decodes_all_answer_fields_without_io() {
        let choice = DecideQuestion::Choice(ChoiceQuestion {
            instructions: None,
            criteria: BTreeMap::from([("a".into(), None), ("b".into(), None), ("c".into(), None)]),
        });
        let score = DecideQuestion::Score(ScoreQuestion {
            instructions: None,
            criteria: vec![
                DecideContent::Text("low".into()),
                DecideContent::Object(BTreeMap::from([("label".into(), json!("medium"))])),
                DecideContent::Text("high".into()),
            ],
        });
        let cases = [
            (boolean(), json!({"type":"noul","noul":0.73})),
            (
                choice,
                json!({"type":"choice","choice":"b","confidence":0.42,"probabilities":{"a":0.7,"b":0.2,"c":0.1}}),
            ),
            (
                score,
                json!({"type":"score","score":1.37,"confidence":0.46,"legend":{"0":"low","1":{"label":"medium"},"2":"high"},"probabilities":{"0":0.15,"1":0.65,"2":0.2}}),
            ),
        ];
        for (question, answer) in cases {
            question_json(&question).unwrap();
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
                            ("0".into(), DecideContent::Text("low".into())),
                            (
                                "1".into(),
                                DecideContent::Object(BTreeMap::from([("label".into(), json!("medium"))]))
                            ),
                            ("2".into(), DecideContent::Text("high".into()))
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

    #[test]
    fn rejects_out_of_bounds_questions() {
        for count in [0, 256] {
            let question = DecideQuestion::Choice(ChoiceQuestion {
                instructions: None,
                criteria: (0..count).map(|n| (n.to_string(), None)).collect(),
            });
            assert!(matches!(
                question_json(&question),
                Err(DecideError::InvalidQuestion { .. })
            ));
        }
        let question = DecideQuestion::Score(ScoreQuestion {
            instructions: None,
            criteria: vec![DecideContent::Text("x".into()); 11],
        });
        assert!(matches!(
            question_json(&question),
            Err(DecideError::InvalidQuestion { .. })
        ));
        assert!(matches!(
            question_json(&DecideQuestion::Score(ScoreQuestion {
                instructions: None,
                criteria: vec![]
            })),
            Err(DecideError::InvalidQuestion { .. })
        ));
    }

    #[tokio::test]
    async fn authentication_and_endpoint_invariant_fail_without_io() {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/v1/systemone",
                post(|State(hits): State<Arc<AtomicUsize>>| async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .with_state(hits.clone());
        let server = test_server::start(app).await;
        assert!(matches!(
            PLUGIN
                .ask(&request(Some(server.base.clone()), boolean()), None)
                .await,
            Err(DecideError::AuthenticationRequired)
        ));
        let mut keyless = credential(&server.base);
        keyless.api_key = None;
        assert!(matches!(
            PLUGIN
                .ask(&request(Some(server.base.clone()), boolean()), Some(&keyless))
                .await,
            Err(DecideError::AuthenticationRequired)
        ));
        assert!(matches!(
            PLUGIN
                .ask(&request(None, boolean()), Some(&credential(&server.base)))
                .await,
            Err(DecideError::Validation { .. })
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sends_authenticated_requests_and_decodes_each_answer() {
        let app = Router::new().route("/v1/systemone", post(|headers: HeaderMap, Json(body): Json<Value>| async move {
            assert_eq!(headers.get(header::AUTHORIZATION).unwrap(), "Bearer test-key");
            assert_eq!(body["model"], "requested");
            assert_eq!(body["state"], json!({"nested":[1,2]}));
            let question = &body["questions"]["q"];
            let answer = match question["type"].as_str().unwrap() {
                "noul" => { assert_eq!(question, &json!({"type":"noul"})); json!({"type":"noul","noul":0.7}) },
                "choice" => { assert_eq!(question, &json!({"type":"choice","criteria":{"a":null}})); json!({"type":"choice","choice":"a","confidence":1.0,"probabilities":{"a":1.0}}) },
                "score" => { assert_eq!(question, &json!({"type":"score","criteria":[{"x":1},["high"]]})); json!({"type":"score","score":0.5,"confidence":0.5,"legend":{"0":{"x":1},"1":["high"]},"probabilities":{"0":0.5,"1":0.5}}) },
                _ => unreachable!(),
            };
            Json(json!({"model":"actual","answers":{"q":answer}}))
        }));
        let server = test_server::start(app).await;
        let choices = DecideQuestion::Choice(ChoiceQuestion {
            instructions: None,
            criteria: BTreeMap::from([("a".into(), None)]),
        });
        let score = DecideQuestion::Score(ScoreQuestion {
            instructions: None,
            criteria: vec![
                DecideContent::Object(BTreeMap::from([("x".into(), json!(1))])),
                DecideContent::Array(vec![json!("high")]),
            ],
        });
        for (index, question) in [boolean(), choices, score].into_iter().enumerate() {
            let outcome = PLUGIN
                .ask(
                    &request(Some(server.base.clone()), question),
                    Some(&credential(&server.base)),
                )
                .await
                .unwrap();
            assert_eq!(outcome.model, "actual");
            assert!(matches!(
                (index, outcome.answer),
                (0, DecideAnswer::Bool(_)) | (1, DecideAnswer::Choice(_)) | (2, DecideAnswer::Score(_))
            ));
        }
    }

    #[tokio::test]
    async fn classifies_statuses_without_leaking_body() {
        for code in [401, 422, 418, 408, 429, 500, 503, 529] {
            let status = StatusCode::from_u16(code).unwrap();
            let app = Router::new().route(
                "/v1/systemone",
                post(move || async move { (status, [(header::RETRY_AFTER, "7")], "sentinel-secret") }),
            );
            let server = test_server::start(app).await;
            let error = PLUGIN
                .ask(
                    &request(Some(server.base.clone()), boolean()),
                    Some(&credential(&server.base)),
                )
                .await
                .unwrap_err();
            assert!(!format!("{error:?} {error}").contains("sentinel-secret"));
            if retryable(code) {
                assert!(matches!(
                    error,
                    DecideError::ProviderRetryable {
                        retry_after_seconds: Some(7),
                        ..
                    }
                ));
            } else {
                assert!(matches!(error, DecideError::ProviderRequestFailed { .. }));
            }
        }
    }

    #[tokio::test]
    async fn redirect_is_not_followed() {
        let hits = Arc::new(AtomicUsize::new(0));
        let target = test_server::start(
            Router::new()
                .route(
                    "/v1/systemone",
                    post(|State(hits): State<Arc<AtomicUsize>>| async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                    }),
                )
                .with_state(hits.clone()),
        )
        .await;
        let location = target.base.join("v1/systemone").unwrap().to_string();
        let server = test_server::start(Router::new().route(
            "/v1/systemone",
            post(move || {
                let location = location.clone();
                async move { (StatusCode::FOUND, [(header::LOCATION, location)]) }
            }),
        ))
        .await;
        assert!(matches!(
            PLUGIN
                .ask(
                    &request(Some(server.base.clone()), boolean()),
                    Some(&credential(&server.base))
                )
                .await,
            Err(DecideError::ProviderRequestFailed { .. })
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_oversized_and_uncorrelated_responses() {
        for body in [
            "x".repeat(4 * 1024 * 1024 + 1),
            json!({"model":"actual","answers":{"q":{"type":"noul","noul":0.5},"extra":{}}}).to_string(),
        ] {
            let server = test_server::start(Router::new().route(
                "/v1/systemone",
                post(move || {
                    let body = body.clone();
                    async move { body }
                }),
            ))
            .await;
            assert!(matches!(
                PLUGIN
                    .ask(
                        &request(Some(server.base.clone()), boolean()),
                        Some(&credential(&server.base))
                    )
                    .await,
                Err(DecideError::ProviderResponseInvalid { .. })
            ));
        }
    }

    #[tokio::test]
    async fn deadline_includes_stalled_response_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = reqwest::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let _ = stream.read(&mut buffer).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{\r\n")
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let mut req = request(Some(base.clone()), boolean());
        req.timeout = Duration::from_millis(200);
        assert!(
            matches!(PLUGIN.ask(&req, Some(&credential(&base))).await, Err(DecideError::ProviderRetryable { message, .. }) if message == "provider request timed out")
        );
        task.abort();
    }
}

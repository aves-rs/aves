//! Shared System One envelope and strictly correlated answer decoding.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use super::super::types::{
    BoolAnswer, ChoiceAnswer, DecideAnswer, DecideContent, DecideError, DecideOutcome, DecideQuestion, ScoreAnswer,
};

pub(super) const QUESTION_ID: &str = "q";

pub(super) fn request_envelope(model: &str, state: Value, question: Value) -> Value {
    json!({ "model": model, "state": state, "questions": { (QUESTION_ID): question } })
}

fn invalid(message: &'static str) -> DecideError {
    DecideError::ProviderResponseInvalid {
        message: message.to_owned(),
    }
}

fn number(value: Option<&Value>, max: f64, message: &'static str) -> Result<f64, DecideError> {
    value
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n >= 0.0 && *n <= max)
        .ok_or_else(|| invalid(message))
}

fn object<'a>(value: Option<&'a Value>, message: &'static str) -> Result<&'a Map<String, Value>, DecideError> {
    value
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(message))
}

fn probabilities(value: Option<&Value>, keys: &BTreeSet<String>) -> Result<BTreeMap<String, f64>, DecideError> {
    let values = object(value, "probabilities must be an object")?;
    if values.len() != keys.len() || values.keys().any(|key| !keys.contains(key)) {
        return Err(invalid("probability keys must match criteria"));
    }
    values
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                number(Some(value), 1.0, "probabilities must be finite and within [0,1]")?,
            ))
        })
        .collect()
}

pub(super) fn decode(response: &Value, question: &DecideQuestion) -> Result<DecideOutcome, DecideError> {
    let response = object(Some(response), "response must be an object")?;
    let model = response
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| invalid("model must be a non-empty string"))?;
    let answers = object(response.get("answers"), "answers must be an object")?;
    if answers.len() != 1 || !answers.contains_key(QUESTION_ID) {
        return Err(invalid("answers must contain exactly the requested question"));
    }
    let answer = object(answers.get(QUESTION_ID), "answer must be an object")?;
    let expected = match question {
        DecideQuestion::Bool(_) => "noul",
        DecideQuestion::Choice(_) => "choice",
        DecideQuestion::Score(_) => "score",
    };
    if answer.get("type").and_then(Value::as_str) != Some(expected) {
        return Err(invalid("answer type must match question"));
    }
    let result = match question {
        DecideQuestion::Bool(_) => DecideAnswer::Bool(BoolAnswer {
            probability: number(answer.get("noul"), 1.0, "noul must be finite and within [0,1]")?,
        }),
        DecideQuestion::Choice(question) => {
            let choice = answer
                .get("choice")
                .and_then(Value::as_str)
                .filter(|choice| question.criteria.contains_key(*choice))
                .ok_or_else(|| invalid("choice must match a criterion"))?;
            let keys = question.criteria.keys().cloned().collect();
            DecideAnswer::Choice(ChoiceAnswer {
                choice: choice.to_owned(),
                confidence: number(
                    answer.get("confidence"),
                    1.0,
                    "confidence must be finite and within [0,1]",
                )?,
                probabilities: probabilities(answer.get("probabilities"), &keys)?,
            })
        }
        DecideQuestion::Score(question) => {
            if question.criteria.is_empty() {
                return Err(invalid("score requires at least one criterion"));
            }
            let keys: BTreeSet<String> = (0..question.criteria.len())
                .map(|i| i.to_string())
                .collect();
            let legend = object(answer.get("legend"), "legend must be an object")?;
            if legend.len() != keys.len() || legend.keys().any(|key| !keys.contains(key)) {
                return Err(invalid("legend keys must match criteria"));
            }
            let mut decoded_legend = BTreeMap::new();
            for (index, criterion) in question.criteria.iter().enumerate() {
                let key = index.to_string();
                let value = legend
                    .get(&key)
                    .and_then(|value| serde_json::from_value::<DecideContent>(value.clone()).ok())
                    .filter(|value| value == criterion)
                    .ok_or_else(|| invalid("legend values must match criteria"))?;
                decoded_legend.insert(key, value);
            }
            DecideAnswer::Score(ScoreAnswer {
                score: number(
                    answer.get("score"),
                    (question.criteria.len() - 1) as f64,
                    "score must be finite and within criterion bounds",
                )?,
                confidence: number(
                    answer.get("confidence"),
                    1.0,
                    "confidence must be finite and within [0,1]",
                )?,
                legend: decoded_legend,
                probabilities: probabilities(answer.get("probabilities"), &keys)?,
            })
        }
    };
    Ok(DecideOutcome {
        model: model.to_owned(),
        answer: result,
    })
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{BoolQuestion, ChoiceQuestion, ScoreQuestion};
    use super::*;

    fn envelope(answer: Value) -> Value {
        json!({"model":"actual", "answers": {"q": answer}})
    }
    fn bool_question() -> DecideQuestion {
        DecideQuestion::Bool(BoolQuestion {
            instructions: None,
            true_description: None,
            false_description: None,
        })
    }
    fn choice_question() -> DecideQuestion {
        DecideQuestion::Choice(ChoiceQuestion {
            instructions: None,
            criteria: BTreeMap::from([("a".into(), None), ("b".into(), None), ("c".into(), None)]),
        })
    }
    fn score_question() -> DecideQuestion {
        DecideQuestion::Score(ScoreQuestion {
            instructions: None,
            criteria: vec![
                DecideContent::Text("low".into()),
                DecideContent::Object(BTreeMap::from([("label".into(), json!("medium"))])),
                DecideContent::Text("high".into()),
            ],
        })
    }
    fn rejected(value: &Value, question: &DecideQuestion) {
        assert!(matches!(
            decode(value, question),
            Err(DecideError::ProviderResponseInvalid { .. })
        ));
    }

    #[test]
    fn envelope_and_boolean_correlation() {
        assert_eq!(
            request_envelope("m", json!({}), json!({"type":"noul"})),
            json!({"model":"m","state":{},"questions":{"q":{"type":"noul"}}})
        );
        let q = bool_question();
        let good = envelope(json!({"type":"noul","noul":0.73}));
        let outcome = decode(&good, &q).unwrap();
        assert_eq!(outcome.model, "actual");
        let DecideAnswer::Bool(answer) = outcome.answer else {
            panic!("expected bool answer")
        };
        assert_eq!(answer.probability, 0.73);
        for model in [Value::Null, json!(""), json!(1)] {
            let mut v = good.clone();
            v["model"] = model;
            rejected(&v, &q);
        }
        let mut v = good.clone();
        v.as_object_mut().unwrap().remove("model");
        rejected(&v, &q);
        for answers in [
            json!({}),
            json!({"other":{}}),
            json!({"q":{},"extra":{}}),
            json!([]),
            json!({"q":null}),
        ] {
            let mut v = good.clone();
            v["answers"] = answers;
            rejected(&v, &q);
        }
        for answer in [
            json!({"type":"choice","noul":0.5}),
            json!({"type":"noul","noul":-0.1}),
            json!({"type":"noul","noul":1.1}),
            json!({"type":"noul","noul":"NaN"}),
            json!({"type":"noul","noul":null}),
        ] {
            rejected(&envelope(answer), &q);
        }
        rejected(&json!([]), &q);
    }

    #[test]
    fn choice_and_score_validate_all_correlations_and_bounds() {
        let choice = choice_question();
        let good = json!({"type":"choice","choice":"b","confidence":0.42,"probabilities":{"a":0.7,"b":0.2,"c":0.1}});
        let outcome = decode(&envelope(good.clone()), &choice).unwrap();
        assert_eq!(outcome.model, "actual");
        let DecideAnswer::Choice(answer) = outcome.answer else {
            panic!("expected choice answer")
        };
        assert_eq!(answer.choice, "b");
        assert_eq!(answer.confidence, 0.42);
        assert_eq!(
            answer.probabilities,
            BTreeMap::from([("a".into(), 0.7), ("b".into(), 0.2), ("c".into(), 0.1)])
        );
        for (field, value) in [
            ("choice", json!("d")),
            ("choice", json!(null)),
            ("confidence", json!(1.1)),
            ("confidence", json!("NaN")),
            ("probabilities", json!({"a":1})),
            ("probabilities", json!({"a":1,"b":0,"c":0,"d":0})),
            ("probabilities", json!({"a":-0.1,"b":1,"c":0})),
            ("probabilities", json!({"a":"Infinity","b":1,"c":0})),
        ] {
            let mut v = good.clone();
            v[field] = value;
            rejected(&envelope(v), &choice);
        }
        let score = score_question();
        let good = json!({"type":"score","score":1.37,"confidence":0.46,"probabilities":{"0":0.15,"1":0.65,"2":0.2},"legend":{"0":"low","1":{"label":"medium"},"2":"high"}});
        let outcome = decode(&envelope(good.clone()), &score).unwrap();
        assert_eq!(outcome.model, "actual");
        let DecideAnswer::Score(answer) = outcome.answer else {
            panic!("expected score answer")
        };
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
        for (field, value) in [
            ("score", json!(-1)),
            ("score", json!(3)),
            ("score", json!("NaN")),
            ("confidence", json!(-0.1)),
            ("legend", json!({"0":"low"})),
            ("legend", json!({"0":"low","1":"wrong","2":"high"})),
            ("legend", json!({"0":"low","1":null,"2":"high"})),
            ("probabilities", json!({"0":1})),
            ("probabilities", json!({"0":1,"1":1.1,"2":0})),
        ] {
            let mut v = good.clone();
            v[field] = value;
            rejected(&envelope(v), &score);
        }
    }
}

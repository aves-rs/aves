use agent_client_protocol::schema::v1::{
    CreateElicitationRequest, CreateElicitationResponse, ElicitationAcceptAction, ElicitationAction,
    ElicitationContentValue, ElicitationFormMode, ElicitationMode, ElicitationPropertySchema, MultiSelectItems,
};
use serde::{Deserialize, Serialize};
use specta::Type;
use std::collections::BTreeMap;

use super::types::DiscussError;

/// Control types supported by ACP's restricted elicitation form.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DiscussRestrictedFormFieldType {
    String,
    Number,
    Integer,
    Boolean,
    Array,
}

/// One titled choice in a restricted elicitation field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussRestrictedFormOption {
    pub value: String,
    pub title: Option<String>,
    pub description: Option<String>,
}

/// Canonical projection of one restricted ACP form property.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussRestrictedFormField {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub field_type: DiscussRestrictedFormFieldType,
    pub options: Vec<DiscussRestrictedFormOption>,
    pub default: Option<serde_json::Value>,
    pub required: bool,
    pub pattern: Option<String>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// Canonical typed projection of ACP's restricted flat form schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussRestrictedFormSchema {
    pub title: Option<String>,
    pub description: Option<String>,
    pub fields: Vec<DiscussRestrictedFormField>,
    pub required: Vec<String>,
    pub meta: Option<serde_json::Value>,
}

/// Typed desktop payload for one agent-initiated elicitation request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussElicitationRequestPayload {
    /// Server-owned restricted-form projection or fallback classification.
    pub classification: DiscussElicitationClassification,
    /// Human-readable question supplied by the agent.
    pub message: String,
    /// Whether Hades should render the raw-schema fallback surface.
    pub raw_fallback: bool,
    /// Original ACP request retained for fallback and transcript handling.
    pub raw_request: CreateElicitationRequest,
}

/// Server-side classification of an incoming ACP elicitation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DiscussElicitationClassification {
    RestrictedForm { schema: DiscussRestrictedFormSchema },
    FormFallback { raw_schema: serde_json::Value },
    UrlDeferred,
    OtherRawFallback { raw_schema: serde_json::Value },
}

/// The user's answer to one elicitation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DiscussElicitationAnswer {
    Accept { content: serde_json::Value },
    Decline,
    Cancel,
}

/// Convert an accepted Sluice answer into the ACP response at the SDK boundary.
pub(crate) fn content_to_sdk_answer(
    answer: DiscussElicitationAnswer,
) -> Result<CreateElicitationResponse, DiscussError> {
    match answer {
        DiscussElicitationAnswer::Accept { content } => {
            let serde_json::Value::Object(_) = content else {
                return Err(DiscussError::ElicitationInvalidRequest {
                    message: "elicitation answer content must be an object".to_string(),
                });
            };
            let content =
                serde_json::from_value::<BTreeMap<String, ElicitationContentValue>>(content).map_err(|error| {
                    DiscussError::ElicitationInvalidRequest {
                        message: format!("invalid elicitation answer content: {error}"),
                    }
                })?;
            Ok(CreateElicitationResponse::new(ElicitationAction::Accept(
                ElicitationAcceptAction::new().content(content),
            )))
        }
        DiscussElicitationAnswer::Decline => Ok(CreateElicitationResponse::new(ElicitationAction::Decline)),
        DiscussElicitationAnswer::Cancel => Ok(CreateElicitationResponse::new(ElicitationAction::Cancel)),
    }
}

/// Project a typed ACP form schema into Discuss's restricted grammar.
pub(crate) fn sdk_to_restricted_schema(mode: &ElicitationFormMode) -> Option<DiscussRestrictedFormSchema> {
    let required = mode.requested_schema.required.clone().unwrap_or_default();
    if required
        .iter()
        .any(|name| !mode.requested_schema.properties.contains_key(name))
    {
        return None;
    }

    let mut fields = Vec::with_capacity(mode.requested_schema.properties.len());
    for (name, property) in &mode.requested_schema.properties {
        fields.push(sdk_to_restricted_field(
            name,
            property,
            required.iter().any(|item| item == name),
        )?);
    }

    Some(DiscussRestrictedFormSchema {
        title: mode.requested_schema.title.clone(),
        description: mode.requested_schema.description.clone(),
        fields,
        required,
        meta: mode
            .requested_schema
            .meta
            .as_ref()
            .and_then(|meta| serde_json::to_value(meta).ok()),
    })
}

fn sdk_to_restricted_field(
    name: &str,
    property: &ElicitationPropertySchema,
    required: bool,
) -> Option<DiscussRestrictedFormField> {
    let (field_type, title, description, default, pattern, min, max, options) = match property {
        ElicitationPropertySchema::String(schema) => {
            let options = match (&schema.enum_values, &schema.one_of) {
                (Some(_), Some(_)) => return None,
                (Some(values), None) if values.is_empty() => return None,
                (Some(values), None) => values
                    .iter()
                    .map(|value| DiscussRestrictedFormOption {
                        value: value.clone(),
                        title: None,
                        description: None,
                    })
                    .collect(),
                (None, Some(values)) if values.is_empty() => return None,
                (None, Some(values)) => values
                    .iter()
                    .map(|value| DiscussRestrictedFormOption {
                        value: value.value.clone(),
                        title: Some(value.title.clone()),
                        description: value.description.clone(),
                    })
                    .collect(),
                (None, None) => Vec::new(),
            };
            (
                DiscussRestrictedFormFieldType::String,
                schema.title.clone(),
                schema.description.clone(),
                schema
                    .default
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
                schema.pattern.clone(),
                schema.min_length.map(f64::from),
                schema.max_length.map(f64::from),
                options,
            )
        }
        ElicitationPropertySchema::Number(schema) => (
            DiscussRestrictedFormFieldType::Number,
            schema.title.clone(),
            schema.description.clone(),
            schema.default.map(|value| serde_json::json!(value)),
            None,
            schema.minimum,
            schema.maximum,
            Vec::new(),
        ),
        ElicitationPropertySchema::Integer(schema) => (
            DiscussRestrictedFormFieldType::Integer,
            schema.title.clone(),
            schema.description.clone(),
            schema.default.map(|value| serde_json::json!(value)),
            None,
            schema.minimum.map(|value| value as f64),
            schema.maximum.map(|value| value as f64),
            Vec::new(),
        ),
        ElicitationPropertySchema::Boolean(schema) => (
            DiscussRestrictedFormFieldType::Boolean,
            schema.title.clone(),
            schema.description.clone(),
            schema.default.map(|value| serde_json::json!(value)),
            None,
            None,
            None,
            Vec::new(),
        ),
        ElicitationPropertySchema::Array(schema) => {
            let options = match &schema.items {
                MultiSelectItems::String(items) if items.values.is_empty() => return None,
                MultiSelectItems::String(items) => items
                    .values
                    .iter()
                    .map(|value| DiscussRestrictedFormOption {
                        value: value.clone(),
                        title: None,
                        description: None,
                    })
                    .collect(),
                MultiSelectItems::Titled(items) if items.options.is_empty() => return None,
                MultiSelectItems::Titled(items) => items
                    .options
                    .iter()
                    .map(|option| DiscussRestrictedFormOption {
                        value: option.value.clone(),
                        title: Some(option.title.clone()),
                        description: option.description.clone(),
                    })
                    .collect(),
                _ => return None,
            };
            (
                DiscussRestrictedFormFieldType::Array,
                schema.title.clone(),
                schema.description.clone(),
                schema
                    .default
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
                None,
                schema.min_items.map(|value| value as f64),
                schema.max_items.map(|value| value as f64),
                options,
            )
        }
        _ => return None,
    };

    Some(DiscussRestrictedFormField {
        name: name.to_string(),
        title,
        description,
        field_type,
        options,
        default,
        required,
        pattern,
        min,
        max,
    })
}

/// Classify one ACP elicitation request without exposing SDK schema details.
pub(crate) fn classify_elicitation_schema(request: &CreateElicitationRequest) -> DiscussElicitationClassification {
    match &request.mode {
        ElicitationMode::Form(mode) => sdk_to_restricted_schema(mode)
            .map(|schema| DiscussElicitationClassification::RestrictedForm { schema })
            .unwrap_or_else(|| DiscussElicitationClassification::FormFallback {
                raw_schema: serde_json::to_value(&mode.requested_schema).unwrap_or(serde_json::Value::Null),
            }),
        ElicitationMode::Url(_) => DiscussElicitationClassification::UrlDeferred,
        ElicitationMode::Other(_mode) => DiscussElicitationClassification::OtherRawFallback {
            raw_schema: serde_json::to_value(request).unwrap_or(serde_json::Value::Null),
        },
        _ => DiscussElicitationClassification::OtherRawFallback {
            raw_schema: serde_json::to_value(request).unwrap_or(serde_json::Value::Null),
        },
    }
}

pub(super) fn validate_elicitation_content(
    classification: &DiscussElicitationClassification,
    content: &serde_json::Value,
) -> Result<(), DiscussError> {
    let DiscussElicitationClassification::RestrictedForm { schema } = classification else {
        if content
            .get("response")
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            return Err(DiscussError::ElicitationInvalidContent {
                message: "fallback elicitation content must contain a string response".to_string(),
            });
        }
        return Ok(());
    };

    let Some(object) = content.as_object() else {
        return Err(DiscussError::ElicitationInvalidContent {
            message: "restricted elicitation content must be an object".to_string(),
        });
    };
    for required in &schema.required {
        if !object.contains_key(required) {
            return Err(DiscussError::ElicitationInvalidContent {
                message: format!("missing required field `{required}`"),
            });
        }
    }
    for (name, value) in object {
        let Some(field) = schema.fields.iter().find(|field| field.name == *name) else {
            return Err(DiscussError::ElicitationInvalidContent {
                message: format!("unknown field `{name}`"),
            });
        };
        validate_elicitation_field(field, value)?;
    }
    Ok(())
}

fn validate_elicitation_field(
    field: &DiscussRestrictedFormField,
    value: &serde_json::Value,
) -> Result<(), DiscussError> {
    let valid_type = match field.field_type {
        DiscussRestrictedFormFieldType::String => value.is_string(),
        DiscussRestrictedFormFieldType::Number => value.is_number(),
        DiscussRestrictedFormFieldType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        DiscussRestrictedFormFieldType::Boolean => value.is_boolean(),
        DiscussRestrictedFormFieldType::Array => value.is_array(),
    };
    if !valid_type {
        return Err(DiscussError::ElicitationInvalidContent {
            message: format!("field `{}` has the wrong type", field.name),
        });
    }
    if !field.options.is_empty() {
        if field.field_type == DiscussRestrictedFormFieldType::Array {
            let Some(values) = value.as_array() else {
                return Err(DiscussError::ElicitationInvalidContent {
                    message: format!("field `{}` must contain an array of offered choices", field.name),
                });
            };
            if values.iter().any(|value| {
                value
                    .as_str()
                    .is_none_or(|value| !field.options.iter().any(|option| option.value == value))
            }) {
                return Err(DiscussError::ElicitationInvalidContent {
                    message: format!("field `{}` contains an unsupported choice", field.name),
                });
            }
        } else {
            let Some(value) = value.as_str() else {
                return Err(DiscussError::ElicitationInvalidContent {
                    message: format!("field `{}` must contain one of its offered choices", field.name),
                });
            };
            if !field.options.iter().any(|option| option.value == value) {
                return Err(DiscussError::ElicitationInvalidContent {
                    message: format!("field `{}` contains an unsupported choice", field.name),
                });
            }
        }
    }
    let measure = value.as_f64().unwrap_or_else(|| {
        value.as_str().map_or_else(
            || value.as_array().map_or(0.0, |value| value.len() as f64),
            |value| value.chars().count() as f64,
        )
    });
    if let Some(min) = field.min
        && measure < min
    {
        return Err(DiscussError::ElicitationInvalidContent {
            message: format!("field `{}` is below its minimum", field.name),
        });
    }
    if let Some(max) = field.max
        && measure > max
    {
        return Err(DiscussError::ElicitationInvalidContent {
            message: format!("field `{}` exceeds its maximum", field.name),
        });
    }
    Ok(())
}

use std::str::FromStr;

use chrono::{DateTime, Utc};
use croner::Cron;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use specta::Type;
use uuid::Uuid;

use super::types::{FlowCronError, FlowError, FlowId, FlowTimestamp};

/// A persisted adapter-backed Flow trigger and its source-owned configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub struct FlowTriggerRecord {
    /// Stable identity of this trigger.
    #[specta(type = String)]
    #[schemars(with = "String")]
    pub trigger_id: Uuid,
    /// Flow started by this trigger.
    pub flow_id: FlowId,
    /// Immutable Flow definition revision pinned by this trigger.
    pub revision: i64,
    /// The source-domain adapter used by this trigger.
    pub adapter_kind: String,
    /// Adapter configuration, retained opaquely by generic Aves Flow.
    pub adapter_config: serde_json::Value,
    /// Adapter state, retained opaquely by generic Aves Flow.
    pub state: Value,
    /// Whether this trigger is eligible to fire.
    pub enabled: bool,
    /// Persisted scheduler cursor, represented in host-local RFC3339 time.
    pub next_fire_at: Option<FlowTimestamp>,
    /// Last accepted occurrence, represented in host-local RFC3339 time.
    pub last_fired_at: Option<FlowTimestamp>,
}

impl FlowTriggerRecord {
    /// Decode Cron configuration only for Cron-owned triggers.
    pub(crate) fn cron_config(&self) -> Result<Option<CronTriggerConfig>, FlowError> {
        if self.adapter_kind != "cron" {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(self.adapter_config.clone())?))
    }
}

impl From<CronTriggerConfig> for serde_json::Value {
    fn from(configuration: CronTriggerConfig) -> Self {
        serde_json::json!({ "cron": configuration.cron })
    }
}

/// Validated configuration for a Cron trigger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub struct CronTriggerConfig {
    /// Five-field schedule used to calculate trigger occurrences.
    pub cron: CronExpression,
}

/// A validated five-field Cron expression.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Type, JsonSchema)]
pub struct CronExpression(String);

impl<'de> Deserialize<'de> for CronExpression {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl CronExpression {
    pub(super) fn parse(value: impl Into<String>) -> Result<Self, FlowCronError> {
        let value = value.into();
        parse_cron(&value).map(|_| Self(value))
    }
    pub(super) fn next_after(&self, after: &DateTime<Utc>) -> Result<FlowTimestamp, FlowCronError> {
        parse_cron(&self.0)?
            .find_next_occurrence(after, false)
            .map(FlowTimestamp::from_datetime)
            .map_err(|source| FlowCronError::NextOccurrence {
                expression: self.0.clone(),
                source,
            })
    }
}

fn parse_cron(expression: &str) -> Result<Cron, FlowCronError> {
    if expression.split_whitespace().count() != 5 {
        return Err(FlowCronError::InvalidExpression {
            expression: expression.to_string(),
        });
    }
    Cron::from_str(expression).map_err(|source| FlowCronError::Parse {
        expression: expression.to_string(),
        source,
    })
}

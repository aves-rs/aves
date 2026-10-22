use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::cron::{CronTriggerConfig, FlowTriggerRecord};
use super::types::{FlowId, FlowInvocationKind, TriggerRegistrationError};

/// The trusted start request produced by a registered source-domain trigger.
#[derive(Clone, Debug, Serialize)]
pub struct FlowStartRequest {
    pub flow_id: FlowId,
    pub blackboard_input: Value,
    pub adapter_kind: String,
    pub invocation_kind: FlowInvocationKind,
}

/// Erases each registered source payload to the typed start-request boundary.
trait TriggerAdapter: Send + Sync {
    fn start_request(&self, flow_id: FlowId, payload: &dyn Any) -> Result<FlowStartRequest, TriggerRegistrationError>;
}

struct TypedTriggerAdapter<T, F> {
    kind: String,
    invocation_kind: FlowInvocationKind,
    convert: F,
    // Preserve the prior function-argument variance and auto-trait behavior.
    marker: PhantomData<fn(T)>,
}

impl<T, F> TriggerAdapter for TypedTriggerAdapter<T, F>
where
    T: Serialize + Send + Sync + 'static,
    F: Fn(&T) -> Result<Value, TriggerRegistrationError> + Send + Sync + 'static,
{
    fn start_request(&self, flow_id: FlowId, payload: &dyn Any) -> Result<FlowStartRequest, TriggerRegistrationError> {
        let payload = payload
            .downcast_ref::<T>()
            .ok_or(TriggerRegistrationError::MessageTypeMismatch)?;
        Ok(FlowStartRequest {
            flow_id,
            blackboard_input: (self.convert)(payload)?,
            adapter_kind: self.kind.clone(),
            invocation_kind: self.invocation_kind,
        })
    }
}

/// Setup-time declaration of a typed source-domain trigger adapter.
pub struct TriggerAdapterRegistration {
    pub kind: String,
    adapter: Arc<dyn TriggerAdapter>,
}

impl TriggerAdapterRegistration {
    /// Builds a typed adapter that serializes source payloads into blackboard input.
    pub fn typed<T, F>(kind: impl Into<String>, invocation_kind: FlowInvocationKind, convert: F) -> Self
    where
        T: Serialize + Send + Sync + 'static,
        F: Fn(&T) -> Result<Value, TriggerRegistrationError> + Send + Sync + 'static,
    {
        let kind = kind.into();
        Self {
            adapter: Arc::new(TypedTriggerAdapter {
                kind: kind.clone(),
                invocation_kind,
                convert,
                marker: PhantomData,
            }),
            kind,
        }
    }
}

/// Flow-owned registry populated by source-domain setup declarations.
#[derive(Clone, Default)]
pub struct TriggerRegistry {
    adapters: HashMap<String, Arc<dyn TriggerAdapter>>,
    configurations: HashMap<String, ConfigurationValidator>,
}

type ConfigurationValidator = Arc<dyn Fn(&serde_json::Value) -> Result<(), TriggerRegistrationError> + Send + Sync>;

impl TriggerRegistry {
    /// Creates an empty registry for source-domain adapters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the source-owned typed configuration for persisted event triggers.
    pub fn register_configuration<T, F>(
        &mut self,
        kind: impl Into<String>,
        validate: F,
    ) -> Result<(), TriggerRegistrationError>
    where
        T: DeserializeOwned + Send + Sync + 'static,
        F: Fn(&T) -> Result<(), TriggerRegistrationError> + Send + Sync + 'static,
    {
        let kind = kind.into();
        if kind == "cron" || self.configurations.contains_key(&kind) {
            return Err(TriggerRegistrationError::DuplicateAdapter);
        }
        self.configurations.insert(
            kind,
            Arc::new(move |configuration| {
                let configuration: T = serde_json::from_value(configuration.clone())?;
                validate(&configuration)
            }),
        );
        Ok(())
    }

    /// Validate persisted configuration without importing the source domain.
    pub fn validate_configuration(&self, trigger: &FlowTriggerRecord) -> Result<(), TriggerRegistrationError> {
        if trigger.adapter_kind == "cron" {
            let _: CronTriggerConfig = serde_json::from_value(trigger.adapter_config.clone())?;
            return Ok(());
        }
        self.configurations
            .get(&trigger.adapter_kind)
            .ok_or(TriggerRegistrationError::UnknownAdapter)?(&trigger.adapter_config)
    }

    pub fn register(&mut self, registration: TriggerAdapterRegistration) -> Result<(), TriggerRegistrationError> {
        if self.adapters.contains_key(&registration.kind) {
            return Err(TriggerRegistrationError::DuplicateAdapter);
        }
        self.adapters
            .insert(registration.kind, registration.adapter);
        Ok(())
    }

    /// Converts a source payload into a start request using the registered adapter.
    pub fn start_request(
        &self,
        kind: &str,
        flow_id: FlowId,
        payload: &dyn Any,
    ) -> Result<FlowStartRequest, TriggerRegistrationError> {
        self.adapters
            .get(kind)
            .ok_or(TriggerRegistrationError::UnknownAdapter)?
            .start_request(flow_id, payload)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SourceConfiguration {
        target: String,
    }

    fn record(configuration: serde_json::Value) -> super::super::cron::FlowTriggerRecord {
        super::super::cron::FlowTriggerRecord {
            trigger_id: uuid::Uuid::new_v4(),
            flow_id: FlowId(uuid::Uuid::new_v4()),
            revision: 1,
            adapter_kind: "example".to_owned(),
            adapter_config: configuration,
            state: serde_json::json!({}),
            enabled: true,
            next_fire_at: None,
            last_fired_at: None,
        }
    }

    #[test]
    fn source_configuration_requires_a_registered_adapter_and_preserves_source_validation() {
        let mut registry = TriggerRegistry::new();
        let valid = record(serde_json::json!({"target": "inbox"}));
        assert!(matches!(
            registry.validate_configuration(&valid),
            Err(TriggerRegistrationError::UnknownAdapter)
        ));
        registry
            .register_configuration::<SourceConfiguration, _>("example", |configuration| {
                if configuration.target.is_empty() {
                    Err(TriggerRegistrationError::InvalidConfiguration {
                        reason: "missing target",
                    })
                } else {
                    Ok(())
                }
            })
            .unwrap();
        registry.validate_configuration(&valid).unwrap();
        assert!(
            registry
                .validate_configuration(&record(serde_json::json!({"target": ""})))
                .is_err()
        );
        assert!(
            registry
                .validate_configuration(&record(serde_json::json!({"unexpected": true})))
                .is_err()
        );
        assert!(matches!(
            registry.register_configuration::<SourceConfiguration, _>("example", |_| Ok(())),
            Err(TriggerRegistrationError::DuplicateAdapter)
        ));
    }
}

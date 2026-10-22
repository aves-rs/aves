use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;

use super::ast::BlackboardBinding;
use super::types::BlackboardError;

/// Flow-owned values exchanged between nodes.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct Blackboard {
    values: BTreeMap<String, Value>,
}

impl Blackboard {
    /// Creates a blackboard from persisted defaults.
    pub(super) fn new(values: BTreeMap<String, Value>) -> Self {
        Self { values }
    }

    /// Reads a value by its Flow-owned key.
    pub(super) fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    /// Writes a value under a Flow-owned key.
    pub(super) fn set(&mut self, key: impl Into<String>, value: Value) {
        self.values.insert(key.into(), value);
    }

    /// Exposes the complete state for Flow-owned snapshotting.
    pub(super) fn values(&self) -> &BTreeMap<String, Value> {
        &self.values
    }

    /// Selects only the named values configured for a node.
    pub(super) fn bind_inputs(&self, bindings: &[BlackboardBinding]) -> Result<BoundInputs, BlackboardError> {
        let mut inputs = BTreeMap::new();
        for binding in bindings {
            let value = self
                .get(&binding.key)
                .cloned()
                .ok_or_else(|| BlackboardError::MissingInput {
                    key: binding.key.clone(),
                })?;
            inputs.insert(binding.name.clone(), value);
        }
        Ok(BoundInputs { values: inputs })
    }

    /// Applies only values named by a node's output bindings.
    pub(super) fn patch_outputs(
        &mut self,
        bindings: &[BlackboardBinding],
        outputs: &BoundOutputs,
    ) -> Result<(), BlackboardError> {
        for (name, value) in &outputs.values {
            let binding = bindings
                .iter()
                .find(|binding| binding.name == *name)
                .ok_or_else(|| BlackboardError::UnknownOutput { name: name.clone() })?;
            self.set(binding.key.clone(), value.clone());
        }
        Ok(())
    }
}

/// The restricted input view sent to an owner.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub struct BoundInputs {
    #[serde(default)]
    values: BTreeMap<String, Value>,
}

impl BoundInputs {
    /// Returns the value exposed under an owner-visible name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// Returns the complete owner-visible input object.
    pub fn as_value(&self) -> Value {
        serde_json::to_value(&self.values).expect("JSON values serialize infallibly")
    }
}

/// Typed output names returned by a node owner.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub struct BoundOutputs {
    #[serde(default)]
    values: BTreeMap<String, Value>,
}

impl BoundOutputs {
    /// Builds an output patch from owner-visible names.
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Self { values }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_inputs_and_applies_only_declared_outputs() {
        let mut board = Blackboard::new(BTreeMap::from([(String::from("price"), serde_json::json!(42))]));
        let bindings = vec![BlackboardBinding {
            name: "input".into(),
            key: "price".into(),
        }];
        assert_eq!(
            board.bind_inputs(&bindings).unwrap().get("input"),
            Some(&serde_json::json!(42))
        );
        board
            .patch_outputs(
                &bindings,
                &BoundOutputs::new(BTreeMap::from([(String::from("input"), serde_json::json!(43))])),
            )
            .unwrap();
        assert_eq!(board.get("price"), Some(&serde_json::json!(43)));
        assert!(matches!(
            board.bind_inputs(&[BlackboardBinding {
                name: "missing".into(),
                key: "nope".into()
            }]),
            Err(BlackboardError::MissingInput { .. })
        ));
        assert!(matches!(
            board.patch_outputs(
                &bindings,
                &BoundOutputs::new(BTreeMap::from([(String::from("unknown"), serde_json::json!(1))]))
            ),
            Err(BlackboardError::UnknownOutput { .. })
        ));
    }
}

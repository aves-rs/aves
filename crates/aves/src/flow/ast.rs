use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use iroh_blobs::Hash;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use specta::Type;

use super::registration::NodeKind;
use super::types::FlowNodeId;

/// Finite fuel budget used when a ScriptNode omits its owner limit.
const DEFAULT_SCRIPT_FUEL_LIMIT: u64 = 1_000_000;
/// Finite wall-clock limit used when a ScriptNode omits its owner timeout.
const DEFAULT_SCRIPT_TIMEOUT_SECS: u64 = 30;

/// A content-addressed workspace root. Host paths are deliberately not part of
/// the persisted graph contract.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct ContentAddressedRoot {
    /// Immutable content digest identifying the root.
    pub digest: ContentHash,
}

/// A validated content-addressed blob hash.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, Type)]
pub struct ContentHash(String);

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<String> for ContentHash {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Hash::from_str(&value)
            .map(|_| Self(value))
            .map_err(|_| "invalid content hash".to_owned())
    }
}

impl ContentHash {
    /// Returns the validated hash's original textual representation.
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ContentHash {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A path within an immutable content-addressed root.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct ContentAddressedPath {
    /// Root containing the path.
    pub root: ContentAddressedRoot,
    /// Relative path within `root`.
    pub path: String,
}

/// The separate immutable code and state inputs available to a node.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct NodeDataSpace {
    /// Content-addressed code paths.
    pub code_paths: Vec<ContentAddressedPath>,
    /// Content-addressed state paths.
    pub state_paths: Vec<ContentAddressedPath>,
}

/// A bounded runtime constraint for a node.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConstraints {
    /// Maximum number of scheduler ticks permitted for one node execution.
    pub max_ticks: Option<u64>,
    /// Maximum execution duration in milliseconds.
    pub max_duration_ms: Option<u64>,
}

/// A declarative capability requested by a node.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequest {
    /// Stable capability name interpreted by the registering owner.
    pub name: String,
    /// Whether the capability is required rather than advisory.
    pub required: bool,
}

/// Runtime limits and owner-declared capability requests.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct NodeConstraints {
    /// Runtime limits enforced by Flow.
    pub runtime: RuntimeConstraints,
    /// Declarative capabilities checked during registration/composition.
    pub capabilities: Vec<CapabilityRequest>,
}

/// A named blackboard binding used by a typed node configuration.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct BlackboardBinding {
    /// Name visible to the node owner.
    pub name: String,
    /// Blackboard key supplying or receiving the value.
    pub key: String,
}

/// ScriptNode configuration owned by the Aves Tasks boundary.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct ScriptNodeConfig {
    /// Optional immutable source root; absent roots represent unavailable legacy scripts.
    pub source_root: Option<ContentAddressedRoot>,
    /// Entry point exported by the script.
    pub entry_point: String,
    /// Fuel budget; zero resolves to the finite owner default.
    pub fuel_limit: u64,
    /// Timeout in seconds; zero resolves to the finite owner default.
    pub timeout_secs: u64,
}

/// Resolves persisted ScriptNode limits to finite execution bounds.
pub(crate) fn resolve_script_limits(fuel_limit: u64, timeout_secs: u64) -> (u64, u64) {
    (
        if fuel_limit == 0 {
            DEFAULT_SCRIPT_FUEL_LIMIT
        } else {
            fuel_limit
        },
        if timeout_secs == 0 {
            DEFAULT_SCRIPT_TIMEOUT_SECS
        } else {
            timeout_secs
        },
    )
}

impl ScriptNodeConfig {
    /// Resolves zero-valued persisted limits without permitting unbounded execution.
    pub(crate) fn resolved_limits(&self) -> (u64, u64) {
        resolve_script_limits(self.fuel_limit, self.timeout_secs)
    }
}

/// OrderNode configuration owned by the Action boundary.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct OrderNodeConfig {
    /// Registered Action command name.
    pub command: String,
}

/// AgentNode configuration owned by the Discuss boundary.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AgentNodeConfig {
    /// Exact installed Provider identifier that owns the Agent.
    pub provider_name: String,
    /// Exact Agent identifier selected by the migrated definition.
    pub agent_name: String,
    /// Prompt sent to the non-interactive agent session.
    pub prompt: String,
    /// Optional bounded session turn limit.
    pub max_turns: Option<u64>,
    /// Optional bounded tool-call limit.
    pub max_tool_calls: Option<u64>,
    /// Optional bounded duration in seconds.
    #[serde(rename = "maxDuration")]
    pub max_duration_secs: Option<u64>,
    /// Sparse Provider/Agent configuration overrides.
    pub config_overrides: BTreeMap<String, String>,
    /// Whether the session may receive Sluice MCP tools.
    pub sluice_mcp_enabled: bool,
    /// Immutable workspace root materialized as the session CWD by Flow.
    pub workspace: Option<ContentAddressedRoot>,
}

/// A Flow-owned node envelope with opaque owner configuration.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Type)]
pub struct FlowNode {
    /// Stable graph identity.
    pub id: FlowNodeId,
    /// Human-readable editor label.
    pub label: String,
    /// Flow-owned constraints.
    pub constraints: NodeConstraints,
    /// Immutable code/state inputs.
    pub data_space: NodeDataSpace,
    /// Named inputs visible to the owner.
    pub inputs: Vec<BlackboardBinding>,
    /// Named outputs visible to the owner.
    pub outputs: Vec<BlackboardBinding>,
    /// Registered owner kind.
    pub node_kind: NodeKind,
    /// Owner-specific configuration, uninterpreted by Flow.
    pub configuration: Value,
}

#[derive(Serialize)]
struct SerializedFlowNode<'a> {
    id: FlowNodeId,
    label: &'a str,
    constraints: &'a NodeConstraints,
    data_space: &'a NodeDataSpace,
    inputs: &'a [BlackboardBinding],
    outputs: &'a [BlackboardBinding],
    node_kind: &'a NodeKind,
    configuration: &'a Value,
}

impl Serialize for FlowNode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SerializedFlowNode {
            id: self.id,
            label: &self.label,
            constraints: &self.constraints,
            data_space: &self.data_space,
            inputs: &self.inputs,
            outputs: &self.outputs,
            node_kind: &self.node_kind,
            configuration: &self.configuration,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FlowNode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Envelope {
            id: FlowNodeId,
            label: String,
            constraints: NodeConstraints,
            data_space: NodeDataSpace,
            #[serde(default)]
            inputs: Vec<BlackboardBinding>,
            #[serde(default)]
            outputs: Vec<BlackboardBinding>,
            node_kind: NodeKind,
            configuration: Value,
        }
        let envelope = Envelope::deserialize(deserializer)?;
        Ok(Self {
            id: envelope.id,
            label: envelope.label,
            constraints: envelope.constraints,
            data_space: envelope.data_space,
            inputs: envelope.inputs,
            outputs: envelope.outputs,
            node_kind: envelope.node_kind,
            configuration: envelope.configuration,
        })
    }
}

impl FlowNode {
    /// Returns the stable identity shared by all leaf variants.
    pub fn id(&self) -> FlowNodeId {
        self.id
    }

    /// Returns the persisted node label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns the node's explicit input and output bindings as borrowed slices.
    pub(super) fn bindings(&self) -> (&[BlackboardBinding], &[BlackboardBinding]) {
        (&self.inputs, &self.outputs)
    }

    /// Returns the node's borrowed Flow constraint configuration.
    pub(super) fn constraints(&self) -> &NodeConstraints {
        &self.constraints
    }

    /// Returns the node's borrowed immutable code and state inputs.
    pub(super) fn data_space(&self) -> &NodeDataSpace {
        &self.data_space
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn script_limits_default_to_finite_values() {
        assert_eq!(
            resolve_script_limits(0, 0),
            (DEFAULT_SCRIPT_FUEL_LIMIT, DEFAULT_SCRIPT_TIMEOUT_SECS)
        );
        assert_eq!(resolve_script_limits(17, 2), (17, 2));
    }

    #[test]
    fn node_envelopes_roundtrip_for_builtins_and_unknown_kinds() {
        let bindings = json!([{"name":"price","key":"market.price"}]);
        let cases = [
            (
                "ScriptNode",
                json!({"source_root":null,"entry_point":"main","fuel_limit":17,"timeout_secs":2}),
            ),
            ("OrderNode", json!({"command":"place"})),
            (
                "AgentNode",
                json!({"providerName":"test","agentName":"analyst","prompt":"review","maxTurns":3,"maxToolCalls":4,"maxDuration":5,"configOverrides":{},"sluiceMcpEnabled":false,"workspace":null}),
            ),
            ("EchoNode", json!({"message":"hello"})),
        ];
        for (kind, configuration) in cases {
            let envelope = json!({
                "id": "00000000-0000-4000-8000-000000000001",
                "label": kind,
                "constraints": {"runtime":{"max_ticks":null,"max_duration_ms":null},"capabilities":[]},
                "data_space": {"code_paths":[],"state_paths":[]},
                "inputs": bindings,
                "outputs": bindings,
                "node_kind": kind,
                "configuration": configuration,
            });
            let node: FlowNode = serde_json::from_value(envelope.clone()).unwrap();
            assert_eq!(node.node_kind.as_str(), kind);
            assert_eq!(serde_json::to_value(node).unwrap(), envelope);
        }
    }

    #[test]
    fn legacy_builtin_leaf_shapes_are_rejected() {
        for (kind, configuration) in [
            (
                "ScriptNode",
                json!({"entry_point":"main","fuel_limit":17,"timeout_secs":2}),
            ),
            ("OrderNode", json!({"command":"place"})),
            (
                "AgentNode",
                json!({"providerName":"test","agentName":"analyst","prompt":"review","configOverrides":{},"sluiceMcpEnabled":false}),
            ),
        ] {
            let mut legacy = configuration.as_object().unwrap().clone();
            legacy.insert("node_type".into(), json!(kind));
            legacy.insert("id".into(), json!("00000000-0000-4000-8000-000000000001"));
            legacy.insert("label".into(), json!(kind));
            legacy.insert("constraints".into(), json!({"runtime":{},"capabilities":[]}));
            legacy.insert("data_space".into(), json!({"code_paths":[],"state_paths":[]}));
            legacy.insert("inputs".into(), json!([]));
            legacy.insert("outputs".into(), json!([]));
            assert!(serde_json::from_value::<FlowNode>(Value::Object(legacy)).is_err());
        }
    }
}

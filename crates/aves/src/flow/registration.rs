use std::any::Any;
use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::error::Error as StdError;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

use crate::auth::Jwt;
use crate::bus::BusRpcContext;

use super::ast::{ContentAddressedRoot, FlowNode};
use super::blackboard::{BoundInputs, BoundOutputs};
use super::mcp::FlowMcpPorts;
use super::messages::LifecycleCorrelation;
use super::types::FlowRegistrationError;

/// Erased future returned by a registered owner lifecycle callback.
pub type NodeLifecycleFuture =
    Pin<Box<dyn Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>> + Send>>;

type LifecycleCallback =
    Arc<dyn Fn(NodeLifecycleContext, Box<dyn Any + Send + Sync>) -> NodeLifecycleFuture + Send + Sync>;

type ConfigDecoder = Arc<dyn Fn(Value) -> Result<Box<dyn Any + Send + Sync>, FlowRegistrationError> + Send + Sync>;

type TypedLifecycleCallback<Config> = Arc<
    dyn Fn(
            NodeLifecycleContext,
            Config,
        ) -> Pin<Box<dyn Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>> + Send>>
        + Send
        + Sync,
>;

type NodeContentRoots = Arc<dyn Fn(&Value) -> Vec<ContentAddressedRoot> + Send + Sync>;

type GraphSeeds =
    Arc<dyn Fn(Vec<Value>) -> OwnedBoxFuture<Result<Vec<WorkspaceSeed>, WorkspacePreparationError>> + Send + Sync>;

type OwnedBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// An open, validated identifier for a registered Flow node kind.
#[derive(Clone, Debug, Eq, Hash, PartialEq, specta::Type, schemars::JsonSchema)]
pub struct NodeKind(String);

impl NodeKind {
    /// Parses and validates a persisted or registered node-kind identifier.
    pub fn parse(value: impl Into<String>) -> Result<Self, FlowRegistrationError> {
        let value = value.into();
        if value.is_empty() {
            return Err(FlowRegistrationError::MalformedNodeKind);
        }
        if value
            .chars()
            .any(|character| character == '/' || character == '\\' || character.is_control())
        {
            return Err(FlowRegistrationError::MalformedNodeKind);
        }
        Ok(Self(value))
    }

    /// Returns the stable identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for NodeKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl Serialize for NodeKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Generic outcome of an owner start/tick/cancel exchange. Terminal outputs and state are owner-supplied accounting consumed by Flow.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeLifecycleResult {
    Running,
    Succeeded { outputs: BoundOutputs, state: Value },
    Failed { message: String, state: Value },
    Cancelled,
}

/// Inputs available to an owner dispatcher; the bus context remains Flow-authenticated.
pub struct NodeLifecycleContext {
    /// Correlation data for the lifecycle operation.
    pub correlation: LifecycleCorrelation,
    /// The execution root directory for the lifecycle operation.
    pub execution_root: PathBuf,
    /// Flow-owned node envelope; owners may read orchestration fields such as data space from it.
    pub node: FlowNode,
    /// Resolved blackboard input view for the node; empty on cancel dispatch.
    pub inputs: BoundInputs,
    /// Present only for start, and distinct from the envelope transport JWT.
    pub node_token: Option<Jwt>,
    /// The bus context remains Flow-authenticated.
    pub bus_context: BusRpcContext,
    /// Flow-execution-owned MCP listener shared by AgentNode workers in this run.
    pub mcp_ports: FlowMcpPorts,
    /// The owner state is passed through the lifecycle context, allowing owner callbacks to access it.
    pub owner_state: Option<Arc<dyn Any + Send + Sync>>,
}

/// Whether an owner can stop work without waiting for a terminal result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CancellationDisposition {
    /// The owner accepts cancellation and fences late results.
    Cancellable,
    /// The owner must be allowed to finish before composition can proceed.
    NonCancellable,
}

/// Whether a node may produce an externally visible side effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SideEffectDisposition {
    /// The node does not produce an external side effect.
    Pure,
    /// The node may produce an external side effect.
    SideEffecting,
}

/// Static contract metadata used by Flow composition policies.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeKindDescriptor {
    /// Registered leaf kind.
    pub kind: NodeKind,
    /// Cancellation behavior promised by the owner.
    pub cancellation: CancellationDisposition,
    /// Side-effect behavior promised by the owner.
    pub side_effects: SideEffectDisposition,
}

/// A registered node kind and its owner-declared message contract.
#[derive(Clone)]
pub struct NodeKindRegistration {
    /// Capability and node-kind metadata.
    pub descriptor: NodeKindDescriptor,
    decode: ConfigDecoder,
    start: LifecycleCallback,
    tick: LifecycleCallback,
    cancel: LifecycleCallback,
    owner_state: Option<Arc<dyn Any + Send + Sync>>,
    pub workspace: WorkspacePreparation,
}

impl NodeKindRegistration {
    /// Returns owner-declared content roots for one node configuration.
    pub fn node_content_roots(&self, configuration: &Value) -> Vec<ContentAddressedRoot> {
        self.workspace
            .node_content_roots
            .as_ref()
            .map_or_else(Vec::new, |hook| hook(configuration))
    }

    /// Resolves owner-selected source seeds for all matching graph nodes.
    pub async fn graph_seeds(
        &self,
        configurations: Vec<Value>,
    ) -> Result<Vec<WorkspaceSeed>, WorkspacePreparationError> {
        match &self.workspace.graph_seeds {
            Some(hook) => hook(configurations).await,
            None => Ok(Vec::new()),
        }
    }
    /// Declares a typed owner lifecycle contract without introducing owner types to Flow.
    pub fn typed<Config, Validate, Start, StartFuture, Tick, TickFuture, Cancel, CancelFuture>(
        descriptor: NodeKindDescriptor,
        validate: Validate,
        start: Start,
        tick: Tick,
        cancel: Cancel,
        owner_state: Option<Arc<dyn Any + Send + Sync>>,
        workspace: WorkspacePreparation,
    ) -> Self
    where
        Config: DeserializeOwned + Send + Sync + 'static,
        Validate: Fn(&Config) -> Result<(), FlowRegistrationError> + Send + Sync + 'static,
        Start: Fn(NodeLifecycleContext, Config) -> StartFuture + Send + Sync + 'static,
        StartFuture: Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>> + Send + 'static,
        Tick: Fn(NodeLifecycleContext, Config) -> TickFuture + Send + Sync + 'static,
        TickFuture: Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>> + Send + 'static,
        Cancel: Fn(NodeLifecycleContext, Config) -> CancelFuture + Send + Sync + 'static,
        CancelFuture: Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>> + Send + 'static,
    {
        let validate = Arc::new(validate);
        let decoder_validate = Arc::clone(&validate);
        let decode: ConfigDecoder = Arc::new(move |value| {
            let configuration: Config =
                serde_json::from_value(value).map_err(|error| FlowRegistrationError::Configuration {
                    message: error.to_string(),
                })?;
            decoder_validate(&configuration)?;
            Ok(Box::new(configuration))
        });
        let erase = |callback: TypedLifecycleCallback<Config>| {
            Arc::new(
                move |context: NodeLifecycleContext, configuration: Box<dyn Any + Send + Sync>| match configuration
                    .downcast::<Config>(
                ) {
                    Ok(configuration) => callback(context, *configuration),
                    Err(_) => Box::pin(async { Err(NodeLifecycleDispatchError::ConfigurationTypeMismatch) })
                        as NodeLifecycleFuture,
                },
            ) as LifecycleCallback
        };
        let start = erase(Arc::new(move |context, configuration| {
            Box::pin(start(context, configuration))
        }));
        let tick = erase(Arc::new(move |context, configuration| {
            Box::pin(tick(context, configuration))
        }));
        let cancel = erase(Arc::new(move |context, configuration| {
            Box::pin(cancel(context, configuration))
        }));
        Self {
            descriptor,
            decode,
            start,
            tick,
            cancel,
            owner_state,
            workspace,
        }
    }

    /// Decodes and owner-validates persisted JSON into an erased typed value.
    pub fn decode_configuration(&self, value: Value) -> Result<Box<dyn Any + Send + Sync>, FlowRegistrationError> {
        (self.decode)(value)
    }

    /// Invokes the typed start dispatcher; the callback owns request/envelope construction and reply projection.
    pub fn dispatch_start(
        &self,
        context: NodeLifecycleContext,
        configuration: Box<dyn Any + Send + Sync>,
    ) -> NodeLifecycleFuture {
        self.dispatch(&self.start, context, configuration)
    }

    /// Invokes the typed tick dispatcher.
    pub fn dispatch_tick(
        &self,
        context: NodeLifecycleContext,
        configuration: Box<dyn Any + Send + Sync>,
    ) -> NodeLifecycleFuture {
        self.dispatch(&self.tick, context, configuration)
    }

    /// Invokes the explicitly registered cancel dispatcher.
    pub fn dispatch_cancel(
        &self,
        context: NodeLifecycleContext,
        configuration: Box<dyn Any + Send + Sync>,
    ) -> NodeLifecycleFuture {
        self.dispatch(&self.cancel, context, configuration)
    }

    fn dispatch(
        &self,
        callback: &LifecycleCallback,
        mut context: NodeLifecycleContext,
        configuration: Box<dyn Any + Send + Sync>,
    ) -> NodeLifecycleFuture {
        context.owner_state = self.owner_state.clone();
        callback(context, configuration)
    }
}

/// Boot-time supply through which owner domains deliver node-kind registrations to Flow.
#[derive(Clone, Default)]
pub struct NodeRegistrationSupply(Arc<Mutex<Vec<NodeKindRegistration>>>);

impl NodeRegistrationSupply {
    /// Adds an owner-declared lifecycle registration for Flow's construction-time registry.
    pub fn push(&self, registration: NodeKindRegistration) {
        self.0
            .lock()
            .expect("node registration supply lock poisoned")
            .push(registration);
    }

    pub(crate) fn drain(&self) -> Vec<NodeKindRegistration> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .expect("node registration supply lock poisoned"),
        )
    }
}

/// Owner-resolved directory seeds copied into a fresh execution workspace.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkspaceSeed {
    /// Source workspace root.
    pub source_root: PathBuf,
    /// Relative paths selected by the owner.
    pub relative_paths: BTreeSet<String>,
}

/// Owner workspace hooks retained by a node registration.
#[derive(Clone, Default)]
pub struct WorkspacePreparation {
    node_content_roots: Option<NodeContentRoots>,
    graph_seeds: Option<GraphSeeds>,
}

impl WorkspacePreparation {
    /// Creates an empty set of owner workspace hooks.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a typed configuration projection; malformed values return no roots and are validated at dispatch.
    pub fn node_content_roots<Config>(
        mut self,
        f: impl Fn(&Config) -> Vec<ContentAddressedRoot> + Send + Sync + 'static,
    ) -> Self
    where
        Config: DeserializeOwned + 'static,
    {
        self.node_content_roots = Some(Arc::new(move |value| {
            serde_json::from_value::<Config>(value.clone()).map_or_else(|_| Vec::new(), |config| f(&config))
        }));
        self
    }

    /// Adds a graph-wide typed seed resolver.
    pub fn graph_seeds<Config, F, Fut>(mut self, f: F) -> Self
    where
        Config: DeserializeOwned + Send + 'static,
        F: Fn(Vec<Config>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<WorkspaceSeed>, WorkspacePreparationError>> + Send + 'static,
    {
        self.graph_seeds = Some(Arc::new(move |values| {
            let configurations = values
                .into_iter()
                .map(|value| {
                    serde_json::from_value(value).map_err(|error| WorkspacePreparationError::Configuration {
                        message: error.to_string(),
                    })
                })
                .collect::<Result<Vec<Config>, _>>();
            match configurations {
                Ok(configurations) => Box::pin(f(configurations)),
                Err(error) => Box::pin(async move { Err(error) }),
            }
        }));
        self
    }
}

/// Flow-owned registry of host lifecycle contracts.
#[derive(Clone, Default)]
pub(super) struct FlowNodeRegistry {
    registrations: Arc<HashMap<NodeKind, NodeKindRegistration>>,
}

impl FlowNodeRegistry {
    /// Creates an empty registry for setup-time owner registrations.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Adds one owner contract; duplicate registration fails explicitly.
    pub(super) fn register(&mut self, registration: NodeKindRegistration) -> Result<(), FlowRegistrationError> {
        let entries = Arc::make_mut(&mut self.registrations);
        let kind = registration.descriptor.kind.clone();
        match entries.entry(kind.clone()) {
            Entry::Occupied(_) => Err(FlowRegistrationError::Duplicate(kind)),
            Entry::Vacant(entry) => {
                entry.insert(registration);
                Ok(())
            }
        }
    }

    /// Resolves a persisted kind without silently dropping malformed dispatch.
    pub(super) fn get(&self, kind: &NodeKind) -> Result<&NodeKindRegistration, FlowRegistrationError> {
        self.registrations
            .get(kind)
            .ok_or_else(|| FlowRegistrationError::Unregistered(kind.clone()))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &NodeKindRegistration> {
        self.registrations.values()
    }
}

/// Typed failures preserved across the erased owner-dispatch boundary.
#[derive(Debug, Error)]
pub enum NodeLifecycleDispatchError {
    #[error("decoded configuration did not match this registration")]
    ConfigurationTypeMismatch,
    #[error("owner request could not be sent")]
    Send(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("owner reply channel was dropped")]
    ReplyDropped,
    #[error("owner rejected lifecycle request")]
    Owner(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Typed failure while a registration prepares workspace inputs.
#[derive(Debug, Error)]
pub enum WorkspacePreparationError {
    #[error("Flow workspace source service is unavailable")]
    ServiceUnavailable,
    #[error("Flow workspace configuration could not be decoded: {message}")]
    Configuration { message: String },
    #[error("Flow workspace source resolution failed: {message}")]
    Resolution {
        message: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct ExampleConfiguration {
        target: String,
    }

    fn registration() -> NodeKindRegistration {
        let descriptor = NodeKindDescriptor {
            kind: NodeKind::parse("example").unwrap(),
            cancellation: CancellationDisposition::NonCancellable,
            side_effects: SideEffectDisposition::SideEffecting,
        };
        let callback = |_, _: ExampleConfiguration| async { Ok(NodeLifecycleResult::Running) };
        NodeKindRegistration::typed::<ExampleConfiguration, _, _, _, _, _, _, _>(
            descriptor,
            |configuration| {
                if configuration.target.is_empty() {
                    Err(FlowRegistrationError::Configuration {
                        message: "target is empty".to_owned(),
                    })
                } else {
                    Ok(())
                }
            },
            callback,
            callback,
            callback,
            None,
            WorkspacePreparation::default(),
        )
    }

    #[test]
    fn typed_registration_decodes_and_owner_validates_configuration() {
        let registration = registration();
        let decoded = registration
            .decode_configuration(serde_json::json!({"target": "inbox"}))
            .unwrap();
        assert_eq!(
            decoded
                .downcast_ref::<ExampleConfiguration>()
                .unwrap()
                .target,
            "inbox"
        );
        assert!(
            registration
                .decode_configuration(serde_json::json!({"target": ""}))
                .is_err()
        );
        assert!(
            registration
                .decode_configuration(serde_json::json!({"unknown": true}))
                .is_err()
        );
    }

    #[test]
    fn rejects_unknown_and_duplicate_owner_registrations() {
        assert_eq!(NodeKind::parse(""), Err(FlowRegistrationError::MalformedNodeKind));
        assert_eq!(
            NodeKind::parse("bad/kind"),
            Err(FlowRegistrationError::MalformedNodeKind)
        );
        assert_eq!(NodeKind::parse("Bogus").unwrap().as_str(), "Bogus");
        let registration = registration();
        let mut registry = FlowNodeRegistry::new();
        registry.register(registration.clone()).unwrap();
        assert!(matches!(
            registry.register(registration),
            Err(FlowRegistrationError::Duplicate(kind)) if kind.as_str() == "example"
        ));
        assert_eq!(
            registry
                .get(&NodeKind::parse("example").unwrap())
                .unwrap()
                .descriptor
                .kind
                .as_str(),
            "example"
        );
        assert!(
            matches!(registry.get(&NodeKind::parse("AgentNode").unwrap()), Err(FlowRegistrationError::Unregistered(kind)) if kind.as_str() == "AgentNode")
        );
    }

    #[test]
    fn node_kind_identifiers_are_open_but_reject_malformed_paths_and_controls() {
        for malformed in ["", "a/b", "a\\b", "bad\nkind", "bad\0kind"] {
            assert_eq!(
                NodeKind::parse(malformed),
                Err(FlowRegistrationError::MalformedNodeKind)
            );
        }
        for valid in ["EchoNode", "Écho节点"] {
            assert_eq!(NodeKind::parse(valid).unwrap().as_str(), valid);
        }
    }

    #[test]
    fn duplicate_registration_does_not_replace_first_contract() {
        let original = registration();
        let replacement = NodeKindRegistration::typed::<ExampleConfiguration, _, _, _, _, _, _, _>(
            original.descriptor.clone(),
            |_| Ok(()),
            |_, _: ExampleConfiguration| async { Ok(NodeLifecycleResult::Cancelled) },
            |_, _: ExampleConfiguration| async { Ok(NodeLifecycleResult::Cancelled) },
            |_, _: ExampleConfiguration| async { Ok(NodeLifecycleResult::Cancelled) },
            None,
            WorkspacePreparation::default(),
        );
        let mut registry = FlowNodeRegistry::new();
        registry.register(original).unwrap();
        assert!(matches!(
            registry.register(replacement),
            Err(FlowRegistrationError::Duplicate(_))
        ));
        let decoded = registry
            .get(&NodeKind::parse("example").unwrap())
            .unwrap()
            .decode_configuration(serde_json::json!({"target":""}));
        assert!(matches!(decoded, Err(FlowRegistrationError::Configuration { .. })));
    }
}

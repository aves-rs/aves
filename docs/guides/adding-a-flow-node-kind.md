---
title: Adding a Flow Node Kind
type: guide
updated: 2026-10-23
references:
  - AGENTS.md
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/flow/ast.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/tasks/handle.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/corv/src/agent_nodes.rs
---

# Adding a Flow Node Kind

Add a new node kind from an owner domain by supplying a `NodeKindRegistration` through the shared `NodeRegistrationSupply`. No Flow-core edits are required.

## Prerequisites

- An owner domain that can publish a typed bus request for start/tick/cancel (or perform the work in-process).
- A typed configuration struct implementing `DeserializeOwned + Send + Sync + 'static`.
- A decision on `CancellationDisposition` (cancellable vs non-cancellable) and `SideEffectDisposition` (pure vs side-effecting).
- Access to the shared `NodeRegistrationSupply` threaded through `ConstructionInputs` by the composition root.

## Steps

### 1. Define the typed configuration

In the owner domain, declare a `#[derive(Deserialize, Serialize, Clone, Debug)]` config struct with `#[serde(deny_unknown_fields)]`. This is the type the registration will decode from the envelope's opaque JSON. The real `ScriptNodeConfig` in `crates/aves/src/flow/ast.rs:118-129` shows the shape:

```rust
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Type)]
#[serde(deny_unknown_fields)]
pub struct ScriptNodeConfig {
    pub source_root: Option<ContentAddressedRoot>,
    pub entry_point: String,
    pub fuel_limit: u64,
    pub timeout_secs: u64,
}
```

Do not include `inputs` or `outputs` — bindings live on the Flow-owned envelope.

### 2. Build the registration

Use `NodeKindRegistration::typed` with the eight type parameters (Config, Validate, Start, StartFuture, Tick, TickFuture, Cancel, CancelFuture). The descriptor carries the validated `NodeKind`, the two dispositions, and nothing else. The real `script_node_registration` in `crates/aves/src/tasks/adapter.rs:26-31` shows the descriptor and owner state construction:

```rust
let descriptor = NodeKindDescriptor {
    kind: script_node_kind(),
    cancellation: CancellationDisposition::Cancellable,
    side_effects: SideEffectDisposition::Pure,
};
let owner_state = Arc::new(ScriptNodeOwnerState { lifecycle, node_bus });
```

The full registration function at `crates/aves/src/tasks/adapter.rs:22-120` invokes `NodeKindRegistration::typed::<ScriptNodeConfig, _, _, _, _, _, _, _>(descriptor, validate, start, tick, cancel, Some(owner_state), workspace)` where `validate` is `Fn(&Config) -> Result<(), FlowRegistrationError>`, and `start`/`tick`/`cancel` are `Fn(NodeLifecycleContext, Config) -> Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>>`.

### 3. Supply owner state when needed

If the cancel path needs owner-local state (e.g. a session map plus a streaming service reader), wrap it in an `Arc<dyn Any + Send + Sync>` and pass it as the `owner_state` argument. Downcast it inside each callback via `context.owner_state.as_ref().and_then(|s| s.downcast_ref::<ScriptNodeOwnerState>())`.

The existing `AgentNode` registration (`crates/corv/src/agent_nodes.rs`) demonstrates this pattern: the `AgentNodeOwnerState` holds the lifecycle service, the streaming reader, the copy-source port, and the session map, all constructed during Discuss-domain construction.

### 4. Push the registration into the shared supply

In the owner domain's `Handle::new` (the same file that publishes the domain's lifecycle service), push the registration into the `NodeRegistrationSupply` received from `ConstructionInputs`:

```rust
node_registration_supply.push(script_node_registration(lifecycle_service_impl.clone(), node_bus));
```

The embedding application constructs one `NodeRegistrationSupply::default()` and threads it through `ConstructionInputs`. Each owner domain reads it via `facade.input::<NodeRegistrationSupply>()` and pushes before Flow's adapter builds. Owners MUST be constructed before FlowDomain because `Actor::new_with_recovery` drains the supply via `mem::take`; pushes after that take are not registered.

### 5. Add workspace hooks if needed

If the kind needs content-addressed roots or graph-wide seeds, chain `.node_content_roots::<ScriptNodeConfig>(...)` and/or `.graph_seeds::<ScriptNodeConfig, _, _>(...)` onto `WorkspacePreparation::new()`. `graph_seeds` runs BEFORE workspace materialization; a resolution failure cannot leak a materialized directory. `WorkspacePreparationError` retains typed `ServiceUnavailable` / `Configuration` / `Resolution { source }` identity.

### 6. Verify

```bash
direnv exec . cargo test --package aves flow::
```

Confirm valid config decodes, invalid config rejects with `FlowRegistrationError::Configuration`, duplicates produce `FlowRegistrationError::Duplicate`, malformed identifiers produce `FlowRegistrationError::MalformedNodeKind`, and start/tick/cancel round-trip through `NodeLifecycleResult` with terminal state persisted.

## Known pitfalls

| Issue | Fix |
|---|---|
| Bindings duplicated inside owner config | Remove them; read `context.node.inputs` / `context.node.outputs` |
| Non-cancellable kind omits disposition | Declare `CancellationDisposition::NonCancellable` explicitly |
| Workspace hook `Config` differs from lifecycle `Config` | Use the same type for both; a mismatch silently yields empty roots (F7 NO FIX) |
| Owner state downcast fails at dispatch | Return `NodeLifecycleDispatchError::Owner` with a typed source, not `io::Error::other` |

## Files to edit

- Owner domain's `handle.rs` (push registration into `NodeRegistrationSupply`)
- Owner domain's `actor.rs` or equivalent (define the registration function)
- Owner domain's `types.rs` (define the typed configuration struct)

No edits to:
- `crates/aves/src/flow/*` (Flow core)
- `crates/aves/src/migrations/*` (no database migration)

## Related

- [Pluggable Flow Nodes](../concepts/pluggable-flow-nodes.md)
- [Pluggable Flow Nodes Architecture](../architecture/aves/pluggable-flow-nodes.md)
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- `crates/aves/AGENTS.md`

---
title: Pluggable Flow Nodes
type: concept
updated: 2026-10-23
references:
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/flow/ast.rs
  - crates/aves/src/flow/adapter.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/tasks/handle.rs
---

# Pluggable Flow Nodes

Flow nodes are an open, registered extension point: Flow owns the envelope and dispatch; owner domains supply typed lifecycle contracts at boot. Adding a new kind requires no Flow-core edits.

## Key points

- **Open validated identifier.** `NodeKind` is a newtype over a non-empty string that rejects only `/`, `\`, and control characters; `my:kind` and `my.kind` are valid. Malformed identifiers produce `FlowRegistrationError::MalformedNodeKind`.
- **Common envelope.** `FlowNode` carries Flow-owned fields (id, label, constraints, data-space, inputs, outputs) plus the open `node_kind` and opaque JSON `configuration`; owners no longer duplicate bindings inside their config.
- **Typed registration.** `NodeKindRegistration::typed<Config, Validate, Start, StartFuture, Tick, TickFuture, Cancel, CancelFuture>` binds the identifier to a typed config decoder, owner validation, three lifecycle callbacks, explicit `CancellationDisposition` and `SideEffectDisposition`, optional erased `owner_state`, and `WorkspacePreparation` hooks.
- **Boot-time supply.** Owner domains push registrations into a shared `NodeRegistrationSupply` during their own construction; `Actor::new_with_recovery` drains it into `FlowNodeRegistry` before the adapter is built. Duplicate identifiers produce `FlowRegistrationError::Duplicate`.
- **Generic dispatch.** `HostLifecycleAdapter` resolves the registration, decodes configuration, and invokes the typed callback; `NodeLifecycleDispatchError` distinguishes `ConfigurationTypeMismatch`, `Send`, `ReplyDropped`, and `Owner`.
- **Envelope-only persistence.** Reads and writes require the `node_kind` + `configuration` envelope. Historical entire-leaf `node_type` data must be converted before it can load; there is no automatic compatibility reader or global schema migration.

## How it works

```text
Embedding application
  │  constructs NodeRegistrationSupply::default(), threads through ConstructionInputs
  ▼
Owner domain handles (e.g. Tasks Handle::new)
  │  read supply from facade.input::<NodeRegistrationSupply>(), push registrations
  ▼
Actor::new_with_recovery (actor.rs:143-149)
  │  drains supply via mem::take → FlowNodeRegistry (duplicate → typed error)
  ▼
HostLifecycleAdapter::start/tick/cancel (adapter.rs)
  │  registry.get(kind) → decode_configuration → dispatch_start/tick/cancel
  ▼
Owner callback
  │  constructs owner's typed bus request, awaits reply, converts to NodeLifecycleResult
  ▼
Flow actor
   maps to terminal/pending state, persists envelope, schedules next tick
```

## How to use it

To add a new node kind from an owner domain, construct a `NodeKindRegistration` and push it into the shared `NodeRegistrationSupply` before Flow's adapter builds. The real ScriptNode registration in `crates/aves/src/tasks/adapter.rs:26-31` shows the descriptor and owner state construction:

```rust
let descriptor = NodeKindDescriptor {
    kind: script_node_kind(),
    cancellation: CancellationDisposition::Cancellable,
    side_effects: SideEffectDisposition::Pure,
};
let owner_state = Arc::new(ScriptNodeOwnerState { lifecycle, node_bus });
```

The full registration invokes `NodeKindRegistration::typed::<ScriptNodeConfig, _, _, _, _, _, _, _>(descriptor, validate, start, tick, cancel, Some(owner_state), workspace)` where `validate` is `Fn(&Config) -> Result<(), FlowRegistrationError>`, and `start`/`tick`/`cancel` are `Fn(NodeLifecycleContext, Config) -> Future<Output = Result<NodeLifecycleResult, NodeLifecycleDispatchError>>`. The owner domain's `Handle::new` pushes the registration into `node_registration_supply`. See [Adding a Flow Node Kind](../guides/adding-a-flow-node-kind.md) for the full step-by-step.

## Known pitfalls

- **Bindings belong on the envelope.** Owners must not redeclare `inputs`/`outputs` inside their config; read them from `context.node.inputs` / `context.node.outputs`.
- **Cancellation is explicit.** `CancellationDisposition::NonCancellable` must be declared; omission is not a default.
- **Transport JWT vs node token.** The envelope's transport JWT is the Flow domain's authenticated `BusRpcContext` for all three operations; the node-scoped token carries `AuthCapability::AllBus` and is issued only at Start (tick/cancel carry `node_token: None`).
- **Workspace hook config type (F7 NO FIX).** `WorkspacePreparation::node_content_roots<Config>` is a separate type parameter from the registration's lifecycle `Config`; a mismatch silently yields empty roots.

## Related

- [Pluggable Flow Nodes Architecture](../architecture/aves/pluggable-flow-nodes.md)
- [Adding a Flow Node Kind](../guides/adding-a-flow-node-kind.md)
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- `crates/aves/AGENTS.md`

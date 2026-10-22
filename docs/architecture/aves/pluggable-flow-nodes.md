---
title: Pluggable Flow Nodes Architecture
type: architecture
updated: 2026-10-23
references:
  - AGENTS.md
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/flow/ast.rs
  - crates/aves/src/flow/adapter.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/flow/workspace.rs
  - crates/aves/src/flow/database.rs
  - crates/aves/src/flow/graph.rs
  - crates/aves/src/flow/catalog.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/corv/src/agent_nodes.rs
  - crates/corv/src/handle.rs
  - crates/aves/src/tasks/handle.rs
---

# Pluggable Flow Nodes Architecture

Aves Flow owns an open, validated `NodeKind` identifier and a common node envelope; owner domains supply complete `NodeKindRegistration` contracts through `NodeRegistrationSupply` at boot. Flow never imports owner request types; owners never edit Flow core to add a kind.

## Ownership and boundaries

- **Flow owns**: the open `NodeKind` newtype (validated at parse and persistence decode), the common `FlowNode` envelope (Flow-owned id/label/constraints/data-space/bindings plus kind and opaque JSON configuration), the registry (`FlowNodeRegistry`), generic dispatch, workspace materialization/checkpoint, and the shared execution directory.
- **Owner domains own**: typed configuration decode and validation, the three lifecycle callbacks (start/tick/cancel), owner-result → `NodeLifecycleResult` conversion, explicit `CancellationDisposition` and `SideEffectDisposition`, and workspace-preparation hooks (`node_content_roots`, `graph_seeds`).
- **Embedding applications compose** a single `NodeRegistrationSupply` shared across owner domains; each owner pushes its registration before Flow's adapter is built.

## Data flow

1. **Boot supply.** The embedding application constructs one `NodeRegistrationSupply::default()` and threads it through `ConstructionInputs`. Tasks' `Handle::new` (`crates/aves/src/tasks/handle.rs`) pushes `script_node_registration`; Discuss's constructor (`crates/corv/src/handle.rs`) pushes `agent_node_registration`. Flow's `Actor::new_with_recovery` (`crates/aves/src/flow/actor.rs`) drains the supply into `FlowNodeRegistry` during Flow construction. Owners MUST be constructed before FlowDomain because pushes after the drain are not registered.
2. **Dispatch.** `HostLifecycleAdapter::start/tick/cancel` (`crates/aves/src/flow/adapter.rs`) resolves the registration via `registry.get(kind)`, decodes the owner configuration, and invokes the registration's typed callback. The envelope's transport JWT is the Flow domain's authenticated `BusRpcContext` for all three operations; the node-scoped token carries `AuthCapability::AllBus` and is issued only at Start (tick/cancel carry `node_token: None`).
3. **Typed error propagation.** `NodeLifecycleDispatchError` distinguishes `ConfigurationTypeMismatch`, `Send`, `ReplyDropped`, and `Owner` — replacing the prior lossy `Option` shape. `map_dispatch_error` converts these to stable `FlowAdapterError` variants.
4. **Workspace.** `Actor::prepare_start_workspace` resolves `workspace_seeds_for_graph` BEFORE `prepare_execution_workspace`, so seed-resolution failure cannot leak a materialized directory. `WorkspacePreparationError` retains typed `ServiceUnavailable` / `Configuration` / `Resolution { source }` identity.

## Persistence

- **Envelope only.** `database.rs` writes the open identifier string and the envelope serialization; `node_kind` TEXT is already unconstrained (migration 0001), so no table rebuild or new migration version is needed.
- **Envelope-only load.** The `FlowNode` deserializer (`crates/aves/src/flow/ast.rs`) requires `node_kind` and `configuration`. Historical `node_type` leaves are no longer accepted; previously stored revisions must be converted before using this reader without changing revision IDs or adding a schema migration.

## Missing-kind handling

- **Boot isolation.** `Actor::new` recovers each interrupted execution individually; `FlowError::UnregisteredNodeKind` terminally marks the affected execution `Failed` without failing boot or unrelated Flows.
- **Start rejection.** `begin_start` resolves every node's registration before dispatch; an unregistered kind produces a typed `FlowError::UnregisteredNodeKind` before any node runs.
- **Read-only preservation.** Definitions containing unregistered kinds load and remain intact; unknown kinds round-trip through persistence unchanged. Save-path validation is intentionally omitted so read-only preservation and mobility import keep working; validation runs at ENABLE, START, and RECOVERY only.

## Caveats

- **Workspace hook config type mismatch (F7 NO FIX).** `WorkspacePreparation::node_content_roots<Config>` and the registration's lifecycle `Config` are independent type parameters; a mismatched hook silently decodes to empty roots rather than failing construction. No built-in currently mismatches.

## Related

- [Pluggable Flow Nodes](../../concepts/pluggable-flow-nodes.md)
- [Adding a Flow Node Kind](../../guides/adding-a-flow-node-kind.md)
- [Aves Flow Domain Architecture](flow-domain.md)
- `crates/aves/AGENTS.md`

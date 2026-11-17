---
title: Flow Workspace Trait
type: concept
updated: 2026-11-17
references:
  - crates/aves/src/flow/hook.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/flow/handle.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/ast.rs
  - crates/aves/src/flow/checkpoints.rs
  - crates/aves/src/flow/mobility.rs
  - crates/aves/src/flow/mod.rs
  - crates/aves/src/auth/types.rs
  - crates/aves/AGENTS.md
---

# Flow Workspace Trait

`FlowAppWorkspaceHook` is Flow's complete workspace system. The embedding application owns it end to end: Aves ships no default workspace implementation and never runs one alongside an application hook.

## Construction

`FlowBootOptions.workspace_hook` is a **required** `FlowWorkspaceHookFactory`. `FlowBootOptions::open(config_dir, workspace_hook)` opens and migrates the Flow database and keeps the factory; there is no implicit default.

```rust
pub type FlowWorkspaceHookFactory =
    Arc<dyn Fn(FlowWorkspaceHookBoot) -> FlowWorkspaceHookFuture + Send + Sync>;
pub type FlowWorkspaceHookFuture =
    Pin<Box<dyn Future<Output = Arc<dyn FlowAppWorkspaceHook>> + Send>>;
```

`handle.rs` assembles the node registry first, then calls the factory once with `FlowWorkspaceHookBoot` — data root, open database, the assembled node registry, the workspace source service and the bus context — and hands the resulting hook to the actor.

There is one hook for the Flow domain, not one per Flow and not one per execution. The factory may capture application state (a shared `Arc`, for example) and use it in every hook call.

## Context

`FlowAppHookContext` carries execution id, flow id, definition revision, execution root and invocation kind; application-owned dispatch routes on it. `FlowAppLandingContext` adds terminal status, node correlations, workspace mappings and the imported workspace root.

## Operations

- `takeoff` prepares the workspace and returns `FlowAppTakeoffResult` (dispatch root, optional imported root). Required; a failure fails Start with `FlowError::AppWorkspaceTakeoff`.
- `checkpoint` captures the workspace and returns `FlowAppCheckpointResult` (optional checkpoint root, path-to-hash projection). The default capture is empty. It runs at start and at every transition, before the snapshot is durable.
- `checkpoint_committed` finalizes a capture once its snapshot has been saved.
- `restore` returns the root used for dispatch. The default keeps the context's own root.
- `landing` runs after the durable terminal snapshot, for every outcome. Its failure is logged and does not block terminalization.
- `release_artifacts` runs during retention cleanup after a replay of `landing`; implementations must tolerate repeated calls.
- Import, download and script source default to `WorkspaceError::ProviderUnavailable`. Artifact projection, `shutdown` and `checkpoint_committed` default to `Ok(())`, `recoverable` to `Ok(true)`, and `execution_root` returns the supplied context root. The default mobility operations reject a non-empty transfer with `FlowError::AppWorkspaceMobilityUnsupported`.

## Reading the hook's own types

The hook receives Aves types, not projections of them. `ContentHash::as_str()` exposes the validated hash's textual form, so an application can persist the path-to-hash projection it returns from `checkpoint` and read it back from `restore`. `MachineIssuerId::public_key()` exposes the issuer identity that `import_mobility` receives as the transfer source. Both are public for that reason; neither widens any wire contract.

## Example

```rust
let shared = shared_state.clone();
let workspace_hook: FlowWorkspaceHookFactory = Arc::new(move |boot: FlowWorkspaceHookBoot| {
    let shared = shared.clone();
    Box::pin(async move {
        Arc::new(MyWorkspace::new(shared, boot)) as Arc<dyn FlowAppWorkspaceHook>
    })
});
let options = FlowBootOptions::open(config_dir, workspace_hook).await?;
```

## Related

- [Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Flow Execution Workspace](flow-execution-workspace.md)
- [Pluggable Flow Nodes](../architecture/aves/pluggable-flow-nodes.md)
- [crates/aves/AGENTS.md](../../../crates/aves/AGENTS.md)

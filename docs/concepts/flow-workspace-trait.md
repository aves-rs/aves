---
title: Flow Workspace Trait
type: concept
updated: 2026-11-14
references:
  - crates/aves/src/flow/hook.rs
  - crates/aves/src/flow/handle.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/checkpoints.rs
  - crates/aves/src/flow/default_workspace.rs
  - crates/aves/src/flow/snapshot.rs
  - crates/aves/src/flow/mobility.rs
  - crates/aves/src/flow/types.rs
---

# Flow Workspace Trait

`FlowAppWorkspaceHook` is Flow's complete workspace system, not an extension layered over native handling. `handle.rs` selects an application implementation when supplied, otherwise `DefaultWorkspace`, before constructing the actor. The actor depends on the trait. The default owns native documents, blobs and pins, file preparation, mapped projection and cleanup.

## Lifecycle

- `execution_root` resolves a root; `takeoff` prepares the workspace and returns `FlowAppTakeoffResult` with the dispatch root and optional imported root.
- `checkpoint` returns `FlowAppCheckpointResult` with an optional checkpoint and path-to-hash projection. After the snapshot is durable, `checkpoint_committed` finalizes the capture.
- `restore` reconstructs an execution and returns its dispatch root. The trait also covers import, download, script sources, artifact projection, recovery, shutdown and mobility operations.
- `landing` receives `FlowAppLandingContext` after terminal persistence. Retention may replay it with empty correlations; implementations must tolerate replay. `release_artifacts` follows landing during cleanup.

Snapshots persist explicit `WorkspaceProvenance` and root. A missing document link does not imply application ownership, and a missing app hook produces `AppWorkspaceUnavailable` rather than a native fallback. Mobility rejects app-owned snapshots.

## Related

- [Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Pluggable Flow Nodes](../architecture/aves/pluggable-flow-nodes.md)

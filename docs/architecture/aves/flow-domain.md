---
title: Aves Flow Domain Architecture
type: architecture
updated: 2026-11-14
references:
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/handle.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/hook.rs
  - crates/aves/src/flow/default_workspace.rs
  - crates/aves/src/flow/checkpoints.rs
  - crates/aves/src/flow/snapshot.rs
  - crates/aves/src/flow/mobility.rs
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/flow/types.rs
---

# Aves Flow Domain Architecture

Flow owns graph orchestration, definition revisions, execution state, recovery and mobility. Node owners supply registrations; workspace operations go through one selected `FlowAppWorkspaceHook` implementation.

## Construction and operation

- `domain.rs` acquires construction inputs and kernel service readers and publishers, then delegates construction to `handle.rs`.
- `handle.rs` assembles the registry, workspace, mobility service and actor. Its authorization hook checks the authenticated issuer for mobility imports and requires `PullCloudBlobs` or `AllBus` for blob downloads.
- Before `Actor::new`, the handle selects the supplied application workspace or `DefaultWorkspace`, never both. The actor operates through the trait, not the default implementation.
- At `StartRuntime`, the actor resolves Machine's late-bound peer service into a boolean indicating peer sync availability.

## Workspace and persistence

`DefaultWorkspace` owns native documents, blobs and pins, preparation, mapped projection and cleanup. A supplied application implementation replaces this entire workspace system. See [Flow Workspace Trait](../../concepts/flow-workspace-trait.md) for lifecycle boundaries.

The actor captures a checkpoint at start and transitions, saves the snapshot, then calls `checkpoint_committed` before dispatching successors. The snapshot durably records `WorkspaceProvenance` and its root; ownership is not inferred from document linkage. A mismatch between persisted ownership and the selected implementation returns `AppWorkspaceUnavailable`, including when an app hook is missing. There is no legacy-native fallback.

Recovery delegates to the selected workspace's `recoverable` and `restore` methods. The native implementation rejects an unlinked active execution and reconstructs from its durable workspace. Mobility rejects app-owned execution snapshots; native transfer uses workspace-owned references.

Terminal landing occurs after the terminal snapshot. Retention replays landing with empty correlations before `release_artifacts`; a failed cleanup keeps its durable anchor for retry.

## Related

- [Flow Workspace Trait](../../concepts/flow-workspace-trait.md)
- [Flow Dataflow](../../concepts/flow-dataflow.md)
- [Pluggable Flow Nodes Architecture](pluggable-flow-nodes.md)
- [Selective Flow Mobility](../../concepts/selective-flow-mobility.md)

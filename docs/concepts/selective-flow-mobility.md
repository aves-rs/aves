---
title: Selective Flow Mobility
type: concept
updated: 2026-10-23
references:
  - crates/aves/src/flow/mobility.rs
  - crates/aves/src/flow/replication.rs
  - crates/aves/src/flow/sync.rs
  - crates/aves/src/flow/database.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/messages.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/migrations/flow_migrations/0009_flow_mobility.sql
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/bus/event_bus.rs
---

# Selective Flow Mobility

Selective Flow Mobility is the Aves Flow domain's per-Flow hand-off of one selected Flow's durability unit — definition, trigger list, execution history, docs links, immutable blob references — to exactly one already-paired peer destination. The destination commits durably **inactive**, the source fences itself, the destination promotes only with the source's yield acknowledgement, and the source cleans up only after the destination activates. Single owner at all times.

## Key points

- **Source / inactive destination / single owner.** The source retains `authoritative` (or `synchronizing`) authority until `yieldFlowAuthority` fences it; the destination sits at `inactive_committed` between commit and promotion and cannot execute work (`FlowAuthorityState::permits_local_work`); only one side holds local-work authority at any time. The six states are `authoritative`, `synchronizing`, `inactive_committed`, `yielded`, `active`, `cleanup_pending`.
- **Two mobility tables.** `flow_mobility_state` persists mobility authority separately from the `flows` lifecycle row — `authority_state` is its own column, with `destination_peer_json`, `destination_commit_ack_json`, and `activation_ack_json` as separate JSON columns; `flow_mobility_snapshots` holds the serialized snapshot and generation. Both tables live alongside `flows` via migration `0009_flow_mobility.sql`. Reads via `load_flow_authority.sql` / `load_mobility_snapshot.sql`; writes via `set_flow_authority.sql` / `upsert_mobility_snapshot.sql`. `fence_and_yield_flow` is a single transaction that atomically also disables triggers and sets `enabled = 0` on the flow row alongside setting authority to `yielded`. Other authority transitions do not touch the flow row or triggers.
- **Generated RPC-over-Iroh orchestration.** Source invokes `flow.selectMobilityDestination` → `flow.synchronizeFlow` → `flow.yieldFlowAuthority` → `flow.finalizeFlowTransfer`; destination invokes `flow.importMobilitySnapshot` → `flow.promoteFlowReplica`. Cross-peer calls go through `RuntimeFlowMobilityService` (`supervisor.rs`) which issues `AllBus` peer JWTs and dispatches remote operations over native Iroh QUIC. See [Aves Iroh Peer Runtime](../architecture/aves/iroh-peer-runtime.md).
- **Snapshot = one selected Flow's transfer unit.** `FlowMobilitySnapshot` carries `flow_id`, `definition`, `triggers`, `executions`, `native_references` (per-execution docs namespace/author, code roots, state roots, workspace checkpoint, workspace entries, imported workspace root). `validate_selected_flow` rejects snapshots mixing another Flow's definition or executions.
- **Atomic import + idempotency.** Destination import is one transaction (`commit_imported_flow`) inserting the flow row, the authority row, revisions, triggers, executions, execution-document links, and snapshot, then setting `inactive_committed`. A repeated `importMobilitySnapshot` with a structurally equal typed snapshot returns the stored acknowledgement without rewriting; a divergent snapshot returns `MobilitySnapshotMismatch`.
- **Handoff order.** `select_destination` (1) → `synchronize_flow` captures `mobility_snapshot` and commits on the destination (2) → `yield_flow_authority` fences the source (`fence_and_yield_flow`: disable triggers, set `enabled = 0`, mark `yielded`) and returns `FlowYieldAcknowledgement` — coordination data carrying `flow_id` and `snapshot_generation`, not a source-bound or cryptographic proof; single-owner behavior is enforced by the orchestration order itself (3) → `promote_flow_replica` on the destination verifies snapshot generation, history/document/blob completeness, and flips to `active` (4) → `finalize_flow_transfer` on the source tears down transferred state and removes the Flow (5).
- **Post-transfer RPC routing.** After `finalize_flow_transfer`, the source has removed the Flow entirely — `definitions.remove(flow_id)`, `triggers.retain`, `delete_transferred_flow` (executions, triggers, nodes, definition revisions, flow row). The source retains no proxy, owner directory, or forwarding entry for that `FlowId`. Callers target the authoritative destination through generated remote RPC; the source does not route on the source's behalf.
- **Cleanup.** Source cleanup is staged: delete transferred execution documents (only when no other execution still links the namespace/author), clean execution directories, release pinned tags, then `delete_transferred_flow` in one transaction. Shared blobs stay GC-reachable through their per-execution tags.

## How it works

```text
source (authoritative)             destination (no replica)
  selectMobilityDestination ──────► (paired peer)
  synchronizeFlow
    snapshot = def + triggers + executions + native_refs
    importMobilitySnapshot ──────► commit_imported_flow (inactive_committed)
    save commit ack
  yieldFlowAuthority
    fence_and_yield_flow → yielded
    return FlowYieldAcknowledgement
                                    promoteFlowReplica
                                      verify gen + history + docs + blobs
                                      → active
  finalizeFlowTransfer
    promoteReplica (remote) ◄────── activation ack
    delete transferred Flow state
    definitions.remove; triggers.retain
```

## Exclusions

Selective Flow Mobility is **not**: a UI surface, global replication, folder sync, distributed execution or failover, deployment, remote signing, or Permission/Fetch node transfer. Only the selected Flow's durability unit moves; everything else stays local on its current owner.

## Known pitfalls

- `InactiveCommitted` and `Yielded` cannot run work (`permits_local_work` is false). An execution scheduled against them returns `FlowError::InactiveReplica`.
- `fence_and_yield_flow` is one transaction; a partial failure leaves the source `synchronizing` and the snapshot still on the destination, so the operator can retry `synchronizeFlow` + `yieldFlowAuthority` against the same generation.
- No native Collection provider→receiver acceptance test. **NO FIX.**

## Related

- [Flow Execution Workspace](flow-execution-workspace.md)
- [Flow Dataflow](flow-dataflow.md)
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Aves Runtime Supervisor](../architecture/aves/runtime-supervisor.md)
- [Aves Iroh Peer Runtime](../architecture/aves/iroh-peer-runtime.md)
- [Aves Auth Domain Architecture](../architecture/aves/auth-domain.md)
- `crates/aves/AGENTS.md`

---
title: Aves Flow Domain Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/aves/src/flow/mod.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/actor/tests.rs
  - crates/aves/src/flow/catalog.rs
  - crates/aves/src/flow/checkpoints.rs
  - crates/aves/src/flow/messages.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/flow/database.rs
  - crates/aves/src/migrations/flows.rs
  - crates/aves/src/flow/workspace.rs
  - crates/aves/src/flow/storage.rs
  - crates/aves/src/flow/files.rs
  - crates/aves/src/flow/fixtures.rs
  - crates/aves/src/flow/snapshot.rs
  - crates/aves/src/flow/mailbox.rs
  - crates/aves/src/flow/tick.rs
  - crates/aves/src/flow/blackboard.rs
  - crates/aves/src/flow/adapter.rs
  - crates/corv/src/actor.rs
  - crates/corv/src/types.rs
  - crates/corv/src/agent_nodes.rs
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/migrations/flow_migrations/0001_flow_definitions_and_execution_snapshots.sql
  - crates/aves/src/migrations/flow_migrations/0002_flow_execution_documents.sql
  - crates/aves/src/migrations/flow_migrations/0003_flow_workspace_checkpoints.sql
  - AGENTS.md
---

# Aves Flow Domain Architecture

Aves Flow owns graph orchestration: immutable definition revisions, the Bonsai tick loop, public start/abort, cron triggers, recovery snapshots, and the **execution-scoped** Iroh workspace. Owner domains push node-kind registrations into a shared `NodeRegistrationSupply`; Flow drains the supply at boot. There is no fixed node-kind set in Flow core.

## Ownership and boundaries

- **Flow owns:** catalog + revisions (associated `Actor` methods in `catalog.rs`), tick loop, mailbox, `flows.db`, local `FsStore` at `{data_root}/flow-workspace-blobs` (native blob/Docs ops in `storage.rs`), per-execution iroh-docs replicas, checkpoint pins (prepare/save/retry/retention in `checkpoints.rs`).
- **Public bus:** `start` / `abort` are `#[operation]` mutations (`messages.rs`). Catalog, script source, trigger, and execution list/detail are also public operations.
- **`FlowControlService` sudo only:** `StartRuntime` / `StopRuntime` / `UpdateRetention`. Not Start/Abort.
- **Tasks owns:** ScriptNode registration (pushes onto `NodeRegistrationSupply` in `tasks/handle.rs`). Flow passes `workspace_path = execution_root`.
- **Discuss owns:** AgentNode registration and lifecycle (pushes onto the supply in `discuss/handle.rs`), using the Flow execution root as `session_cwd`.
- **Embedding applications supply:** `FlowBootOptions` and a shared `NodeRegistrationSupply` before Flow construction.
- **Does not own:** owner-domain execution, ACP sessions, or browser runtimes.

## Data flow

1. Public mutations persist revisions. Create requires `revision > 0` (host sends `1`); update requires `expected + 1` (`database.rs`). Definition/trigger/node CRUD and revision commits are associated `Actor` methods in `catalog.rs`. `importWorkspace` copies a host-local path into tagged blobs (`storage.rs`). `downloadWorkspaceBlob` is a normal op gated by `PullCloudBlobs` | `AllBus` (`authorize_public_request` in `domain.rs`) before any fetch.
2. `Start { flow_id, pulse, automatic, invocation_kind }` creates `{data_root}/executions/{id}`, materializes mapped inputs into **one** directory (no `code/{digest}` / `agents/{id}` copies; path safety and mapping validation in `files.rs`), copies selected Discuss Provider sources when the graph contains AgentNodes, creates the docs replica, captures checkpoint sequence 0, then spawns owners. Automatic starts require `automatic_enabled`; manual starts ignore that gate (`begin_start`).
3. Completions accumulate per `NodeExecutionId`. Terminal failure removes `writer_active` and persists the error (no retry; F9 — non-cancellation errors terminalize the node). User-declined cancellation via `flow.abort` pauses new writer spawn and waits owner terminals. BoundOutputs patch the blackboard and persist only on `TerminalStatus::Success` (`tick.rs`; the Flow Blackboard is Bonsai's generic blackboard carried as `BT<FlowAction, TickState>`, with private `NodeProgress` consolidating the old synchronized bookkeeping maps). When `writer_active` is empty, capture runs, then `SnapshotStore::save` binds `workspace_checkpoint` (checkpoint prepare/save/retry/retention are associated `Actor` methods in `checkpoints.rs`; `ExecutionState`, `PendingTerminalPersistence`, `PreparedCheckpointCommit` declarations live in `types.rs`). After that save, Flow releases the superseded checkpoint pin (`flow/execution/{id}/checkpoint/{seq}` for `prepared.superseded_sequence`). Successor dispatches spawn **once** from `PreparedCheckpointCommit` after that save. Bindings, Script `__sluice_inputs` / `__sluice_outputs`, artifacts, inspector: [Flow Dataflow](../../concepts/flow-dataflow.md).
4. Abort pauses new writer spawn, `discard_pending` (mail only), waits owner terminals, and finalizes against the last committed checkpoint **without** mapped write-back. Tasks and Discuss use privileged cancellation requests.

## Persistence

- DB: `{config_dir}/flows.db`; workspace `data_root` is `{config_dir}/flows`. `FlowBootOptions::open(config_dir)` awaits migrations before domain construction. Existing on-disk data is not relocated automatically.
- Workspace: `import_path` copies into tagged collections (`flow/workspace/{digest}`; native blob read/write via iroh-blobs `get_bytes`/`add_slice` in `storage.rs`); checkpoints pin `flow/execution/{id}/checkpoint/{seq}`. Symlinks rejected (path safety in `files.rs`). Docs keys are path + NUL.
- Cron: actor/store own `next_fire_at`. `update_trigger` keeps the stored cursor unless the cron expression changed.
- Per-node retained results: `ExecutionSnapshot.nodes` stores `NodeExecutionSnapshot` (`node_execution_id`, `status`, `state`, `inputs`, `outputs`, timestamps, `error`). Mailbox `TerminalResult.state` is owner-neutral terminal detail kept for inspection.
- `ExecutionSnapshot` also stores blackboard, roots, `workspace_checkpoint`, `workspace_entries`, reconstruction, scratchpad. **No** token or owner-runtime field.
- Retention keeps the newest N terminal runs per Flow (`DEFAULT_RETENTION_COMPLETED_RUNS = 10`). Prune is idempotent per resource; boot prune failure warns and keeps the anchor.

## Recovery

`Actor::new` drops active snapshots with no document linkage, reopens saved NamespaceId/AuthorId (never a fresh replica), `materialize_clean`s the committed checkpoint (or input mapping if none), then `FlowTickLoop::restore`. Crash-surviving extra files are never trusted. Terminal rows are not re-adopted as active.

## Known pitfalls

- Capture/load failure **before** `PreparedCheckpointCommit` can drop successor `Start`. **NO FIX.**
- No native Collection transfer acceptance test. **NO FIX.**
- A Provider source saved as `./opencode.jsonc` passes Discuss validation but fails Flow's path check at start. **NO FIX.**

## Related

- [Flow Execution Workspace](../../concepts/flow-execution-workspace.md)
- [Flow Dataflow](../../concepts/flow-dataflow.md)
- [Pluggable Flow Nodes Architecture](pluggable-flow-nodes.md)
- [Aves Tasks Domain Architecture](tasks-domain.md)
- [Aves Runtime Supervisor](runtime-supervisor.md)
- `crates/aves/AGENTS.md`

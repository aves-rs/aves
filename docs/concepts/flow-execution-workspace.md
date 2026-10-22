---
title: Flow Execution Workspace
type: concept
updated: 2026-10-23
references:
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/workspace.rs
  - crates/aves/src/flow/storage.rs
  - crates/aves/src/flow/files.rs
  - crates/aves/src/flow/fixtures.rs
  - crates/aves/src/flow/checkpoints.rs
  - crates/aves/src/flow/snapshot.rs
  - crates/aves/src/flow/tick.rs
  - crates/aves/src/flow/messages.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/types.rs
  - crates/aves/src/tasks/sandbox.rs
  - crates/aves/src/tasks/messages.rs
  - crates/corv/src/handle.rs
  - crates/corv/src/actor.rs
  - crates/corv/src/provider.rs
  - crates/corv/src/agent_nodes.rs
  - crates/aves/src/config/types.rs
  - crates/aves/src/auth/types.rs
---

# Flow Execution Workspace

Each Flow run owns one mutable directory `{data_root}/executions/{execution_id}`. Nodes share it; Iroh holds durable bytes and path identity. Disk is the live projection, not a second authority.

## Key points

- Full roots land at workspace top level; `ContentAddressedPath` entries land at their recorded relative path. Same path + same hash dedups; different hash is `WorkspaceError::InputCollision`.
- iroh-blobs store immutable files; one per-execution iroh-docs replica records path→hash (NUL-framed keys). Native blob read/write uses iroh-blobs `get_bytes`/`add_slice` directly (no temp-file scaffolding; `storage.rs`). Immutable checkpoint collections bound on `ExecutionSnapshot` are the restore authority (`flow/execution/{id}/checkpoint/{seq}`). No docs sync, gossip, or peer discovery.
- ScriptNode: sole WASI preopen (`host = execution root`, `guest = "/"`). Module staging stays beside the entry parent; guest RPC is not a filesystem. Guest globals `__sluice_inputs` / `__sluice_outputs` and success-only publication: [Flow Dataflow](flow-dataflow.md).
- `saveScriptSource` (`storage.rs`): Mapped writes the Knowledge file (`write_source_text`) and returns `None` (no `source_root` retarget). Stored `replace_collection_text` keeps companion hashes, retags `flow/workspace/{digest}`, returns the new root. New `import_text_at` returns a new root.
- Mapped import pins `flow/execution/{id}/mapped`. Checkpoints pin `flow/execution/{id}/checkpoint/{seq}`. Cleanup `release_execution_pins` deletes prefix `flow/execution/{id}/`.
- Three mapping policies: `ReadWriteDelete` / `ReadWrite` / `ReadOnly`. Import before each run. Write-back (`project_mapped_workspace`) after natural terminal only; abort skips it. Editor Save is independent of run write-back. Path safety (symlink rejection, checked-relative, mapping validation, artifact projection, copy/seeding helpers) lives in `files.rs`.
- Public `flow.start` / `flow.abort` are ordinary bus `#[operation]`s (`messages.rs`).
- AgentNode: Tasks passes the execution root as Discuss `session_cwd`; ordinary chats still use `discuss.agents_workspace`. CWD is not confinement. On a new run with AgentNodes, Flow copies the union of their Providers' selected relative sources from the Discuss workspace after input preparation and before checkpoint 0. No copying on restore. Seed selection (which Provider sources to copy) is resolved before directory materialization (F2).
- **Registration-owned preparation.** Each `NodeKindRegistration` may carry a `WorkspacePreparation` with `node_content_roots` and `graph_seeds` hooks; Flow invokes them during execution directory setup so the owner can seed files or validate constraints before checkpoint 0.
- Retention: Flow `settings_registration` registers `FlowSettings` (`retention_completed_runs`, default 10). Config persists the section; Flow `on_change` calls `update_retention`.
- Transfer: `importWorkspace` is host-local ingress (`import_path` in `storage.rs` copies a file by basename or a directory by relative path and tags `flow/workspace/{digest}`). `downloadWorkspaceBlob` pulls a Collection-format ticket only after `PullCloudBlobs` or `AllBus`; raw and doc tickets are rejected. Both return `WorkspaceImported { root }`.

## How it works

```text
inputs → materialize_inputs → seed AgentNode sources (if any) → capture seq 0
     → owners read/write disk
     → writer-quiescent capture → pin collection → save snapshot
     → boot: discard dirty dir → checkpoint: materialize_clean; else: materialize_inputs_clean
```

Abort pauses new writer spawn, waits real owner terminals (Tasks: fuel/timeout only), and binds to the last committed checkpoint. Checkpoint prepare/save/retry/retention are associated `Actor` methods in `checkpoints.rs`.

## Known pitfalls

- Capture/document-load failure **before** `PreparedCheckpointCommit` can drop successor `Start` dispatches; retry may `Tick` the next owner. **NO FIX.**
- Config RPC may show `config.FlowRetentionRejected` without nested Flow `domain`/`code`/`details`. **NO FIX.**
- No native Collection provider→receiver test; no AgentNode worker-timer test. **NO FIX.**
- Workspace sources copy synchronously in the Flow actor at start; large directories can delay other Flow events. **NO FIX.**
- Discuss accepts a saved source such as `./opencode.jsonc` that Flow's path check rejects at start. **NO FIX.**

## Related

- [Flow Dataflow](flow-dataflow.md) — BoundInputs/BoundOutputs, per-node capture/commit, Script globals, artifacts, inspector
- [Pluggable Flow Nodes](pluggable-flow-nodes.md) — registration supply, preparation hooks, unknown-kind terminalization
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Aves Tasks Domain Architecture](../architecture/aves/tasks-domain.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)
- `crates/aves/AGENTS.md`

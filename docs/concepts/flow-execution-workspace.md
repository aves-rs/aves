---
title: Flow Execution Workspace
type: concept
updated: 2026-11-14
references:
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/handle.rs
  - crates/aves/src/flow/hook.rs
  - crates/aves/src/flow/default_workspace.rs
  - crates/aves/src/flow/default_workspace/native.rs
  - crates/aves/src/flow/default_workspace/storage.rs
  - crates/aves/src/flow/default_workspace/files.rs
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

`handle.rs` selects either an application-supplied `FlowAppWorkspaceHook` or `DefaultWorkspace` before constructing the actor; the application hook replaces the default entirely, never runs alongside it. The actor imports the trait and its operation types, not the default implementation. See [Flow Workspace Trait](flow-workspace-trait.md).

The default owns the native workspace machinery: each run uses `{data_root}/executions/{execution_id}`, shared by nodes. Iroh holds durable bytes and path identity; disk is the live projection, not a second authority. The native-specific details below do not automatically apply to an application hook.

## Key points

- Full roots land at workspace top level; `ContentAddressedPath` entries land at their recorded relative path. Same path + same hash dedups; different hash is `WorkspaceError::InputCollision`.
- iroh-blobs store immutable files; one per-execution iroh-docs replica records path→hash (NUL-framed keys). Native blob read/write uses iroh-blobs `get_bytes`/`add_slice` directly (no temp-file scaffolding; `storage.rs`). Immutable checkpoint collections bound on `ExecutionSnapshot` are the restore authority (`flow/execution/{id}/checkpoint/{seq}`). No docs sync, gossip, or peer discovery.
- ScriptNode: sole WASI preopen (`host = execution root`, `guest = "/"`). Module staging stays beside the entry parent; guest RPC is not a filesystem. Guest globals `__application_inputs` / `__application_outputs` and success-only publication: [Flow Dataflow](flow-dataflow.md).
- `saveScriptSource` (`storage.rs`): Mapped writes the Knowledge file (`write_source_text`) and returns `None` (no `source_root` retarget). Stored `replace_collection_text` keeps companion hashes, retags `flow/workspace/{digest}`, returns the new root. New `import_text_at` returns a new root.
- Mapped import pins `flow/execution/{id}/mapped`. Checkpoints pin `flow/execution/{id}/checkpoint/{seq}`. Cleanup `release_execution_pins` deletes prefix `flow/execution/{id}/`.
- Three mapping policies: `ReadWriteDelete` / `ReadWrite` / `ReadOnly`. Import before each run. Write-back (`project_mapped_workspace`) after natural terminal only; abort skips it. Editor Save is independent of run write-back. Path safety (symlink rejection, checked-relative, mapping validation, artifact projection, copy/seeding helpers) lives in `files.rs`.
- Public `flow.start` / `flow.abort` are ordinary bus `#[operation]`s (`messages.rs`).
- AgentNode: its execution root is passed to Discuss as `session_cwd`; ordinary chats still use `discuss.agents_workspace`. CWD is not confinement. On a new run with AgentNodes, the default copies the union of their Providers' selected relative sources from the Discuss workspace after input preparation and before checkpoint 0. No copying on restore. Seed selection is resolved before directory materialization.
- **Registration-owned preparation.** Each `NodeKindRegistration` may carry a `WorkspacePreparation` with `node_content_roots` and `graph_seeds` hooks; the default invokes them during execution directory setup so the owner can seed files or validate constraints before checkpoint 0.
- Retention: Flow `settings_registration` registers `FlowSettings` (`retention_completed_runs`, default 10). Config persists the section; Flow `on_change` calls `update_retention`.
- Transfer: `importWorkspace` is host-local ingress (`import_path` in `storage.rs` copies a file by basename or a directory by relative path and tags `flow/workspace/{digest}`). `downloadWorkspaceBlob` pulls a Collection-format ticket only after `PullCloudBlobs` or `AllBus`; raw and doc tickets are rejected. Both return `WorkspaceImported { root }`.

## How it works

```text
default: inputs → materialize_inputs → seed AgentNode sources (if any) → capture seq 0
     → owners read/write disk
     → writer-quiescent capture → pin collection → save snapshot → checkpoint_committed
     → boot: discard dirty dir → checkpoint: materialize_clean; else: materialize_inputs_clean
```

Snapshots durably record `WorkspaceProvenance` (native or application) and the selected root. Recovery does not infer ownership from a missing document link: an app-owned snapshot without an application hook fails with `AppWorkspaceUnavailable`; there is no legacy-native fallback. Application hooks own their own takeoff, checkpoint, restore, landing and cleanup behavior.

Abort pauses new writer spawn, waits real owner terminals (Tasks: fuel/timeout only), and binds to the last committed checkpoint. Checkpoint prepare/save/retry/retention are associated `Actor` methods in `checkpoints.rs`.

## Known pitfalls

- Capture/document-load failure **before** `PreparedCheckpointCommit` can drop successor `Start` dispatches; retry may `Tick` the next owner. **NO FIX.**
- Config RPC may show `config.FlowRetentionRejected` without nested Flow `domain`/`code`/`details`. **NO FIX.**
- No native Collection provider→receiver test; no AgentNode worker-timer test. **NO FIX.**
- The default copies workspace sources synchronously at start; large directories can delay other Flow events. **NO FIX.**
- Discuss accepts a saved source such as `./opencode.jsonc` that Flow's path check rejects at start. **NO FIX.**

## Related

- [Flow Dataflow](flow-dataflow.md) — BoundInputs/BoundOutputs, per-node capture/commit, Script globals, artifacts, inspector
- [Flow Workspace Trait](flow-workspace-trait.md) — workspace ownership and lifecycle
- [Pluggable Flow Nodes](../architecture/aves/pluggable-flow-nodes.md) — registration supply, preparation hooks, unknown-kind terminalization
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Aves Tasks Domain Architecture](../architecture/aves/tasks-domain.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)
- `crates/aves/AGENTS.md`

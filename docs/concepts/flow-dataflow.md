---
title: Flow Dataflow
type: concept
updated: 2026-10-23
references:
  - crates/aves/src/flow/blackboard.rs
  - crates/aves/src/flow/tick.rs
  - crates/aves/src/flow/workspace.rs
  - crates/aves/src/flow/storage.rs
  - crates/aves/src/flow/files.rs
  - crates/aves/src/flow/snapshot.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/flow/adapter.rs
  - crates/aves/src/migrations/flow_migrations/0001_flow_definitions_and_execution_snapshots.sql
  - crates/aves/src/migrations/flow_migrations/0002_flow_execution_documents.sql
  - crates/aves/src/migrations/flow_migrations/0003_flow_workspace_checkpoints.sql
  - crates/aves/src/migrations/flow_migrations/0007_flow_node_execution_dataflow.sql
  - crates/aves/src/migrations/flows.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/aves/src/tasks/sandbox.rs
  - crates/corv/src/actor.rs
  - crates/corv/src/agent_nodes.rs
  - crates/corv/src/plugins/mod.rs
---

# Flow Dataflow

Owner-neutral `BoundInputs` / `BoundOutputs` move JSON across nodes. Files stay on one shared execution root. No second output protocol, per-node workspace, or extra durability store.

## Two data lanes
- **Blackboard:** Flow-owned named JSON. Bindings expose owner names; guest/prompt see `as_value()` (plain name→value). Persist/RPC wrap `{ values }`.
- **Filesystem:** `{data_root}/executions/{execution_id}`. Disk is live; Iroh is durable.

## `flows.db`
`{config_dir}/flows.db`: catalog + immutable revisions (`blackboard_defaults_json`); `flow_triggers`; `flow_executions` (blackboard, roots, reconstruction); `flow_node_executions` (`state_json` = owner inspection; `inputs_json` / `outputs_json` default `'{"values":{}}'`); `flow_execution_documents` (NamespaceId/AuthorId); checkpoint + entry JSON on the execution row. Artifact path lists are never stored.

## Execution tree
`{data_root}/executions/{id}/`
- `workflow/trigger.json` — `kind`, `triggeredAt`, pulse `data` (object as-is; scalar `{ "value": … }`)
- `code/` and `state/` — materialized execution inputs (`NodeDataSpace` roles)
- `scripts/` — optional user-selected Script path; not a materialize role
- `workspace/knowledge/` — Knowledge mapping `destination` when configured
- `artifacts/<node-id>/` — every executable leaf, created at prepare and restore

## Backing stores
`{config_dir}/flows/flow-workspace-blobs/` (iroh-blobs; native blob read/write via `get_bytes`/`add_slice` in `storage.rs`) and `{config_dir}/flows/flow-workspace-docs/` (per-execution replica). Path safety (symlink rejection, checked-relative, mapping validation, artifact projection, copy/seeding helpers) lives in `files.rs`. Immutable checkpoint collections pin `flow/execution/{id}/checkpoint/{seq}`. Recovery: `materialize_clean` when a checkpoint exists; `materialize_inputs_clean` when none. Crash-surviving directories are discarded.

## Lifecycle
Defaults plus invocation `pulse` (`blackboard.set("pulse", …)` when non-null). Tick `bind_inputs` → retain `node_inputs` → `LifecycleStart { payload: inputs }` before dispatch. `MailboxResolution::Terminal` + `TerminalStatus::Success` is the only `patch_outputs` / `node_outputs` path. Reconstruct copies outputs only for `Succeeded`. Retention: newest N terminal runs (`flow.retention_completed_runs`) and per-node snapshots. The Flow Blackboard is Bonsai's generic blackboard carried as `BT<FlowAction, TickState>` in `tick.rs`; private `NodeProgress` consolidates the old synchronized bookkeeping maps. Mailbox sequence numbering has been removed (`mailbox.rs`).

## Script
```js
const price = __sluice_inputs.price;
__sluice_outputs.isTradable = price < 100;
const flows = await sluice.rpc("flow.listFlows", {});
```
Guest `/` is the execution root (one WASI preopen). `artifacts/<node-id>/` is a relative convention — not a BoundInput, lifecycle field, or Script global. `tasks/adapter.rs` builds the ScriptNode registration (start/tick/cancel callbacks + owner state); `flow/adapter.rs` is the generic `HostLifecycleAdapter` bridge that looks up the registration by `node_kind` and dispatches `LifecycleStart`/`LifecycleTick`/`LifecycleCancel`.

## Agent
Nonempty BoundInputs append once: `--- Sluice task inputs ---` compact `as_value()` JSON `--- End Sluice task inputs ---`. Empty inputs leave the prompt unchanged. Tick does not repeat. Discuss starts AgentNode sessions with the shared execution root as `session_cwd`; see [Flow Execution Workspace](flow-execution-workspace.md) for one-time Provider source seeding. No new Agent output contract.

## Known pitfalls
- Bare SQL `'{}'` needs `#[serde(default)]` on `values`.
- Missing blackboard key is `BlackboardError::MissingInput` before Start.
- Artifact-dir IO is `WorkspaceError::Io` and aborts prepare. Agent sessions do not see `artifacts/<node-id>/`.

## Related
- [Flow Execution Workspace](flow-execution-workspace.md)
- [Pluggable Flow Nodes](pluggable-flow-nodes.md)
- [Aves Flow Domain Architecture](../architecture/aves/flow-domain.md)
- [Aves Tasks Domain Architecture](../architecture/aves/tasks-domain.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)
- `crates/aves/AGENTS.md`

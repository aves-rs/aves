---
title: Aves Tasks Domain Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/aves/src/tasks/mod.rs
  - crates/aves/src/tasks/domain.rs
  - crates/aves/src/tasks/actor.rs
  - crates/aves/src/tasks/messages.rs
  - crates/aves/src/tasks/types.rs
  - crates/aves/src/tasks/compiler.rs
  - crates/aves/src/tasks/sandbox.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/aves/src/flow/registration.rs
  - AGENTS.md
---

# Aves Tasks Domain Architecture

Aves Tasks owns ScriptNode Flow registration: Oxc TypeScript compile and a fresh QuickJS-in-Wasmtime guest per run. Discuss owns AgentNode registration and execution. Flow owns the shared execution directory and recovery; Tasks does not persist graphs or checkpoints.

## Ownership and boundaries

- **Tasks owns:** `TypeScriptCompiler`, `ScriptSandbox` (bundled `qjs-wasi-reactor.wasm`), privileged `StartScript` / `TickScript`, fuel + wall-clock timeout, opaque `ScriptNodeCompletion` outputs. Tasks pushes `script_node_registration` onto `NodeRegistrationSupply` in `tasks/handle.rs`; Discuss owns the AgentNode registration, lifecycle requests, limits, completions, and ACP sessions.
- **Flow owns:** when a ScriptNode or AgentNode starts/ticks, the execution workspace, and durable node status. `StartScript.workspace_path` is the shared execution root.
- **No public RPC/MCP.** Tasks has only privileged lifecycle operations.
- **Does not own:** extra host mounts, Node.js APIs, Browser guests, remote Iroh, AgentNode, or ACP sessions. ScriptNode cancellation is a privileged lifecycle request (`CancelScriptExecution`); AgentNode cancellation (`CancelAgentExecution`) belongs to Discuss.

## Data flow

1. Flow dispatches `StartScript` with source, **workspace-relative filename**, BoundInputs (`as_value()`), limits, `workspace_path`, and a Flow-scoped JWT + `BusIngress` (serde-skipped) for guest RPC.
2. Actor transpiles with Oxc. Parse mode is always TypeScript module (`SourceType::ts().with_module(true)` in `compiler.rs`). Filename is diagnostics / import resolution only — not parse-mode selection. Then it stores process-local `pending` and runs a **new** Wasmtime engine/module (tests may cache the runtime; production does not).
3. Sandbox preopens exactly one directory: host = `workspace_path`, guest = `/`. Compiled source is staged beside the entry parent so `./helper.js` resolves next to `src/main`. Bootstrap injects `__sluice_inputs` / `__sluice_outputs`, then module-mode `await import(guestPath)`, then `js_std_loop_once` drains jobs before `SLUICE_OUTPUT`.
4. Guest emits `ScriptNodeCompletion { correlation, outputs, errors, logs }` from `__sluice_outputs`. Flow publishes BoundOutputs only on success ([Flow Dataflow](../../concepts/flow-dataflow.md)). File I/O never uses the guest RPC bridge.
5. `TickScript` requires a prior start in this process (`ScriptNotStarted` otherwise).

```rust
// crates/aves/src/tasks/compiler.rs
let source_type = SourceType::ts().with_module(true);
```

## Persistence

None. Execution state is process-local and **resets on restart**. Flow recovery re-issues Start/Tick for unfinished ScriptNodes. In-process `completed` makes duplicate `StartScript` for the same `node_execution_id` a no-op `Started`.

Capacity: 4 concurrent executions (`MAX_CONCURRENT_EXECUTIONS`). Stop clears `pending`.

## Related

- [Flow Execution Workspace](../../concepts/flow-execution-workspace.md)
- [Flow Dataflow](../../concepts/flow-dataflow.md)
- [Pluggable Flow Nodes Architecture](pluggable-flow-nodes.md)
- [Aves Flow Domain Architecture](flow-domain.md)
- [Aves Runtime Supervisor](runtime-supervisor.md)
- `crates/aves/AGENTS.md`

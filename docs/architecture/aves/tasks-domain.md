---
title: Aves Tasks Domain Architecture
type: architecture
updated: 2026-11-15
references:
  - crates/aves/src/tasks/mod.rs
  - crates/aves/src/tasks/domain.rs
  - crates/aves/src/tasks/actor.rs
  - crates/aves/src/tasks/messages.rs
  - crates/aves/src/tasks/types.rs
  - crates/aves/src/tasks/compiler.rs
  - crates/aves/src/tasks/sandbox.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/aves/src/tasks/shell.rs
  - crates/aves/src/flow/registration.rs
  - AGENTS.md
---

# Aves Tasks Domain Architecture

Aves Tasks owns two Flow node kinds: `ScriptNode` (Oxc TypeScript compile and a fresh QuickJS-in-Wasmtime guest per run) and `ShellNode` (a host shell process). Discuss owns AgentNode registration and execution. Flow owns the shared execution directory and recovery; Tasks does not persist graphs or checkpoints.

## Ownership and boundaries

- **Tasks owns:** `TypeScriptCompiler`, `ScriptSandbox` (bundled `qjs-wasi-reactor.wasm`), privileged `StartScript` / `TickScript`, fuel + wall-clock timeout, opaque `ScriptNodeCompletion` outputs. Tasks pushes `script_node_registration` and `shell_node_registration` onto `NodeRegistrationSupply` in `tasks/handle.rs`; Discuss owns the AgentNode registration, lifecycle requests, limits, completions, and ACP sessions.
- **ShellNode boundary:** `ShellNode` runs `<interpreter> -c <script>` as a host process outside Wasmtime. Timeout, cancel, and `StopRuntime` kill only the interpreter child — no process groups, no Job Objects — so descendants survive. Shell state, capacity, and the stop window are separate from ScriptNode. See [Using Shell Nodes](../../guides/using-shell-nodes.md).
- **Flow owns:** when a ScriptNode or AgentNode starts/ticks, the execution workspace, and durable node status. `StartScript.workspace_path` is the shared execution root.
- **No public RPC/MCP.** Tasks has only privileged lifecycle operations.
- **Does not own:** extra host mounts, Node.js APIs, Browser guests, remote Iroh, AgentNode, or ACP sessions. ScriptNode cancellation is a privileged lifecycle request (`CancelScriptExecution`); ShellNode cancellation is `CancelShellExecution`; AgentNode cancellation (`CancelAgentExecution`) belongs to Discuss.

## Data flow

1. Flow dispatches `StartScript` with source, **workspace-relative filename**, BoundInputs (`as_value()`), limits, `workspace_path`, and a Flow-scoped JWT + `BusIngress` (serde-skipped) for guest RPC.
2. Actor transpiles with Oxc. Parse mode is always TypeScript module (`SourceType::ts().with_module(true)` in `compiler.rs`). Filename is diagnostics / import resolution only — not parse-mode selection. Then it stores process-local `pending` and runs a **new** Wasmtime engine/module (tests may cache the runtime; production does not).
3. Sandbox preopens exactly one directory: host = `workspace_path`, guest = `/`. Compiled source is staged beside the entry parent so `./helper.js` resolves next to `src/main`. Bootstrap injects `__application_inputs` / `__application_outputs`, then module-mode `await import(guestPath)`, then `js_std_loop_once` drains jobs before `APP_OUTPUT`.
4. Guest emits `ScriptNodeCompletion { correlation, outputs, errors, logs }` from `__application_outputs`. Flow publishes BoundOutputs only on success ([Flow Dataflow](../../concepts/flow-dataflow.md)). File I/O never uses the guest RPC bridge.
5. `TickScript` requires a prior start in this process (`ScriptNotStarted` otherwise).

```rust
// crates/aves/src/tasks/compiler.rs
let source_type = SourceType::ts().with_module(true);
```

## Shell

1. Flow dispatches `StartShell` with the interpreter, ordered commands, `cwd`, timeout, and BoundInputs. The actor rejects a missing or non-directory `cwd` before spawning.
2. The runner joins the commands with newlines and executes `<interpreter> -c <script>` with null stdin, piped stdout/stderr, and the inherited host environment.
3. `TickShell` reports `Running` until the runner yields one terminal cause, then projects `exit_code` / `stdout` / `stderr` as bound outputs. On failure the same values, plus dropped-byte and abandoned-tail markers, live in the node state.

## Persistence

None. Execution state is process-local and **resets on restart**. Flow recovery re-issues Start/Tick for unfinished nodes (both ScriptNode and ShellNode). In-process `completed` makes duplicate `StartScript` for the same `node_execution_id` a no-op `Started`.

Capacity: 4 concurrent script executions (`MAX_CONCURRENT_EXECUTIONS`) and 4 concurrent shells (`MAX_CONCURRENT_SHELL_EXECUTIONS`); a fifth shell stays pending. Stop clears script `pending` and fails pending shells. `StopRuntime` gives running shells a 10s supervision window before reporting `ShellStopCleanupFailed`.

## Related

- [Using Shell Nodes](../../guides/using-shell-nodes.md)
- [Flow Execution Workspace](../../concepts/flow-execution-workspace.md)
- [Flow Dataflow](../../concepts/flow-dataflow.md)
- [Pluggable Flow Nodes Architecture](pluggable-flow-nodes.md)
- [Aves Flow Domain Architecture](flow-domain.md)
- [Aves Runtime Supervisor](runtime-supervisor.md)
- `crates/aves/AGENTS.md`

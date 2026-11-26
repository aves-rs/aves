---
title: Aves Tasks Domain Architecture
type: architecture
updated: 2026-11-26
references:
  - crates/aves/src/tasks/mod.rs
  - crates/aves/src/tasks/domain.rs
  - crates/aves/src/tasks/actor.rs
  - crates/aves/src/tasks/messages.rs
  - crates/aves/src/tasks/types.rs
  - crates/aves/src/tasks/compiler.rs
  - crates/aves/src/tasks/sandbox.rs
  - crates/aves/src/tasks/runtime/fs.js
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
3. Sandbox preopens exactly one directory: host = `workspace_path`, guest = `/`. Compiled source is staged beside the entry parent so `./helper.js` resolves next to `src/main`. Bootstrap injects `__application_inputs` / `__application_outputs`, imports the staged filesystem library, then module-mode `await import(guestPath)`, then `js_std_loop_once` drains jobs before `APP_OUTPUT`.
4. Guest emits `ScriptNodeCompletion { correlation, outputs, errors, logs }` from `__application_outputs`. Flow publishes BoundOutputs only on success ([Flow Dataflow](../../concepts/flow-dataflow.md)). File I/O never uses the guest RPC bridge.
5. `TickScript` requires a prior start in this process (`ScriptNotStarted` otherwise).

```rust
// crates/aves/src/tasks/compiler.rs
let source_type = SourceType::ts().with_module(true);
```

## Filesystem library (`application.fs`)

The wrapper sets `globalThis.application` to a frozen object with `fs` (the module namespace of the staged [`runtime/fs.js`](../../../crates/aves/src/tasks/runtime/fs.js)) and `rpc`. It runs before the user module, so the four methods are available at module top level with no import:

```ts
// entry: src/main.ts (workspace-relative)
application.fs.makeDir("out");
application.fs.writeTextFile("out/note.txt", "hello");
const text = application.fs.readTextFile("out/note.txt");
const names = application.fs.listDir("out");

__application_outputs.text = text;
__application_outputs.entries = names;
```

Inside `fs.js`, `std` and `os` are imports from `qjs:std` and `qjs:os`. In user code, `application`, `console`, and `__application_outputs` are wrapper globals; the committed escape test calls the global `std.open` directly, which is not `application.fs`.

| Method | Behavior | Errors (all plain `Error`, except type checks) |
|---|---|---|
| `readTextFile(path)` | Returns `std.loadFile(path)`. Text only. | Non-string `path` → `TypeError`. `null` result → `readTextFile: <path>`. Exceptions from `std.loadFile` are not wrapped. |
| `writeTextFile(path, contents)` | Both arguments must be strings; returns `undefined`. Empty `contents` uses `std.open(path, "w")` then `close()`. Non-empty `contents` uses `std.writeFile`. | Open `null` → `writeTextFile: <path>: errno N`. Non-zero close → `close result N`. Exceptions from open, close, or writeFile → `writeTextFile: <path>: <message>`. |
| `listDir(path)` | Returns `os.readdir` entry names with `.` and `..` removed. Order is whatever `os.readdir` returns; it is not sorted here. | Non-zero errno → `listDir: <path>: errno N`. |
| `makeDir(path)` | One `os.mkdir(path)` call. No recursive creation is implemented. | Non-zero errno → `makeDir: <path>: errno N`. |

The module exports exactly these four functions. It has no binary I/O, delete, rename, stat, search, or symlink handling. Every method is synchronous.

## Staging

Each run writes up to three files named by `node_execution_id` (`staging_filenames` in `sandbox.rs`):

| Artifact | Location | Write |
|---|---|---|
| `.application-script-{id}.mjs` | Parent of the entry (`src/` for `src/main.ts`) | `fs::write` (compiled source with a module-alias prefix) |
| `.application-fs-{id}.mjs` | Workspace root | `create_new(true)` from the embedded `fs.js` |
| `.application-bootstrap-{id}.mjs` | Workspace root | `fs::write` (wrapper) |

- **Entry validation** (`staging_paths`): empty, absolute, root, prefix, or `..` components are rejected with `invalid ScriptNode entry point`.
- **Containment:** after `create_dir_all`, the script's parent is canonicalized and must start with the canonical workspace; otherwise the run fails with `ScriptNode module directory escapes the workspace`.
- **Exclusive fs file:** `create_new` never overwrites an existing `.application-fs-{id}.mjs`. `fs_created` is set only after that open succeeds, so cleanup never removes a file this call did not create.
- **Cleanup** runs after the guest finishes or fails. It removes the fs file (only if created), the script, and the bootstrap. `NotFound` is ignored. Precedence: if the run already returned an error, that error wins and cleanup errors are dropped; if the run succeeded but cleanup failed, the result is `failed to clean up ScriptNode staging files`.
- **No broader guarantee:** cleanup covers only these three artifacts. This doc makes no claim about what checkpoint capture or other readers see while a run is active.

## Evidence

At landed commit `986e5f2` (`crates/aves/src/tasks/sandbox.rs` tests):

- `wrapper_emits_the_generic_script_node_completion_shape` — asserts the wrapper imports the staged fs module and sets `fs: __application_fs`. Wiring only.
- `module_guest_resolves_workspace_imports_and_drains_promise_jobs` — a successful run; asserts the three staged artifacts are gone afterward. This is the normal-run cleanup evidence.
- `guest_filesystem_is_scoped_to_the_execution_workspace` — uses raw global `std.open`, not `application.fs`; checks an in-workspace write and a rejected `../` write.

Not present at this revision:

- Direct tests of the four `application.fs` methods, and no committed test executes their error paths.
- Library escape, error, or collision tests for the fs artifact, including `create_new` collision and cleanup-error precedence.
- Shared workspace fixtures for these cases.
- Entry-validation tests for `staging_paths`.

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

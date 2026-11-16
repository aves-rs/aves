---
title: Using Shell Nodes
type: guide
updated: 2026-11-15
references:
  - crates/aves/src/tasks/types.rs
  - crates/aves/src/tasks/shell.rs
  - crates/aves/src/tasks/adapter.rs
  - crates/aves/src/tasks/actor.rs
  - crates/aves/src/tasks/messages.rs
  - crates/aves/src/tasks/handle.rs
  - crates/aves/src/tasks/domain.rs
  - crates/aves/src/flow/registration.rs
  - crates/aves/src/flow/controllers.rs
  - crates/aves/src/flow/tick.rs
---

# Using Shell Nodes

`ShellNode` runs a host shell process for one Flow node. Aves Tasks registers it beside
`ScriptNode`, but the two share nothing but the registration mechanism: a ShellNode is a
backend-only host process — no WASM guest, no guest RPC, no fuel budget. Tasks owns the
process lifetime; the flow author owns the shell dialect and the error handling.

## Configuration

`ShellNodeConfig` (`crates/aves/src/tasks/types.rs`) decodes from the node envelope's
opaque `configuration` JSON. Unknown keys are rejected (`deny_unknown_fields`).

| Field | Required | Default | Meaning |
|---|---|---|---|
| `interpreter` | no | `"sh"` | Any locally installed shell that accepts `-c`: `sh`, `bash`, `zsh`, `fish`. |
| `commands` | yes | — | Ordered command lines, joined into one script. |
| `cwd` | yes | — | Working directory. Must exist and be a directory when the node starts. |
| `timeout_secs` | no | `0` → 300s | Wall-clock limit for the whole script. |

`cwd` is supplied by whoever configures the node; Tasks never looks up a project root.
An empty `interpreter`, an empty `commands` list, or an empty `cwd` fails configuration
validation before the node runs.

## Execution semantics

- One process per node: `<interpreter> -c "<commands joined by newline>"`. The commands
  share one script, so `cd` and `export` persist between them.
- The child inherits the full host environment, including `PATH`.
- `stdin` is null; a command that reads stdin sees EOF immediately.
- No fail-fast flags are added. Write `set -e`, `&&`, or explicit checks yourself — the
  script's exit status is the only failure signal Tasks reports.

## Outputs and node state

On success the node emits three bindable outputs: `exit_code` (number), `stdout` and
`stderr` (strings). Bind them by name on the node envelope; any other bound name fails
the node because the kind emits exactly these three.

The persisted node `state` carries the same values plus capture markers:
`stdout_dropped_bytes`, `stderr_dropped_bytes`, `stdout_abandoned_tail`,
`stderr_abandoned_tail`. At most 1 MiB per stream is retained — the rest is counted, not
stored. On failure nothing is bound; read `state` instead.

## Terminal mapping

| Outcome | Result |
|---|---|
| Exit code `0` | Succeeded, `exit_code = 0` |
| Nonzero exit | Failed — `ShellExitedNonZero { code }` |
| Killed by signal | Failed — `ShellSignaled { description }` |
| Timeout reached | Failed — `ShellTimeout { seconds }` |
| Cancelled | Failed — `ShellCancelled` |
| Interpreter missing | Failed — `ShellSpawnFailed { interpreter }` |
| `cwd` missing or not a directory | Start rejected — `ShellInvalidWorkingDirectory` |
| Pipe read, kill, or reap failure | Failed — `ShellPostSpawnFailed { kind }` |

## Cancellation, timeout, and stop

- Timeout, cancel, and `StopRuntime` kill **only the interpreter child**. There are no
  process groups and no Job Objects, so background jobs and daemons the script started
  survive.
- A surviving descendant holding the output pipes costs the 5s drain window and marks
  that stream's tail abandoned (`*_abandoned_tail = true`).
- Cancel answers `NodeLifecycleResult::Failed` with the terminal snapshot — never
  `NodeLifecycleResult::Cancelled`.
- Dispositions are `Cancellable` + `SideEffecting`, so a ShellNode may sit inside a
  `WhenAny` or `Race` controller.
- Capacity is 4 concurrent shells. A fifth stays pending and reports `Running`; it is not
  failed.
- Recovery belongs to Flow. An interrupted shell may be re-started on recovery, so
  `commands` should be safe to re-run.

## Worked example

```json
{
  "version": 1,
  "root": {
    "type": "Leaf",
    "node": {
      "id": "11111111-1111-1111-1111-111111111111",
      "label": "build",
      "constraints": { "runtime": {}, "capabilities": [] },
      "data_space": { "code_paths": [], "state_paths": [] },
      "inputs": [],
      "outputs": [
        { "name": "exit_code", "key": "build_exit_code" },
        { "name": "stdout", "key": "build_stdout" }
      ],
      "node_kind": "ShellNode",
      "configuration": {
        "interpreter": "bash",
        "commands": ["set -e", "cargo build --release"],
        "cwd": "/srv/project",
        "timeout_secs": 900
      }
    }
  }
}
```

## Known limitations

- **Cancel reply is held (NO FIX).** A cancel on a running shell is answered only when the
  runner acknowledges — up to ~7s (2s reap + 5s drain), or 10s for a lost runner. The
  generated Tasks request handler awaits one reply at a time, so other Tasks lifecycle
  requests queue behind the pending cancel.
- **Unbounded timeout (NO FIX).** `timeout_secs` is not range-checked. An absurdly large
  value overflows the deadline and leaves the node Running.
- **No dedicated ShellNode tests (NO FIX).** The kind has no tests of its own; Flow actor
  tests only stub the shell service.

## Related

- [Aves Tasks Domain Architecture](../architecture/aves/tasks-domain.md)
- [Pluggable Flow Nodes](../concepts/pluggable-flow-nodes.md)
- [Adding a Flow Node Kind](adding-a-flow-node-kind.md)
- `crates/aves/AGENTS.md`

# Aves crate

Aves owns the application-neutral runtime supervisor, generic bus, Auth, Config, Flow, Tasks, and shared utilities. An embedding application composes domains and provides the root broadcast and transport bindings. Aves must not import Corv, Kea, Tyto, or an application crate.

## Runtime and bus

- One `EventBus` is created per runtime. Domains construct sequentially against its registration borrow. The runtime then finalizes one crate-private `BusHandle` and retains it privately.
- Start callbacks run concurrently. Shutdown stops domains under a 30-second cap before bus/listener shutdown. Successful shutdown is idempotent; bus shutdown failure resets the guard for retry.
- Opaque envelopes route only by domain-marker `TypeId`; `aves_macros` generates public clients and translation. Keep internals private unless an embedding application needs a typed projection.
- Auth constructs before other domains. Public ingress requires a valid JWT; privileged service calls use typed sudo channels.

## Shared utilities

Import helpers from concrete `aves::utils` modules; there are no root utility re-exports. `utils::transport` owns generic RPC serving, while Corv owns MCP serving. `utils::database` owns generic connection and maintenance mechanisms, not an application's schemas or policies. Generic time lives in `utils::time`; `SharedInfraError` is public for cross-package conversion, never a wire type.

## Config, Flow, and Tasks

- Config (`src/config/`) registers typed sections before sealing. Public `getConfigSection` and `saveConfigSection` operate on one section; saves validate merged values and broadcast `ConfigChanged`.
- Flow (`src/flow/`) owns the open `NodeKind` + opaque `configuration` envelope. Owners push `NodeKindRegistration` into the shared `NodeRegistrationSupply` before Flow drains it at boot. Tasks registers ScriptNode and ShellNode; Corv registers AgentNode. New kinds require no Flow-core dispatch changes.
- Unregistered kinds load as definitions, but START/ENABLE reject them. Interrupted executions with missing kinds become persisted FAILED at recovery without preventing boot.
- Each execution has one writable directory under `{data_root}/executions/{execution_id}`. Disk is the live projection; Iroh blobs, per-execution docs, and immutable checkpoint collections are durable. Restore uses `materialize_clean`, never a crash-surviving directory.
- ScriptNode has exactly one WASI preopen (guest `/`). ScriptNode and AgentNode cancellation use privileged lifecycle requests. Guest RPC is not a filesystem.
- `importWorkspace` is host-local ingress. `downloadWorkspaceBlob` checks `PullCloudBlobs` or `AllBus` before network I/O; only Collection tickets are accepted.
- Capture/load failure before `PreparedCheckpointCommit` can drop successor Start (accepted NO FIX). Native Collection transfer lacks an acceptance test (accepted NO FIX).

## ShellNode

Tasks registers `ShellNode` beside `ScriptNode`: a host process, not a WASM guest. See [Using Shell Nodes](../../docs/guides/using-shell-nodes.md).

- **Child-only kill.** Timeout, cancel, and `StopRuntime` kill only the interpreter child — no process groups, no Job Objects. Descendants survive; one holding the output pipes costs the 5s drain window and marks that tail abandoned.
- **Cancel shape.** Cancel is a privileged lifecycle request (`CancelShellExecution`); it answers `NodeLifecycleResult::Failed` with the terminal snapshot, never `NodeLifecycleResult::Cancelled`.
- **Separate state.** Shell state and shell capacity are independent of ScriptNode, and `StopRuntime` has a shell-only 10s stop window.
- **Test doubles.** `TasksLifecycleSudoService` implementations must provide `start_shell`, `tick_shell`, and `cancel_shell_execution`.

## Peer runtime

Aves owns one native Iroh substrate with Bus, Auth, blobs, gossip, and docs ALPN slots. Trust is local, unilateral, and file-backed. `AuthAdmissionRegistry` is the only peer-state projection.

**First-match trust predicate:** `PairingCoordinator::is_confirmed` checks the first persisted record for a peer key. A first-revoked, later-confirmed duplicate denies issuance. Do not replace `.iter().find(...)` with `.any(...)`.

The Bus ALPN carries `RemoteOperationRequest { qualified_operation, input, jwt }`. Iroh authenticates connection identity; JWT verification pins issuer, audience, and expiry. Flow validates `source_peer_key` against `AuthProjection.issuer` before import. Mobility commits an inactive replica, fences the source, then promotes the destination; one owner at a time. No custom possession-proof handshake, transitive trust, or second peer cache.

## Flow app workspace hook

Flow requires an application-owned workspace. `FlowBootOptions.workspace_hook` is a required `FlowWorkspaceHookFactory`, and `FlowBootOptions::open(config_dir, workspace_hook)` takes it. Aves ships no default workspace implementation and never runs one alongside an application hook.

- **Construction.** `handle.rs` assembles the node registry, then calls the factory once with `FlowWorkspaceHookBoot` (data root, database, node registry, workspace source service, bus context) before the actor is constructed. One hook serves the whole Flow domain; the factory may capture application state.
- **Steps and defaults.** `takeoff`, `landing` and `release_artifacts` are required. `checkpoint` defaults to an empty capture, `restore` and `execution_root` to the context root, `recoverable` to `Ok(true)`, artifact projection, `shutdown` and `checkpoint_committed` to `Ok(())`; import, download and script source fail with `WorkspaceError::ProviderUnavailable`.
- **Context.** `FlowAppHookContext` carries execution id, flow id, definition revision, execution root and invocation kind. `FlowAppLandingContext` adds terminal status and node correlations for `landing`.
- **Application-facing visibility.** `ContentHash::as_str()` and `MachineIssuerId::public_key()` are public so an application hook can persist and read back the path-to-hash projection it returns from `checkpoint` and can identify a mobility transfer source. No other visibility is widened for applications.
- **Call-site guarantees.**
  - `takeoff` is the workspace preparation boundary before the first tick: the application returns `FlowAppTakeoffResult` with the dispatch root and an optional imported root. A takeoff failure fails Start with `FlowError::AppWorkspaceTakeoff`, and the application must clean up its own failed preparation. A takeoff root needs no native document.
  - `checkpoint` runs at start and at every transition, before the snapshot is durable; the application returns an optional checkpoint root and path-to-hash projection. `checkpoint_committed` follows once the snapshot is saved.
  - `restore` runs at restart and selects the root used for node dispatch.
  - `landing` runs after the durable terminal snapshot and before the terminal broadcasts, for every outcome. An abort takes no checkpoint; the durable aborted snapshot is followed by `landing`, which is the handoff. A landing failure is logged and does not block terminalization.
  - `release_artifacts` runs at retention cleanup after a replay of `landing`, followed by the execution record's deletion. Cleanup replays carry no correlations; implementations must handle repeated calls. A hook failure keeps the cleanup anchor for retry without suppressing the new run's terminal broadcasts.
- **Tests.** `flow/actor/tests.rs` exercises the hook through a `RecordingWorkspaceHook`: takeoff ordering and context, takeoff failure, landing failure, landing statuses, and retention cleanup replaying `landing` before `release_artifacts`.

## References and tests

Start at [engineering navigation](../../docs/navigation.md). See [Flow Execution Workspace](../../docs/concepts/flow-execution-workspace.md), [Flow Workspace Trait](../../docs/concepts/flow-workspace-trait.md), [Pluggable Flow Nodes](../../docs/architecture/aves/pluggable-flow-nodes.md), [Using Shell Nodes](../../docs/guides/using-shell-nodes.md), and [Iroh Peer Runtime](../../docs/architecture/aves/iroh-peer-runtime.md).

Run `direnv exec . cargo test --package aves` from the repository root. Downstream crate tests use Aves's existing `test-support` feature; never widen production APIs for fixtures.

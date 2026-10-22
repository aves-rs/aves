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
- Flow (`src/flow/`) owns the open `NodeKind` + opaque `configuration` envelope. Owners push `NodeKindRegistration` into the shared `NodeRegistrationSupply` before Flow drains it at boot. Tasks registers ScriptNode; Corv registers AgentNode. New kinds require no Flow-core dispatch changes.
- Unregistered kinds load as definitions, but START/ENABLE reject them. Interrupted executions with missing kinds become persisted FAILED at recovery without preventing boot.
- Each execution has one writable directory under `{data_root}/executions/{execution_id}`. Disk is the live projection; Iroh blobs, per-execution docs, and immutable checkpoint collections are durable. Restore uses `materialize_clean`, never a crash-surviving directory.
- ScriptNode has exactly one WASI preopen (guest `/`). ScriptNode and AgentNode cancellation use privileged lifecycle requests. Guest RPC is not a filesystem.
- `importWorkspace` is host-local ingress. `downloadWorkspaceBlob` checks `PullCloudBlobs` or `AllBus` before network I/O; only Collection tickets are accepted.
- Capture/load failure before `PreparedCheckpointCommit` can drop successor Start (accepted NO FIX). Native Collection transfer lacks an acceptance test (accepted NO FIX).

## Peer runtime

Aves owns one native Iroh substrate with Bus, Auth, blobs, gossip, and docs ALPN slots. Trust is local, unilateral, and file-backed. `AuthAdmissionRegistry` is the only peer-state projection.

**First-match trust predicate:** `PairingCoordinator::is_confirmed` checks the first persisted record for a peer key. A first-revoked, later-confirmed duplicate denies issuance. Do not replace `.iter().find(...)` with `.any(...)`.

The Bus ALPN carries `RemoteOperationRequest { qualified_operation, input, jwt }`. Iroh authenticates connection identity; JWT verification pins issuer, audience, and expiry. Flow validates `source_peer_key` against `AuthProjection.issuer` before import. Mobility commits an inactive replica, fences the source, then promotes the destination; one owner at a time. No custom possession-proof handshake, transitive trust, or second peer cache.

## References and tests

Start at [engineering navigation](../../docs/navigation.md). See [Flow Execution Workspace](../../docs/concepts/flow-execution-workspace.md), [Pluggable Flow Nodes](../../docs/architecture/aves/pluggable-flow-nodes.md), and [Iroh Peer Runtime](../../docs/architecture/aves/iroh-peer-runtime.md).

Run `direnv exec . cargo test --package aves` from the repository root. Downstream crate tests use Aves's existing `test-support` feature; never widen production APIs for fixtures.

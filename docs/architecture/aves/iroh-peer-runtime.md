---
title: Aves Iroh Peer Runtime
type: architecture
updated: 2026-11-14
references:
  - crates/aves/src/machine/peering.rs
  - crates/aves/src/machine/domain.rs
  - crates/aves/src/machine/content.rs
  - crates/aves/src/bus/remote.rs
  - crates/aves/src/auth/pairing.rs
  - crates/aves/src/auth/peer.rs
  - crates/aves/src/auth/types.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/coordination/domain.rs
  - crates/aves/src/coordination/actor.rs
---

# Aves Iroh Peer Runtime

Aves uses one shared native Iroh endpoint and router. Machine owns the substrate and peer services; Auth owns identity and admission, and Bus owns its generated-operation protocol handler. The router accepts Auth, blobs, gossip and docs ALPNs, plus Bus when a remote-operation installer is supplied.

## Construction and lifecycle

The runtime constructs Auth first, then prepares `IrohSubstrate` under the data root's `flows` directory using Auth's machine key. The native blob and docs stores reside in `flow-workspace-blobs` and `flow-workspace-docs`. With `ConstructionInputs`, the runtime supplies `MachinePeerServices` and the pairing owner to domains. Machine publishes `dyn MachinePeerService` during construction; Flow and Coordination use late-bound readers, so their construction order relative to Machine does not determine service discovery. Flow's default workspace still requires the concrete peer services construction input. Flow records sync availability at `StartRuntime`; Coordination requires the published service at `StartServer`.

After domain construction registers the generated receivers, the runtime installs the optional Bus handler, starts the substrate router, and finalizes the bus. Domain start callbacks run concurrently. Machine's start and stop requests use the connectivity controller: effective networking depends on both server state and preference. A transition can close and replace the endpoint and native handles; consumers can subscribe to replacement notifications. When disabled, the endpoint is bound locally without relay or IP transports.

Shutdown stops Flow first and then the remaining domains under the shared timeout, shuts down the router and blob store, closes the endpoint, and shuts down the bus. A bus shutdown failure permits retry.

## Trust and admission

Auth stores peer records in a local JSON `PeerConfig`; trust is unilateral and non-transitive. First contact uses an invitation containing a machine ID and one-time secret. The receiver consumes the secret before recording the candidate; Iroh authenticates the connection identity, and separate local confirmation grants trust. Auth projects admitted issuers into the shared `AuthAdmissionRegistry`; Machine status reads that registry rather than maintaining a second peer cache. Admission does not establish online status.

Outbound remote calls use `machine::call_remote_operation_with`, which refreshes the peer audience and requests a peer JWT at call time. The Bus handler checks that the JWT issuer matches the authenticated Iroh peer before dispatch; normal bus admission verifies the JWT. There is no runtime `call_remote_operation` helper.

## Bus and native protocols

`bus/remote.rs` owns the Bus ALPN and `IrohProtocolHandler`. Its JSON request contains `qualified_operation`, `input` and `jwt`; generated public operation descriptors select dispatch, without privileged operations. The handler caps requests and responses at 1 MiB; the operation layer caps serialized requests and replies at 512 KiB. The client verifies the remote endpoint identity and uses a native bidirectional stream.

`MachinePeerServices` exposes the shared blob store and replaceable native docs, endpoint and gossip handles. Higher-level applications can use this public carrier to join the peer network. The blob-serving handler itself has no admission check; Machine's content fetch separately restricts its chosen remote issuer to those admitted by Auth. See [Machine Domain](machine-domain.md) for content and encryption limits.

## Related

- [Machine Domain](machine-domain.md) — peer services, content and encryption
- [Auth Domain](auth-domain.md) — identity and peer admission
- [Flow Domain](flow-domain.md) — Flow execution
- [Runtime Supervisor](runtime-supervisor.md) — runtime composition

---
title: Aves Iroh Peer Runtime
type: architecture
updated: 2026-10-23
references:
  - crates/aves/src/bus/iroh.rs
  - crates/aves/src/bus/remote.rs
  - crates/aves/src/bus/mod.rs
  - crates/aves/src/auth/pairing.rs
  - crates/aves/src/auth/peer.rs
  - crates/aves/src/auth/types.rs
  - crates/aves/src/auth/jwt.rs
  - crates/aves/src/auth/keystore.rs
  - crates/aves/src/auth/domain.rs
  - crates/aves/src/flow/sync.rs
  - crates/aves/src/flow/mobility.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/workspace.rs
  - crates/aves/src/runtime/supervisor.rs
---

# Aves Iroh Peer Runtime

Aves owns one shared native Iroh endpoint/router (`IrohSubstrate` / `RunningIrohSubstrate`) and five ALPN slots: the Bus's generated public-operation ALPN (`BUS_ALPN = b"sluice/bus"`), Auth's first-contact ALPN (`AUTH_ALPN = b"sluice/auth"`), and the three native Iroh protocols (`iroh_blobs::protocol::ALPN`, `iroh_gossip::net::GOSSIP_ALPN`, `iroh_docs::net::ALPN`). Each pluggable surface owns a distinct concern; the substrate only owns transport lifecycle.

## Ownership

- **Substrate** (`crates/aves/src/bus/iroh.rs`): constructs one `Endpoint`, owns the native `iroh::Router`, hands out `IrohProtocolHandler` slots and the running router handle. Re-exported privately from `crates/aves/src/bus/mod.rs:8`.
- **Bus** (`crates/aves/src/bus/remote.rs`): installs `BUS_ALPN` via `RemoteOperationInstaller::for_operations`. Transport stays with the substrate; the installer only carries operation descriptors.
- **Auth** (`crates/aves/src/auth/pairing.rs`, `auth/peer.rs`): owns durable peer state, the `AUTH_ALPN` slot, the `AuthFirstContactHandler`, and `AuthAdmissionRegistry` (`crates/aves/src/auth/types.rs`) — the dynamic issuer/audience projection forwarded into bus admission.
- **Flow** (`crates/aves/src/flow/sync.rs`, `flow/mobility.rs`, `flow/domain.rs`): owns `FlowSyncServices { blobs, docs_api, gossip, endpoint }` plus the typed `FlowMobilityService` trait (narrow: `import_snapshot` + `promote_replica` only). Select, yield, synchronize, and promote-replica orchestration live on the Flow `Actor`; the trait is the peer-call seam only. Flow borrows the substrate's endpoint and never modifies Bus or Auth admission.

## Unilateral, non-transitive, file-backed trust

`PeerConfig` (`crates/aves/src/auth/peer.rs`) is the sole durable peer store: a JSON file holding `peers: Vec<PeerRecord { peer: PublicKey, endpoint: PeerEndpoint, state: PeerTrustState::{Confirmed,Revoked}, updated_at }>`. `PairingCoordinator::load` reads it, enforces `record.endpoint.public_key() == record.peer`, and `persist` writes atomically via a temp file + rename.

Trust is **unilateral**: each machine maintains its own `PeerConfig`; there is no transitive trust propagation. Native Iroh already authenticates the QUIC connection via `connection.remote_id()`. Sluice uses N0 discovery, so the remote machine's `EndpointId` is sufficient dialing information; persisted `EndpointAddr` metadata is learned during authenticated first contact rather than embedded in the invitation.

`CreatePairingInvitation` returns the compact form `pair::<machine-id>::<one-time-secret>`. Auth stores only the secret's SHA-256 digest with its expiry and consumed state. `FirstContact` parses the invitation, dials its machine ID through Iroh discovery, and sends the one-time secret with `FirstContactMetadata { machine_id, endpoint, audience, refresh, invitation_secret }`. The receiving `AuthFirstContactHandler` atomically validates and consumes that secret before recording the authenticated candidate. The token admits exactly one first-contact attempt; it is not identity proof and does not grant trust. Identity remains the Iroh-authenticated `connection.remote_id()`.

Confirmation is a separate local privileged backend action (`PairingCoordinator::confirm`) that accepts only a machine observed through authenticated first contact, writes it to the local file, and upserts `AuthAdmissionRegistry`. Revocation is likewise local only.

The runtime owns no second cache: every reload rebuilds the in-memory `peers` vector and projects it into `AuthAdmissionRegistry::replace_remotes`. `AuthAdmissionRegistry` is the only projection of `PeerConfig`; nothing else reads peer state.

## Dynamic JWT issuer/audience admission

`AuthAdmissionRegistry` (`crates/aves/src/auth/types.rs`) holds an immutable local `ApprovedIssuer` (the machine's own `MachineIssuerId`) and an `Arc<RwLock<HashMap<MachineIssuerId, ApprovedIssuer>>>` for remotes. `upsert_remote` / `remove_remote` mutate the map; `replace_remotes` runs after durable pairing reconciliation. `current_issuers` is the projection the runtime forwards into bus admission. JWTs carry `iss`, `aud` (the audience minted at Auth boot), `sub`, `cap`, `jti`, `exp` (`crates/aves/src/auth/jwt.rs`); verification in `verify_jwt_unchecked` pins issuer, audience, and expiration with zero leeway. On the wire, `RemoteOperationRequest.jwt` is checked against the Iroh peer's `PublicKey` (`crates/aves/src/bus/remote.rs:152-159`): `jwt_issuer != peer_issuer` returns `RemoteOperationError::IssuerMismatch` before any operation dispatch.

## Generated public operations over native QUIC

The Bus ALPN (`BUS_ALPN = b"sluice/bus"`, `crates/aves/src/bus/remote.rs:17`) carries exactly one payload shape: `RemoteOperationRequest { qualified_operation, input, jwt }`. `RemoteOperationInstaller::for_operations` wraps the existing `DomainPublication` descriptors — privileged operations are absent — into an `IrohProtocolHandler`. `RemoteOperationRequest` is `serde_json` (limit `MAX_OPERATION_BYTES = 512 * 1024`); the reply is `RemoteOperationReply::{Success, DomainError, Rejected}`. `call<Request,Response>` (`crates/aves/src/bus/remote.rs:193`) opens a QUIC bidirectional stream on the substrate endpoint, writes the JSON, finishes the send half, and reads up to `MAX_OPERATION_BYTES` for the response. Identity is checked at the Iroh layer (`connection.remote_id() == target.id`); admission is checked at the JWT layer. The substrate's `IrohProtocolHandler` accepts raw reads up to `MAX_REQUEST_SIZE = 1024 * 1024`; the dispatch layer enforces the tighter `MAX_OPERATION_BYTES`.

## source_peer_key validated against AuthProjection.issuer

Flow's `authorize_public_request` (`crates/aves/src/flow/domain.rs:193`) gates `ImportMobilitySnapshot`: the request's `source_peer_key` must equal `projection.issuer` (the authenticated JWT issuer projected from the bus admission layer). Mismatch returns `FlowError::MobilityPeer` before any import work begins. The runtime's `RuntimeFlowMobilityService` (`crates/aves/src/runtime/supervisor.rs:40`) supplies `sourcePeerKey: self.source_issuer` (the local machine's `MachineIssuerId`) as the outbound payload; the remote Flow domain verifies it against the JWT-derived projection.

## Native Docs/Gossip/Blobs

`FlowSyncServices::new(blobs: FsStore, docs_api: DocsApi, gossip: Gossip, endpoint: Endpoint)` (`crates/aves/src/flow/sync.rs`) is the typed carrier for Flow's native Iroh services. Flow decides which authority, snapshot, and commit belong to a flow via `MobilityCoordinator` (`crates/aves/src/flow/mobility.rs`), but the durable artifacts always ride native Iroh — not bespoke sockets. `FlowMobilityService` is the narrow peer-call seam (`import_snapshot`, `promote_replica`); select/yield/synchronize/promote orchestration live on the Flow `Actor`.

**Yield acknowledgement is coordination data**, not cryptographic proof: `FlowYieldAcknowledgement { flow_id, snapshot_generation }` carries the flow identity and revision, but single-owner-at-all-times is enforced by protocol ordering (source yields before destination promotes), not by cryptographic binding.

## Lifecycle ordering

1. Auth domain constructed first (`crates/aves/src/auth/domain.rs`): `MachineIdentityKeyring::new(namespace)` loads or mints Ed25519, `AuthAdmissionRegistry::new` built, actor spawned, boot-minted audience UUID + root JWT returned.
2. `IrohSubstrate::prepare` called with `{config_dir}/flows` + machine secret key; its blobs/docs live at `flows/flow-workspace-blobs` and `flows/flow-workspace-docs`. No automatic relocation of the old `iroh/` store. `PairingCoordinator::attach_endpoint` attaches the substrate endpoint.
3. `FlowSyncServices` + `RuntimeFlowMobilityService` composed into construction inputs; `bus.install_auth` with registry/verifier/audience; root JWT/context built.
4. Sequential domain construction (Auth already done); each domain receives the same mutable registration borrow.
5. `RemoteOperationInstaller::install(bus.input_sender())` — **only after** all generated-operation receivers are registered. This keeps startup from admitting a request into a partially composed runtime.
6. `IrohSubstrate::start(remote_handler, first_contact_handler)` — `Router::builder` accepts all five ALPNs and spawns.
7. `BusHandle::new` finalizes the bus.

**Audience refresh is call-time**, not a startup sweep: `PairingCoordinator::refresh_audience` runs before each outbound peer JWT in `call_remote_operation_with` (`crates/aves/src/runtime/supervisor.rs:108-148`), not at boot.

**Shutdown** stops Flow first (sequential, under a 30s cap) so the Flow actor runs `shutdown_runtime` (`crates/aves/src/flow/actor.rs:264`) — draining background tasks, then delegating to `WorkspaceStore::shutdown` (`crates/aves/src/flow/workspace.rs:437`), which closes open documents, waits the blob store idle, and syncs the blob DB. Then remaining domains stop concurrently. Only after domain stops complete does the runtime call `RunningIrohSubstrate::shutdown_router` (30s timeout), then `close_endpoint`, then `bus.shutdown`. Failed bus shutdown resets the guard for retry; successful shutdown is idempotent.

## Non-goals

- **No custom handshake framing.** Wire payloads are `serde_json` over native QUIC bi-streams; ALPN is the only framing boundary.
- **No additional possession-proof handshake.** Native Iroh authenticates the QUIC connection. The one-time invitation secret is consumed as first-contact admission, not used as identity proof; the metadata exchange remains a mutual observation of endpoint addresses and audiences. Confirmation is a separate local privileged backend action. No transitive trust propagation.
- **No operation policy beyond admission.** Dispatch finds `operator.qualified_operation == request.qualified_operation` and returns `UnknownOperation` otherwise; capability gating is enforced earlier by `current_issuers` + `verify_jwt_unchecked`, not here.
- **No transport-origin admission classes.** Bus-context proof is the single safety boundary.
- **No second peer cache.** `AuthAdmissionRegistry::replace_remotes` is the only projection of `PeerConfig`; nothing else reads peer state.

## Related

- [Aves Runtime Supervisor](runtime-supervisor.md) — substrate construction order
- [Aves Auth Domain Architecture](auth-domain.md) — Auth-first construction, JWT issuance
- [Aves Flow Domain Architecture](flow-domain.md) — Flow ownership and authority state
- `crates/aves/AGENTS.md` — local Aves rules

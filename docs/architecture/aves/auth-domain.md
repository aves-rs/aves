---
title: Aves Auth Domain Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/aves/src/auth/mod.rs
  - crates/aves/src/auth/actor.rs
  - crates/aves/src/auth/actor/tests.rs
  - crates/aves/src/auth/domain.rs
  - crates/aves/src/auth/handle.rs
  - crates/aves/src/auth/jwt.rs
  - crates/aves/src/auth/keystore.rs
  - crates/aves/src/auth/messages.rs
  - crates/aves/src/auth/pairing.rs
  - crates/aves/src/auth/peer.rs
  - crates/aves/src/auth/types.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/bus/event_bus.rs
  - crates/aves/tests/event_bus_admission.rs
  - crates/aves/tests/auth_sealing.rs
---

# Aves Auth Domain Architecture

Aves Auth is a built-in control-plane domain that issues signed JWTs, tracks JTI liveness in RAM, and persists a machine identity in the OS keyring. It is installed unconditionally by `Runtime::new` before rail-domain construction and bus finalization.

## Ownership and boundaries

- **Aves owns:** the Auth actor, handler, messages, types, JWT issuance/verification, machine-identity keystore, and JTI registry. Auth is a domain-shaped component with the same actor/handler/message separation and `aves::domain!` declaration model used by rail domains.
- **Embedding applications own:** supplying a non-empty keyring namespace via `RuntimeOptions.keyring_service_namespace` and adapting `AvesAuthError` to their root error at the composition boundary.
- **Discuss owns:** obtaining non-root delegated JWTs (`sub = "agent"`) through `AuthSudoService` for chat and transient workers, storing the returned JTI with worker lifecycle state, and revoking it on every terminal path. Flow AgentNode reuses Flow's `flow-node:` token; Discuss does not mint or revoke that JTI.

## Sudo-only messaging

Auth declares **only** privileged sudo operations in its `aves::domain!` declaration (`crates/aves/src/auth/domain.rs`). All **eleven** `AuthRequestPayload` arms — `IssueJwt`, `IssueRootJwt`, `IssuePeerJwt`, `VerifyJwt`, `RevokeJti`, `PairPeer`, `RevokePeer`, `ListPeers`, `ListPairingCandidates`, `CreateInvitation`, `FirstContact` — are `#[privileged]` (`crates/aves/src/auth/messages.rs`) and mapped through `AuthSudoService`:

- **No public surface.** `AUTH_REQUEST_PAYLOAD_OPERATIONS` is empty and `auth_rpc_router()` yields zero procedures — asserted from an external crate in `crates/aves/tests/auth_sealing.rs`. That test does not prove generated-client exclusion of `IssueJwt` / `RevokeJti` / `VerifyJwt`.
- **No Serde participation.** All nine `AuthResponse` variants are `#[serde(skip)]`; sudo-only structures never serialize.
- **Bus admission dependency.** Bus ingress calls `VerifyJwt` through this sudo service before routing, avoiding recursion through the bus it is authenticating.
- **Delegated issuance returns `{ token, jti }`** (`AuthResponse::DelegatedJwtIssued`), so the requesting domain never parses a token to learn its JTI. The JTI is computed via claim verification at issuance.
- **`RevokeJti` is idempotent success** for already-revoked, expired, or unknown delegated JTIs.
- **Root message exports and the privileged-only generated service remain public.** Actor-only inputs (`ActorRequest`, `ActorResponse`) and internal JWT/peer helpers (`issuer_from_jwt`, peer JWT issue request) use narrower visibility.

## Machine identity and keyring

`MachineIdentityKeyring` (`crates/aves/src/auth/keystore.rs`) is Aves-owned; it stores the runtime's signing identity, not application credentials.

- **Keyring service namespace** is supplied via `RuntimeOptions.keyring_service_namespace`. Aves never hard-codes the application name. Empty namespace is rejected at boot.
- **Load-or-create:** the `iroh::SecretKey` loads on first boot; if no entry exists, it creates one. This is a read-then-write sequence, not an atomic operation; concurrent first-boot processes may race and create multiple keys, with the last write winning. The key survives restarts and uses zeroizing private-byte carriers.
- **Issuer material:** derives the canonical `iroh::PublicKey` Display identity as the issuer string plus its 32 public bytes as verification material. The bus stores accepted issuers as a vector; this pass inserts only the local machine issuer.
- **Mock keyring store:** `MachineIdentityKeyring::new_mock` uses `keyring_core::mock::Store` for deterministic tests, avoiding OS-keyring variance across platforms.

## JWT contract

`Jwt` (`crates/aves/src/auth/jwt.rs`) is an opaque compact signed JWT/JWS carrier with redacting `Debug` (`<redacted>`) and no `Display`. Claims are `iss`, `aud`, `sub`, `jti`, `exp`, `cap`:

- **`iss`:** persistent Aves Iroh machine identity managed by Auth. The issuer is the canonical `iroh::PublicKey` Display identity derived from the keyring-stored `iroh::SecretKey`; verification uses its 32 public bytes.
- **`aud`:** ephemeral UUID minted by the bus on every boot. Bus restart invalidates all prior tokens while the issuer survives restarts.
- **`sub`:** caller-supplied non-empty subject. Delegated issuance rejects empty and `"root"`; root issuance fixes `sub = "root"`.
- **`jti`:** UUID v4, active at issuance. Delegated JTIs are revocable through `RevokeJti`; root JTIs are not.
- **`exp`:** required and strictly checked. The construction root is `now + 365 days`; delegated worker issuance supplies an explicit expiration (Discuss chat/transient workers use 30 days).
- **`cap`:** `AllBus` (wildcard) and additive `PullCloudBlobs` (`crates/aves/src/auth/types.rs`). Flow's `downloadWorkspaceBlob` is the only public op that checks `PullCloudBlobs` today; other generated handlers still default-admit after JWT verification.

## Actor-owned JTI registry

`Actor` (`crates/aves/src/auth/actor.rs`) exclusively owns mutable state: the signing identity and **two** in-memory JTI registries — `active_jtis` (delegated) and `root_jtis` (root) — each a `HashMap<Uuid, DateTime<Utc>>`.

- **Delegated lifecycle:** active at issuance; `RevokeJti` removes the entry, and verification of a removed JTI fails immediately with `InactiveToken` (the registry lookup fails closed on absence — removal *is* revocation).
- **Root lifecycle:** root JTIs are **never removed by delegated revocation** — `RevokeJti` touches only `active_jtis`. Root tokens stay live until expiry or runtime teardown.
- **Idempotent revocation:** revoking an already-revoked, expired, or unknown JTI is success.
- **Opportunistic pruning:** `prune_expired` runs on every issuance, verification, and revocation. No cleanup ticker, no persistence.
- **Expiry is the fallback:** if a delegated-token owner cannot complete revocation, the token dies at its `exp`.

## Verification and peer-issuance flow

- **JWT verification** (`Actor::verify`): parse the unverified issuer hint from the JWT payload (`issuer_from_jwt`), look up that issuer in the caller-supplied `accepted_issuers` list, then run cryptographic `verify_jwt` against the matched approved issuer's verification key. For local-issuer tokens, perform a local-only JTI liveness gate (lookup in `active_jtis` ∪ `root_jtis`) before building the single shared `AuthProjection`. One projection is built after the gate — not duplicated per branch.
- **Peer JWT issuance** (`Actor::issue_peer`): rejects local target, then borrows `PairingCoordinator::is_confirmed(&target)` under a brief mutex guard instead of cloning the full peer list. Short lifetime (`PEER_JWT_LIFETIME = 5 min`), no local JTI registration.
- **Duplicate-peer invariant (CRITICAL):** `PairingCoordinator::is_confirmed` uses `.iter().find(|r| r.peer == requested_public_key()).is_some_and(|r| r.state == Confirmed)` — only the **first** matching persisted record decides trust. A first-revoked + later-confirmed duplicate for the same peer public key **denies** issuance. Do not replace with `.any(...)` — that would grant on any confirmed record regardless of order.
- **Outbound pairing wire exchange:** one private `PairingCoordinator::outbound_exchange` helper carries the shared Iroh `open_bi` → `serde_json` → `read_to_end` → deserialize sequence; each caller (`outbound_first_contact`, `refresh_audience`) supplies its own error-mapping function and preserves its caller-specific target identity and trust checks. No mutex is held across `.await`.

## Auth-first construction

`Runtime::new` (`crates/aves/src/runtime/supervisor.rs:122-199`) constructs Auth before rail domains and bus finalization:

1. Validates the keyring namespace is non-empty.
2. Creates `AuthBootOptions { namespace, audience }` with a boot-minted audience UUID.
3. Constructs the Auth domain against a `ConstructionFacade` with auth boot options. Auth constructs its machine-identity keystore, loads or creates the Iroh signing key, and returns the machine issuer material plus a root JWT. The construction root JWT is issued by an internal `Actor::issue_root` call inside `handle::construct` (`crates/aves/src/auth/handle.rs:32`) before the sudo request bridge loop is spawned — an internal construction call using the same actor logic, not a public/bus/RPC/MCP path (accepted disposition; there is no external surface to close).
4. Receives the machine issuer material directly from Auth construction.
5. Sets the accepted-issuer vector (this pass: exactly the local machine issuer) and the generated sudo verifier into the bus via `bus.install_auth`.
6. Retains the root JWT and constructs a root `BusRpcContext` from it. This root context is passed to each rail-domain construction callback via `ConstructionFacade::new_with_root_context`, and the same root JWT is passed to `BusHandle::new`, which retains it and uses it to mint `BusRpcContext` values via `rpc_context()`.
7. Constructs rail domains sequentially with the root context.
8. Finalizes the bus into `BusHandle`.

The root JWT is minted once at construction and retained by the bus handle. An embedding application may request additional root tokens later through the sudo `IssueRootJwt` path.

## Handle routing and construction visibility

`handle::construct` bridges the sudo channel to the actor and returns `ConstructionResult`. The `ActorResponse` → `AuthResponse` conversion is centralized in one private `impl From<ActorResponse> for AuthResponse` in `handle.rs` — an exhaustive match over every actor reply arm, preserving closed-channel failure behavior. `AuthBootOptions` and `ConstructionResult` remain `pub` because they appear in public runtime construction signatures (`RuntimeOptions`, `ConstructionFacade`); narrowing them would break external-crate consumers. `AuthDomain` is `pub(crate)` (re-exported `pub` for `runtime/supervisor.rs` composition). Internal registry mutators, peer config, and JWT claim helpers are narrowed to `pub(super)` / `pub(crate)` based on actual call sites.

## Test layout

Primary actor, types-registry, and lifecycle tests (15 total) live in `crates/aves/src/auth/actor/tests.rs`, wired via `#[cfg(test)] mod tests;` in `actor.rs`. Ancillary `keystore.rs` and `pairing.rs` unit tests remain local to those files. Test relocation preserved every fixture and assertion; only module-relative import paths changed. `Actor::new` (test-only constructor) and `MachineIdentityKeyring::new_mock` / `from_store` remain available to the relocated tests without widening production visibility.

## Error integration

Auth failures use the typed `AvesAuthError` contract (`crates/aves/src/auth/types.rs`). Generated Auth operation publication remains generic over the embedding application's root error.

## Related

- [Aves Runtime Supervisor](runtime-supervisor.md) — Auth-first construction ordering
- [Bus JWT Admission](../../concepts/bus-jwt-admission.md) — opaque envelope, projection, mandatory verified JWT
- [MCP-over-ACP Tooling](../../concepts/mcp-over-acp-tooling.md) — delegated worker-token issuance and revocation
- `crates/aves/AGENTS.md` — local Aves rules

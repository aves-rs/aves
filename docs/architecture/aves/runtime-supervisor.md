---
title: Aves Runtime Supervisor
type: architecture
updated: 2026-10-23
references:
  - crates/aves/src/runtime/mod.rs
  - crates/aves/src/runtime/context.rs
  - crates/aves/src/runtime/domains.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/auth/domain.rs
  - crates/aves/src/auth/keystore.rs
  - crates/aves/src/bus/event_bus.rs
  - crates/aves/src/bus/handle.rs
  - crates/aves/src/bus/control.rs
  - crates/aves/src/bus/messages.rs
  - crates/aves/macros/src/lib.rs
---

# Aves Runtime Supervisor

Aves owns the reusable runtime supervisor and the generic bus. The runtime and
its bus are one-to-one: `Runtime<RootBroadcast>` creates exactly one
`EventBus`, and the bus outlives construction only inside the runtime-owned
crate-private `BusHandle` (`crates/aves/src/bus/handle.rs`). Embedding
applications never touch `EventBus` after construction.

## Construction order

`Runtime::new(options, domains)` (`crates/aves/src/runtime/supervisor.rs`) runs a fixed
sequence:

1. **Validate keyring namespace.** The `RuntimeOptions.keyring_service_namespace` must be non-empty; empty namespace returns `BusControlError::EmptyKeyringServiceNamespace`.
2. **One `EventBus`** is created for the runtime.
3. **Auth-first construction.** The built-in Auth domain is constructed before rail domains and bus finalization. `AuthBootOptions { namespace, audience }` is built with a boot-minted audience UUID. Auth constructs its machine-identity keystore, loads or creates the Ed25519 signing key, and returns the machine issuer material plus a root JWT. The runtime sets the accepted-issuer vector and the generated sudo verifier into the bus via `bus.install_auth`. The root JWT is retained by `BusHandle::new`, which uses it to mint `BusRpcContext` values via `rpc_context()`.
4. **`ConstructionFacade`** (`runtime/context.rs`) is built over the bus borrow; it holds explicitly supplied construction inputs (`input::<T>()`), the `ServiceSlotRegistry`, and optionally the Auth boot options or root JWT context. Config is not a `Runtime` type parameter.
5. **Sequential rail-domain construction.** Each `DomainDescriptor` (`runtime/domains.rs`) invokes its erased `new` callback against a fresh `ConstructionFacade`. Construction is deliberately serial: every callback receives the same mutable registration borrow, and a construction panic is fatal with no Aves rollback or compensation.
6. **Bus finalization last.** Routing starts with `EventBus::new` so domains can register during sequential construction. After all `new` callbacks complete, the bus is transferred into `BusHandle::new`, which starts the listener-control actor, publishes `dyn BusSudoService`, and is the only runtime-owned public projection (`request_sender`, broadcasts, RPC context).

## Opaque ingress and TypeId routing

`EventBus` (`crates/aves/src/bus/event_bus.rs`) exposes one shared
`mpsc::Sender<OpaqueRequestEnvelope>`. The envelope carries only the
marker-derived `TypeId` and the concrete request erased behind `Box<dyn Any>`;
no route strings, raw bodies, or transport metadata enter it. The routing task
forwards each envelope to the registered marker's adapter, which downcasts to
the concrete request. Unregistered markers and body/marker mismatches panic —
they are construction-time contract violations, not runtime error paths.

Domains never receive `KernelContext`. During construction each sees only a
`ConstructionFacade` (`runtime/context.rs`) exposing `bus_mut()` for
registration, typed `ServicePublisher`/`ServiceReader` slots
(`OnceLock<Arc<Service>>` keyed by `TypeId`), and `input::<T>()` for
explicitly supplied construction inputs. Slots are late-bound: a consumer
reads when it needs the service; an empty slot before publish or during
shutdown is the owning domain's concern.

## Typed runtime projections

The runtime exposes only narrow projections: `request_sender()`,
`subscribe_broadcasts()`, `rpc_context()`, and `service_reader::<Service>()`.
`BusRpcContext` (`bus/messages.rs`) carries the bus sender and a server-issued
JWT. `BusHandle::rpc_context` mints an authenticated context from the root
JWT; admission verifies the JWT against the current issuer set and audience
before routing.

`ConstructionFacade` exposes generic `reader::<Service>()` and
`publisher::<Service>()`. Aves does not compile-time-filter which traits a
callback may request.

Domain descriptors come from `DomainDescriptorFactory` on empty markers; the
erased `DomainCallbacks { new, start, stop }` are the only lifecycle surface
(`runtime/domains.rs`). `runtime_domains!` emits lifecycle, one nested RSPC
router, per-domain MCP publications, and `From<DomainBroadcast> for RootBroadcast`.
`aves::domain!` generates `DomainProviderMetadata` for service-bearing domain
declarations; service-free markers implement `ProviderSet = ()` by hand.

## Shutdown

`Runtime::shutdown` (`runtime/mod.rs`) is idempotent with a specific ordering:

1. An `AtomicBool` guard makes concurrent calls no-ops.
2. All domain `stop` callbacks run concurrently in a `JoinSet`, raced against
   one 30-second timer via `tokio::select!` — a single all-domains deadline,
   not per-domain timeouts. During this phase, the bus remains live and normal
   ingress continues; domains can still send and receive bus messages. When the
   timer wins, dropping the `JoinSet` aborts remaining stop tasks; domains still
   contain their own stop failures.
3. After all stop callbacks complete (or timeout), bus shutdown closes routing
   and the listener-control actor (`BusHandle::shutdown`), which gracefully
   closes every RPC/MCP listener. At this point, normal ingress stops and the
   bus is no longer available for new requests.
4. Bus failure resets the guard so a later call may retry; successful shutdown
   remains idempotent.

`start()` is symmetric: all domain `start` callbacks run concurrently
(`JoinSet`), no imposed order, no rollback; failures are domain-contained.

## Related

- [Aves Auth Domain Architecture](auth-domain.md) — Auth-first construction, machine identity, JWT issuance
- [Bus JWT Admission](../../concepts/bus-jwt-admission.md) — opaque envelope, projection, mandatory verified JWT
- [Aves Domain RPC](../../guides/aves-domain-rpc.md) — macro-generated surfaces
- `crates/aves/AGENTS.md` — local Aves rules

---
title: Declaration-Driven Handler Routing
type: concept
updated: 2026-10-23
references:
  - crates/aves/macros/src/lib.rs
  - crates/aves/macros/src/domain.rs
  - crates/aves/macros/src/routing.rs
  - crates/aves/macros/src/tests.rs
  - crates/aves/src/config/domain.rs
  - crates/corv/src/domain.rs
  - crates/tyto/src/domain.rs
  - crates/aves/src/flow/domain.rs
  - crates/kea/src/domain.rs
  - crates/kea/src/handle.rs
---

# Declaration-Driven Handler Routing

The `aves::domain!` macro (crate-root entry `src/lib.rs`, owned by `src/domain.rs` and `src/routing.rs`) generates complete handler routing from declarative inbound and outbound mapping blocks in each domain's `domain.rs`. Domains declare how root broadcasts map to actor commands, how actor responses/events map to domain broadcasts, and optionally how the generated forwarder times out actor replies; the macro generates the select loop, routing traits, timeout policy, and projection checker.

## Key points

- **Inbound mapping:** `inbound RootBroadcast => TargetPayload { pattern [if guard] => expr, ... }` declares how root broadcast variants map to domain request payloads. Guards render as Rust match guards; unmatched broadcasts fall through to `None` and are ignored. When an inbound block is declared, the generated handler self-subscribes to root broadcasts and forwards mapped payloads to the actor mailbox (fire-and-forget — no caller reply). Domains that declare no inbound block get no broadcast subscription at all.
- **Response timeout:** `response_timeout { predicate = ..., duration = ... }` declares per-request reply timeouts. The generated `forward_request` applies `tokio::time::timeout(duration, actor_response)` only when the predicate is false (non-side-effecting); side-effecting requests await without a timeout. Timeout/reply failures keep the typed transport variants (`OneshotReplyTimeout`, `OneshotReplyMissing`, `ChannelRecv`) with the same `target_domain`/`operation` context.
- **Outbound mapping:** `outbound DomainResponse => DomainBroadcast { ... }` or `outbound DomainEvent => DomainBroadcast { ... }` declares how actor responses or events map to domain broadcasts.
- **Generated handler:** The macro generates a `{Domain}GeneratedHandler` struct with a `run()` method containing a `tokio::select!` loop that handles public requests, sudo requests, inbound broadcasts, and outbound events.
- **Projection checker:** The generated `check_authenticated_subject_and_capabilities` function is a no-op foundation for future capability policy; it currently returns `Ok(())` for all projections.
- **Normal/sudo convergence:** Both normal public requests and sudo service requests converge on the same `forward_request` function that sends to the actor and awaits the response (under the declared timeout policy, if any).

Kea Knowledge uses generated request and outbound handling. Its `handle.rs` also subscribes to the generic root broadcast and converts Config changes through `ConfigBroadcast::try_from(root)` without depending on an embedding application's root enum.

## How it works

A domain declaration includes optional response-timeout, inbound, and outbound blocks. See `crates/aves/src/config/domain.rs` and `crates/corv/src/domain.rs` for real declarations.

The macro generates:

1. **Routing traits:** `__InboundRouting{index}` (`route(Source) -> Option<TargetPayload>`, declared guards as match arms, `_ => None` fallback unless a mapping is irrefutable) and `__OutboundRouting{index}<Broadcast>` (`route(Source) -> Option<Broadcast>`).
2. **Generated handler:** `{Domain}GeneratedHandler` whose `run()` is a `tokio::select!` loop over: public requests (projection check, then `forward_request` under the declared timeout policy), one sudo branch per service (plain `forward_request`), one inbound branch per mapping (route, then fire-and-forget `forward_payload` to the actor), and one outbound branch per mapping (route, then broadcast adapter send).

## Normal/sudo path convergence

Both public requests and sudo requests use generated `forward_request`: send to the actor, await the oneshot reply, reply to the caller. A declared `response_timeout` applies only to requests classified as non-side-effecting. See macro tests in `crates/aves/macros/src/tests.rs` for generated output validation.

## Projection checker foundation

The generated `check_authenticated_subject_and_capabilities(_projection: &AuthProjection) -> Result<(), ()>` currently returns `Ok(())` and the public branch discards its result. It is intentionally a no-op foundation for future capability policy; future enforcement requires propagating checker failure before this can be called an authorization enforcement point.

## Known pitfalls

- **Inbound mapping must return `Option`.** Returning `None` ignores the broadcast; `Some(payload)` forwards it to the actor with no reply channel.
- **Guarded mappings are match guards.** Keep guards side-effect-free — they run on every received broadcast.
- **Outbound mapping must match variant types.** The outbound mapping must construct a broadcast variant matching the declared `DomainBroadcast` type; mismatches are caught at compile time by the generated routing trait.
- **Timeout predicates run per request.** Classification stays on the payload; the declaration only names the predicate.
- **Generated handler is `pub(crate)`.** It must be re-exported from the domain's `mod.rs` (or an internal `domain.rs` alias) if construction code needs to instantiate it.
- **Macro rendering is string-based.** New declaration surfaces need a regression test that parses generated output with `syn::parse_file` (`crates/aves/macros/src/tests.rs`).

## Related

- [Aves Auth Domain Architecture](../architecture/aves/auth-domain.md) — Auth domain declaration example
- [Bus JWT Admission](bus-jwt-admission.md) — AuthProjection source
- `crates/aves/AGENTS.md` — Aves macro rules

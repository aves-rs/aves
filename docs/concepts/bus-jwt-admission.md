---
title: Bus JWT Admission
type: concept
updated: 2026-10-23
references:
  - crates/aves/src/bus/event_bus.rs
  - crates/aves/src/bus/messages.rs
  - crates/aves/src/bus/handle.rs
  - crates/aves/src/auth/types.rs
  - crates/aves/src/auth/messages.rs
  - crates/aves/src/auth/actor.rs
  - crates/aves/tests/event_bus_admission.rs
---

# Bus JWT Admission

Every public bus request carries a required JWT metadata field beside the erased domain request. Pre-route admission validates the JWT and forwards only a typed `{ sub, cap }` projection to generated handlers. The bus remains contractually payload-opaque; it inspects only auth metadata and the route key, never the domain request body.

## Key points

- `OpaqueRequestEnvelope` carries a required `Jwt` field beside the erased request; the generated public dispatcher always attaches a `Jwt` value.
- Pre-route admission validates Ed25519 signature, strict expiry, issuer membership, exact bus audience, and JTI liveness through Auth's generated sudo `VerifyJwt` private channel.
- Successful admission drops the raw JWT and routes only `AuthProjection { sub, cap }`.
- There is **no empty-auth bypass**: every public envelope must carry a non-empty JWT that passes normal Auth verification before routing. Missing Auth configuration rejects with a typed internal error — admission is fail-closed.
- Typed rejection preserves `AvesAuthError` identity through the generated public bus client.

## How it works

The envelope (`crates/aves/src/bus/event_bus.rs:23-29`) carries `jwt: Option<Jwt>`, `projection: Option<AuthProjection>`, and a rejection callback. The `admit` method (`event_bus.rs:114-139`) takes the JWT, requires an installed `AuthAdmissionConfiguration`, and calls Auth's sudo verifier:

```rust
async fn admit(mut self, auth: Option<&AuthAdmissionConfiguration>) -> Option<Self> {
    let jwt = self.jwt.take()?;
    let Some(configuration) = auth else {
        self.reject(AvesAuthError::Internal(
            "Auth admission is unavailable".to_owned(),
        ));
        return None;
    };
    let verified = match configuration
        .verifier
        .verify_jwt(VerifyJwtInput { jwt, accepted_issuers, audience })
        .await
    {
        Ok(verified) => verified,
        Err(error) => { self.reject(error); return None; }
    };
    self.projection = Some(verified.projection);
    Some(self)
}
```

The `reject` callback (`event_bus.rs:141-145`) delivers the typed error to the caller before the request reply, eliminating the rejection/drop race. `downcast_with_projection` (`event_bus.rs:81-112`) recovers the request and projection after admission; the raw JWT is consumed before a request reaches a domain adapter. A malformed non-empty JWT is rejected with `AvesAuthError::MalformedJwt` before public routing — proven end-to-end in `crates/aves/tests/event_bus_admission.rs`.

## Token sources

The runtime holds a root JWT for construction contexts. Discuss workers obtain delegated JWTs through `AuthSudoService` for MCP or internal carriers. Embedding applications supply their own authenticated transport contexts.

Missing arbitrary metadata is a construction/ingress error and is never silently treated as a token. There is no empty compatibility carrier.

## Known pitfalls

- **Never use `.ok()?` at typed-error boundaries.** The typed error identity must be preserved through the generated public bus client so callers can discriminate failure modes. Use typed `From` conversion instead.
- **Bus admission must not recurse.** Verification calls Auth's sudo service through the private channel, not through the bus it is authenticating. The private-channel call path must not touch bus ingress.
- **Raw JWT must not leak into routed forms.** The drop-after-verify boundary is correctness-critical. `downcast_with_projection` drops the JWT before delivering the request to the domain adapter.
- **Do not reintroduce an empty-JWT carrier.** Every caller must hold a real issued token (runtime root context or delegated issuance through `AuthSudoService`).

## Related

- [Aves Auth Domain Architecture](../architecture/aves/auth-domain.md) — Auth ownership, sudo-only messaging, machine identity, JTI registry
- [MCP-over-ACP Tooling](mcp-over-acp-tooling.md) — worker JWT paths

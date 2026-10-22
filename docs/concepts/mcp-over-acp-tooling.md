---
title: MCP-over-ACP Tooling
type: concept
updated: 2026-10-23
references:
  - crates/corv/src/mcp.rs
  - crates/corv/src/mcp_http.rs
  - crates/aves/src/utils/transport.rs
  - crates/corv/src/mcp_direct.rs
  - crates/corv/src/mcp_relay.rs
  - crates/corv/src/actor.rs
  - crates/corv/src/session.rs
  - crates/corv/src/messages.rs
  - crates/aves/src/config/types.rs
  - crates/aves/src/bus/control.rs
  - crates/aves/src/bus/messages.rs
  - crates/aves/macros/src/lib.rs
  - crates/aves/src/auth/messages.rs
  - crates/aves/src/auth/domain.rs
---

# MCP-over-ACP Tooling

Corv exposes eligible application bus operations as flat MCP tools to opted-in
ACP agents. The preferred transport is native MCP-over-ACP; a loopback HTTP
lease is the fallback when the agent lacks native ACP. External MCP access is
not permitted.

## Key points

- One MCP tool per qualified bus op; non-privileged `#[operation]`s generate into both RPC and MCP (`mcp_eligible: true`). Privileged variants are omitted from generated descriptors.
- Chat MCP policy is Provider-level `AcpProvider.sluice_mcp_enabled` (defaults false). Sharing a connection does not inherit descriptors or leases.
- `ApplicationMcpService` collates descriptors and calls the generated dispatcher with a caller-supplied `BusRpcContext`.

## Transport selection

`select_mcp_transport` in `crates/corv/src/actor.rs` runs on new and loaded/reconnected sessions:

```text
sluice_mcp_enabled=false  → None (no MCP server)
native mcp_capabilities.acp → NativeAcp (McpServer::Acp via mcp_direct.rs)
no native ACP + http cap  → Http (loopback lease via mcp_relay.rs)
no supported transport    → None + SessionWarning broadcast
```

### Native ACP (preferred)

`McpAcpShim` in `crates/corv/src/mcp_direct.rs` is chat-worker-local
even on a shared ACP transport (`_owner` is the session id). It
installs dynamic handlers (`mcp/connect`, `mcp/message`, `mcp/disconnect`),
supplies an `McpServer::Acp` descriptor, and is never persisted.
### Loopback HTTP lease (fallback)

`McpHttpLease` in `crates/corv/src/mcp_relay.rs` opens a bus-owned
loopback listener (`127.0.0.1:0`, `BusSudoService::open_mcp`). The Discuss
worker obtains a non-root delegated JWT (`sub = "agent"`, 30-day expiry)
through `AuthSudoService` (`issue_worker_jwt`, `crates/corv/src/session.rs`)
and adds it to each bus-provided `McpServer::Http` descriptor as an
`Authorization: Bearer <JWT>` header.
`run_mcp` in `crates/corv/src/mcp_http.rs` serves `/mcp/{domain}` via `require_http_mcp_bearer` middleware, validates exactly one well-formed compact JWT bearer header, and forwards the extracted token as `HttpMcpBearer` in request extensions; missing, malformed, duplicated, empty, or non-compact-JWT headers are rejected with UNAUTHORIZED before bus ingress.

- Loopback only; reconnect opens a fresh lease; close is bus `close_mcp`. Lease open failure keeps the ACP session alive without Sluice tools and emits a `SessionWarning`.
- The worker JWT is never logged, persisted, or broadcast. Its JTI is revoked through `AuthSudoService` on every terminal path; 30-day expiry is the fallback.

### Unsupported transport

When policy enables MCP but the agent advertises neither native ACP
nor HTTP MCP capability, the session continues without Sluice tools and a
`DiscussBroadcast::SessionWarning` is published once per session.

## Shared MCP core

Corv owns the shared MCP core in `src/mcp.rs` and HTTP serving in `src/mcp_http.rs`. RPC serving remains in Aves `utils/transport.rs`, with no MCP implementation or Corv import. The embedding application composes both factories through `TransportBindings::new`; the bus retains generic listener lifecycle ownership.

```text
  Aves descriptors (mcp_eligible + schemas)
  → collate_mcp_spec() → McpSpec → ApplicationMcpService
  → generated aves::domain! dispatcher → domain actor
```

Both transports reach the same `ApplicationMcpService` with a worker-local `BusRpcContext`; the tool registry is identical and both paths converge on the same pre-route bus admission boundary.

## Image presentation seam

`McpToolSpec.image_response` flag is set by `is_image_output_schema()` when the output schema has `image` in `required` plus `image`/`content`/`data` properties. `to_tool()` omits `output_schema` for image tools. `image_tool_result()` maps `image` → `ContentBlock::image(base64, "image/png")` (raw base64, no data-URI prefix), `content` → `ContentBlock::text`, `data` → `result.structured_content` (null omitted). No `readOnlyHint` is added. Covers both native ACP (`mcp_direct.rs`) and HTTP fallback (`mcp_relay.rs`).

## Known pitfalls

- **Do not add an external MCP endpoint.** Loopback admission is deliberate.
- **Do not persist lease material.** Reconnect opens a fresh loopback listener.
- **Do not bypass the shared `ApplicationMcpService`.** Both transports must dispatch through it; no parallel tool registry.
- **Sharing a connection ≠ sharing MCP.** Chat uses Provider `sluice_mcp_enabled`. Descriptors/leases stay per-worker.
- **Do not write “Discuss omitted from `mcp_publications()`."** Eligibility is per-operation.
- **Do not add `readOnlyHint` to image tools.** The user explicitly declined that article recommendation.

## Related

- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md) — worker-token lifecycle and typed errors
- [Bus JWT Admission](bus-jwt-admission.md) — worker JWT admission boundary
- [Aves Auth Domain Architecture](../architecture/aves/auth-domain.md) — delegated worker JWT issuance and JTI revocation
- `crates/aves/AGENTS.md`

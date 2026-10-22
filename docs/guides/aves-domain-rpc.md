---
title: Aves Domain RPC Wiring
type: guide
updated: 2026-10-23
references:
  - crates/aves/macros/AGENTS.md
  - crates/aves/macros/src/request.rs
  - crates/aves/macros/src/domain.rs
  - crates/aves/src/config/messages.rs
  - crates/aves/src/bus/control.rs
  - crates/aves/src/runtime/mod.rs
  - crates/aves/src/utils/transport.rs
  - crates/corv/src/mcp_http.rs
---

# Aves Domain RPC Wiring

Annotated request and response enums produce typed public operations, RPC routers, and internal services. See `crates/aves/macros/AGENTS.md` for attribute syntax and privileged-mapping rules.

## 1. Declare operations

`crates/aves/src/config/messages.rs` declares `#[aves::bus_requests]` on `ConfigRequestPayload`. `getConfigSection` and `saveConfigSection` are public operations; `RegisterSection` and `SealRegistration` are `#[privileged]` and do not enter the public RPC or MCP catalogs. `#[aves::bus_responses]` generates output projections from `ConfigResponse`.

## 2. Publish and route

- `bus_requests` generates typed inputs, dispatchers, `rpc_router()`, and public operation descriptors with JSON adapters. The bus routes opaque envelopes by marker `TypeId` and authenticates JWT metadata before handing the request to its domain.
- `aves::domain!` maps privileged variants to typed sudo services. `crates/aves/src/bus/control.rs` declares `BusSudoService`; privileged calls use its channel-backed service instead of public bus ingress.
- An embedding application declares ordered domains with `aves::runtime_domains!`, nests their generated RPC routers, and publishes their eligible MCP operation descriptors. It may compose Aves `run_rpc` with Corv `run_mcp` through `TransportBindings::new`.

## 3. Check contracts

Every privileged variant needs a generated service mapping. Operation names must be unique within each domain; response paths must end in the generated output name. Run targeted package tests for the macro crate and the changed domain; do not hand-write parallel routing.

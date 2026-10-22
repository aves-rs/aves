# aves_macros

Rust procedural macro crate that generates bus and RPC boilerplate at compile time. Crate-root entry `src/lib.rs` declares four public proc macros; sibling modules own parsing, rendering, and tests. No runtime logic.

The Cargo package is `aves_macros`; its source directory is `crates/aves/macros/`.

## Module map

- `lib.rs` — crate-root entry, four public proc macros (`bus_requests`, `bus_responses`, `domain`, `ErrorCode`), mounts `#[cfg(test)] mod tests;`
- `request.rs` — request enum parsing, operation metadata, public client rendering (`public_projection_type`, `send_<op>`, `dispatch_<op>`)
- `response.rs` — response enum parsing and output struct rendering
- `domain.rs` — `domain!` DSL parsing (`SudoServices`, `SudoToken`), service trait/impl/metadata rendering, routing block rendering
- `routing.rs` — handler branch generation, timeout policy, `explicit_transport_error_conversions`, sudo signature/impl rendering
- `registry.rs` — proc-time sudo registry (`register_request`, `register_services`), opportunistic non-privileged mapping rejection when request is known
- `error_code.rs` — `ErrorCode` derive macro rendering (stable RPC codes from variant names)
- `syntax.rs` — shared naming helpers (`request_base_name`, `to_snake_case`, `to_upper_snake`, `compile_error`)
- `tests.rs` — `#[cfg(test)]` module relocated from `lib.rs`, 50 macro parsing/rendering tests

## Macros

- **`#[aves::bus_requests]`** — For request payload enums. Generates:
  - `{VariantName}Input` structs (one per variant; privileged variants get
    `#[doc(hidden)]` internal projections)
  - `From<{VariantName}Input>` impls back to the request enum
  - `{ENUM_NAME}_OPERATIONS` constant with operation descriptors
    (non-privileged, `mcp_eligible: true` only)
  - A typed `rpc_router()` function returning an `rspc::Router`
    (privileged variants excluded)
  - Typed public dispatchers (`send_{operation}`) and JSON adapters
    (`dispatch_{operation}_json`) for non-privileged operations, returning
    `Result<Output, GeneratedCallError<DomainError>>`
  - A `{Base}SudoServicePrivilegeMapping` marker trait plus one
    `{Base}Privileged{Variant}` marker and assertion fn per privileged
    variant (compile-time service-mapping enforcement)

- **`#[aves::bus_responses]`** — For response enums. Generates:
  - `{VariantName}Output` structs (one per variant)
  - `TryFrom<ResponseEnum>` impls to extract specific response variants

- **`aves::domain!`** — One declaration per domain generates the typed
  sudo-service surface from `pub service Name { Variant => ResponseVariant }`
  mappings:
  - The exported `#[async_trait]` service trait (one typed method per
    mapping: `{variant_snake}(input: {Variant}Input) -> Result<{Variant}Output, DomainError>`)
  - A channel-backed impl (`{Name}Impl`) that privately constructs the
    request, sends it over the domain handler channel, awaits the reply,
    verifies the mapped response variant, and projects the typed output;
    transport failures map to the declared `transport =` error type
  - A compile-time slot key (`{Name}Key`), mapping metadata constants, and
    the aggregate `{BASE}_SUDO_SERVICES` registry
  - With `marker = ...`: `impl DomainProviderMetadata for {Marker}` with the
    generated `ProviderSet` tuple
  - A proc-time sudo registry (opportunistic / order-dependent) plus generated
    trait assertions that supply order-independent mapping enforcement:
    every `#[privileged]` variant must be mapped by at least one service, and
    mappings must not include non-privileged variants

## Attribute syntax

Request enum variants:

```rust
#[operation(name = "operationName", kind = "query" | "mutation", response = ResponseType, response_variant = "VariantName")]
```

Privileged variants are excluded from public RPC, public dispatchers, and MCP
generation:

```rust
#[privileged(reason = "Listener control is internal bus-only infrastructure")]
```

Privileged variants must be mapped through the Aves domain surface:

```rust
aves::domain!(...);
```

Non-privileged operations enter both the generated RPC and MCP surfaces. See [MCP-over-ACP Tooling](../../../docs/concepts/mcp-over-acp-tooling.md).

## Usage

```rust
#[aves::bus_requests(domain = "config")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigRequestPayload {
    #[operation(name = "getConfigValue", kind = "query", response = ConfigValueGetOutput, response_variant = "ConfigValueGet")]
    GetConfigValue { key: String },
}

#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigResponse {
    ConfigValueGet { value: serde_json::Value },
}
```

## Non-obvious rules

- **Domain inference**: If `domain = "..."` is omitted, the domain is inferred from the enum name by stripping `RequestPayload` and lowercasing. For response enums, `Response` suffix is stripped.
- **Type projection**: Generated input/output structs preserve declared field types, including `serde_json::Value`.
- **Sensitive field redaction**: Fields named `password`, `private_key`, `passphrase`, `local_passphrase`, `digest`, `api_key`, `secret`, or `token` are redacted as `"<redacted>"` in generated `Debug` impls. The account-secret names (`api_key`, `secret`, `token`) cover unified enclave account operations such as `SecretWriteRequest`/`SecretRotateRequest` (raw `secret` field) and any inline account create/update variants carrying raw provider API keys or Flex tokens.
- **Response linkage**: The `response` path must end with `{response_variant}Output`. E.g., `response_variant = "Config"` requires `response = ConfigOutput` or `response = some::path::ConfigOutput`.
- **Duplicate operations**: Operation names must be unique within a domain. Qualified names (`domain.operation`) must be unique across all domains.
- **Serde skip**: Response variants with `#[serde(skip)]` are excluded from output struct generation.
- **Privileged mapping enforcement**: Every `#[privileged]` variant must be mapped by at least one `domain!` service. Generated trait assertions (`{Base}SudoServicePrivilegeMapping` + `{Base}Privileged{Variant}` markers) enforce this order-independently across crate expansion. The proc-time registry (`registry.rs`) opportunistically rejects non-privileged mappings only when the request is already registered (uses `.find()`, first match wins); it does not perform full mapping validation or emit targeted missing-mapping errors. Repeated `#[error_code]` aliases on the same variant take the first via `.find()`; this is accepted behavior, not a desirable pattern.

## Boundaries

- Compile-time only — no runtime logic, business logic, or I/O.
- Generated code requires the consuming crate to have `serde`, `specta`, `rspc`, and `serde_json`.

## Engineering context lookup

For cross-cutting repository knowledge, start at `docs/navigation.md`. This AGENTS.md file remains the authority for local rules in this scope; the engineering docs are supporting context and must not override it.

## Related

- [Declaration-Driven Handler Routing](../../../docs/concepts/declaration-driven-handler-routing.md) — `domain!` declaration and routing string generation
- [Aves Domain RPC](../../../docs/guides/aves-domain-rpc.md) — how `bus_requests` annotations become typed RPC, MCP, and sudo services

# Aves

Build typed async domains with authenticated messaging, RPC, and durable workflows.

## Installation

Add Aves to your Rust application:

```toml
[dependencies]
aves = "=0.5.0"
```

Use Rust 1.97 or newer. Import macros from `aves`; do not depend on `aves_macros` directly.

## Quick Start

Read the [Config request declarations](src/config/messages.rs) and [domain wiring](src/config/domain.rs).

```rust
use aves::{bus_requests, bus_responses, domain};
```

## Usage

Use public operations for ordinary requests. Keep internal control on typed sudo services.

```rust
// src/config/messages.rs: public operation
#[operation(name = "getConfigSection", kind = "query", response = ConfigSectionOutput, response_variant = "ConfigSection")]
GetConfigSection { section: String },
```

Compose owner domains before Flow drains their node registrations. Pass construction inputs explicitly.

```text
Auth (built in) → owner domains → Flow → bus finalization
```

Use `NodeRegistrationSupply` to add Flow node kinds without changing Flow core.
See [the node-kind guide](../../docs/guides/adding-a-flow-node-kind.md).
Use [the RPC guide](../../docs/guides/aves-domain-rpc.md) for generated client and service contracts.

## Options

| Feature | Purpose |
|---|---|
| `keyring` | Run tests requiring a desktop keyring |
| `wasm-tests` | Run QuickJS-in-Wasmtime guest tests |
| `test-support` | Expose downstream test fixtures |

Disable default features for headless CI when the keyring or WASM guest is unavailable.

## Contributing

Follow [crate rules](AGENTS.md) and [engineering standards](../../docs/standards/code-quality.md).
Run `direnv exec . cargo test --package aves` from the repository root.

## License

Apache-2.0. See [LICENSE](../../LICENSE).

# Tyto

Embed a specialized messaging domain for the **Aves async application runtime**.

Observe Email, Slack, and Telegram deliveries and send outbound messages.
Name Tyto after Tytonidae: barn owls, vigilant observers of their environment.

## Installation

Add Tyto to your application:

```toml
[dependencies]
tyto = { package = "aves-tyto", version = "=0.5.0" }
aves = "=0.5.0"
```

Use Rust 1.97 or newer.
Tyto depends on Aves and Kea, never the embedding application's trading implementation.

## Quick Start

Import the domain marker into your Aves composition:

```rust
use tyto::MessagingDomain;
```

Supply the configuration directory and shared Flow registration supply through your host runtime.
Provide credentials through the privileged `MessagingRuntimeService`, not public request payloads.
Read the [Aves README](https://docs.rs/crate/aves/0.5.0) for application wiring.

## Usage

Manage persisted connections through `subscription.listConnections`, `createConnection`, `updateConnection`, and related operations.
Use `subscription.send` for outbound delivery through an existing connection UUID.
Query `subscription.sendMetadataSchema` for provider-specific message metadata.

Use one enabled connection for shared inbound listening and filtered subscriber delivery.
Use `MessageNode` and `PermissionNode` for Flow automation.
PermissionNode supports Slack and Telegram, not Email.
Disabling inbound listening does not disable outbound sends.

Keep credential values outside public RPC, configuration, logs, and persistence.
Supply typed, memory-only credentials through the trusted in-process service.
Use credential UUIDs in connection records.

Persist messaging configuration and history in `config_dir/messages.db`.
Preserve the existing `__peitho_migrations` ledger when upgrading from the previous crate name.
The crate rename does not rename `subscription.*` operations or migrate database identities.

Choose optional Knowledge persistence for received messages or accepted outbound sends.
Knowledge handoff failures do not gate delivery or turn accepted sends into failures.

Run unit tests from the repository root:

```bash
cargo test --package aves-tyto
```

## Contributing

Follow contribution guidance in the [source repository](https://github.com/aves-rs/aves).
Use the [Aves README](https://docs.rs/crate/aves/0.5.0) for runtime and domain mechanisms.

## License

Use Tyto under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---
title: Tyto Credential Supply Architecture
type: architecture
updated: 2026-10-29
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/domain.rs
  - crates/tyto/src/messages.rs
  - crates/tyto/src/types.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/connections.rs
  - crates/tyto/src/database.rs
---

# Tyto Credential Supply Architecture

The embedding application's credential owner supplies messaging credentials to Tyto. Tyto holds a
UUID-keyed memory map of nonserializable zeroizing credentials; only the UUID
crosses public RPC, MCP, config, persistence, and logs.

## Ownership and boundaries

- **Credential owner** decides when to supply, replace, or remove credentials through the privileged `MessagingRuntimeService`.
- **Tyto** owns the in-memory `credentials: HashMap<SpectaUuid, MessagingCredentials>`
   plus a per-UUID `credential_generation: HashMap<SpectaUuid, u64>` counter. Credential
   supply/remove handlers live inline in `actor.rs`.

## Data flow

1. The credential owner dispatches `MessagingRuntimeService::supply_credential` with `SupplyCredentialInput { credential_id, credentials }`.
2. Tyto's actor inserts the credential, bumps
    `credential_generation[credential_id]`, and reconciles — only inflows
    keyed on that UUID rebuild; peers are untouched.
3. Removing a credential calls `MessagingRuntimeService::remove_credential`; metadata broadcasts alone do not supply credentials.
4. Supplied credentials enable `TestConnection` pre-save verification (see [Connection Architecture](connection.md#pre-save-connection-verification)) — the probe reads the in-memory credential map without accessing persisted connection rows.

## Persistence

- `MessagingCredentials` derives `Clone` only; secret fields are
  `Zeroizing<String>`, `Debug` prints `<redacted>`, and the
  `SupplyCredential.credentials` field is `#[serde(skip)]`. Values never
  cross public RPC/MCP/config/persistence/logs.
- The `SpectaUuid` lives on `messaging_connections.credential_id` in
  Tyto's `messages.db` (the retired trigger-level `credential_id` field
  is gone). The same UUID appears in `SupplyCredentialInput` /
  `RemoveCredentialInput` on the privileged bus surface.

## Related

- [Tyto Connection Architecture](connection.md)
- [Tyto Subscription Architecture](subscriptions.md)
- [Tyto Send Architecture](send.md)
- [Messaging Send and Persistence](../../concepts/messaging-send-and-persistence.md)
- [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md)

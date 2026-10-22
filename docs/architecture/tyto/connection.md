---
title: Tyto Connection Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/database.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/connections.rs
  - crates/tyto/src/reconciliation.rs
  - crates/tyto/src/types.rs
  - crates/tyto/migrations/0001_messaging_store.sql
  - crates/tyto/src/queries/list_connections.sql
  - crates/tyto/src/queries/create_connection.sql
  - crates/tyto/src/queries/update_connection.sql
  - crates/tyto/src/queries/set_connection_enabled.sql
  - crates/tyto/src/queries/delete_connection.sql
  - crates/tyto/src/queries/mark_deleted_for_connection.sql
  - crates/tyto/src/channels/mod.rs
  - crates/tyto/src/channels/email/connection.rs
  - crates/tyto/src/channels/slack/connection.rs
  - crates/tyto/src/channels/telegram/connection.rs
---

# Tyto Connection Architecture

One stateful `MessagingPlugin` trait owns connect/listen/subscribe/send per
provider. Each provider module follows the same internal layout. One inflow
runs per **enabled persisted connection**, keyed by the connection UUID —
the retired `(listener_type, credential_id, config_key)` tuple is gone.

## Ownership and boundaries

- `channels/mod.rs` is contract-only: the `MessagingPlugin` trait, the
  `MessagingSubscription` target/ACL policy, `for_kind` selector, and the
  `CHANNEL_MESSAGE_BUFFER_SIZE` constant. No provider branching, validation,
  retry, or persistence lives here.
- Each provider module (`channels/{email,slack,telegram}/`) owns:
  - `connection.rs` — authenticated transport from non-secret settings plus
    the supplied credential.
  - `subscription.rs` — one unfiltered inflow for the authenticated
    bot/mailbox.
  - `filter.rs` — target/ACL normalization and per-subscription filtering.
  - `send.rs` — `Send`-only outbound path.
  - `helpers.rs` — reusable utilities.
  - `mod.rs` — the `MessagingPlugin` impl and provider-specific state.

## `messages.db` as connection authority

`config_dir/messages.db` (opened by `MessageStore::open` via
`aves::MigrationRunner`, `__peitho_migrations`) owns the
`messaging_connections` table:

```
id TEXT PK, display_name, provider_type CHECK ('Email'|'Slack'|'Telegram'),
credential_id TEXT, transport_settings TEXT (JSON ConnectionTransport),
enabled INTEGER CHECK (0|1), retention_seconds INTEGER
```

The retired `peitho` Config section, `ReloadSettings`, `listChannelConfigs`,
`config_key`, `MessagingConfigMap`, and `MessagingSettings` are gone.

Public connection bus ops: `listConnections`, `createConnection`,
`updateConnection`, `setConnectionEnabled`, `deleteConnection`. Connection CRUD
handlers live in `connections.rs` (a private `#[path]` child of `actor.rs`); the
actor is the sole writer; every mutation runs `reconcile` afterward so the inflow
set matches enabled DB state. `reconcile` and `run_inflow` live in `reconciliation.rs`.

## Data flow

1. `reconcile` lists enabled rows from `messages.db` and builds a
   `desired_connections: HashMap<SpectaUuid, (transport, credentials, generation, retention_seconds)>`
   map.
2. One `Inflow` per enabled connection UUID holds one connected plugin, the
   resolved `MessagingConfig` (derived from `ConnectionTransport`), the
   supplied `MessagingCredentials`, the current `credential_generation`, and
   a `Subscribers` map of `trigger_id → mpsc::Sender<NormalizedMessage>`.
3. `run_inflow` calls `plugin.connect` then `plugin.listen`; the bounded
   ingress queue (`CHANNEL_MESSAGE_BUFFER_SIZE = 100`) forwards every
   received `InboundEvent` / `MessagingInteraction` to the actor over
   bounded channels — the actor is the sole writer of `messages.db`.
4. Each subscriber has its own `filter.subscribe` task validating the
   `MessagingSubscription` (target channels + allowed senders) and
   forwarding accepted messages into a dedicated bounded output queue.

## Actor-owned persistence boundary

Inflow tasks never write to `messages.db` directly. They send
`ActorInbound { connection_id, event }` and `ActorInteraction { connection_id, interaction }`
to the actor, which performs `insert_inbound`, `insert_callback`,
`insert_accepted_send`, and permission-request lifecycle writes. This keeps
the SQLite connection single-writer and the retention prune consistent.

## Persistence

- `subscribed_channels: None` admits every delivered channel;
  `Some([])` is invalid and rejected at deserialization and at
  `MessagingSubscription::validate`.
- Provider polling/keepalive is provider-internal; the actor only observes
  the inflow task's `JoinHandle` and reconnects with bounded exponential
  backoff (`RetryBackoff::new(10s, 60s)`).
- `deleteConnection` walks Flow for subscription triggers bound to the
   connection on a best-effort basis: listing Flows, listing triggers, or
   deleting a trigger each log a warning and continue on failure — the
   operation still `mark_deleted_for_connection`s pending `permission_requests`,
   runs `delete_connection`, reconciles, and returns `Success`. A failed Flow
   walk can leave trigger rows pointing at a deleted connection; reconcile
   will skip them (no live inflow matches), but the orphan trigger rows
   remain in Flow until manually removed. History rows for the deleted
   connection remain until the orphan-fallback prune path removes them.

## Related

- [Tyto Credential Supply Architecture](credentials.md)
- [Tyto Subscription Architecture](subscriptions.md)
- [Tyto Send Architecture](send.md)
- [Messaging Send and Persistence](../../concepts/messaging-send-and-persistence.md)
- [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md)

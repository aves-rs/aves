---
title: Tyto Connection Architecture
type: architecture
updated: 2026-10-29
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/lib.rs
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
  - crates/tyto/src/channels/email/plugin.rs
  - crates/tyto/src/channels/email/connection.rs
  - crates/tyto/src/channels/email/send.rs
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
  - `plugin.rs` — the `MessagingPlugin` impl and provider-specific state.
  - `connection.rs` — authenticated transport from non-secret settings plus
    the supplied credential.
  - `subscribe.rs` — one unfiltered inflow for the authenticated
    bot/mailbox.
  - `filter.rs` — target/ACL normalization and per-subscription filtering.
  - `send.rs` — `Send`-only outbound path.
  - `helpers.rs` — reusable utilities.
  - `mod.rs` — composition: re-exports `plugin` and declares submodules.

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
handlers live in `connections.rs`; the actor is the sole writer; every mutation runs `reconcile` afterward so the inflow
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

## Pre-save connection verification

`TestConnection` is a public bus operation (wire name `testConnection`) that verifies credentials authenticate remotely against every configured transport **before** any connection record is saved. Input: proposed `ConnectionTransport` plus `credential_id` identifying credentials already supplied through `MessagingRuntimeService`. Success returns existing `SuccessOutput`; failure returns existing `MessagingError::ChannelConnectionFailed`. Exported: `TestConnectionInput`, `dispatch_test_connection`.

**Execution model:** On-loop credential lookup (missing credential fails immediately); off-loop `verify_remote` spawn with direct oneshot reply; single overall 60-second deadline; single attempt (no retry). A dropped caller does not cancel the probe — it runs to deadline, reply send fails quietly, temporary plugin/clients/sessions drop. Concurrent requests spawn independent probes. No `MessageStore` access, no connection-row reads/writes, no `reconcile`, no listener/inflow changes, no history writes, no broadcasts, no message sends.

**Authentication semantics:** Email verifies IMAP and SMTP when both supplied (SMTP-only when IMAP absent); Slack verifies both bot token (`auth_test`) and app token (`apps_connections_open`); Telegram verifies bot token (`get_me`). Authentication success does NOT prove destination permissions or future message delivery — the input carries no destination. Secrets stay in the supplied-credential map; error messages follow existing redaction conventions and never embed token/password content.

**Provider SDK auth behavior:** Lettre SMTP `test_connection` authenticates during connection acquisition before NOOP (configured credentials trigger `auth`). Slack `auth_test` verifies Web API token identity; `apps_connections_open` checks app token scope (WebSocket URL deliberately not opened). Telegram `get_me` tests bot authentication. Versioned sources: [lettre 0.11.23 AsyncSmtpTransport::test_connection](https://docs.rs/crate/lettre/0.11.23/source/src/transport/smtp/async_transport.rs) (lines 314-322, 485-497), [slack-morphism 2.31.1 auth_test](https://docs.rs/crate/slack-morphism/2.31.1/source/src/api/auth.rs), [slack-morphism 2.31.1 apps_connections_open](https://docs.rs/crate/slack-morphism/2.31.1/source/src/api/apps.rs), [teloxide-core 0.13.0 get_me](https://docs.rs/crate/teloxide-core/0.13.0/source/src/requests/requester.rs). See also [external documentation](../../external/navigation.md) for additional SDK details.

**Shared SMTP builder:** Email SMTP probe reuses `build_smtp_transport` (defined in `channels/email/send.rs`, imported by `channels/email/connection.rs`) — behavior-preserving sharing, unchanged Send semantics. IMAP probe reuses existing login connect. No new SMTP protocol code.

**Accepted constraints:** Telegram `api_url` override: Tyto accepts non-base overrides; SDK path construction can panic for syntactically valid non-hierarchical URLs (e.g., `mailto:`). Accepted NO FIX. Mock coverage gaps exercise the mock entry directly, not the live network; accepted as sufficient coverage.

**connect vs verify_remote contrast:** `connect` may prepare local provider state or authenticate a subset of transports (e.g., IMAP-only when SMTP is also configured), but does not exercise every configured transport end-to-end. `verify_remote` is the only authoritative all-configured authentication probe (see [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md#connect-vs-verify_remote)).

## Pre-save connection verification workflow

The embedding application verifies credentials authenticate remotely against every configured transport **before** any connection record is saved:

1. Supply the proposed credentials through the privileged `MessagingRuntimeService::supply_credential`.
2. Call the generated `dispatch_test_connection` with the proposed `ConnectionTransport` and the supplied `credential_id`.
3. Inspect the result: success returns existing `SuccessOutput`; failure returns existing `MessagingError::ChannelConnectionFailed`.
4. Optionally save or create the connection record through the public bus ops.

`testConnection` does not save anything; it only probes authentication. The credential stays in the supplied-credential map until explicitly removed.

## Related

- [Tyto Credential Supply Architecture](credentials.md)
- [Tyto Subscription Architecture](subscriptions.md)
- [Tyto Send Architecture](send.md)
- [Messaging Send and Persistence](../../concepts/messaging-send-and-persistence.md)
- [External Documentation](../../external/navigation.md)
- [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md)

---
title: Messaging Send and Persistence
type: concept
updated: 2026-10-23
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/messages.rs
  - crates/tyto/src/types.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/delivery.rs
  - crates/tyto/src/database.rs
  - crates/tyto/src/registrations.rs
  - crates/tyto/src/queries/prune_expired.sql
  - crates/kea/src/messages.rs
  - crates/kea/src/types.rs
---

# Messaging Send and Persistence

Tyto sends outbound Email, Slack, and Telegram messages through a persisted
`messaging_connections` row, and retains accepted inbound / outbound history in
`messages.db`. Optional Knowledge handoff is independent and never changes the
Send result.

## Key points

- **Connection reference model.** `MessagingSenderConfig` carries a
  `connection_id: SpectaUuid` plus a destination field (`from` / `channel_id` /
  `chat_id`); the actor resolves the credential and transport from
  `messages.db`.
- **Send** is `subscription.send` (mutation): provider sender config plus a
  `NormalizedMessage`; Email recipients come from `EmailSendMetadata`.
- **Metadata shape** is per-provider: query `subscription.sendMetadataSchema`
  with the `ListenerType` to get the JSON Schema.
- **Persistence policy** is `None | Note { directory } | Data { namespace }`
  (default `None`), set on `SubscriptionTriggerConfig` or the outbound sender.
  `NewConnection` takes explicit `retention: RetentionDuration`; no provider defaults.
- **Persistence fires async** via `dispatch_create_knowledge` /
  `dispatch_upsert_data` (in `delivery.rs`); Tyto never writes files or DB for Knowledge.
- **Credential must already be supplied** by the embedding application; Send rejects unknown
  UUIDs immediately.

## How it works

```rust
// Inbound trigger config (persisted in flow_triggers.adapter_config)
SubscriptionTriggerConfig {
    connection_id: connection_uuid,  // references messages.db row
    subscribed_channels: None,
    allowed_senders: vec!["boss@example.com".to_owned()],
    persistence: PersistencePolicy::Note { directory: "inbox".to_owned() },
}

// Outbound sender config (per-call)
MessagingSenderConfig::Email(EmailSenderConfig {
    connection_id: connection_uuid,  // actor resolves credential + transport
    from: "me@example.com".to_owned(),
    persistence: PersistencePolicy::Data { namespace: "sent".to_owned() },
})
```

## Flow node kinds

Two Aves Flow node kinds are registered by Tyto in `registrations.rs`:

- **`MessageNode`** — sends one outbound message through a referenced
  connection; terminal state emits a `receipt` output. Replaces the previously
  deferred `SendMessageNode`.
- **`PermissionNode`** — sends an interactive approval prompt (Slack/Telegram
  only) and persists a `permission_requests` row. Finalizes on provider
  callback or expiry; provider message edits are retried until cleared.

## History and retention

The actor writes four history tables in `messages.db`: `inbound_messages`,
`permission_callbacks`, `accepted_sends`, and `permission_requests`
(lifecycle-managed, not retention-pruned). Prune runs on a 5-minute worker
tick with two-mode semantics per row:

- **Live connection** — `received_at + messaging_connections.retention_seconds <= now`
  (current connection retention governs).
- **Orphan fallback** — when the connection is gone, use the row's own
   `expires_at` (snapshot taken at write time) so history does not linger.

## Known pitfalls

- **Outbound is independent of inbound.** A Send connects on demand.
- **Persistence never changes the Send result.** Knowledge handoff failure
  warns; a 30s timeout caps it.
- **History writes never gate delivery.** Failed `insert_*` rows warn —
  delivery is authoritative, retention is best-effort.

## Related

- [Tyto Credential Supply Architecture](../architecture/tyto/credentials.md)
- [Tyto Connection Architecture](../architecture/tyto/connection.md)
- [Tyto Subscription Architecture](../architecture/tyto/subscriptions.md)
- [Tyto Send Architecture](../architecture/tyto/send.md)
- [Tyto AGENTS.md](../../crates/tyto/AGENTS.md)

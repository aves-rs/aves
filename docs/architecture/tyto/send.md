---
title: Tyto Send Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/messages.rs
  - crates/tyto/src/types.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/delivery.rs
  - crates/tyto/src/permission_node.rs
  - crates/tyto/src/database.rs
  - crates/tyto/src/queries/insert_accepted_send.sql
  - crates/tyto/src/channels/mod.rs
  - crates/tyto/src/channels/email/mod.rs
  - crates/tyto/src/channels/email/send.rs
  - crates/tyto/src/channels/slack/mod.rs
  - crates/tyto/src/channels/slack/send.rs
  - crates/tyto/src/channels/telegram/mod.rs
  - crates/tyto/src/channels/telegram/send.rs
  - crates/aves/src/utils/retry.rs
---

# Tyto Send Architecture

Public `subscription.send` is a mutation that takes a connection-referenced
`MessagingSenderConfig` plus a `NormalizedMessage`, resolves the connection
row from `messages.db`, looks up the credential, and dispatches through the
single `MessagingPlugin::send` method with bounded retry.

## Ownership and boundaries

- `MessagingSenderConfig` (defined in `types.rs`) is a tagged enum (`Email | Slack | Telegram`)
  carrying a `connection_id: SpectaUuid` and the provider destination or
  sender identity: `channel_id` (Slack) and `chat_id` (Telegram) are
  destinations; `from` (Email) is the sender address — Email recipients
  (`to`/`cc`/`bcc`) come from `EmailSendMetadata` inside
  `NormalizedMessage.metadata`. The credential and transport are NOT
  carried on the sender — the actor resolves them from `messages.db`.
- Each provider plugin matches its own variant in `send` and returns a
  typed `MessagingSendErrorCode::Validation` terminal error for any other
  variant.
- `subscription.sendMetadataSchema` returns per-provider JSON Schema via
  `schemars::schema_for!` from the same type that validates metadata
  (`EmailSendMetadata`, `SlackSendMetadata`, `TelegramSendMetadata`).

## Two send paths

Public `subscription.send` and the `MessageNode` Flow kind share one path;
`PermissionNode` uses a separate interactive path with different retry and
failure semantics. Do not conflate them.

### `send_message` (subscription.send, MessageNode)

1. The actor extracts `connection_id` from the sender variant and loads the
   row via `MessageStore::get_connection`; missing row returns
   `MessagingError::ConnectionNotFound` immediately.
2. The actor parses `credential_id` from the row, then resolves the
   credential from its memory map; missing credential returns
   `ChannelConnectionFailed` immediately.
3. The actor parses `ConnectionTransport` from the row's
   `transport_settings` JSON and builds the provider `MessagingConfig`.
4. The actor spawns `send_message` (in `delivery.rs`) off the actor loop so a slow provider
    does not block other requests, capturing the row's `retention_seconds`
    and the sender's `persistence` for the completion callback.
5. `send_message` calls `for_kind(listener_type)`, `plugin.connect`, then
   runs `plugin.send` through `aves::utils::retry::retry_with_backoff`
   with exactly **three total attempts** (10s–60s exponential backoff).
   Only `MessagingSendError::Retryable` (transport/server/rate-limit,
   honoring `retry_after_seconds`) retries; invalid credentials,
   destination, permission, and validation are terminal.
6. On acceptance, the completion channel returns
   `MessagingSendReceipt` (`ProviderMessageId { message_id }` or
   `SmtpAccepted { response }`) to the actor.
7. The actor writes one `accepted_sends` row
   (`insert_accepted_send`) using the connection's `retention_seconds` and
   `now + retention_seconds` for the orphan-fallback prune path.
8. If the sender's `persistence` policy is not `None`, one async
   `spawn_knowledge_handoff` fires (never per retry, never for failed
   sends).

### `run_interactive_send` (PermissionNode)

`PermissionNode` does NOT reuse `send_message`. Its `run_interactive_send` (in `permission_node.rs`)
performs exactly one `plugin.connect` followed by one
`plugin.send_interactive` — no `retry_with_backoff`, no second attempt. A
single failure (connect or send) finalizes the `permission_requests` row as
`failed` immediately. Only provider acceptance proceeds to `mark_pending`
and the late-receipt edit path.

## Persistence

- Email Send is SMTP-only — no IMAP prerequisite; `EmailPlugin::connect`
  short-circuits to send-only mode when `smtp_server` is set and
  `imap_server` is absent.
- Slack Send uses the bot token from the supplied credential via
  `SlackPlugin::send`.
- Outbound Knowledge persistence is fire-and-forget with a 30s timeout;
  failure warns but does not alter the `Send` result.
- `accepted_sends` history is written by the actor only after provider
  acceptance; a failed history write logs a warning and does not fail the
  reply.

## Related

- [Tyto Credential Supply Architecture](credentials.md)
- [Tyto Connection Architecture](connection.md)
- [Tyto Subscription Architecture](subscriptions.md)
- [Messaging Send and Persistence](../../concepts/messaging-send-and-persistence.md)
- [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md)

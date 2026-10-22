---
title: Tyto Subscription Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/tyto/AGENTS.md
  - crates/tyto/src/domain.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/reconciliation.rs
  - crates/tyto/src/types.rs
  - crates/tyto/src/messages.rs
  - crates/tyto/src/database.rs
  - crates/tyto/src/channels/mod.rs
  - crates/tyto/src/channels/email/filter.rs
  - crates/tyto/src/channels/slack/filter.rs
  - crates/tyto/src/channels/telegram/filter.rs
  - crates/aves/src/flow/triggers.rs
  - crates/aves/src/flow/catalog.rs
---

# Tyto Subscription Architecture

Tyto owns inbound listeners and reconciles them against Aves Flow's
persisted trigger table. Trigger config lives in Flow; Tyto validates it
without Flow importing Tyto.

## Ownership and boundaries

- **Aves Flow** owns `flow_triggers` persistence and the
  `FlowTriggerSourceService::ListSourceTriggers` / `FireSourceTriggerInput`
  surface.
- **Tyto** owns `SubscriptionTriggerConfig` (adapter kind `"subscription"`),
   the `messaging_trigger_registry()` validator in `domain.rs`, and the
    reconcile loop in `reconciliation.rs` (a private sibling module of `actor.rs`).
- There is no Tyto-consumable live trigger-change seam — Tyto re-lists
  at startup, credential seams, and connection mutations only (accepted
   limitation; no polling or application-side trigger-inspection loop).

## Trigger config shape

`SubscriptionTriggerConfig` carries only:

```rust
pub struct SubscriptionTriggerConfig {
    pub connection_id: SpectaUuid,             // references messages.db row
    pub subscribed_channels: Option<Vec<String>>,
    pub allowed_senders: Vec<String>,
    pub persistence: PersistencePolicy,
}
```

The retired fields (`listener_type`, `config_key`, `credential_id`) are
gone — provider, credential, and transport are resolved from the
`messaging_connections` row at runtime.
`messaging_trigger_registry` validates that `connection_id` is non-nil
without Flow importing Tyto.

## Reconcile triggers

`reconcile` runs after:

- `StartServer`
- `SupplyCredential` / `RemoveCredential`
- `CreateConnection` / `UpdateConnection` / `SetConnectionEnabled` / `DeleteConnection`

It calls `flows.list_source_triggers(ListSourceTriggersInput { adapter_kind: "subscription" })`
and walks the returned records. `ReloadSettings` is retired.

## Data flow

1. Each returned `FlowTriggerRecord.adapter_config` is deserialized into
   `SubscriptionTriggerConfig`; invalid rows warn and skip. A trigger
   whose `connection_id` has no live inflow (disabled connection, missing
   credential, malformed transport) also skips with a warning.
2. Per desired trigger, the actor opens (or reuses) the shared `Inflow`
   for the connection UUID, registers a `filter.subscribe` task with the
   trigger's `MessagingSubscription` (subscribed channels + allowed senders),
   and spawns `dispatch_messages`.
3. `dispatch_messages` broadcasts `ChannelMessage` for observability, then
   calls `flows.fire_source_trigger` asynchronously. Flow rechecks
   enablement, authority, and revision before starting the execution.

## Persistence

- `SubscriptionTriggerConfig` is persisted by Flow as JSON in
   `flow_triggers.adapter_config`.
- Optional `PersistencePolicy` (None | `Note { directory }` | `Data { namespace }`)
   fires an async `CreateKnowledge` / `UpsertData` bus request via
   `spawn_knowledge_handoff`; it never gates Flow triggering and never
   writes files or DB directly.
- Deleting a connection cascades best-effort: the actor walks Flow and
   removes every subscription trigger whose `adapter_config.connection_id`
   matches; list/delete failures warn and continue, then
   `mark_deleted_for_connection` finalizes pending `permission_requests`,
   `delete_connection` removes the row, and reconcile tears the listener
   down. A failed Flow walk can leave trigger rows pointing at a deleted
   connection.

## Related

- [Tyto Credential Supply Architecture](credentials.md)
- [Tyto Connection Architecture](connection.md)
- [Tyto Send Architecture](send.md)
- [Messaging Send and Persistence](../../concepts/messaging-send-and-persistence.md)
- [Tyto AGENTS.md](../../../crates/tyto/AGENTS.md)

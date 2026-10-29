# Tyto

Aves-based inbound Email/Slack/Telegram listener runtime AND outbound `subscription.send`.
Tyto never depends on Poseidon or knows how the embedding application stores credentials.

## Ownership

- `actor.rs`: Actor state (credentials, listeners, inflows, channels), lifecycle (`new`/`start_server`/`stop_server`/`run` select loop), worker schedule, lazy `store()`, reconcile orchestration, `handle_message` public request routing, and credential supply/remove handlers inline.
- `connections.rs`: Connection CRUD handlers (`create_connection`, `update_connection`, `delete_connection`, `set_connection_enabled`, `list_connections`) and `start_test_connection` pre-save verification probe.
- `reconciliation.rs`: `reconcile`, `desired_connections`, `reconcile_inflows`, inflow management (`run_inflow`, `start_inflows`), subscriber dispatch (`dispatch_messages`), Flow trigger listener attachment (`trigger_inventory`, `desired_listeners`, `attach_listeners`, `remove_stale_listeners`).
- `sender.rs`: `prepare_send` connection/credential/transport resolution, Send dispatch setup (`start_send`).
- `delivery.rs`: `send_message` (Aves retry), Knowledge handoff, history event handlers (inbound/interaction/send completion).
- `flow_nodes.rs`: MessageNode/PermissionNode command routing (`handle_node_command`) and shared `NodeSetupError`.
- `messaging_nodes.rs`: MessageNode start/tick/cancel implementation.
- `permission_node.rs`: PermissionNode start/tick/cancel, interactive send, permission-request persistence.
- `permission_callbacks.rs`: Permission callback arbitration (`handle_interaction`), provider edit dispatch, expiry scanning.
- `domain.rs`: Generic runtime registration, Flow trigger validation, and the privileged `MessagingRuntimeService` (`SupplyCredential` / `RemoveCredential`).
- `handle.rs`: Generated service routing and source-event forwarding.
- `messages.rs`: Request/response envelopes, broadcast events, and generated RPC types.
- `types.rs`: Non-secret settings, trigger configuration, `MessagingCredentials`, `MessagingSenderConfig` (Email/Slack/Telegram variants), `PersistencePolicy`, `MessagingSendError`, `MessagingError` (stable `subscription` domain), `RetentionDuration`, `ConnectionRecord`, `NewConnection`, `EditConnection`, and provider config types.
- `database.rs`: `MessageStore` — `messages.db` query interface (connection CRUD, history writes, permission lifecycle, retention prune). The actor is the sole caller and writer.
- `src/migrations.rs` registers the SQL in `migrations/` (a peer of `src/`) with Aves `MigrationRunner` for `messages.db` (`__peitho_migrations`).
- `queries/`: SQL for connection CRUD, history writes, permission lifecycle, and the two-mode retention prune (`prune_expired` / `prune_callbacks` / `prune_accepted_sends`).
- `registrations.rs`: `NodeKindRegistration` for `MessageNode` and `PermissionNode`.
- `channels/mod.rs`: Contract-only — the stateful `MessagingPlugin` trait (`connect` / `verify_remote` / `send` / `send_metadata_schema` / `listen` / `subscribe` / `send_interactive` / `edit_interactive`), the `MessagingSubscription` target/ACL policy, `for_kind` selector, and the `CHANNEL_MESSAGE_BUFFER_SIZE` constant.
- `channels/{email,slack,telegram}/`: Per-provider module with `plugin.rs` (MessagingPlugin impl) / `connection.rs` / `subscribe.rs` / `filter.rs` / `send.rs` / `helpers.rs` / `mod.rs` (composition, re-exports plugin).

### connect vs verify_remote

`connect` may prepare local provider state or authenticate a subset of transports (e.g., IMAP-only when SMTP is also configured), but does not exercise every configured transport end-to-end:
- **Slack**: calls `configure(settings, credentials)` to build the local `SlackClient` (plugin.rs:42-50); does not verify tokens.
- **Telegram**: builds the `Bot` client locally; does not call the Bot API.
- **Email**: opens an authenticated IMAP login session (only when IMAP is configured); SMTP-only send paths skip IMAP entirely.

`verify_remote` is the only authoritative all-configured authentication probe. It exercises every configured transport end-to-end without retaining connection state: IMAP login plus SMTP authentication (Email), `auth_test` plus `apps_connections_open` (Slack), and `get_me` (Telegram). The `MessagingPlugin` trait requires both methods.

## `messages.db` ownership

`MessageStore::open(&config_dir)` opens and migrates `config_dir/messages.db` via
`aves::MigrationRunner` (`__peitho_migrations`). `Actor::new` opens it eagerly;
`actor.store()` lazily reopens on demand after `StopServer` clears it. `StopServer` calls
`store.maintain()` before dropping the handle — Aves DB maintenance runs once per stop.
The actor is the **sole writer** of `messages.db`; inflow tasks forward `InboundEvent` and
`MessagingInteraction` to the actor over bounded channels, and the actor performs every
`insert_inbound` / `insert_callback` / `insert_accepted_send` / permission-request write.

## Connection model

`messages.db.messaging_connections` is the **sole authority** for provider connections.
The retired `peitho` Config section, `ReloadSettings`, `listChannelConfigs`,
`config_key`, `MessagingConfigMap`, and `MessagingSettings` are gone. One inflow runs
per enabled connection, keyed by the connection UUID. Each row holds `id`,
`display_name`, `provider_type`, `credential_id` (Enclave UUID), `transport_settings`
(JSON `ConnectionTransport`), `enabled`, and `retention_seconds`. Public bus ops:
`listConnections`, `createConnection`, `updateConnection`, `setConnectionEnabled`,
`deleteConnection`. `DeleteConnection` walks Flow on a best-effort basis — list
or delete failures warn and continue — then `mark_deleted_for_connection`
finalizes pending permission requests and `delete_connection` removes the row.
A failed Flow walk can leave trigger rows pointing at a deleted connection.

## Sender config

`MessagingSenderConfig::Email | Slack | Telegram` (defined in `types.rs`) references a
`connection_id: SpectaUuid` plus a destination field (`channel_id` / `chat_id` / `from`).
The credential and transport are resolved from `messages.db` by the actor — senders no
longer carry a `credential_id` or inline transport.

## Subscription trigger config

`SubscriptionTriggerConfig` is `{ connection_id, subscribed_channels, allowed_senders, persistence }`
only — no `listener_type`, no `config_key`, no `credential_id`. The connection UUID carries
provider, credential, and transport. `messaging_trigger_registry` validates only that
`connection_id` is non-nil.

## Flow node kinds

`registrations.rs` pushes two `NodeKindRegistration`s onto the shared
`NodeRegistrationSupply` during `Actor::new`:

- **`MessageNode`** — `MessageNodeConfig { connection_id, provider, destination, body, metadata }`.
  Typed start/tick/cancel; `body`/`metadata` inputs override config on start. Terminal
  state carries a `receipt` output.
- **`PermissionNode`** — `PermissionNodeConfig { connection_id, provider, destination, body, metadata, duration_seconds }`.
  Rejects `ListenerType::Email`. `duration_seconds` must be positive. Persists a
  `permission_requests` row at start, transitions through `prepared` → `pending` on
  provider receipt, and finalizes on callback.

## History tables and retention

Four history tables, all written by the actor:

- `inbound_messages` — raw payload + optional normalized body/metadata per delivery.
- `permission_callbacks` — raw callback payload with optional `request_id` / `action`.
- `accepted_sends` — body + `provider_receipt` for accepted outbound (Send and
  PermissionNode); `raw_payload` is deliberately SQL `NULL`.
- `permission_requests` — not retention-pruned; lifecycle-managed (prepared/pending/approved/
  denied/expired/cancelled/deleted/failed).

`NewConnection` takes explicit `retention: RetentionDuration`; no provider defaults.

**Two-mode prune** — `prune_expired` runs three DELETE queries (one per history table)
with the same semantics:

- **Live connection**: `received_at + messaging_connections.retention_seconds <= now` —
  honors the current connection retention, so a connection edit changes expiry of
  existing rows on the next prune tick.
- **Orphan fallback**: when the connection is gone, use the row's own `expires_at`
  (snapshot of `received_at + retention_seconds` at write time) so history doesn't
  linger forever after deletion.

Prune runs on a 5-minute worker tick (`worker_loop`).

## Credential supply

Enclave is the sole credential supplier. It acquires Tyto's privileged
`MessagingRuntimeService` through its own `ServiceReader` (no `app.rs` wiring)
and calls `supply_credential` inline on `StartServer`, credential create,
credential replace, and credential rename; it calls `remove_credential`
inline on credential delete. The `CredentialMetadataChanged` and
`CredentialChanged` broadcasts are separate metadata-refresh signals for
other consumers and do not drive supply. Tyto holds a UUID-keyed
`credentials: HashMap<SpectaUuid, MessagingCredentials>` plus a per-UUID
`credential_generation` counter; only the UUID crosses public RPC, MCP, config,
persistence, and logs. The three messaging credential plugins and their
schema/decode live under `pkg/poseidon/src/plugins/messaging/`; there is no
`pkg/poseidon/src/messaging/` module.

## Runtime

Aves Flow owns trigger persistence in its existing `flow_triggers` table.
One inflow runs per enabled connection (keyed by the connection UUID), only while the
credential is supplied. Its bounded input queue (`CHANNEL_MESSAGE_BUFFER_SIZE = 100`)
fans out into dedicated bounded subscriber queues; messages are not divided among
competing receivers. Subscriptions filter before emitting accepted updates.
`subscribed_channels: None` admits every delivered channel; `Some([])` is invalid.
An empty sender allowlist is unrestricted; restricted subscriptions reject updates
lacking sender identity. Email lists all selectable folders and tracks each mailbox's
UID cursor; provider polling timing is internal. Protocol SDK types remain private.

## Outbound Send

`subscription.send` is a public mutation taking a provider-specific
`MessagingSenderConfig` (connection UUID + destination) plus a `NormalizedMessage`.
The actor resolves the connection row, the credential from its memory map, and the
transport from the connection's `transport_settings`, spawns `send_message` off the
actor loop, selects and connects the plugin, and runs `plugin.send` through
`aves::utils::retry::retry_with_backoff` with exactly **three total attempts**
(10s–60s exponential backoff). Only `MessagingSendError::Retryable`
(transport/server/rate-limit, honoring `retry_after_seconds`) retries; invalid
credentials, destination, permission, and validation are terminal. Email Send is
SMTP-only — `EmailPlugin::connect` short-circuits to send-only mode when
`smtp_server` is set and `imap_server` is absent. Slack Send uses the bot token
from the supplied credential. `subscription.sendMetadataSchema` returns the
per-provider JSON Schema (`EmailSendMetadata` / `SlackSendMetadata` /
`TelegramSendMetadata`). Accepted sends are written to `accepted_sends` by the
actor on provider receipt.

## Optional Knowledge persistence

Each inbound `SubscriptionTriggerConfig` and each outbound sender config carries
an optional `PersistencePolicy` (`None | Note { directory } | Data { namespace }`,
default `None`). A non-`None` policy fires one async
`CreateKnowledge` / `UpsertData` bus request via `spawn_knowledge_handoff`; it
never gates Flow triggering, never gates Send result, and never writes files or
DB directly. A 30s timeout caps the handoff; failure warns.

## Reconcile

`reconcile` calls `flows.list_source_triggers(ListSourceTriggersInput { adapter_kind: "subscription" })`
at `StartServer`, `SupplyCredential`, `RemoveCredential`, and on every connection
mutation (`CreateConnection`, `UpdateConnection`, `SetConnectionEnabled`,
`DeleteConnection`). It also runs after connection CRUD. There is no live
trigger-change seam — Tyto does not poll and does not subscribe to Flow trigger
mutations (accepted limitation).

## Lifecycle

Connection edits and credential changes apply live. Stopping the runtime aborts
listeners, runs `store.maintain()`, and drops the `MessageStore` handle — the
next `StartServer` reopens and re-migrates. Flow remains authoritative for
enablement, revision selection, and local ownership admission.

## Crate exports

Minimal: `MessagingDomain`, `MessagingRuntimeService`, `messaging_trigger_registry`, `MessagingBroadcast`, `RemoveCredentialInput`, `SupplyCredentialInput`, `SuccessOutput`, `messaging_operation_descriptors_with_root_error`, `rpc_router`, `MessagingCredentials`, `MessagingError`, `SubscriptionTriggerConfig`. Wire types (MessagingSenderConfig, NormalizedMessage) are accessible through generated RPC/TS bindings but not direct tyto:: Rust exports.

**Consumer reexports** (delivered 2026-10-29): The following symbols are reexported at the crate root for embedding applications:
- Connection DTOs: `ConnectionRecord`, `NewConnection`, `EditConnection`, `ConnectionTransport`
- Provider configs: `EmailConfig`, `SlackConfig`, `TelegramConfig`
- Enums: `RetentionDuration`, `ListenerType`
- Connection CRUD dispatchers: `ListConnectionsInput`, `ConnectionsOutput`, `dispatch_list_connections`, `CreateConnectionInput`, `ConnectionOutput`, `dispatch_create_connection`, `UpdateConnectionInput`, `dispatch_update_connection`, `SetConnectionEnabledInput`, `dispatch_set_connection_enabled`, `DeleteConnectionInput`, `dispatch_delete_connection`
- TestConnection: `TestConnectionInput`, `dispatch_test_connection` (pre-save credential verification)

Implementation modules (`actor`, `channels`, `connections`, `database`, ...) stay private; only consumer contracts move to the root.

## Testing

Tests live in `src/actor/tests.rs` (actor + nested types tests); provider helper tests
remain co-located. Follow repository code/test standards. Use `cargo test --package aves-tyto`
for unit coverage; protocol tests MUST NOT exercise live external providers — use mocks
exclusively; live credentials are prohibited.

## Related

- [Tyto Credential Supply Architecture](../../docs/architecture/tyto/credentials.md)
- [Tyto Connection Architecture](../../docs/architecture/tyto/connection.md)
- [Tyto Subscription Architecture](../../docs/architecture/tyto/subscriptions.md)
- [Tyto Send Architecture](../../docs/architecture/tyto/send.md)
- [Messaging Send and Persistence](../../docs/concepts/messaging-send-and-persistence.md)

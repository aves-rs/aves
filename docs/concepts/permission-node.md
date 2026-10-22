---
title: PermissionNode
type: concept
updated: 2026-10-14
references:
  - crates/tyto/src/registrations.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/permission_node.rs
  - crates/tyto/src/permission_callbacks.rs
  - crates/tyto/src/database.rs
  - crates/tyto/migrations/0001_messaging_store.sql
  - crates/tyto/src/channels/mod.rs
  - crates/tyto/src/types.rs
---

# PermissionNode

Tyto-owned Aves Flow node kind `PermissionNode` posts a Slack or Telegram message with Approve/Deny buttons and gates the Flow on the first durable human decision. It exists so automation can request human authorization — a deploy, a spend, a trade — with a bounded expiration and a single authoritative answer, even across restarts and duplicate callbacks.

## Key points
- Registered via `NodeKindRegistration::typed::<PermissionNodeConfig, …>` pushed onto the shared `NodeRegistrationSupply` in `Actor::new` alongside MessageNode.
- `PermissionNodeConfig` carries `connection_id`, `provider` (Slack/Telegram only), `destination`, `body`, `metadata`, and `duration_seconds`; the registration validator rejects Email (no interactive buttons) and zero/negative duration at decode time.
- The `duration_seconds` is a strictly-positive relative duration converted once, at send time, to an absolute UTC `expires_at` timestamp stored on the `permission_requests` row.
- Pre-send persistence order: `MessageStore::insert_prepared` writes a `prepared` row in `messages.db` BEFORE any provider call; only after successful send acceptance does `mark_pending` attach the provider message id and receipt.
- The request UUID embedded in button callback payloads (`PermissionButtonPayload::encode`) is the canonical lookup key; the listener decodes the action and forwards `MessagingInteraction` to the actor.
- The listener acks the provider immediately (transport only) and the actor arbitrates first-durable-decision-wins via `finalize_atomic` — a conditional update that returns false if another transition already won.
- The actor also persists the raw callback row (`insert_callback`) independently of arbitration outcome.
- Approve succeeds the node with `approved: true`, provider receipt, and UTC `decided_at`; Deny / expiry / cancel / failed all fail the node.
- A five-minute expiration worker (`scan_unfinished` + `scan_outstanding_edits` loop on the actor's worker tick) finalizes rows past `expires_at` and retries any outstanding provider edits — there is no listener-side timer.
- Provider edit (`edit_interactive`) removes the buttons and shows the terminal outcome while preserving the original `request_text`.
- Pending state survives restart because `prepared`/`pending` rows live in `messages.db` and the worker resolves them by `(flow_execution_id, flow_node_id)` correlation on next tick.

## How it works
On start, the registration closure sends `NodeCommand::PermissionStart` with the decoded config. The actor: (1) resolves connection + credentials; (2) calls `insert_prepared` with `(request_id, connection_id, destination, request_text, expires_at, flow_execution_id, flow_node_id)`; (3) spawns `run_interactive_send` which `connect`s the plugin and calls `send_interactive`; (4) on provider acceptance, calls `mark_pending` to attach `provider_message_id` and `provider_receipt`. If `mark_pending` loses the race (a concurrent finalize already transitioned the row), the actor calls `attach_provider_message` to write the late receipt onto the already-terminal row, and if the row's `outstanding_edit` flag is set, calls `edit_finalized` to replace the buttons with the outcome label — buttons do not stay live.

The listener decodes the callback payload via `PermissionButtonPayload::decode` into `(request_id, InteractionAction)`, acks the provider, and forwards `MessagingInteraction` to the actor. The actor persists the raw callback and calls `finalize_atomic(request_id, status, decided_at, outstanding_edit = true)`. If the conditional update wins, the actor calls `edit_finalized`, which only spawns `run_provider_edit` when the row already carries a `provider_message_id`; a decision that wins while the row is still `prepared` returns early from `edit_finalized`, and the provider-side edit is performed later by the late-receipt path when the provider receipt finally arrives. The worker loop, driven by `worker_tick_tx` every ~5 minutes, scans `scan_unfinished(now)` for rows past `expires_at` (finalized as `expired`) and `scan_outstanding_edits` for rows whose provider edit must be retried.

## Known pitfalls
- Email is rejected at registration — there is no email interactive path. Do not attempt to add one without implementing a reply-parsing arbitration path.
- The listener ack and the actor arbitration are deliberately decoupled; the provider sees an immediate ack, and the durable decision is whoever writes first to `finalize_atomic`. Duplicate callbacks after the first decision are recorded as raw callbacks but return `false` from `finalize_atomic` and do not re-edit the message.
- Restart does not lose pending state: the worker resumes from the persisted row, resolves the Flow correlation, and finalizes by `expires_at`. There is no in-memory pending map that must survive.

## Related
- [MessageNode](message-node.md) — the sibling send-only node sharing `TytoNodeHandle`.
- [Tyto Send](../architecture/tyto/send.md) — the underlying send path reused here for the initial button post.
- [Messaging Send and Persistence](messaging-send-and-persistence.md) — outbound Send history contract.
- [Pluggable Flow Nodes](pluggable-flow-nodes.md) — the open-kind envelope and registration supply.
- `crates/tyto/AGENTS.md` — local Tyto conventions.

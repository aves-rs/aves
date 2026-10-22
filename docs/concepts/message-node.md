---
title: MessageNode
type: concept
updated: 2026-10-14
references:
  - crates/tyto/src/registrations.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/messaging_nodes.rs
  - crates/tyto/src/delivery.rs
  - crates/tyto/src/types.rs
  - crates/tyto/src/channels/mod.rs
  - crates/aves/src/flow/registration.rs
---

# MessageNode

Tyto-owned Aves Flow node kind `MessageNode` sends exactly one outbound message through a persisted messaging connection. It exists so a Flow can notify a Slack channel, Telegram chat, or email recipient as a side effect of automation, using the same retry and history path as `subscription.send` — without introducing a second send API.

## Key points
- Registered via `NodeKindRegistration::typed::<MessageNodeConfig, …>` pushed onto the shared `NodeRegistrationSupply` in `Actor::new` (`crates/tyto/src/actor.rs`); one handle shared with PermissionNode.
- `MessageNodeConfig` carries `connection_id` (UUID), `provider` (Email/Slack/Telegram), provider-specific `destination` (JSON), static `body`, and optional `metadata`; the `body` and `metadata` inputs in the Flow blackboard override the static config at start.
- Nil `connection_id` is rejected at registration; all three providers are admitted — no provider gating here.
- Start dispatches `NodeCommand::MessageStart` onto the actor, which records a `node_pending` slot, spawns `send_message` off the actor loop using `for_kind` + `connect` + the existing three-attempt `retry_with_backoff` (10s–60s), and returns `Running` immediately.
- The spawned task delivers its result over `node_done_tx`; the actor stores the result in the `node_pending` slot but does NOT write `accepted_sends` at provider accept.
- A later Tick drains the slot: on `Ok(receipt)` it writes `MessageStore::insert_accepted_send` and replies `Succeeded { receipt }`; on `Err(message)` it replies `Failed`. Until the slot resolves, Tick replies `Running`.
- Terminal provider errors (invalid credentials, destination, permission, validation) fail the node; only `MessagingSendError::Retryable` retries.
- Cancel removes the `node_pending` slot and replies `Cancelled` without aborting the spawned task; a later completion arrives at `node_done_rx` but finds no slot and is discarded. After restart, `node_pending` is empty, so a recovered Tick with no slot replies `Running` indefinitely until the in-flight task's result lands or the node is explicitly cancelled.

## How it works
At start, the registration closure reads Flow blackboard inputs `body` (string) and `metadata` (JSON), overlays them onto the decoded `MessageNodeConfig`, and sends `NodeCommand::MessageStart` through the shared `TytoNodeHandle` channel to the actor. The actor resolves the connection from `messages.db`, looks up the credential by UUID, inserts a `node_pending` slot keyed by the Flow correlation, and spawns `send_message` — the reply returns `Running` before the send completes. There is no template language — `body` is the literal message text supplied by the Flow author, and `metadata` is provider-specific (e.g. `EmailSendMetadata` recipients/subject, empty for Slack/Telegram). Provider accept is not the terminal transition: the accepted-send row is written on the next Tick after the spawned task delivers its result.

## Known pitfalls
- `body` and `metadata` are overridden by Flow inputs if present; authors who bind both get the bound values, not the config defaults.
- There is no per-send PersistencePolicy on MessageNode — it shares the outbound Send retention row only, not the Knowledge handoff path that inbound triggers use.
- Cancel during an in-flight send does not abort the spawned task; the task continues to completion, but the actor has already dropped the `node_pending` slot, so the eventual result is discarded. Restart loses the in-flight correlation — the new actor sees no slot and replies `Running` until the next completion arrives.

## Related
- [PermissionNode](permission-node.md) — the sibling interactive node sharing `TytoNodeHandle`.
- [Tyto Send](../architecture/tyto/send.md) — the underlying send path MessageNode reuses.
- [Messaging Send and Persistence](messaging-send-and-persistence.md) — outbound Send history contract.
- [Pluggable Flow Nodes](pluggable-flow-nodes.md) — the open-kind envelope and registration supply.
- `crates/tyto/AGENTS.md` — local Tyto conventions.

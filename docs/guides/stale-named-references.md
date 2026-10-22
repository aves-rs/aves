---
title: Stale Named References
type: error
updated: 2026-10-14
references:
  - crates/tyto/src/actor.rs
  - crates/tyto/src/connections.rs
  - crates/tyto/src/reconciliation.rs
  - crates/tyto/src/database.rs
  - crates/tyto/src/types.rs
---

# Stale Named References

## Symptom

`unknown connection` (on `MessageNode` / `PermissionNode` start) or a skipped
trigger during reconcile (no inflow matches).

A Flow subscription trigger, or a Flow messaging node, references a
`connection_id` whose `messages.db` row has been deleted — or references a
credential UUID that Enclave has not yet supplied. The listener cannot connect;
the node fails with a typed error and the trigger is skipped on reconcile,
without silently substituting another connection.

## Fix

Recreate the connection through `subscription.createConnection` (or update the
trigger/node config to reference an existing `connection_id`) and ensure the
credential has been supplied to Tyto by Enclave via
`MessagingRuntimeService::supply_credential`. Connection and trigger
updates reconcile live listeners. Do not rename credentials to repair this
reference: credentials are independently UUID-keyed.

Malformed persisted trigger configuration is surfaced as a typed error. A
failed reconciliation does not discard the previously valid listener
inventory.

## Related

- [Named Config References](../concepts/named-config-references.md)

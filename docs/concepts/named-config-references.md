---
title: Named Config References
type: concept
updated: 2026-10-14
references:
  - crates/tyto/src/types.rs
  - crates/tyto/src/database.rs
  - crates/tyto/src/actor.rs
  - crates/tyto/src/connections.rs
---

# Named Config References

Persist a lightweight reference when configuration belongs to a different owner.
Choose identity appropriate to that owner: a connection row identifies
non-secret transport and credential binding; a credential must use its stable
UUID, never its display name.

Tyto's `SubscriptionTriggerConfig` demonstrates the distinction:

- `connection_id` (UUID) references a `messages.db` `messaging_connections` row;
  the actor resolves credential, transport, and destination defaults from it.
- The credential binding on that row is itself a UUID, resolved from the actor's
  in-memory `credentials` map (supplied by Enclave).
- The Flow trigger record supplies Flow identity and pinned revision.
- Access lists (`subscribed_channels`, `allowed_senders`) belong to the trigger,
  not to the connection row.

A missing connection row or an unsupplied credential UUID fails explicitly
during listener setup; there is no default credential or silent substitution.
Deleting a connection makes the reference stale — the actor skips the trigger
on reconcile (no live inflow) but the orphan trigger row remains in Flow until
manually removed; a credential display-name change does not change its identity.

## Related

- [Stale Named References](../guides/stale-named-references.md)

---
title: Aves Machine Domain Architecture
type: architecture
updated: 2026-11-14
references:
  - crates/aves/src/machine/mod.rs
  - crates/aves/src/machine/domain.rs
  - crates/aves/src/machine/handle.rs
  - crates/aves/src/machine/messages.rs
  - crates/aves/src/machine/types.rs
  - crates/aves/src/machine/actor.rs
  - crates/aves/src/machine/content.rs
  - crates/aves/src/machine/encryption.rs
  - crates/aves/src/machine/peering.rs
  - crates/aves/src/bus/remote.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/flow/domain.rs
  - crates/aves/src/flow/actor.rs
  - crates/aves/src/coordination/domain.rs
  - crates/aves/src/coordination/actor.rs
---

# Aves Machine Domain Architecture

Machine owns inventory, shared peer services, content operations and machine-key encryption. Auth owns the underlying identity and peer admission; Config owns the persisted machine name.

## Peering and consumers

The runtime prepares `IrohSubstrate` with Auth's machine key and supplies `MachinePeerServices` as a construction input. Machine publishes `dyn MachinePeerService` into the kernel service registry when that input is present. `MachinePeerServices` is public for higher-level applications joining the peer network. The substrate has one endpoint and router, accepting Bus (when installed), Auth, iroh_blobs, gossip and docs ALPNs. Machine's `StartServer` and `StopServer` go through the connectivity controller; preference and server state determine whether networking is enabled, and transitions can replace native handles.

Flow and Coordination hold late-bound `ServiceReader`s: construction order between these consumers and Machine is not a dependency. At `StartRuntime`, Flow records whether the service is published and can run without peer sync. Its current default workspace still requires `MachinePeerServices` as a construction input, even without the Machine domain. Coordination resolves the reader at `StartServer` and panics if the service is absent. Config must be constructed before Machine registers its section.

The Bus handler is in `bus/remote.rs`: its request and response handler limits are 1 MiB, while serialized operations and replies are limited to 512 KiB. Machine's `call_remote_operation_with` refreshes the peer audience and obtains a peer JWT before calling the Bus transport; there is no runtime `call_remote_operation` helper. See [Iroh Peer Runtime](iroh-peer-runtime.md) for trust and lifecycle details.

## Inventory

Machine loads its name from Config, falling back to the boot default; rename saves the section before updating memory and sends `MachineRenamed`. Status projects admitted remote issuers from the single Auth admission registry, not a second peer cache. Admission is not liveness: peer online state is `Unavailable`.

## Content and encryption

Content operations import, read, pin and unpin raw blobs or collections. Fetch accepts only admitted remote issuers and requires enabled connectivity. It fetches the root first, pins it, then fetches collection children; a later failure removes the pin. Unpin removes only the tag, not the blob.

Encryption uses the local iroh machine key and machine recipient identities, without root, enclave or certificate dependencies. The producer always gets a self-wrap; signed recipient envelopes carry wrapped content keys, never unwrapped keys over the bus. Decrypt verifies envelopes and opens the local wrap; adding a recipient authenticates the ciphertext before wrapping the key again.

**Limits:** the native blob-serving handler has no admission check. Local blob reads materialize the whole blob in memory; this path has no streaming size cap. The Bus operation limits do not impose a native blob-transfer limit.

## Related

- [Iroh Peer Runtime](iroh-peer-runtime.md) — transport, admission and lifecycle
- [Auth Domain](auth-domain.md) — identity and peer trust
- [Flow Domain](flow-domain.md) — Flow execution

---
title: Aves Coordination Domain Architecture
type: architecture
updated: 2026-11-14
references:
  - Cargo.toml
  - crates/aves/src/coordination/mod.rs
  - crates/aves/src/coordination/domain.rs
  - crates/aves/src/coordination/handle.rs
  - crates/aves/src/coordination/messages.rs
  - crates/aves/src/coordination/types.rs
  - crates/aves/src/coordination/actor.rs
  - crates/aves/src/coordination/abci.rs
  - crates/aves/src/coordination/chain.rs
  - crates/aves/src/coordination/transaction.rs
  - crates/aves/src/coordination/network.rs
  - crates/aves/src/coordination/serialization.rs
  - crates/aves/src/coordination/signing.rs
  - crates/aves/src/coordination/persistence.rs
  - crates/aves/src/migrations/coordination.rs
  - crates/aves/src/migrations/coordination_migrations/0001_coordination_state.sql
  - crates/aves/src/migrations/coordination_migrations/0002_coordination_retired.sql
---

# Aves Coordination Domain Architecture

Coordination hosts a reusable in-process chain over Malachite 0.8.2, pinned through the `arc-malachitebft-*` dependencies. Blockchain implementation and typed coordination records live in Aves; applications compose and consume the domain. Each domain instance owns at most one active chain, with retained predecessors.

## Ownership and routing

- The generated bus handler forwards requests to the standard `run_actor` loop. `handle_message` routes bus operations; `handle_consensus_message` routes Malachite host callbacks. `abci.rs` owns actor-local application state and recovery; `chain.rs` implements its Malachite callbacks. There is no application host trait or mutex around this state.
- Malachite requires a ractor `ActorRef<HostMsg>`; `ConsensusMessageAdapter` in `transaction.rs` is the host bridge into the actor's channel. Malachite's own adapter blocks on replies, so the actor forwards deferred replies separately and remains able to accept transactions.
- `StartServer` attaches transport and starts the engine; `StopServer` stops it and clears restart intent. The late-bound Machine peer-service reader must resolve at start: `StartServer` panics if Machine has not published the service. An active chain is also needed to start.

## Operations and commitment

- `Genesis { validators: Vec<(PublicKey, u64)> }` selects initial validator keys and caller-supplied voting power; it creates a fresh chain only on an empty host. `Regenesis` goes through `begin_successor`, not ordinary transaction submission: it checks the predecessor snapshot and authorized current user root, retains the predecessor and installs the successor atomically. Successor validators must be retained predecessor entries with the same powers.
- Signed user, machine, execution and schema submissions return `TransactionSubmitted` after durable admission, **not commitment**. Reads expose committed user, machine, execution, schema, chain and regenesis projections. No operation is marked privileged.
- Export returns the active chain or a retired predecessor snapshot. Adoption requires an independently trusted expected hash, validates recovery and commit certificates, and cannot replace a different initialized host. The hash is an integrity identifier, not a certificate over the projection.
- Aves never proposes an empty value: idle `GetValue` replies wait for admitted work or the proposal deadline. `ConsensusReady` and `Finalized` are not withheld to avoid empty blocks; finalization advances to the next height after retaining the decision.

## Signing and serialization

`verify_body` verifies Ed25519 over the exact concatenation `PURPOSE ‖ DomainId bytes ‖ serde_json(body)`. Consensus vote and proposal adapters similarly bind purpose, domain and JSON message; the underlying signer and verifier operate on bytes. Validator proofs instead use Malachite's exact `ValidatorProof::signing_bytes(public_key, peer_id)`. Serialization uses Serde JSON, not Borsh.

Signature verification is not universal authorization: execution and schema records check the signature but do not check signer registration. Execution state transitions and schema JSON validity are checked separately; user and machine records have additional identity rules.

## Persistence and network

- `<config>/coordination.db` uses `__coordination_migrations`. Its recovery unit stores the committed projection, pending admissions, decided values and proposal replay data; Malachite's per-chain WAL is the only consensus WAL. Snapshots include complete decided history, and retired predecessor recovery is retained for export.
- Transport uses Machine's published peer service and shared iroh Gossip, with Auth admission for direct peers. The network emits `Listening` to each subscriber on subscription; without it consensus never starts. Native handle replacement triggers reconnect while the server is requested; explicit stop clears that intent.
- `CoordinationJsonCodec` supplies Malachite's WAL and sync codec bounds. Network messages and sync envelopes use JSON; vote extensions are unsupported.

## Related

- [Aves Runtime Supervisor](runtime-supervisor.md)
- [Iroh Peer Runtime](iroh-peer-runtime.md)
- `crates/aves/AGENTS.md`

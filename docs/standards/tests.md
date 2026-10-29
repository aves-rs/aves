---
title: Testing Standards
type: standard
updated: 2026-10-29
references:
  - crates/aves/src/config/actor/tests.rs
  - crates/aves/macros/src/tests.rs
  - crates/corv/AGENTS.md
  - crates/kea/AGENTS.md
  - crates/tyto/AGENTS.md
---

# Testing Standards

Use the owning package's `AGENTS.md` for its test layout and target command. Rust actor tests live beside their owner (often `src/actor/tests.rs`); helper tests stay co-located. The Aves macro crate mounts its private `src/tests.rs` from `lib.rs`.

- Test behavior, error paths, persistence boundaries, lifecycle transitions, and serialization where they matter. Avoid reimplementing production logic in assertions.
- **NEVER exercise real external providers (Email, Slack, Telegram, etc.) in automated tests.** All provider tests MUST use mocks exclusively. Live-provider credentials and network calls are prohibited across all Aves crates. Mock the provider layer (transport, SDK client, network) and assert dispatch, branching, success/failure classification, and side-effect absence.
- Keep tests deterministic and isolated; use temporary directories for persistent state and mock provider protocols rather than requiring live services.
- Do not widen production APIs for tests. Aves exposes its existing `test-support` feature for downstream fixtures.
- Run targeted package tests when relevant: `direnv exec . cargo test --package aves`, `aves_macros`, `aves-corv`, `aves-kea`, or `aves-tyto`.

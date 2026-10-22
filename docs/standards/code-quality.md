---
title: Code Quality Standards
type: standard
updated: 2026-10-23
references:
  - AGENTS.md
  - crates/aves/AGENTS.md
  - crates/aves/macros/AGENTS.md
  - crates/corv/AGENTS.md
  - crates/kea/AGENTS.md
  - crates/tyto/AGENTS.md
---

# Code Quality Standards

Follow the nearest `AGENTS.md` for ownership, public contracts, file layout, and test commands. Keep the shared runtime application-neutral: Aves does not depend on Corv or Kea; Corv, Kea, and Tyto depend on Aves, not an embedding application.

- Prefer typed Rust errors and identifiers. Keep internal state private; export only contracts an actual consumer needs.
- Use generated bus routing and typed services rather than parallel dispatch paths. Domain-specific state and persistence belong to the owning package.
- Reuse Aves utilities through their concrete `aves::utils` modules; do not create barrel exports or import application-specific mechanisms into them.
- Remove unused code and resolve warnings; avoid suppressing diagnostics. Keep comments minimal and document public contracts.
- Keep workspace Rust dependencies pinned. Check language-server diagnostics during edits; use targeted package tests when validating behavior.

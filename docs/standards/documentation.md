---
title: Documentation Standards
type: standard
updated: 2026-10-23
references:
  - AGENTS.md
  - docs/navigation.md
---

# Documentation Standards

| Surface | Purpose |
|---|---|
| Nearest `AGENTS.md` | Binding local ownership, constraints, and commands |
| `docs/` | Cross-package architecture, concepts, guides, and standards |
| `.agents/skills/` | Agent workflow, not repository facts |
| Package README | Human onboarding |

Local `AGENTS.md` takes precedence. Link to it rather than copying it. Verify every claim against current source before writing; if its owner has been peeled off, remove the obsolete doc instead of adapting an application-only narrative.

- Engineering docs need `title`, `type`, `updated`, and `references` frontmatter. `references` must point to real files proving the claims; update the date when verified content changes.
- Keep one topic per file and register every doc in [navigation](../navigation.md) in the same change. Repair inbound links when removing or moving docs.
- Concepts: at most 100 lines. Architecture, guides, and standards: at most 150 lines. Prefer removing duplicated, historical, or inferable content to splitting.
- Use kebab-case filenames. Document exported Rust contracts in source comments, not by copying entire APIs into docs.

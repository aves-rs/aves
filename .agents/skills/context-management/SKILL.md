---
name: context-management
description: Use when maintaining the Aves engineering context: removing stale docs, updating existing knowledge, compacting or reorganizing docs, repairing navigation, or validating references. For a new doc use docs-writing; for lookup only use superpowers.
---

# Context Management

Keep the existing context truthful, small, and navigable. Prefer removing an obsolete document over rewriting it for an absent application. For new docs, load `docs-writing`; for ordinary lookup, use `superpowers`.

| Role | Location |
|---|---|
| Binding local rules | Nearest `AGENTS.md` |
| Cross-package knowledge | `docs/` |
| Agent process | `.agents/skills/` |
| Durable session facts | `ctx_memory` |

`docs/navigation.md` indexes architecture, concepts, guides, and standards. A doc not linked there cannot be discovered. The `docs/external/` tree caches third-party library documentation; see `docs/external/navigation.md` for its index.

## Maintain

1. Read the nearest `AGENTS.md`, [documentation standards](../../../docs/standards/documentation.md), and [navigation](../../../docs/navigation.md). Verify code claims against existing source rather than other docs.
2. Classify each candidate: absent owner or stale historical redirect → remove; still relevant but mixed with removed application material → trim; accurate and uniquely useful → keep. Do not preserve a page solely because it mentions Aves.
3. Preview exact destructive changes and get approval before deleting, merging, moving, or overwriting files. Never delete session/scratch files without separate confirmation. Preserve unrelated working-tree changes.
4. In the same change, remove dead links from navigation, retained docs, `AGENTS.md`, and skill instructions. Update surviving `references:` and `updated:` dates only after verifying current source. Prefer links to binding `AGENTS.md` rather than duplicating it.
5. For newly needed docs, use `docs-writing` for placement and templates. Capture from session artifacts only after the user selects what belongs in durable context.
6. Validate and report files removed, retained, and edited, including any gaps that remain.

## Validation

- Every retained engineering doc has a navigation entry; each navigation target exists.
- Every `references:` path in retained docs exists and supports its claim. Check relative Markdown links from their own directory; check `AGENTS.md` and `.agents/skills/` for dead inbound pointers.
- Concepts stay ≤100 lines; architecture, guides, and standards ≤150 lines. Compact duplicated AGENTS content and historical narration rather than splitting by default.
- A missing source or link is a failure to report, not an invitation to invent a replacement.

Use the repository's read/search tools for discovery and targeted link checks. Avoid broad shell grep/find pipelines and never treat the old Sluice tree as the source of truth.

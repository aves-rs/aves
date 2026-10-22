---
title: Knowledge Markdown Directory (TurboVault)
type: concept
updated: 2026-10-23
references:
  - crates/kea/src/documents.rs
  - crates/kea/src/types.rs
  - crates/kea/src/actor.rs
---

# Knowledge Markdown Directory

TurboVault backs the **only** Knowledge note implementation. It accepts an ordinary Markdown directory or a directory managed by Obsidian; no vault metadata or Obsidian installation is required. `KnowledgeConfig` carries four live-reloadable fields: `root` (default `config_dir/knowledge`, or an existing configured directory), `trash_path` (single root-level component, default `.trash`), `excluded_paths` (root-level component names excluded from discovery), and `allowed_extensions` (file extensions admitted by list/tree/import and on read; non-empty semantic search remains Markdown-indexed; default `md`, `markdown`, `txt`, `canvas`). The note endpoints and the Flow workspace source switch to a new root after a successful Config change, without a restart or file migration.

`VaultKnowledge.backend_id` is the vault-relative path. The directory path validator, symlink guards, watcher, and file-write semantics are preserved for both kinds of directory. List and tree both use TurboVault `scan_vault_with_metadata`, which filters by configured `allowed_extensions`, size limits, and `excluded_paths` (plus the configured `trash_path`); the tree is derived from the parent paths of admitted files, so only directories containing an admitted file appear. The original `tags` projection strips duplicated frontmatter tags on read and rehydrates them on write. The initial create uses a title-derived filename with `.md` when no path or extension is given, suffixes collisions, and writes into the root; deletes move files into the configured `trash_path` (default `.trash`) via a host hard-link and unlink under the vault root.

`searchKnowledge` and non-empty `listKnowledge` queries use TurboVault 3's cached `SimilarityEngine` (TF-IDF cosine over Markdown notes). They do not use embeddings or substring scoring. Semantic candidates are ranked by TurboVault; Knowledge applies existing path/tag/frontmatter/date filters, then limits the results. Empty queries keep the ordinary filtered list behavior. TurboVault write events invalidate the index; the actor reconciles external edits on watcher events before broadcasting. A root switch creates a fresh index for the new directory.

Inline note content and `input_file` are resolved by the Kea Knowledge actor before calling TurboVault. `input_file` takes precedence; agents SHOULD use absolute paths. An absolute path is used directly; a relative path resolves only beneath the fixed `config_dir/discuss` prefix, independently of Discuss settings, and fails if the source is missing. Directory inputs import eligible files, rather than storing a path string as note content.

No backend selector, note plugin, read-only setting, inbox override, or semantic encoder setting exists. `trash_path`, `excluded_paths`, and `allowed_extensions` are configurable through `KnowledgeConfig`; TurboVault's own defaults (such as `.obsidian`) are always applied in addition to configured exclusions. The file watcher currently watches the full vault recursively because TurboVault exposes no watcher-exclude API; configured exclusions filter scan results, not watcher registration. File-backed Canvas dashboards are separate from the notes directory; JSON Data objects are stored in the independent KV store.

See [Knowledge Domain Architecture](../architecture/kea/knowledge-domain.md).

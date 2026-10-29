# Kea

Kea owns the Knowledge domain on the Aves runtime. Notes live in one TurboVault-managed Markdown directory; structured JSON objects live in a separate Kea-managed `knowledge.db` KV table. Poseidon composes the domain and adapts its RPC errors/broadcasts. Kea depends on Aves, never Poseidon; Aves must not depend on or import Kea. No database note plugin, backend selector, embedding model, or another semantic encoder exists.

## Ownership

- `actor.rs` owns note, JSON Data, and file-backed Canvas dashboard request handling; it reloads the active `KnowledgeConfig` (root, trash_path, excluded_paths, allowed_extensions) after a Knowledge `ConfigChanged` broadcast. Kea does not read Discuss settings or react to Discuss config changes.
- `documents.rs` owns the single TurboVault directory implementation, its lifecycle, change watcher, and Flow workspace source. List and tree both use TurboVault `scan_vault_with_metadata`, which filters by configured `allowed_extensions`, size limits, and `excluded_paths` (plus the configured `trash_path`); non-empty list queries and `searchKnowledge` then rank through TurboVault 3's TF-IDF `SimilarityEngine`, not Sluice substring scoring. Filters apply after semantic ranking, before the 100-row cap. The index is cached, invalidated on TurboVault changes, and refreshed after watcher events; root changes reopen the directory. This is term-based similarity, not embeddings. Path checks, collisions, and trash behavior apply equally to ordinary Markdown folders and Obsidian vaults. There is no note plugin layer.
- `data.rs` implements JSON-object `upsertData`, `getData`, `listData`, and `deleteData` against `config_dir/knowledge.db`; `queries/*data.sql` hold its SQL. The exact key is case-sensitive. `listData` returns keys and one last-write timestamp, with inclusive RFC3339 date bounds and offset pagination (200 rows maximum per call). No Data UI is provided.
- `src/migrations.rs` registers historical migration 2 and replacement migration 3 from SQL files in `migrations/` (a peer of `src/`). Migration 3 discards old note/index/embedding tables and creates `knowledge_data`; `__knowledge_migrations` remains the migration ledger. Old database notes are intentionally discarded, not copied into the Markdown root.
- `messages.rs` owns typed note, Data, and Canvas RPC/MCP operations. `types.rs` owns the error identity and request/response DTOs. `domain.rs` publishes the bus and Flow workspace source. `handle.rs` forwards relevant Config events into the actor.

## Directory configuration

- `KnowledgeConfig` carries four fields: `root` (optional directory; `None` uses `config_dir/knowledge`, creating it on first run; an explicit folder must exist to open), `trash_path` (single root-level component for the note implementation, default `.trash`), `excluded_paths` (component names excluded from discovery; default empty — TurboVault's own defaults such as `.obsidian` are always applied), and `allowed_extensions` (file extensions admitted by list/tree/import and on read; non-empty semantic search remains Markdown-indexed; default `md`, `markdown`, `txt`, `canvas`). Relative directory paths remain supported and `~` is not expanded. All four fields reload live through a `ConfigChanged` broadcast; invalid values are not rejected on save, and a failed reopen retains the active directory.
- A saved root change reopens the note implementation and watcher, swaps Flow's shared directory source, and broadcasts `KnowledgeRecordsChanged` to refresh Hades note queries. A failed reopen leaves the old implementation active and logs the error; changing the root does not move files.
- Create accepts inline content or `input_file`. Agents SHOULD use absolute input paths, which are used as supplied. Relative inputs resolve only beneath the fixed `config_dir/discuss` prefix, ignoring Discuss settings; missing inputs fail with the resolved path and absolute-path guidance. A directory input imports existing eligible text files. File input takes precedence, and neither source path nor raw bytes are persisted as separate note metadata. Keep this guidance in the request struct/field doc comments so MCP JSON schemas advertise it.
- Canvas dashboards remain separate JSON files under `config_dir/dashboards` and do not enter the Data table or note directory.

## Testing

Source modules live under `src/`; `src/lib.rs` exposes only the domain marker and the concrete contracts needed by consumers. Keep actors, handlers, implementation types, and migrations private. Preserve the `knowledge` RPC/config/error identities and existing storage paths.

**Consumer reexports** (delivered 2026-10-29): `KnowledgeConfig` is reexported from the Kea crate root for embedding applications. Implementation modules remain private; only consumer contracts are reexported.

Primary tests live in `src/actor/tests.rs`; `data.rs` and `documents.rs` have co-located `#[cfg(test)] mod tests`. `messages.rs` and `handle.rs` have no tests. Target them with `cargo test --package aves-kea`. Tests use Aves's `test-support` feature for existing bus-envelope fixtures.

See [Knowledge Domain Architecture](../../docs/architecture/kea/knowledge-domain.md) for cross-package RPC ownership. Follow the repository code and test standards. This file is the local authority for the Knowledge domain.

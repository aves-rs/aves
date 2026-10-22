---
title: Knowledge Domain Architecture
type: architecture
updated: 2026-10-23
references:
  - crates/kea/AGENTS.md
  - crates/kea/src/actor.rs
  - crates/kea/src/data.rs
  - crates/kea/src/domain.rs
  - crates/kea/src/handle.rs
  - crates/kea/src/messages.rs
  - crates/kea/src/documents.rs
  - crates/kea/src/migrations.rs
  - crates/aves/src/utils/database.rs
---

# Knowledge Domain Architecture

Kea owns Knowledge and depends on the Aves runtime; Aves has no dependency on Kea. An embedding application composes the domain and projects its errors and broadcasts. Notes are files in one TurboVault-managed directory; JSON Data objects use a separate, Kea-owned `knowledge.db` table. These are distinct storage surfaces, not selectable backends.

## Request path

`KnowledgeDomain` registers its Config section and a Flow workspace source. Its handle routes typed bus requests to the actor. The actor opens one note directory and one JSON Data store. The note endpoints (`createKnowledge`, `getKnowledge`, `updateKnowledge`, `deleteKnowledge`, `listKnowledge`, `listKnowledgeTree`, `searchKnowledge`) use TurboVault; non-empty list queries and search use its TF-IDF similarity engine, not an embedding model. Data endpoints (`upsertData`, `getData`, `listData`, `deleteData`) use exact keys in the KV table. Dashboard endpoints still read and write separate JSON files under `config_dir/dashboards`.

`KnowledgeConfig` carries four live-reloadable fields: `root` (optional directory; `None` creates `config_dir/knowledge`; an explicit root must exist to open), `trash_path` (single root-level component for the note implementation, default `.trash`), `excluded_paths` (component names excluded from discovery), and `allowed_extensions` (file extensions admitted by list/tree/import and on read; non-empty semantic search remains Markdown-indexed). Config persists the typed section without a Knowledge-specific pre-write validator. A Config broadcast attempts to reopen the note directory and watcher; on success the actor swaps the Flow source root and broadcasts `KnowledgeRecordsChanged` so the desktop refreshes its note queries. A failed reopen is logged and leaves the active directory in place, even though the new settings were saved. Notes do not move between roots. Relative directory roots remain valid under the existing path semantics.

`createKnowledge` accepts inline content or `input_file`. Agents SHOULD use absolute source paths; relative inputs resolve only beneath the fixed `config_dir/discuss` prefix. Kea does not import Discuss settings, query the Discuss config section, or follow its workspace changes. Missing sources fail with the resolved path and absolute-path guidance. Request struct and field doc comments expose the rule in MCP JSON schemas. Files and eligible directory inputs are read before TurboVault writes the resulting notes; `input_file` takes precedence over inline content.

## Data persistence

Kea opens `config_dir/knowledge.db` with Aves's database API and tracks versions in `__knowledge_migrations`. Existing migrations live in `crates/kea/migrations/`, beside `src/`. Migration 2 is retained for already-deployed databases; migration 3 drops the former note, keyword, chunk, job, and embedding tables and creates `knowledge_data`. Existing database-backed notes are discarded, not imported into the directory. After migrations, `DataStore::open` calls the shared `aves::utils::database::maintain_database` helper, which checkpoints the WAL and conditionally reclaims free space (VACUUM only when the helper's thresholds are met). `KnowledgeDomain::stop` dispatches an internal `StopRuntime` request to the actor, which calls `DataStore::maintain_db` — the same helper — before acknowledging shutdown. There is no central database registry; each database owner invokes the shared helper at its own lifecycle boundaries.

`knowledge_data` holds an exact case-sensitive key, a validated JSON object, and one UTC last-write timestamp. Upsert replaces the entire object; get and delete target an exact key and return not-found on absence. List returns keys and timestamps with inclusive RFC3339 date bounds, deterministic newest-first order, and limit/offset pagination.

## Related

- [Knowledge Markdown Directory](../../concepts/knowledge-markdown-directory.md) — shared TurboVault directory behavior.
- [Knowledge Data Store](../../concepts/knowledge-data-store.md) — JSON-object KV persistence.

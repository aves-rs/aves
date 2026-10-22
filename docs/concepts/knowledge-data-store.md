---
title: Knowledge Data Store
type: concept
updated: 2026-10-23
references:
  - crates/kea/src/data.rs
  - crates/kea/src/messages.rs
  - crates/kea/src/types.rs
  - crates/kea/src/domain.rs
  - crates/kea/src/actor.rs
  - crates/aves/src/utils/database.rs
  - crates/kea/src/migrations.rs
  - crates/kea/migrations/0003_knowledge_data.sql
---

# Knowledge Data Store

There is **no database plugin for Knowledge notes**. Notes always live in a TurboVault-managed Markdown directory. The remaining `config_dir/knowledge.db` is a narrow JSON-object KV store for Data RPCs; it does not index notes, tokenize content, encode embeddings, or offer semantic search.

The historical version-2 migration remains registered so both fresh and previously deployed `knowledge.db` files can advance. Version 3 removes every old Knowledge note, tag, keyword, chunk, job, and embedding table and creates `knowledge_data`. Old database notes are discarded; existing files in a configured Markdown directory are unaffected. `__knowledge_migrations` retains migration bookkeeping. After migrations, `DataStore::open` calls the shared `aves::utils::database::maintain_database` helper, which checkpoints the WAL and conditionally reclaims free space (VACUUM only when the helper's thresholds are met). `KnowledgeDomain::stop` dispatches an internal `StopRuntime` request to the actor, which calls `DataStore::maintain_db` — the same helper — before acknowledging shutdown. There is no central database registry; each database owner invokes the shared helper at its own lifecycle boundaries.

`upsertData` takes an exact key and JSON object, creates or fully replaces the value, and updates one UTC timestamp. `getData` and `deleteData` use exact case-sensitive keys and return a typed not-found error for missing keys. `listData` returns keys and write timestamps only, filtered by inclusive RFC3339 date bounds and ordered newest-first with limit/offset pagination.

See [Knowledge Domain Architecture](../architecture/kea/knowledge-domain.md) for request routing and directory ownership.

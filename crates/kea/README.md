# Kea

Embed a specialized knowledge domain for the **Aves async application runtime**.

Manage notes, JSON Data, and Canvas dashboards through typed domain operations.

Named for the kea parrot — curious, dexterous, and thorough.

Embed Kea as a library, not a standalone application.
Use workspace sources while `publish = false` and local dependencies remain.

## Installation

Add Kea to another workspace package:

```toml
[dependencies]
kea = { package = "aves-kea", path = "../kea" }
```

Add `aves` directly when calling its types from your embedding crate.
Compose the domain through Aves and adapt its errors and broadcasts.

## Quick Start

Create a note through your host's `knowledge.createKnowledge` RPC or MCP adapter:

```json
{
  "request": {
    "backend_id": "notes/meeting.md",
    "title": "Meeting notes",
    "content": "# Agenda\nReview Q3 results",
    "input_file": null,
    "content_type": "Markdown",
    "frontmatter": {},
    "tags": ["meeting"]
  }
}
```

These are operation arguments, not a standalone transport envelope.
Select the endpoint and authentication through your embedding application.

## Usage

### Domain registration

Import the domain marker into the composition root:

```rust
use kea::KnowledgeDomain;
```

Register `KnowledgeDomain` in the application's domain set.

- Construct Aves Config first and supply `ConfigDirectory` through `ConstructionInputs`.
- Let Kea register its typed `knowledge` section through Config's registration service.
- Convert `KnowledgeBroadcast` into the application's root broadcast.
- Support `ConfigBroadcast: TryFrom<RootBroadcast>` for live configuration updates.
- Construct Kea before Flow when using Knowledge-backed workspace mappings.

Kea publishes `FlowWorkspaceSourceService`; callers do not construct that publisher manually.
Compose Kea through an embedding application's Aves runtime.

### Embedding and transports

Kea is a library, not a process. It exposes typed bus operations through the Aves RPC mechanism. The embedding application decides how to surface them — rspc, Tauri commands, MCP tools, or direct bus dispatch. Kea has no CLI, no HTTP server, and no transport of its own.

Use an authenticated `BusRpcContext` for the exported typed dispatchers.
Use host-generated RPC/MCP adapters for operations without crate-root Rust dispatcher exports.

The composition root wires the RPC router and operation descriptors:

```rust
use kea::{KnowledgeDomain, KnowledgeBroadcast, knowledge_operation_descriptors_with_root_error, rpc_router};
```

### Notes

Store notes in one TurboVault-managed directory.
Use `{config_dir}/knowledge` by leaving the configured root unset.
The default directory is created when absent; explicitly configured directories must already exist.

List and tree both scan with TurboVault's `scan_vault_with_metadata`, which filters by `allowed_extensions`, size limits, and `excluded_paths` (plus the configured `trash_path`). Non-empty list queries and `searchKnowledge` rank through TurboVault's TF-IDF `SimilarityEngine`. This is term-based similarity, not embeddings. Filters apply after ranking, before the 100-row cap.

Create a note from inline content:

```rust
use aves::BusRpcContext;
use kea::{CreateKnowledgeInput, CreateKnowledgeRequest, KnowledgeError, dispatch_create_knowledge};

async fn create_note(context: &BusRpcContext) -> Result<(), KnowledgeError> {
  let request: CreateKnowledgeRequest = serde_json::from_value(serde_json::json!({
    "backend_id": null,
    "title": "Meeting notes",
    "content": "# Agenda\nReview Q3 results",
    "input_file": null,
    "content_type": "Markdown",
    "frontmatter": {},
    "tags": ["meeting"]
  }))?;
  dispatch_create_knowledge(context, CreateKnowledgeInput { request }).await?;
  Ok(())
}
```

### Import from file or directory

`CreateKnowledge` accepts either inline `content` or an `input_file` path. Agents SHOULD supply absolute paths; they are used as supplied. Relative paths resolve only beneath the fixed `{config_dir}/discuss` prefix, independent of any Discuss configuration. A missing path returns a typed error with the resolved path and absolute-path guidance.

A directory input scans eligible files using the active extension and exclusion rules.
File input takes precedence over inline content when both are present.
Neither the source path nor raw bytes become separate note metadata.
Do not treat host path resolution as filesystem confinement.

### Search

TF-IDF similarity search, term-based, not embeddings:

Pass these arguments to `knowledge.searchKnowledge` through your host adapter:

```json
{
  "request": {
    "query": "quarterly revenue",
    "filters": {},
    "limit": 10
  }
}
```

The index is cached and invalidated on vault changes. Root changes reopen the directory and rebuild the index.

### Tree

Lightweight navigation tree with Directory and File entries:

Call `knowledge.listKnowledgeTree` with empty operation arguments:

```json
{}
```

Tree entries are sorted by vault-relative path. Only directories that contain at least one admitted file appear.

### JSON-object Data

Data lives in `knowledge.db`, a separate SQLite database managed by Kea. Keys are exact and case-sensitive. Writes replace the entire object.

```rust
use aves::BusRpcContext;
use kea::{KnowledgeError, UpsertDataInput, UpsertDataRequest, dispatch_upsert_data};

async fn store_metrics(context: &BusRpcContext) -> Result<(), KnowledgeError> {
  let request = UpsertDataRequest {
    key: "metrics".into(),
    value: serde_json::Map::from_iter([("pnl".into(), serde_json::json!(1234))]),
  };
  dispatch_upsert_data(context, UpsertDataInput { request }).await?;
  Ok(())
}
```

`listData` returns keys and one last-write timestamp, with inclusive RFC3339 date bounds and offset pagination (200 rows maximum per call). No Data UI is provided.

### Canvas dashboards

Canvas dashboards are separate JSON files under `{config_dir}/dashboards`. They do not enter the Data table or note directory. CRUD operations carry a UUID dashboard identity, a title, and HTML content.

### Configuration

`KnowledgeConfig` has four fields, all live-reloaded through `ConfigChanged`:

| Field | Default | Notes |
|---|---|---|
| `root` | `None` → `{config_dir}/knowledge` | Explicit root must exist |
| `trash_path` | `.trash` | Single root-level component |
| `excluded_paths` | `[]` | Component names excluded from discovery |
| `allowed_extensions` | `md`, `markdown`, `txt`, `canvas` | Filters list, tree, import, and read |

A saved root change reopens the directory and watcher, swaps Flow's shared directory source, and broadcasts `KnowledgeRecordsChanged`. A failed reopen retains the active directory and logs the error. Invalid values are not rejected on save.

### Storage and migrations

| Artifact | Location | Owner |
|---|---|---|
| Notes | `{config_dir}/knowledge` (or configured root) | Kea via TurboVault |
| Data KV | `{config_dir}/knowledge.db` | Kea |
| Canvas dashboards | `{config_dir}/dashboards` | Kea |
| Migrations | `migrations/` (peer of `src/`) | Kea |

Migration 3 discards old note/index/embedding tables and creates `knowledge_data`. `__knowledge_migrations` is the migration ledger. Old database notes are intentionally discarded.

### Operations

| Operation | Kind | Description |
|---|---|---|
| `createKnowledge` | mutation | Create a note from content or input file |
| `listKnowledgeTree` | query | Lightweight vault navigation tree |
| `listKnowledge` | query | List notes with optional filters |
| `getKnowledge` | query | Get a note by relative path |
| `updateKnowledge` | mutation | Update an existing note |
| `deleteKnowledge` | mutation | Move note to trash |
| `searchKnowledge` | query | TF-IDF similarity search |
| `upsertData` | mutation | Replace or create a JSON object |
| `listData` | query | List keys with date bounds and pagination |
| `getData` | query | Read a JSON object by exact key |
| `deleteData` | mutation | Delete a JSON object by exact key |
| `createCanvasDashboard` | mutation | Create a Canvas dashboard |
| `getCanvasDashboard` | query | Get a dashboard by id |
| `updateCanvasDashboard` | mutation | Replace a dashboard |
| `deleteCanvasDashboard` | mutation | Delete a dashboard by id |
| `listCanvasDashboards` | query | List dashboard summaries |

Prefix operation names with `knowledge.` when using generated RPC dispatch.
Publish the operation descriptors through your host's selected transports.
Public operations still require normal Aves admission; public does not mean unauthenticated.
Do not confuse generated RPC operations with crate-root Rust re-exports.

### Broadcasts

| Broadcast | Description |
|---|---|
| `RecordsChanged` | Note visibility changed; clients should refresh |
| `CanvasDashboardCreated` | Dashboard created with `dashboard_id` |
| `CanvasDashboardUpdated` | Dashboard updated with `dashboard_id` |
| `CanvasDashboardDeleted` | Dashboard deleted with `dashboard_id` |

### Limitations

- No embedding model or semantic encoder. Search is TF-IDF term similarity.
- No database note plugin or backend selector. One TurboVault directory per runtime.
- No Data UI. Data operations are RPC/MCP-only.
- Relative `input_file` paths resolve only under `{config_dir}/discuss`, not Discuss settings.
- `~` is not expanded in directory paths.

### Development

Run Kea's tests:

```bash
cargo test --package aves-kea
```

Tests use Aves's `test-support` feature for bus-envelope fixtures. Primary tests live in `src/actor/tests.rs`; `data.rs` and `documents.rs` have co-located unit tests.

## Contributing

See [AGENTS.md](AGENTS.md) for local conventions and the [Knowledge Domain Architecture](../../docs/architecture/kea/knowledge-domain.md) for integration details.

## License

Licensed under the [Apache License, Version 2.0](../../LICENSE).

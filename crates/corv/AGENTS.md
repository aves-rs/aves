# Corv

Corv owns Discuss on the Aves runtime. Named for Corvidae (ravens/crows), the planning, research, and discussion family. Poseidon composes it; Zeus and Hades consume its stream and RPC surface. Corv depends on Aves, never Poseidon. Corv owns application-neutral asset schemas and requires an injected `AssetLibrary`; Poseidon owns the trading content in `content/` and builds its catalog in `src/utils/agents.rs`. No built-in application content, global catalog, or retired Phanes dependency. Aves must not depend on or import Corv; do not add a Aves compatibility re-export.

## Ownership

- **Corv Discuss** (`crates/corv/src/`): actor, workers, persistence, typed errors, stream sender, Provider/Team/session lifecycle, MCP transport, plugin materialization.
- **Poseidon** (`pkg/poseidon/src/app.rs`): composition root; installs Discuss on the Aves runtime, supplies mandatory `PoseidonHarnessPlug.discuss_stream_tx` through `PoseidonStartOptions.harness`, and projects `PoseidonBroadcast`.
- **Zeus** (`pkg/zeus/src/bridges/discuss/`): `Channel<AcpBridgeEvent>` wrapper, stream receiver, routing by `sluiceSessionId`.
- **Hades** (`pkg/hades/src/app/acp/`): ACP SDK interpretation, Zustand store keyed by `sluiceSessionId`, chat rail.

## File owner map

| File | Owns |
|---|---|
| `actor.rs` | Single Actor state, construction, request/event dispatch, stop order. |
| `agent_nodes.rs` | Flow AgentNode registration, start/tick/cancel actor methods, terminal completion handling, workspace hooks. |
| `provider.rs` | Actor-associated Provider lifecycle methods and DTOs (add/remove, runtime state, config refresh, transient validation). |
| `teams.rs` | Actor-associated Team catalog/install/upgrade methods and DTOs (boot upgrade, retention scopes, member overrides). |
| `sessions.rs` | Actor-associated session/turn/inspector operations (new/load/remove/rename, turn send/cancel, inspector reserve/start/dispose). |
| `session.rs` | ACP SDK connection and router: shared/isolated dispatch, SDK session new/load attachment, `SessionCallbackRouter`, callback routing, worker JWT, initialize/load requests. |
| `worker.rs` | Single connected ACP worker event loop: `run_isolated` startup, `run_connected_session` command/event handlers, permission/elicitation/config/inspector branches. **File-length exception:** soft target 1,200 LOC; hard cap **1,750 LOC** (raised from 1,500). Current ~1,341 LOC. This exception applies ONLY to this file. |
| `agents.rs` | Agent message and permission projection: turn buffer, transcript position allocation, permission/elicitation request preparation, responder resolution, message-to-user dispatch. |
| `elicitation.rs` | Restricted-form DTO projection (`DiscussRestrictedFormField*`, `DiscussElicitationClassification`, `DiscussElicitationAnswer`) and validation (`validate_elicitation_content`, `classify_elicitation_schema`, `content_to_sdk_answer`). Reuses SDK `CreateElicitationRequest::scope` and typed `ElicitationContentValue` conversion; retains Sluice required-field/enum/pattern/numeric-bounds checks. |
| `database.rs` | Ordered persistence queue: `PersistenceJob` variants, `enqueue_persistence_job`, `spawn_persistence_queue`, drain task, anchor merge, retention-scope helpers, cleanup warnings. |
| `storage.rs` | Typed SQL storage: `include_str!` constants, Turso `Connection` helpers, row types, CRUD functions, decoders. **Subagent upsert + retention prune MUST execute inside one Turso transaction** (`upsert_and_prune_discuss_subagent` takes `&mut Connection`, begins transaction, executes upsert, executes prune, commits only after both succeed). |
| `messages.rs` | Typed request/response/broadcast contracts: `DiscussRequest`, `DiscussRequestPayload` (bus macro), `DiscussResponse`, `DiscussBroadcast`. |
| `library.rs` | Neutral Agent/Skill/Command/Team schemas, destination-safe IDs, and immutable application-supplied `AssetLibrary`. No application prompts or concrete catalog. |
| `migration.rs` | Existing Discuss SQL migration registration; SQL files live in `../migrations/`, a peer of `src/`. Use plain `mod migration;`, never a `#[path]` import. |
| `types.rs` | Authoritative Discuss error enum (`DiscussError`, thiserror), DTOs (identities, stream events, provider/runtime state, session info, config, team catalog, ACP payloads), AgentNode limits/results, workspace copy source port and actor inbound port bundle. |
| `domain.rs` | Aves registration and start/stop boundary: `DiscussDomain`, `DiscussStreamingService`, `DiscussAgentNodeLifecycleService`, `DiscussKernelServices`, routing error, lifecycle callbacks, AgentNode registration supply. |
| `handle.rs` | Message router: `new` constructor, service unpacking, actor construction and spawn, port creation/publication, generated router wiring. |
| `boot.rs` | Boot boundaries: `DiscussBootOptions`, `DiscussConfigBridge`, `DiscussConfigSnapshot`, `DiscussMcpDomain`, `DiscussMcpService`. |
| `config.rs` | Config preference selection: merged config preferences, config options/settings parsing from JSON. |
| `constants.rs` | Buffer constants: `DEFAULT_BUFFER_SIZE`. |
| `supervisor.rs` | Provider supervisor state: `ProviderSupervisor`, `ProviderSupervisorResources`, `CallbackRouter`, `CallbackRouteRegistry`. |
| `mcp.rs` | Tool collation, schemas, shared `ApplicationMcpService`, image presentation, native `discuss_mcp_domains` adapter. |
| `mcp_direct.rs` | Typed ACP/RMCP handlers: `McpAcpShim`, `McpAcpTransport`. |
| `mcp_http.rs` | HTTP MCP serving (`run_mcp`), session manager, strict bearer middleware and admission tests. Poseidon supplies the factory to the bus through `TransportBindings::new`. |
| `mcp_relay.rs` | Aves bus-owned loopback listeners: `McpHttpLease`. |
| `plugins/` | Provider-local materialization: `plugins/mod.rs` (trait + shared helpers), `plugins/claude/`, `plugins/codex/`, `plugins/grok/`, `plugins/opencode/`. |
| `actor/tests.rs` | Relocated primary-file tests (unchanged assertions/fixtures). |
| `queries/` | Existing SQL assets. |
| `*.sql` (root) | Root-level SQL assets (`todo_snapshot_update.sql`, `opencode_child_session_lookup.sql`). |

## Terminology

- **Provider** = ACP runtime/harness (OpenCode, Claude, Codex, Grok, custom).
- **Agent** = Phanes persona.
- **Team** = one Phanes leader + ordered followers + ordered skills.

## Key contracts

- **One Actor, one ordered persistence queue, one connected ACP worker per session.**
- **`sluiceSessionId`** is the only Sluice routing key. Five `DiscussStreamEvent` variants: `SessionFailed`, `AgentMessage`, `UtilityUpdate`, `PermissionRequested`, `ElicitationRequested`.
- **`PoseidonHarnessPlug.discuss_stream_tx`** is mandatory (via `PoseidonStartOptions.harness`). Discuss is constructed for every runtime.
- **Privileged bus-only:** session control, inspector, provider-config lifecycle. **Public:** `StartServer` / `StopServer` / `GetServerStatus` / `StartProvider` / `StopProvider`, Provider/Team mutations, reads.
- **Provider runtimes:** Actor-owned `provider_supervisors` (memory only). Shared: `opencode`, `codex-acp`, `claude-acp`. Isolated: `grok-build` and custom.
- **Worker tokens:** Chat and transient workers use delegated JWT (`sub = "agent"`, 30-day expiration). Flow AgentNode reuses Flow-issued token. No Job worker.
- **Discuss Jobs are retired.** Durable automation is Aves Flow.
- **AgentNode ownership:** Discuss registers AgentNode during construction through the shared `NodeRegistrationSupply`. Start/tick/cancel are privileged requests on `DiscussAgentNodeLifecycleService`; the actor owns live completion receivers and clears them on stop. Tasks owns ScriptNode only; Discuss must not import Tasks.

## Related

- `crates/aves/AGENTS.md` — Aves runtime and bus lifecycle
- `pkg/poseidon/AGENTS.md` — Poseidon composition rules
- [Discuss Domain Architecture](../../docs/architecture/corv/discuss-domain.md)
- [MCP-over-ACP Tooling](../../docs/concepts/mcp-over-acp-tooling.md)

## Extraction and verification

Preserve `Discuss*` Rust names, `discuss.*` RPC/config/error identities, `discuss.db`, `__discuss_migrations`, migration versions/SQL, workspace paths, and existing tests. The crate rename is not a domain or storage rename. Keep implementation modules private and re-export only concrete consumer contracts from `src/lib.rs`; do not publish unused DTO/helper/privileged-service aliases. `DiscussBootOptions.library` is required. Listing, upgrades, retention, and provider renderers use that catalog; never consult an ambient application registry.

Run `cargo test --package aves-corv` from the repository root. Actor tests remain in `src/actor/tests.rs`; other tests stay co-located. Tests use Aves's `test-support` feature; do not widen production APIs for relocated tests.

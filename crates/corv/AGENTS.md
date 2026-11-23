# Corv

Corv owns two domains on the Aves runtime: Discuss (`src/discuss/`) and Decide (`src/decide/`). `src/lib.rs` is the only crate-root export boundary; both domain modules are private. Named for Corvidae (ravens/crows), the planning, research, and discussion family. Poseidon composes it; Zeus and Hades consume its stream and RPC surface. Corv depends on Aves, never Poseidon. Corv owns application-neutral asset schemas and requires an injected `AssetLibrary`; Poseidon owns the trading content in `content/` and builds its catalog in `src/utils/agents.rs`. No built-in application content, global catalog, or retired Phanes dependency. Aves must not depend on or import Corv; do not add a Aves compatibility re-export.

## Ownership

- **Corv Discuss** (`crates/corv/src/discuss/`): actor, workers, persistence, typed errors, stream sender, Provider/Team/session lifecycle, MCP transport, plugin materialization.
- **Poseidon** (`pkg/poseidon/src/app.rs`): composition root; installs Discuss on the Aves runtime, supplies mandatory `PoseidonHarnessPlug.discuss_stream_tx` through `PoseidonStartOptions.harness`, and projects `PoseidonBroadcast`.
- **Zeus** (`pkg/zeus/src/bridges/discuss/`): `Channel<AcpBridgeEvent>` wrapper, stream receiver, routing by `discussSessionId`.
- **Hades** (`pkg/hades/src/app/acp/`): ACP SDK interpretation, Zustand store keyed by `discussSessionId`, chat rail.

## File owner map (Discuss)

Paths below are relative to `src/discuss/`. Discuss SQL migrations stay at crate-root `migrations/`; `discuss/migration.rs` includes them via `../../migrations`.

| File | Owns |
|---|---|
| `actor.rs` | Single Actor state, construction, request/event dispatch, stop order. |
| `agent_nodes.rs` | Flow AgentNode registration, start/tick/cancel actor methods, terminal completion handling, workspace hooks. |
| `provider.rs` | Actor-associated Provider lifecycle methods and DTOs (add/remove, runtime state, config refresh, transient validation). |
| `teams.rs` | Actor-associated Team catalog/install/upgrade methods and DTOs (boot upgrade, retention scopes, member overrides). |
| `sessions.rs` | Actor-associated session lifecycle methods (new/load/remove/rename, spawn, launch preparation, session info projection). |
| `session_turns.rs` | Actor-associated turn, permission, elicitation and inspector request handlers (`handle_send_user_turn`, `handle_cancel_turn`, inspector reserve/start/dispose, elicitation persistence). |
| `session.rs` | ACP SDK connection and router: shared/isolated dispatch, SDK session new/load attachment, `SessionCallbackRouter`, callback routing, worker JWT, initialize/load requests. |
| `worker.rs` | Single connected ACP worker event loop: `run_isolated` startup, `run_connected_session` command/event handlers, permission/elicitation/config/inspector branches. **File-length exception:** soft target 1,200 LOC; hard cap **1,750 LOC** (raised from 1,500). Current ~1,671 LOC. This exception applies ONLY to this file. |
| `agents.rs` | Agent message and permission projection: turn buffer, transcript position allocation, permission/elicitation request preparation, responder resolution, message-to-user dispatch. |
| `elicitation.rs` | Restricted-form DTO projection (`DiscussRestrictedFormField*`, `DiscussElicitationClassification`, `DiscussElicitationAnswer`) and validation (`validate_elicitation_content`, `classify_elicitation_schema`, `content_to_sdk_answer`). Reuses SDK `CreateElicitationRequest::scope` and typed `ElicitationContentValue` conversion; retains required-field/enum/pattern/numeric-bounds checks. |
| `database.rs` | Ordered persistence queue: `PersistenceJob` variants, `enqueue_persistence_job`, `spawn_persistence_queue`, drain task, anchor merge, retention-scope helpers, cleanup warnings. |
| `storage.rs` | Typed SQL storage: `include_str!` constants, Turso `Connection` helpers, row types, CRUD functions, decoders. **Subagent upsert + retention prune MUST execute inside one Turso transaction** (`upsert_and_prune_discuss_subagent` takes `&mut Connection`, begins transaction, executes upsert, executes prune, commits only after both succeed). |
| `messages.rs` | Typed request/response/broadcast contracts: `DiscussRequest`, `DiscussRequestPayload` (bus macro), `DiscussResponse`, `DiscussBroadcast`. |
| `library.rs` | Neutral Agent/Skill/Command/Team schemas, destination-safe IDs, immutable application-supplied `AssetLibrary`, custom asset definitions, `ResolvedAgent`/`ResolvedSkill`/`ResolvedTeam`, `AgentSelectionView`. No application prompts or concrete catalog. |
| `migration.rs` | Existing Discuss SQL migration registration; SQL files live in `../migrations/`, a peer of `src/`. Use plain `mod migration;`, never a `#[path]` import. |
| `types.rs` | Authoritative Discuss error enum (`DiscussError`, thiserror), DTOs (identities, stream events, provider/runtime state, session info, config, team catalog, ACP payloads), AgentNode limits/results, workspace copy source port and actor inbound port bundle. |
| `domain.rs` | Aves registration and start/stop boundary: `DiscussDomain`, `DiscussStreamingService`, `DiscussAgentNodeLifecycleService`, `DiscussKernelServices`, routing error, lifecycle callbacks, AgentNode registration supply. |
| `handle.rs` | Message router: `new` constructor, service unpacking, actor construction and spawn, port creation/publication, generated router wiring. |
| `boot.rs` | Boot boundaries: `DiscussBootOptions`, `DiscussConfigBridge`, `DiscussConfigSnapshot`, `DiscussMcpDomain`, `DiscussMcpService`. |
| `config.rs` | Config preference selection: merged config preferences, config options/settings parsing from JSON. |
| `supervisor.rs` | Provider supervisor state: `ProviderSupervisor`, `ProviderSupervisorResources`, `CallbackRouter`, `CallbackRouteRegistry`. |
| `mcp.rs` | Tool collation, schemas, shared `ApplicationMcpService`, image presentation, native `discuss_mcp_domains` adapter, custom MCP descriptor and assignment CRUD, launch-time composition. |
| `mcp_direct.rs` | Typed ACP/RMCP handlers: `McpAcpShim`, `McpAcpTransport`. |
| `mcp_http.rs` | HTTP MCP serving (`run_mcp`), session manager, strict bearer middleware and admission tests. Poseidon supplies the factory to the bus through `TransportBindings::new`. |
| `mcp_relay.rs` | Aves bus-owned loopback listeners: `McpHttpLease`. |
| `plugins/` | Provider-local materialization: `plugins/mod.rs` (trait + shared helpers), `plugins/launch.rs`, `plugins/files.rs`, `plugins/claude/`, `plugins/codex/`, `plugins/grok/`, `plugins/opencode/`, `plugins/antigravity/`. |
| `usage.rs` | Worker-local turn accounting: `UsageTurnAccumulator`, standard ACP usage capture, evidence assembly, terminal-only ledger writes. |
| `pricing.rs` | models.dev catalog lifecycle, exact-decimal price index, per-turn cost calculation. |
| `allowance.rs` | quota-core registry lifecycle, windowed allowance observation writer, display-provider mapping. |
| `reports.rs` | Accounting report aggregation and the token/spend/allowance query handlers. |
| `assets.rs` | Custom asset rows, `discuss_cache` storage, atomic swap/restore, install records, instruction rows, import lifecycle, merged-catalog resolution. |
| `asset_sources.rs` | Bounded SSRF-safe URL and GitHub file/directory fetching. |
| `identity.rs` | Session identity bindings: `(issuer, subject)` to session, provider, agent, node execution id. |
| `actor/tests.rs` | Relocated primary-file tests (unchanged assertions/fixtures). |
| `queries/` | Existing SQL assets, including `todo_snapshot_update.sql` and the accounting, custom-asset, identity-binding and custom-MCP assets. Buffer sizing uses `aves::utils::buffers::DEFAULT_BUFFER_SIZE`. |

## Terminology

- **Provider** = ACP runtime/harness (OpenCode, Claude, Codex, Grok, custom).
- **Agent** = Phanes persona.
- **Team** = one Phanes leader + ordered followers + ordered skills.

## Key contracts

- **One Actor, one ordered persistence queue, one connected ACP worker per session.**
- **`discussSessionId`** is the only corv routing key. Five `DiscussStreamEvent` variants: `SessionFailed`, `AgentMessage`, `UtilityUpdate`, `PermissionRequested`, `ElicitationRequested`.
- **`PoseidonHarnessPlug.discuss_stream_tx`** is mandatory (via `PoseidonStartOptions.harness`). Discuss is constructed for every runtime.
- **Privileged bus-only:** session control, inspector, provider-config lifecycle, URL imports, custom MCP mutations, identity resolution. **Public:** `StartServer` / `StopServer` / `GetServerStatus` / `StartProvider` / `StopProvider`, Provider/Team mutations, custom content CRUD, reads and reports.
- **Provider runtimes:** Actor-owned `provider_supervisors` (memory only). Shared: `opencode`, `codex-acp`, `claude-acp`. Isolated: `grok-build` and custom.
- **Worker tokens:** Chat and transient workers use delegated JWT (`sub = "agent"`, 30-day expiration). Flow AgentNode reuses Flow-issued token. No Job worker.
- **Discuss Jobs are retired.** Durable automation is Aves Flow.
- **AgentNode ownership:** Discuss registers AgentNode during construction through the shared `NodeRegistrationSupply`. Start/tick/cancel are privileged requests on `DiscussAgentNodeLifecycleService`; the actor owns live completion receivers and clears them on stop. Tasks owns ScriptNode only; Discuss must not import Tasks.
- **Provider launch policy.** Unsupported native permission controls or distinct unsupported plugin references do not by themselves refuse a launch: use the native best-effort outcome and typed `LaunchLimitation` facts. Provider-specific malformed/duplicate input validation, workspace IO, ACP transport, storage and typed CLI tooling failures remain errors.
- **Plugin boundary.** Provider-specific launch behaviour lives in `plugins/<provider>/`; the shared `plugins/launch.rs` holds the provider-neutral helpers (`intersect_rules`, `validate_supplied_inputs`, `merge_metadata`) and the `LaunchContext` / `LaunchPreparation` / `LaunchSelections` types.
- **Duplicate plugin references.** `validate_supplied_inputs` is called by the Claude and Codex preparations only. Do not assume a universal rejection of duplicate `plugin_references`.
- **Session activity.** The worker's turn flag is the only activity state. `DiscussBroadcast::SessionActivity` publishes each transition; `DiscussSessionInfo.active_turn` is the snapshot. The actor accepts an activity update only when the worker entry exists **and** its `JoinHandle::is_finished()` is false — registry membership alone is insufficient. A worker failure publishes no idle event.
- **Accounting.** Ledger rows are written only at a turn's terminal boundary. The turn ordinal comes from the ledger itself at session spawn. `pricing.rs` fetches models.dev itself; the `models-dev-rs` crate is **not** used and must not be reintroduced (its `serde_json` `arbitrary_precision` feature unified workspace-wide and broke Aves Config TOML writes). `allowance.rs` uses `quota-core` in-process only: no `subc`, no daemon, `auto_use_resets` disarmed.
- **Turn usage sidecar.** `DiscussStreamEvent::AgentMessage.turn_usage` is optional and carries real provider counts only. A dispatched turn always has its ordinal, even with missing counts (`usage_is_incomplete`). `UsageTurnAccumulator::finish` finalizes once. Late evidence updates only the retained turn, and only when turn-correlatable, through its exact ordinal and bound assistant row. Elapsed time exists only for completed turns; Corv records no TTFT.
- **Subagent reporting.** Subagent state comes from what the provider reports on the session's own connection: ACP `subagent_update`, OpenCode `opencode/session/child_update`, Grok `x.ai/session_notification`, then tool-call fallbacks. Corv never queries a provider's separate service or scans processes. A spawn's completion is not its child's completion. Subagent states never change `active_turn`. Details: [Subagent Reporting](../../docs/concepts/subagent-reporting.md).
- **Primary agent option.** `DiscussProviderConfig.agent_config_option_id` comes only from the plugin's private `PrimaryAgentSelection::ConfigOption`. `ConfigOptionsOutput` reuses it; no lookup operation exists.
- **Custom content.** Built-in and custom assets resolve through one `AgentSelectionView`. Custom asset ids must not collide with built-in ids. Appended instructions are keyed by the physical installed path, so two Providers sharing one file share one instruction row.
- **Accepted limits.** Settled behaviour in accounting, custom content and Teams is recorded in [Discuss Accepted Limits](../../docs/concepts/discuss-accepted-limits.md). Do not re-raise or "fix" it.

## Scope-local rules

- **Operation placement.** A Discuss operation that performs an outbound fetch, launches an arbitrary process, controls a live session, or mutates actor-owned Provider configuration is `#[privileged]` and bus-only. Local workspace CRUD and reads stay public `#[operation]`s. Every new operation needs one or the other; the bus-context gate is the only safety boundary.
- **No test-only production wrappers.** Do not add a production helper that only tests call. `install_agent`, `upgrade_installed_agents` and `agent_versions_match` were removed for this reason; call the production path the test needs instead.
- **ACP import path.** Import ACP schema types from `agent_client_protocol::v1::schema::…`. The crate-root `agent_client_protocol::schema::…` module is an unversioned alias — do not mix the two paths for the same type.
- **Clippy.** `aves::domain!` expands into four sudo-service receivers, which trips `clippy::too_many_arguments`. `discuss/domain.rs` carries a module-level `#![allow(clippy::too_many_arguments)]` for that reason; do not add the lint to unrelated code.

## Related

- `crates/aves/AGENTS.md` — Aves runtime and bus lifecycle
- `pkg/poseidon/AGENTS.md` — Poseidon composition rules
- [Discuss Domain Architecture](../../docs/architecture/corv/discuss-domain.md)
- [Harness Configuration](../../docs/architecture/corv/harness-configuration.md)
- [Decide Domain Architecture](../../docs/architecture/corv/decide-domain.md)
- [MCP-over-ACP Tooling](../../docs/concepts/mcp-over-acp-tooling.md)
- [Discuss Accounting Ledgers](../../docs/concepts/discuss-accounting-ledgers.md)
- [Discuss Accounting Reports](../../docs/concepts/discuss-accounting-reports.md)
- [Discuss Custom Content](../../docs/concepts/discuss-custom-content.md)
- [Discuss Custom MCP](../../docs/concepts/discuss-custom-mcp.md)
- [Discuss Accepted Limits](../../docs/concepts/discuss-accepted-limits.md)

## Decide (`src/decide/`)

Decide asks one bounded typed question (`bool`, `choice`, `score`; no `box`) through provider plugins. It is not an ACP session. See [Decide Domain Architecture](../../docs/architecture/corv/decide-domain.md) and [Using the Decide Domain](../../docs/guides/using-the-decide-domain.md).

| File | Owns |
|---|---|
| `types.rs` | `DecideError`/RPC codes, content, questions, answers, `DecideCredential`, endpoint validation, node configs. |
| `messages.rs` | `DecideRequestPayload`, `DecideResponse`, uninhabited `DecideBroadcast`. |
| `domain.rs` | `aves::domain!`, `DecideRuntimeService`, `DecideNodeLifecycleService`, Tyto-style start/stop `lifecycle()` helper. |
| `handle.rs` | Bus registration, service publication, `Actor::new` + `run_actor`, node registration push. |
| `actor.rs` | Lifecycle, credential custody, `handle_question` (spawned per ask), shared `QuestionContext::prepare`, retry, node completions, node-kind constants. |
| `nodes.rs` | `DecideBoolNode`, `DecideChoiceNode`, `DecideScoreNode` registrations; context-free `start_with`/`tick_with`/`cancel_with`. |
| `plugins/` | `DecidePlugin`, `for_provider`, shared `post_json`, `systemone` envelope/decoder, `jev`, `ollama`. |

- **Provider-free core (hard rule):** every Decide file outside `plugins/` contains no provider names, endpoints, model defaults, wire schemas, authentication, or provider branches. `bool` ↔ wire `noul` mapping lives only in plugins. Unknown providers fail with `UnknownProvider`; no fallback.
- **Surface:** public `startServer`, `stopServer`, `askBool`, `askChoice`, `askScore`; privileged `SupplyCredential`/`RemoveCredential` and `StartNode`/`TickNode`/`CancelNode`. No broadcasts.
- **Lifecycle (Tyto):** starts not running; `StopServer` cancels in-flight work and clears completions but keeps credentials; credentials clear only on channel close.
- **Credentials:** memory-only, supplied by the application; each key is bound to one HTTPS (or numeric-loopback HTTP) endpoint. A keyed override must canonically equal the binding.
- **No persistence:** Decide has no database, migrations, replay or recovery. Flow owns durability.
- **Tests:** decoder tests are pure unit tests on JSON; actor tests inject a mock `DecidePlugin`; never call live providers.

## Consumer reexports

The following symbols are reexported from the Corv crate root for embedding applications:
- `DiscussSettings` (from `boot.rs`)
- `AcpProvider`, `ProviderRuntimeStatus`, `ProviderRuntimeFailure`, `CleanupWarning` (from `types.rs`)
- Decide: `DecideDomain`, `DecideBroadcast`, `DecideError`, `DecideCredential`, `DecideContent`, `DecideRuntimeService`, `SupplyCredentialInput`, `RemoveCredentialInput`, `SuccessOutput as DecideSuccessOutput`, `AskBoolInput`, `AskChoiceInput`, `AskScoreInput`, `BoolOutput`, `ChoiceOutput`, `ScoreOutput`, `dispatch_ask_bool`, `dispatch_ask_choice`, `dispatch_ask_score`, the question/answer types, `decide_operation_descriptors_with_root_error`, `rpc_router as decide_rpc_router`. Decide's `StartServerInput`/`StopServerInput`/`dispatch_start_server`/`dispatch_stop_server` are intentionally not exported (they collide with Discuss exports).
- Provider operation DTOs and dispatchers: `ListProvidersInput`, `ProvidersListOutput`, `dispatch_list_providers`, `AddProviderInput`, `SuccessOutput`, `dispatch_add_provider`, `RemoveProviderInput`, `ProviderRemoveSuccessOutput`, `dispatch_remove_provider` (generated from `messages.rs` declarations)
- Custom content: `GetCustomAgentOutput`, `GetCustomSkillOutput`, `ImportCustomAgentFromUrlInput`, `ImportCustomSkillFromUrlInput`, `InstalledAssetKind`
- Custom MCP: `CustomMcpEntry`, `CustomMcpScope`, `SensitiveMcpDescriptor`, `ListCustomMcpsInput`, `ListCustomMcpsOutput`, `CreateCustomMcpInput`, `UpdateCustomMcpInput`, `DeleteCustomMcpInput`, `AssignMcpInput`, `UnassignMcpInput`, `dispatch_list_custom_mcps`
- Managed Provider configuration and identity: `GetProviderManagedConfigInput`, `ProviderManagedConfigOutput`, `dispatch_get_provider_managed_config`, `ResolveSessionIdentityInput`, `SessionIdentityResolvedOutput`, `DiscussIdentityService`

Implementation modules remain private; only consumer contracts are reexported. Provider-config lifecycle operations remain privileged as declared.

## Extraction and verification

Preserve `Discuss*` Rust names, `discuss.*` RPC/config/error identities, `discuss.db`, `__discuss_migrations`, migration versions/SQL, workspace paths, and existing tests. The crate rename is not a domain or storage rename. Keep implementation modules private and re-export only concrete consumer contracts from `src/lib.rs`; do not publish unused DTO/helper/privileged-service aliases. `DiscussBootOptions.library` is required. Listing, upgrades, retention, and provider renderers use that catalog; never consult an ambient application registry.

Run `cargo test --package aves-corv` from the repository root. Actor tests remain in `src/discuss/actor/tests.rs`; other tests stay co-located. Tests use Aves's `test-support` feature; do not widen production APIs for relocated tests.

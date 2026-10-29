---
title: Discuss Domain Architecture
type: architecture
updated: 2026-10-29
references:
  - crates/corv/AGENTS.md
  - crates/corv/src/lib.rs
  - crates/corv/src/actor.rs
  - crates/corv/src/agent_nodes.rs
  - crates/corv/src/session.rs
  - crates/corv/src/worker.rs
  - crates/corv/src/supervisor.rs
  - crates/corv/src/messages.rs
  - crates/corv/src/domain.rs
  - crates/corv/src/provider.rs
  - crates/corv/src/types.rs
  - crates/corv/src/handle.rs
  - crates/corv/src/teams.rs
  - crates/corv/src/sessions.rs
  - crates/corv/src/database.rs
  - crates/corv/src/storage.rs
  - crates/corv/src/elicitation.rs
  - crates/corv/src/agents.rs
  - crates/corv/src/plugins/mod.rs
  - crates/corv/src/mcp_direct.rs
  - crates/corv/src/mcp_relay.rs
  - crates/corv/src/mcp.rs
  - crates/corv/src/mcp_http.rs
  - crates/corv/src/migration.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/flow/actor.rs
  - crates/corv/AGENTS.md
---

# Discuss Domain Architecture

Corv owns Discuss: Provider config, Team install linkage, chat session lifecycle, the per-session persistence queue, and the bounded `DiscussStreamEvent` sender. An embedding application composes Discuss on the Aves runtime. Corv depends on Aves, not its embedding application; Aves does not import Corv. `Discuss*` names and `discuss.*` contracts remain stable.

**Terminology:** Provider = ACP harness. Agent = Phanes persona. Team = leader + followers + skills.

## Ownership

- **Corv Discuss** (`crates/corv/src/`): actor, workers, `discuss.agents_workspace`, `discuss_sessions` / `discuss_chat_turns`, typed `DiscussError`, stream sender, Provider/Team/session lifecycle, MCP transport, plugin materialization. See [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md) for the file owner map.
- **Embedding application:** supplies the required stream sender and application-neutral asset library during composition.

## Startup

### Asset library

`crates/corv/src/library.rs` owns neutral asset definitions, ID/path validation, and `AssetLibrary`. The required `DiscussBootOptions.library` supplies immutable host definitions; Corv contains no built-in application catalog. The actor uses the same supplied catalog for listing, upgrades, retention, and provider materialization. Provider plugins receive the catalog explicitly rather than querying ambient globals.

`Runtime::start` runs every domain start callback concurrently (`JoinSet`). Discuss's callback dispatches `StartServer` (auto-start Providers, reconnect persisted sessions). There is no History-before-Discuss order.

## Workspace vs AgentNode

Discuss constructs the AgentNode registration in `agent_nodes.rs`, pushing it onto `NodeRegistrationSupply` through its domain construction services. `DiscussAgentNodeLifecycleService` routes privileged start/tick/cancel requests to the actor, which owns process-local terminal receivers. Tasks owns ScriptNode only; Discuss has no Tasks dependency.

Chat `cwd` is `discuss.agents_workspace`. Flow AgentNode passes `{data_root}/executions/{id}` as `SpawnSessionOptions.session_cwd`. CWD is not confinement. Limit breach: `session/cancel` then `DiscussError::AgentNodeLimitReached`. See [Flow Execution Workspace](../../concepts/flow-execution-workspace.md).

For new AgentNode runs, Flow asks Discuss's actor-serialized `WorkspaceCopySourcePort` for the effective workspace root and sources of every graph Provider. Discuss owns the Provider lookup; Flow copies their union before checkpoint 0. An absent Provider source list uses its built-in default; an explicit empty list still includes `.agents`. Ordinary chat CWD and restored executions are unchanged.

## Identity and stream

`sluiceSessionId` is the only routing key. Five `DiscussStreamEvent` variants: `SessionFailed`, `AgentMessage`, `UtilityUpdate`, `PermissionRequested`, `ElicitationRequested`.

## Control and broadcasts

Privileged bus-only: session control, inspector, provider-config lifecycle. Public: `StartServer` / `StopServer` / `GetServerStatus` / `StartProvider` / `StopProvider`, Provider/Team mutations, reads. Discuss is in MCP publications.

Broadcasts: `ServerStarted` / `ServerStopped`, `SessionReady`, `SessionWarning`, `ProviderRuntimeChanged`. Do not add Job catalog/run broadcasts.

`StopServer` gates new work, cancels pending permission/elicitation, aborts live session workers, and keeps persisted sessions.

## Provider runtimes

Actor-owned `provider_supervisors` (memory only). Shared: `opencode`, `codex-acp`, `claude-acp`. Isolated: `grok-build` and custom. Shared connections use `SessionCallbackRouter` (`session.rs`). Missing queue returns the intact `CallbackEvent`; `fail_closed` answers Permission with internal error and Elicitation with Cancel.

## Worker tokens

- Chat and transient workers: delegated JWT (`sub = "agent"`, `SESSION_WORKER_JWT_EXPIRATION` = 30 days) via `issue_worker_jwt`. Discuss stores the JTI and revokes on terminal paths (`revoke_worker_jti`).
- Flow AgentNode: reuses the Flow-issued `flow-node:{node_execution_id}` token (`node_token`). Discuss does not mint a worker JTI; Flow revokes via `revoke_execution_tokens`.
- There is no Job worker.

## Persistence

SQL lives in `crates/corv/migrations/`, a peer of `src/`; `src/migration.rs` registers it through plain `mod migration;`. Existing `discuss.db`, `__discuss_migrations`, migration versions, and SQL remain unchanged.

`database.rs` owns the ordered persistence queue; `storage.rs` owns typed SQL storage (include constants, connection helpers, row types, CRUD, decoders). **Subagent upsert + retention prune MUST execute inside one Turso transaction** (`upsert_and_prune_discuss_subagent` takes `&mut Connection`, begins transaction, executes upsert, executes prune, commits only after both succeed).

## Legacy Job tables

Migrations `0037_discuss_jobs` (and follow-ons) still create `discuss_jobs` / `discuss_job_runs`. No readers or writers. Automation is Aves Flow.

## Consumer reexport surface

The following symbols are reexported from the Corv crate root for embedding applications (verified against `crates/corv/src/lib.rs`):

- `DiscussSettings` (from `boot.rs`)
- `AcpProvider`, `ProviderRuntimeStatus`, `ProviderRuntimeFailure`, `CleanupWarning` (from `types.rs`)
- Provider operation DTOs and dispatchers: `ListProvidersInput`, `ProvidersListOutput`, `dispatch_list_providers`, `AddProviderInput`, `SuccessOutput`, `dispatch_add_provider`, `RemoveProviderInput`, `ProviderRemoveSuccessOutput`, `dispatch_remove_provider` (generated from `messages.rs` declarations)

Implementation modules remain private; only consumer contracts are reexported. Provider-config lifecycle operations remain privileged as declared.

## Related

- [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md) — file owner map and local conventions
- `crates/aves/AGENTS.md` — Aves runtime and bus lifecycle
- [Aves Flow Domain Architecture](../aves/flow-domain.md)
- [MCP-over-ACP Tooling](../../concepts/mcp-over-acp-tooling.md)

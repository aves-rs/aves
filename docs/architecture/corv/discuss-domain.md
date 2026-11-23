---
title: Discuss Domain Architecture
type: architecture
updated: 2026-11-21
references:
  - crates/corv/AGENTS.md
  - crates/corv/src/lib.rs
  - crates/corv/src/discuss/actor.rs
  - crates/corv/src/discuss/agent_nodes.rs
  - crates/corv/src/discuss/session.rs
  - crates/corv/src/discuss/worker.rs
  - crates/corv/src/discuss/supervisor.rs
  - crates/corv/src/discuss/messages.rs
  - crates/corv/src/discuss/domain.rs
  - crates/corv/src/discuss/provider.rs
  - crates/corv/src/discuss/types.rs
  - crates/corv/src/discuss/handle.rs
  - crates/corv/src/discuss/teams.rs
  - crates/corv/src/discuss/sessions.rs
  - crates/corv/src/discuss/session_turns.rs
  - crates/corv/src/discuss/database.rs
  - crates/corv/src/discuss/storage.rs
  - crates/corv/src/discuss/elicitation.rs
  - crates/corv/src/discuss/agents.rs
  - crates/corv/src/discuss/usage.rs
  - crates/corv/src/discuss/pricing.rs
  - crates/corv/src/discuss/allowance.rs
  - crates/corv/src/discuss/reports.rs
  - crates/corv/src/discuss/assets.rs
  - crates/corv/src/discuss/asset_sources.rs
  - crates/corv/src/discuss/identity.rs
  - crates/corv/src/discuss/plugins/mod.rs
  - crates/corv/src/discuss/mcp_direct.rs
  - crates/corv/src/discuss/mcp_relay.rs
  - crates/corv/src/discuss/mcp.rs
  - crates/corv/src/discuss/mcp_http.rs
  - crates/corv/src/discuss/migration.rs
  - crates/aves/src/runtime/supervisor.rs
  - crates/aves/src/flow/actor.rs
  - crates/corv/src/discuss/types.rs
  - crates/corv/src/discuss/provider.rs
  - crates/corv/src/discuss/sessions.rs
  - crates/corv/src/discuss/messages.rs
  - crates/corv/src/discuss/plugins/opencode/mod.rs
---

# Discuss Domain Architecture

Corv owns Discuss: Provider config, Team install linkage, chat session lifecycle, the per-session persistence queue, session accounting, and the bounded `DiscussStreamEvent` sender. An embedding application composes Discuss on the Aves runtime. Corv depends on Aves, not its embedding application; Aves does not import Corv. `Discuss*` names and `discuss.*` contracts remain stable.

**Terminology:** Provider = ACP harness. Agent = Phanes persona. Team = leader + followers + skills.

## Ownership

- **Corv Discuss** (`crates/corv/src/discuss/`): actor, workers, `discuss.agents_workspace`, `discuss_sessions` / `discuss_chat_turns`, typed `DiscussError`, stream sender, Provider/Team/session lifecycle, MCP transport, plugin materialization, accounting ledgers, custom content. See [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md) for the file owner map.
- **Embedding application:** supplies the required stream sender and application-neutral asset library during composition.

## Startup

### Asset library

`crates/corv/src/discuss/library.rs` owns neutral asset definitions, ID/path validation, and `AssetLibrary`. The required `DiscussBootOptions.library` supplies immutable host definitions; Corv contains no built-in application catalog. The actor uses the same supplied catalog for listing, upgrades, retention, and provider materialization. Provider plugins receive the catalog explicitly rather than querying ambient globals.

`Runtime::start` runs every domain start callback concurrently (`JoinSet`). Discuss's callback dispatches `StartServer` (auto-start Providers, reconnect persisted sessions). There is no History-before-Discuss order.

## Workspace vs AgentNode

Discuss constructs the AgentNode registration in `agent_nodes.rs`, pushing it onto `NodeRegistrationSupply` through its domain construction services. `DiscussAgentNodeLifecycleService` routes privileged start/tick/cancel requests to the actor, which owns process-local terminal receivers. Tasks owns ScriptNode only; Discuss has no Tasks dependency.

Chat `cwd` is `discuss.agents_workspace`. Flow AgentNode passes `{data_root}/executions/{id}` as `SpawnSessionOptions.session_cwd`. CWD is not confinement. Limit breach: `session/cancel` then `DiscussError::AgentNodeLimitReached`. See [Flow Execution Workspace](../../concepts/flow-execution-workspace.md).

For new AgentNode runs, Flow asks Discuss's actor-serialized `WorkspaceCopySourcePort` for the effective workspace root and sources of every graph Provider. Discuss owns the Provider lookup; Flow copies their union before checkpoint 0. An absent Provider source list uses its built-in default; an explicit empty list still includes `.agents`. Ordinary chat CWD and restored executions are unchanged.

## Identity and stream

`discussSessionId` is the only routing key. Five `DiscussStreamEvent` variants: `SessionFailed`, `AgentMessage`, `UtilityUpdate`, `PermissionRequested`, `ElicitationRequested`.

## Session state

The worker's own turn flag is the single source of session activity. A turn that is dispatched, generating, working, or waiting on a permission is **thinking**; a session with no live worker is **idle**. A rejected busy turn changes nothing.

- `DiscussBroadcast::SessionActivity { discuss_session_id, active }` publishes each transition over the lossy broadcast channel.
- The actor accepts an activity update only when the worker registry entry exists **and** its `JoinHandle::is_finished()` is false. Membership alone is insufficient: a failed-but-not-yet-cleaned worker would leave the mirror stuck on thinking.
- `DiscussSessionInfo.active_turn` is the snapshot for `listSessions` / `loadSession`. It uses the same liveness rule, so a finished, stopped, never-started, or reloaded session reads idle.
- A worker failure publishes no synthetic idle event. `DiscussStreamEvent::SessionFailed` is the terminal signal and the snapshot reads idle.

`SendUserTurn` carries optional `images` (`DiscussImageAttachment { mime_type, data }`, base64). Validation checks non-empty data and a `type/subtype` MIME shape only — no format allowlist, no size cap, no transcoding. Images are admitted regardless of the harness's advertised `PromptCapabilities.image`; the harness decides. They are send-only: nothing durable is stored and `last_user_prompt` stays the text.

User turns also carry optional resource links. The worker sends them as ACP resource-link content blocks following the text, without a capability check.

Discuss exposes `archiveSession`, `unarchiveSession`, `getProviderAuthStatus`, `getSessionModes`, and `setSessionMode`. The worker tracks modes reported on session open and subsequent mode updates. Archive and authentication requests are sent only when advertised; closing and deleting provider sessions are best-effort.

## Identity bindings

`discuss_identity_bindings` (migration 0007) maps a verified `(issuer, subject)` to a session, its Provider and agent, and an optional Flow execution id. The foreign key to `discuss_sessions` cascades on delete, so removing a session removes its bindings.

- A chat worker writes its binding after the worker JWT is issued. A write failure revokes the worker JTI and fails the session.
- A Flow AgentNode writes its binding after `spawn_session`; a write failure only warns and the run continues.
- A transient connectivity session has no persisted anchor and writes no binding.
- The privileged `ResolveSessionIdentity` request returns the bound session, or `SessionIdentityUnknown`, `SessionIdentityAmbiguous`, or `SessionIdentityUnassigned`.

## Control and broadcasts

Privileged bus-only: session control, inspector, provider-config lifecycle, URL imports, custom MCP mutations, identity resolution. Public: `StartServer` / `StopServer` / `GetServerStatus` / `StartProvider` / `StopProvider`, Provider/Team mutations, custom content CRUD, reads and reports. Discuss is in MCP publications.

Broadcasts: `ServerStarted` / `ServerStopped`, `SessionReady`, `SessionActivity`, `SessionWarning`, `McpStatus`, `ProviderRuntimeChanged`. Do not add Job catalog/run broadcasts.

`StopServer` gates new work, cancels pending permission/elicitation, aborts live session workers, and keeps persisted sessions.

## Provider runtimes

Actor-owned `provider_supervisors` (memory only). Shared: `opencode`, `codex-acp`, `claude-acp`. Isolated: `grok-build` and custom. Shared connections use `SessionCallbackRouter` (`session.rs`). Missing queue returns the intact `CallbackEvent`; `fail_closed` answers Permission with internal error and Elicitation with Cancel.

`DiscussProviderConfig.agent_config_option_id` names the provider option that selects the primary agent, from the plugin's private selection. It is `None` for system-prompt, session-metadata or launch-argument selections. `ConfigOptionsOutput` carries the same optional id; no lookup operation exists.

Team primary-Agent precedence (`open_configured_session`, `session.rs`): when the Agent is selected through that option, Corv sets the advertised value after open, then removes the same option from the resolved stored preferences before reconciling, so a saved value cannot undo it. This runs on both new and reconnect opens. If the Provider does not advertise the Agent, the open fails with `Validation`.

## Background work

Subagents reach the parent as `UtilityUpdate`s carrying `DiscussUtilityPayload::Subagent` records, whose `surface_state` is separate from `active_turn`; subagent states never change parent activity. Sources and state mapping per provider are in [Subagent Reporting](../../concepts/subagent-reporting.md). `DiscussUtilityPayload::BackgroundWork` remains in the contract, but no plugin currently produces it.

## Worker tokens

- Chat and transient workers: delegated JWT (`sub = "agent"`, `SESSION_WORKER_JWT_EXPIRATION` = 30 days) via `issue_worker_jwt`. Discuss stores the JTI and revokes on terminal paths (`revoke_worker_jti`).
- Flow AgentNode: reuses the Flow-issued `flow-node:{node_execution_id}` token (`node_token`). Discuss does not mint a worker JTI; Flow revokes via `revoke_execution_tokens`.
- There is no Job worker.

## Owned subsystems

| Subsystem | Contract |
|---|---|
| Accounting ledgers | [Discuss Accounting Ledgers](../../concepts/discuss-accounting-ledgers.md) |
| Accounting reports | [Discuss Accounting Reports](../../concepts/discuss-accounting-reports.md) |
| Custom agents, skills, Teams | [Discuss Custom Content](../../concepts/discuss-custom-content.md) |
| Custom MCP servers | [Discuss Custom MCP](../../concepts/discuss-custom-mcp.md) |
| Settled behaviours | [Discuss Accepted Limits](../../concepts/discuss-accepted-limits.md) |

## Persistence

SQL lives in `crates/corv/migrations/`, a peer of `src/`; `crates/corv/src/discuss/migration.rs` registers it through `../../migrations`, through plain `mod migration;`. Existing `discuss.db`, `__discuss_migrations`, migration versions, and SQL remain unchanged.

`database.rs` owns the ordered persistence queue; `storage.rs` owns typed SQL storage (include constants, connection helpers, row types, CRUD, decoders). **Subagent upsert + retention prune MUST execute inside one Turso transaction** (`upsert_and_prune_discuss_subagent` takes `&mut Connection`, begins transaction, executes upsert, executes prune, commits only after both succeed).

Migrations `0001`–`0009` create the session, turn, config, team and subagent tables, the session-launch context, launch-config and setup-kind columns, the launch-limitations column, and the accounting (0005), custom-asset (0006), identity-binding (0007) and custom-MCP (0008) tables. There is no Job table: durable automation is Aves Flow.

Embedding applications import only the consumer contracts re-exported from `crates/corv/src/lib.rs`; the list is maintained there and in [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md).

## Related

- [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md) — file owner map and local conventions
- `crates/aves/AGENTS.md` — Aves runtime and bus lifecycle
- [Aves Flow Domain Architecture](../aves/flow-domain.md)
- [Harness Configuration](harness-configuration.md) — provider launch configuration and disclosed limitations
- [MCP-over-ACP Tooling](../../concepts/mcp-over-acp-tooling.md)

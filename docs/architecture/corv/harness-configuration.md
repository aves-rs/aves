---
title: Harness Configuration
type: architecture
updated: 2026-11-17
references:
  - crates/corv/src/discuss/plugins/mod.rs
  - crates/corv/src/discuss/plugins/launch.rs
  - crates/corv/src/discuss/plugins/files.rs
  - crates/corv/src/discuss/plugins/claude/mod.rs
  - crates/corv/src/discuss/plugins/codex/mod.rs
  - crates/corv/src/discuss/plugins/opencode/mod.rs
  - crates/corv/src/discuss/plugins/antigravity/mod.rs
  - crates/corv/src/discuss/sessions.rs
  - crates/corv/src/discuss/actor.rs
  - crates/corv/src/discuss/session.rs
  - crates/corv/src/discuss/worker.rs
  - crates/corv/src/discuss/teams.rs
  - crates/corv/src/discuss/mcp.rs
  - crates/corv/src/discuss/database.rs
  - crates/corv/src/discuss/storage.rs
  - crates/corv/src/discuss/types.rs
  - crates/corv/src/discuss/migration.rs
  - crates/corv/migrations/0009_session_launch_limitations.sql
  - crates/corv/src/lib.rs
  - crates/corv/AGENTS.md
---

# Harness Configuration

How a Discuss session turns caller intent into a provider launch, and how every requested setting that could not be applied exactly is disclosed instead of dropped. Provider-specific behaviour lives behind `DiscussAgentPlugin` implementations; the shared Discuss modules stay provider-neutral.

## The request

`SessionLaunchConfig` (`types.rs`) is the typed launch request: `rule_sources`, `mcp_servers`, `plugin_references`, `cli_tools`. It carries no credentials — `SessionCliTool` holds only a command and a login requirement.

- `Operation` is `Read | Write | Execute`; `Effect` is `Allow | Ask | Deny`.
- `intersect_rules` merges sources per operation and treats an omitted operation as `Deny`. No sources at all means "inherit"; one empty source means "deny everything".

## Provider-neutral contract

`DiscussAgentPlugin` (`plugins/mod.rs`) owns the launch hooks and their defaults:

| Hook | Default | Purpose |
|---|---|---|
| `launch_server` | clone | effective launch spec; never mutates persisted Provider configuration |
| `uses_mcp_launch_env` | `false` | custom MCP descriptors reach the launch environment |
| `configured_launch_server` | `Ok(server.clone())` | pass-through; the Provider owns any transformation |
| `launch_preparation` | disclose inherited permissions and unsupported plugin references | returns session metadata plus typed limitations |
| `launch_option_selections` | no selections | runs after the Provider advertised its options |
| `managed_config` | `None` | the Provider's one editable document: a `WorkspaceFile` in the agents workspace or a `LaunchEnv` on Corv's launch spec |

`LaunchPreparation` carries `meta` (ACP session metadata) and `limitations` (`LaunchLimitation` values).

## Best effort, not refusal

A Provider does not refuse a launch because it lacks a native permission control or because a distinct plugin reference is unsupported; malformed or conflicting inputs still fail the provider's own validation.

- Native best-effort permission fallback: the session still starts with whatever the harness enforces natively; the difference is disclosed.
- A distinct unsupported plugin reference is skipped and disclosed as `UnsupportedPluginReference`.
- Real failures stay errors: workspace IO, ACP transport, storage, and typed CLI tooling failures (`SessionCliToolUnsupported`, `SessionCliLoginUnsupported`).

## Provider behaviour

| Provider | Preparation | Permissions |
|---|---|---|
| `claude-acp` | writes `settings.json`, returns `claudeCode` metadata | native tool groups |
| `codex-acp` | discloses unsupported plugin references | native mode |
| `opencode` | no metadata, no limitations | its own launch configuration |
| `grok-build`, `antigravity-acp`, custom | inherited default | inherited and unverified |

- **Claude.** `Read` maps to Read/Glob/Grep, `Write` to Edit/Write/NotebookEdit, `Execute` to Bash/PowerShell. The rendered `settings.json` is written under the configured agents workspace at `sessions/{discussSessionId}/settings.json`, and the session metadata carries `_meta.claudeCode.options` (`settings`, `disallowedTools`). No HOME or login relocation.
- **Codex.** `agent` mode when `Write` or `Execute` is `Allow`, otherwise `read-only`; Corv never automatically selects `agent-full-access`. A missing advertised `mode` option yields `Inherited` plus `UnavailableAdvertisedOption`; when the option exists, a native mode that couples permissions yields `ModeCoupling` and a non-allowed `Read` yields `UnsupportedNativeControl`.
- **Default preparation** (Grok, Antigravity, custom) reports requested permissions as `Inherited` with `InheritedUnverifiedConfiguration`.
- **No rules, no change.** With no rule sources Claude writes no settings file and Codex selects no mode, so the harness's own permissions and mode stay in place.

## Provider-managed configuration

| Provider | Kind | Document |
|---|---|---|
| `opencode` | WorkspaceFile | `.opencode/opencode.json` (default: the generated OpenCode config) |
| `claude-acp` | WorkspaceFile | `.claude/settings.json` (default `{}`) |
| `codex-acp` | LaunchEnv | `CODEX_CONFIG` (default `{}`) |
| `grok-build` | WorkspaceFile | `.grok/config.toml` (default empty; only an `mcp_servers` table is allowed) |
| `antigravity-acp` | WorkspaceFile | `.agents/mcp_config.json` (default `{"mcpServers":{}}`) |

`discuss.getProviderManagedConfig` is a public read. `discuss.setProviderManagedConfig` is privileged: it replaces the complete Corv-managed document, validates it through the plugin, installs it, and persists it. Validation failure is `ManagedConfigValidation` and changes nothing. The document takes effect on the next Provider start; no restart is forced. A corrupted persisted document falls back to the plugin default with a typed warning.

Corv never touches a personal file: workspace documents exist only in the central agents workspace, and a Flow execution root receives the user's personal files through the existing copy sources. `antigravity-acp` appends `--process-per-session` to its launch when the composed MCP set contains an HTTP server.

## Validation is provider-dependent

`validate_supplied_inputs` rejects a duplicate plugin reference with a typed `Validation` error. Claude and Codex call it during `launch_preparation`; the default preparation path does not, so duplicates reach Grok, Antigravity and custom Providers as individual disclosures. Do not assume a universal duplicate rejection.

## Order of operations

1. `compose_custom_mcps` builds the launch copy: Provider scope, then Team, then session. A name reserved by a native domain is `ReservedMcpName`; a repeated name is `DuplicateMcpServerName`. See [Discuss Custom MCP](../../concepts/discuss-custom-mcp.md).
2. `prepare_session_launch` computes the effective server, calls `launch_preparation`, then `configured_launch_server`.
3. Isolation starts from `explicit_config || uses_mcp_launch_env() || configured != launch_server || restores_team_at_root`, and a `PrimaryAgentSelection::LaunchArguments` Agent selection forces it as well; an isolated launch cannot reuse the shared Provider connection.
4. Before the ACP session is created or loaded, the launch metadata is merged with `merge_metadata`; a conflicting top-level key is a typed `Validation` error.
5. Stored configuration preferences are reconciled, then `launch_option_selections` runs against the resulting advertised options and its settings are applied to the session.
6. For a durable session the limitations are persisted before the worker reports ready.

## Persistence and disclosure

- `discuss_sessions.launch_limitations_json` (migration `0009_session_launch_limitations.sql`) is nullable TEXT holding the serialized `LaunchLimitation` list.
- Decoding: NULL becomes an empty list, malformed JSON becomes a typed `DiscussError::Persistence`.
- A write replaces the whole list, including an empty list.
- For a durable session the worker writes through the ordered per-session persistence queue and awaits the acknowledgement before signalling `WorkerReady`; a transient connectivity probe skips the write.
- Sessions expose the list through `DiscussSessionInfo.launch_limitations`. There is no separate broadcast or service for it.

## Team at root

`DiscussSessionSetup::TeamAtRoot` launches a Team whose assets the caller materialized in the session root; the central Agent workspace is not used.

- Member files are re-rendered from the persisted installation's member overrides on every root launch, reconnect included.
- `InstallTeamAndStartSession` passes the leader's session overrides only on the initial root launch (no saved ACP session id). A reconnect passes no new leader overrides, so the persisted manual model and effort survive.
- Setup settings are applied first and explicit session overrides last, so an explicit override wins.
- The central homebase is unchanged.
- `write_rendered_files` is a plain writer: it creates parent directories and writes each file. It follows symlinks and performs no temporary-file rename or rollback. The plugin batch path (`install_team_with_member_overrides`) renders every file before writing any of them; the member-by-member root materialization path has no all-or-nothing guarantee.

## Example

```json
{
  "rule_sources": [{ "rules": [{ "operation": "read", "effect": "allow" }] }],
  "plugin_references": ["acme.pack"]
}
```

A Claude launch of that request writes `sessions/{id}/settings.json` with `permissions.allow = ["Read","Glob","Grep"]`, denies the write and execute groups, lists those denied tools as `disallowedTools`, and discloses `acme.pack` as an unsupported plugin reference.

### Reconnect diagnosis

Read `launch_limitations` on the session:

| Symptom | Meaning |
|---|---|
| `Inherited` + `InheritedUnverifiedConfiguration` | the requested effect was left to the harness's own defaults; Discuss cannot verify what the harness enforced |
| `Mode` + `ModeCoupling` | the native mode grants or denies more than the request |
| `UnavailableAdvertisedOption` | the Provider never advertised the option the request needed |
| `PluginReference` + `UnsupportedPluginReference` | the reference was skipped, not installed |

Claude applies its permission groups natively and therefore reports no permission limitation. An empty list records no launch limitation; it is not proof of the effective permissions or of exact enforcement.

## Related

- [Discuss Domain Architecture](discuss-domain.md)
- [crates/corv/AGENTS.md](../../../crates/corv/AGENTS.md)
- [MCP-over-ACP Tooling](../../concepts/mcp-over-acp-tooling.md)
- [Discuss Custom MCP](../../concepts/discuss-custom-mcp.md) and [Discuss Custom Content](../../concepts/discuss-custom-content.md)

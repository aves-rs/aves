---
title: Discuss Custom MCP
type: concept
updated: 2026-11-17
references:
  - crates/corv/migrations/0008_custom_mcp.sql
  - crates/corv/src/discuss/mcp.rs
  - crates/corv/src/discuss/messages.rs
  - crates/corv/src/discuss/sessions.rs
  - crates/corv/src/discuss/plugins/opencode/mod.rs
  - crates/corv/src/discuss/plugins/antigravity/mod.rs
---

# Discuss Custom MCP

A custom MCP is a user-supplied MCP server definition that Corv composes into a
session's launch. It is the mirror image of [MCP-over-ACP Tooling](mcp-over-acp-tooling.md):
that concept exposes bus operations *to* an agent, this one lets a user point an agent
*at* an MCP server.

## Schema (migration 0008)

| Table | Holds |
|---|---|
| `discuss_custom_mcp_servers` | one row per server name: the ACP `McpServer` descriptor as JSON |
| `discuss_mcp_assignments` | `(scope_kind, scope_id, mcp_name)`; scope kind is `provider`, `team`, or `session` |

The descriptor is the ACP wire shape — stdio `{ command, args, env }` or HTTP
`{ url, headers }`. Environment and header values are persisted because the user's
descriptor is the product decision; they are never logged and never echoed in errors.

## Operations

`listCustomMcps` is a **public** read and redacts every environment variable and header
value to `<redacted>`, because it is published to harness agents.

`createCustomMcp`, `updateCustomMcp`, `deleteCustomMcp`, `assignMcp`, and `unassignMcp`
are **privileged** bus-only: a stdio descriptor is an arbitrary process-launch
specification. A name that an application MCP domain already serves is
`ReservedMcpName`. Creating an existing name is `DuplicateMcpServerName`; the descriptor
name must match the requested name.

Assignment validates its scope: the provider must be installed, the Team must resolve in
the merged catalog, and the session must exist.

## Composition

Composition is additive and happens at every spawn:

1. provider-scoped servers,
2. Team-scoped servers, when the session's Team is known,
3. session-scoped servers,
4. the persisted `SessionLaunchConfig.mcp_servers` of the session anchor.

A repeated name across scopes is `DuplicateMcpServerName`; a reserved application name is
`ReservedMcpName`. The composed set is **never persisted** — it is recomputed in memory
at each spawn, so a reconnect composes identically and no scope can leak into another.

Removing a session, a Provider, or a Team deletes that scope's assignments.

## Transport

Every provider except OpenCode receives the composed descriptors through the ACP
`session/new` descriptor list; the harness decides whether it accepts them. OpenCode
keeps its existing launch-environment translation. No native MCP configuration file is
written anywhere by composition — the Provider's own managed document stays the user's
editable native configuration.

Antigravity appends `--process-per-session` to its launch when the composed set contains
an HTTP server, because that flag is required for editor-configured HTTP MCP servers.

## Related

- [Discuss Custom Content](discuss-custom-content.md)
- [Harness Configuration](../architecture/corv/harness-configuration.md)
- [MCP-over-ACP Tooling](mcp-over-acp-tooling.md)

---
title: Subagent Reporting
type: concept
updated: 2026-11-23
references:
  - crates/corv/src/discuss/plugins/subagents.rs
  - crates/corv/src/discuss/plugins/mod.rs
  - crates/corv/src/discuss/plugins/opencode/mod.rs
  - crates/corv/src/discuss/plugins/claude/mod.rs
  - crates/corv/src/discuss/plugins/codex/mod.rs
  - crates/corv/src/discuss/plugins/grok/mod.rs
  - crates/corv/src/discuss/session.rs
  - crates/corv/src/discuss/agents.rs
  - crates/corv/src/discuss/worker.rs
---

# Subagent Reporting

Every provider session reports the subagents it delegates to as `DiscussUtilityPayload::Subagent` records keyed by `capture_id`. Each record has a `surface_state`: `Loading` (not yet known), `Live` (working), `Done`, or `Error`. A worker-local `SubagentTracker` merges partial updates, so a later update that omits the name or description still yields a complete record. When one update changes several subagents, the worker forwards each queued change after the capture.

## Sources, in order of authority

1. **ACP subagents.** `initialize_request` declares `clientCapabilities.subagents` (the `unstable_subagents` feature of `agent-client-protocol`). Adapters that implement it (claude-agent-acp, codex-acp) send `session/update` with `sessionUpdate: "subagent_update"` on the parent. `sessionId` names the child and is the capture id. `title` and `description` are display metadata. `state` maps as follows: `running` and `requires_action` → `Live`; `unknown` → `Loading`; `idle` → `Done`, or `Error` when `stopReason` is `error` or `cancelled`.
2. **OpenCode child updates.** OpenCode does not implement the ACP capability. Corv opts into its own channel with `_meta["opencode/child-session-updates"]: true`. OpenCode then sends `opencode/session/child_update` notifications, routed to the session by `rootSessionId`. Status messages for direct children map `created` → `Loading`, `running` → `Live`, `completed` → `Done`, and `failed` or `interrupted` → `Error`. The capture ignores nested children and child transcript updates. The agent name comes from the v2 `subagent` tool call's `rawInput.agent`, matched by the child `sessionID` in its output.
3. **Grok Build.** Corv reads `x.ai/session_notification` `subagent_spawned`, `subagent_progress` and `subagent_finished`.
4. **Tool-call fallbacks** apply only while a session has reported no lifecycle of type 1 or 2: Claude's `Agent`/`Task` call, Codex's `spawnAgent` collaboration calls, and OpenCode's legacy `task` call.

A reported lifecycle may reopen a finished subagent, because providers reuse a child session for later work. A tool-call guess never moves a finished subagent back to running. Antigravity's ACP server is closed source and reports nothing.

## Limits

- Child sessions' own transcript events (`session_message`, OpenCode child `update`s) are not stored. The subagent inspector loads the child session itself where the provider supports it (OpenCode only).
- OpenCode's job API is not used. `opencode api` reaches the background service, not the `opencode acp` process that hosts the session, so its active-session list never contains ACP sessions. No `BackgroundWork` snapshot is produced for OpenCode.
- Subagent states never change the parent's `active_turn`.

## Related

- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)

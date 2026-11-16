---
title: Discuss Custom Content
type: concept
updated: 2026-11-17
references:
  - crates/corv/migrations/0006_custom_assets.sql
  - crates/corv/src/discuss/assets.rs
  - crates/corv/src/discuss/asset_sources.rs
  - crates/corv/src/discuss/library.rs
  - crates/corv/src/discuss/teams.rs
  - crates/corv/src/discuss/messages.rs
  - crates/corv/src/discuss/plugins/mod.rs
---

# Discuss Custom Content

Corv owns runtime-defined agents, skills, and Teams alongside the application-supplied built-in catalog. The static `AssetLibrary` is unchanged; custom definitions live in Corv's own storage and are merged at a typed resolution boundary.

## Schema (migration 0006)

| Table | Holds |
|---|---|
| `discuss_custom_agents` / `discuss_custom_skills` | identity, metadata, version, provenance (`local_markdown`, `url_markdown`, `github_file`, `github_directory`), last pull result |
| `discuss_custom_agent_installations` / `discuss_custom_skill_installations` | which Provider has which asset installed, and at which version |
| `discuss_custom_teams` | leader key, display name, ordered followers and skills, revision |
| `discuss_asset_instructions` | one row per **physical** installed path |

Cached content lives on disk under `config_dir/discuss_cache`: `agents/<id>.md` and `skills/<id>/…`. The database rows hold identity, metadata, and provenance only.

## Resolution

`ResolvedAgent`, `ResolvedSkill`, and `ResolvedTeam` are `Builtin` or `Custom`. A single borrowed `AgentSelectionView` is the one selection surface for both, so built-in and custom agents follow the same render path. `resolve_agent` / `resolve_skill` / `resolve_team` prefer the built-in catalog and fall back to the custom rows.

## Operations

Local CRUD is public: `listCustomAgents`, `listCustomSkills`, `getCustomAgent`, `getCustomSkill`, `createCustomAgent`, `createCustomSkill`, `updateCustomAgent`, `updateCustomSkill`, `deleteCustomAgent`, `deleteCustomSkill`, `installCustomAgent`, `uninstallCustomAgent`, `installCustomSkill`, `uninstallCustomSkill`, `createCustomTeam`, `updateCustomTeam`, `deleteCustomTeam`, and the two `…InstalledAssetInstructions` operations.

A custom id that collides with a built-in catalog id is `CustomAssetIdCollision`. A custom asset that is still installed or still referenced by a custom Team cannot be deleted; the error lists the references (`CustomAssetReferenced`). `DiscussSessionSetup::Agent { agent_id }` starts a standalone custom-agent session; the agent must be custom and installed for the target Provider.

## Custom Teams

A custom Team's leader may be any built-in or custom agent. It is rejected only when the id already identifies another Team — a built-in Team's leader or an existing custom Team's leader (`TeamLeaderInUse`). Members may mix built-in and custom agents and skills. Built-in Team compositions stay immutable. The leader id remains the Team key; there is no separate Team id. Install, edit, and uninstall reuse the existing Team machinery, including scope-keyed retention, and boot refreshes custom Team members through the same install path.

## URL imports

`importCustomAgentFromUrl` and `importCustomSkillFromUrl` are **privileged**: they fetch external content. The fetch runs in a spawned task, so the actor is never blocked; the completion returns to the actor, which validates, persists, refreshes installed copies, and replies in order. `StopServer` aborts pending imports and replies `ShuttingDown`.

`asset_sources.rs` owns the fetch boundary:

- HTTPS only, no embedded credentials; redirects are refused.
- DNS is resolved first and the client connects to the validated addresses, so a rebind cannot slip through. Private, loopback, link-local, ULA and multicast addresses are rejected with `ImportAddressBlocked`.
- GitHub support is unauthenticated and public. A `blob` URL resolves one file; a `tree` URL walks the directory through the contents API.
- Limits: 1 MiB per file, 64 files, 8 MiB total, depth 4, 32 API calls, 10 s per request.

## Atomicity

A cached asset is replaced by `swap_and_update`: each file is written to a temporary file and renamed into place, files absent from the new bundle are deleted, and a `SwapGuard` restores the previous bundle — removing files it created — if any step or the database update fails. A failed import keeps the last good cached and installed content. Files dropped from a custom skill are removed from installed copies through each plugin's `destroy_custom_skill`.

## Appended instructions

Instructions are keyed by the **physical** installed path, resolved through the plugin's layout. Any Providers whose installed physical paths coincide share one instruction row, and two Teams that share an installed asset share its instructions. Today Antigravity (agents and skills) and Codex (skills only) resolve to the default plugin's `.agents/agents` and `.agents/skills` paths, so those files carry one shared instruction set. Codex's agents live under `.codex/agents` and are not shared.

Each plugin owns how the instructions are added: the default appends them to the rendered file, while Codex places them inside its TOML `developer_instructions`. Setting instructions rewrites the installed file immediately; active sessions are not restarted.

## Related

- [Discuss Accepted Limits](discuss-accepted-limits.md)
- [Harness Configuration](../architecture/corv/harness-configuration.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)

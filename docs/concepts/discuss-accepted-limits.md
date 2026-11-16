---
title: Discuss Accepted Limits
type: concept
updated: 2026-11-17
references:
  - crates/corv/src/discuss/usage.rs
  - crates/corv/src/discuss/reports.rs
  - crates/corv/src/discuss/assets.rs
  - crates/corv/src/discuss/database.rs
  - crates/corv/src/discuss/teams.rs
  - crates/corv/src/discuss/plugins/grok/mod.rs
  - crates/corv/src/discuss/worker.rs
  - crates/corv/src/discuss/actor.rs
  - crates/corv/src/discuss/storage.rs
---

# Discuss Accepted Limits

These behaviours were reviewed and settled. They are recorded here so nobody "fixes" them
or re-raises them as new findings. Each entry states the current behaviour, not a plan.

## Accounting

- **A forced stop loses the active turn's accounting.** `StopServer` and `StopProvider`
  abort the worker future outright, so the worker-failure path that writes a `failed`
  ledger row never runs. Terminal-only writes are preserved: turns that completed before
  the abort stay recorded.
- **A cancelled turn's late response is credited to the next turn.** After cancelling
  turn A and sending turn B, A's late `PromptResponse` or turn boundary finalizes B.
  B's real usage is lost and B is recorded as cancelled.
- **A reconnect can drop one ledger row.** The turn ordinal is read from the ledger when
  the session spawns. A reconnect inside the previous worker's persistence-drain window
  can read a stale `MAX(turn_ordinal) + 1`; the later insert conflicts and is discarded
  by `ON CONFLICT … DO NOTHING`.
- **A Grok turn flagged `usageIsIncomplete` can still add to the complete harness total.**
  `harness_per_turn_cost` excludes turns flagged `cost_is_partial` but not turns flagged
  `usage_is_incomplete`. Such a turn is still counted in `turns_usage_incomplete`, so the
  sum and the coverage counter disagree about its completeness.

## Content and Teams

- **Team cleanup ignores standalone custom installs.** `retained_team_assets` considers
  only other Team installations, so uninstalling or editing a Team can delete a file that
  a standalone custom install in the same physical scope still uses. The install row
  survives; the file does not.
- **Grok's leader ignores appended instructions.** Grok delivers its primary agent
  through `_meta.rules` built from the asset body, bypassing the installed file, so saved
  appended instructions never reach the Grok leader.
- **A failed first URL import records no row.** The pull-failure update targets an
  existing row. An import that fails before any row exists returns the error and leaves
  no provenance to show.
- **Instructions are saved before the installed file is rewritten.** If the rewrite fails,
  the saved instruction value runs ahead of the file until the next materialization.

## Test coverage

- `pricing.rs`, `reports.rs`, `usage.rs`, `allowance.rs`, `assets.rs`,
  `asset_sources.rs`, `identity.rs`, `session_turns.rs`, `teams.rs`, and `worker.rs` carry
  no owner-local unit tests.
- The URL-import skill path is not covered: `complete_custom_import` is exercised only
  with an agent, so the stale-skill-file cleanup never runs in a test. The
  `prepare_custom_import` / `start_custom_import` / `finish_custom_import` spawn and
  completion wiring is not covered either.

## Related

- [Discuss Accounting Ledgers](discuss-accounting-ledgers.md)
- [Discuss Accounting Reports](discuss-accounting-reports.md)
- [Discuss Custom Content](discuss-custom-content.md)

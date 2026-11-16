---
title: Discuss Accounting Ledgers
type: concept
updated: 2026-11-17
references:
  - crates/corv/migrations/0005_accounting_ledgers.sql
  - crates/corv/src/discuss/usage.rs
  - crates/corv/src/discuss/pricing.rs
  - crates/corv/src/discuss/allowance.rs
  - crates/corv/src/discuss/plugins/mod.rs
  - crates/corv/src/discuss/plugins/grok/mod.rs
  - crates/corv/src/discuss/worker.rs
  - crates/corv/src/discuss/boot.rs
---

# Discuss Accounting Ledgers

Corv records what each Discuss turn consumed, what it cost, and what the provider's subscription allowance looked like. Accounting is session-level: interactive chats and Flow AgentNode sessions share one ledger.

## Schema (migration 0005)

| Table | Holds |
|---|---|
| `discuss_usage_turns` | one row per turn; unique on `(discuss_session_id, turn_ordinal)` |
| `discuss_usage_model_entries` | one row per model observed in a turn; unique on `(session, ordinal, IFNULL(model_id,''))` |
| `discuss_allowance_observations` | sampled allowance snapshots per `(provider, account)` |
| `discuss_models_catalog` | singleton last-good models.dev body plus cache validators |

Token counts are nullable `INTEGER`; costs and prices are exact decimal `TEXT`. A missing observation stays `NULL` — absent is unknown, never zero.

## Capture

`usage.rs` owns a worker-local `UsageTurnAccumulator`. `capture_standard_usage` — the default `DiscussAgentPlugin::capture_usage_evidence` — reads `PromptResponse.usage` (per-turn token counts from the typed SDK struct) and the `session/update` `usage_update` (context occupancy `used`/`size` and the cumulative harness `cost`).

A provider may override the default. Grok reads `PromptResponse._meta.usage` with `_meta.modelId`, plus the `x.ai/session/prompt_complete` extension notification, its `usageIsIncomplete` / `costIsPartial` flags, and `costUsdTicks` (10^10 ticks = 1 USD).

Raw provider metadata is never retained: `evidence_json` holds only normalized counts, model ids, and cost fields. `evidence_source` records which surface won (`prompt_response`, `prompt_response_meta`, `prompt_complete`).

## Per-provider billing semantics

`BillingCategorySemantics` declares which categories a provider guarantees and whether input already contains cache tokens.

| Provider | Guaranteed categories | `input_includes_cache` | models.dev id |
|---|---|---|---|
| `claude-acp` | input, output, total | `false` | `anthropic` |
| `codex-acp` | all six | `false` | `openai` |
| `grok-build` | all six | `true` | `xai` |
| `antigravity-acp` | none declared | – | none |
| `opencode` | none declared | – | none |

`input_includes_cache = true` subtracts cache reads and cache writes from input before the input rate applies, so Grok's cache tokens are not billed twice. Reasoning tokens are never billed separately: they stay inside output. A provider that declares no semantics produces **no calculated cost**; its occupancy and harness cost are still recorded.

## Spend

`pricing.rs` fetches `https://models.dev/api.json` itself. It keeps the `ETag` and `Last-Modified` validators, refreshes daily, and retains the last good catalog when a refresh fails. Prices are parsed as exact `rust_decimal` values from the raw JSON text; no price passes through `f64`.

> The `models-dev-rs` crate is **not** used and must not be reintroduced: it enables serde_json's `arbitrary_precision` feature workspace-wide, which broke Aves Config TOML writes.

A model is priced only on an exact `(provider, model)` catalog match. `pricing_basis` records `base tier; tiered/batch/long-context rates not applied`.

## Allowance

`allowance.rs` runs one `quota_core::Registry::with_defaults` (pinned to the `insula` tag `v0.3.0`) in-process. No `subc` binary, no daemon, no spawned helper process. `codex.auto_use_resets` stays disabled and the registry's reset-redemption API is never called.

Observations are written at most once per 15-minute window per `(provider, account)`, plus an immediate row on a structural transition (error class change, account appears or disappears, reset time crossed). The live snapshot is the actor's in-memory copy and is exact; the history is a sample series.

## Writes, ordinals, and retention

Ledger rows are written only at a turn's terminal boundary — completion, cancellation, or worker failure. A turn's ordinal comes from the ledger itself (`MAX(turn_ordinal) + 1` for the session) when the session spawns, then increments in memory.

`DiscussSettings.accounting_retention_days` defaults to **90**; `0` disables pruning. The actor prunes once per day: model entries before their turns, allowance observations last.

## Related

- [Discuss Accounting Reports](discuss-accounting-reports.md)
- [Discuss Accepted Limits](discuss-accepted-limits.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)

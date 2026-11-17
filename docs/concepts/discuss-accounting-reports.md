---
title: Discuss Accounting Reports
type: concept
updated: 2026-11-17
references:
  - crates/corv/src/discuss/reports.rs
  - crates/corv/src/discuss/messages.rs
  - crates/corv/src/discuss/storage.rs
  - crates/corv/src/discuss/types.rs
  - crates/corv/src/discuss/allowance.rs
---

# Discuss Accounting Reports

Four public read operations expose the retained accounting evidence. Aggregation happens
in Rust over indexed range scans; the SQL layer only selects rows.

## Operations

| Operation | Filters | Grouping |
|---|---|---|
| `discuss.getTokenUsageReport` | `from_ms?`, `to_ms?`, `provider_name?`, `model_id?` | `session`, `provider`, `model`, `utc_hour`, `utc_day`, `utc_week`, `utc_month` |
| `discuss.getSpendReport` | same | same |
| `discuss.getAllowanceReport` | `from_ms?`, `to_ms?`, `provider_name?` | `provider`, `account`, `utc_hour`, `utc_day`, `utc_week`, `utc_month` |
| `discuss.getCurrentAllowance` | none | none |

There is no session filter. A model filter selects matching model entries; a turn with no
matching entry is excluded from the report.

## Range and grouping

`from_ms` and `to_ms` are optional and default to the whole retained history.
`from_ms > to_ms` is `ReportRangeInvalid`. Weeks start Monday 00:00 UTC; months are
calendar UTC. Grouping is bounded at 5,000 groups: the response is truncated
deterministically by key order and carries `truncated: true`.

## Coverage

Every row states how much of the underlying data it actually saw:

| Counter | Meaning |
|---|---|
| `turns_total` | turns in the group |
| `turns_with_token_evidence` | turns contributing at least one token count |
| `turns_with_calculated_cost` | turns contributing a calculated cost |
| `turns_with_throughput` | turns eligible for the end-to-end rate |
| `turns_usage_incomplete` | turns whose persisted usage flag is incomplete |
| `turns_harness_cost_partial` | turns whose persisted cost flag is partial |
| `turns_excluded_multi_model` | model-grain rows dropped from the per-model rate |

Completeness is read only from the persisted flags, never inferred from which categories
are present. Sums never masquerade as complete totals.

## Three distinct cost columns

Spend never mixes scopes:

- `calculated_cost_usd` — exact `rust_decimal` sum of calculated per-model spend.
- `harness_per_turn_cost` — per-currency sum of `per_turn` harness amounts. Currencies are
  never cross-summed.
- `harness_session_cost_latest` — session-grain rows only: the latest `session_cumulative`
  observation for that session. It is never summed across turns or sessions and never
  added to per-turn amounts. Other groupings return it as null with a stated reason.

Decimal overflow is `ReportAggregationOverflow`; nothing saturates silently.

## Throughput

The rate is end-to-end: output tokens divided by elapsed time from prompt dispatch to
turn completion, including tool work and waiting. A turn qualifies only when it is not
usage-incomplete, every contributing entry reports output tokens, the turn completed,
its duration is positive, and — for model-grain or model-filtered rows — it used exactly
one model.

Multi-model turns are excluded from a per-model rate and counted in
`turns_excluded_multi_model`. Duration is never split across models. The value renders at
scale 6 with round-half-even.

## Allowance

`getAllowanceReport` groups retained samples and projects each observation's typed
provider usage: windows, pools, saved resets, stale marker, and error class. Provider
names are the registry names; `display_provider` maps them to Discuss provider names for
display only.

`getCurrentAllowance` reads the actor's in-memory snapshot, so it reflects the freshest completed observation. While the server is not running, or before the first observation arrives, it returns `AccountingUnavailable` — never empty data presented as "no allowance".

## Related

- [Discuss Accounting Ledgers](discuss-accounting-ledgers.md)
- [Discuss Domain Architecture](../architecture/corv/discuss-domain.md)

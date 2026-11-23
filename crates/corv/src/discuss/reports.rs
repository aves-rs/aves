//! Aggregation of retained accounting evidence.

use std::collections::{BTreeMap, HashMap};

use chrono::{Datelike, Utc};
use rust_decimal::Decimal;

use super::actor::Actor;
use super::error::DiscussError;
use super::messages::DiscussResponse;
use super::report_types::{
    DiscussAllowanceGroupBy, DiscussAllowanceProjection, DiscussAllowanceReport, DiscussAllowanceReportRow,
    DiscussAllowanceSample, DiscussCompletionKind, DiscussHarnessCost, DiscussHarnessCostKind, DiscussModelUsageEntry,
    DiscussReportKey, DiscussTurnRecord, DiscussUsageGroupBy, DiscussUsageReport, DiscussUsageReportRow,
};
use super::storage::{select_allowance_range, select_usage_range};

const LIMIT: usize = 5_000;
const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum GroupKey {
    Session(String),
    Provider(String),
    Model(Option<String>),
    Account(String, Option<String>),
    Time(i64),
}

impl GroupKey {
    fn projection(&self) -> DiscussReportKey {
        let mut key = DiscussReportKey::default();
        match self {
            Self::Session(value) => key.session_id = Some(value.clone()),
            Self::Provider(value) => key.provider = Some(value.clone()),
            Self::Model(value) => key.model_id = value.clone(),
            Self::Account(provider, account) => {
                key.provider = Some(provider.clone());
                key.account = account.clone();
            }
            Self::Time(value) => key.bucket_start_ms = Some(*value),
        }
        key
    }
}

fn range(from: Option<i64>, to: Option<i64>) -> Result<(i64, i64), DiscussError> {
    let from = from.unwrap_or(i64::MIN);
    let to = to.unwrap_or(i64::MAX);
    if from > to {
        return Err(DiscussError::ReportRangeInvalid);
    }
    Ok((from, to))
}

fn bucket(timestamp: i64, dimension: u8) -> Result<i64, DiscussError> {
    match dimension {
        0 => timestamp.div_euclid(3_600_000).checked_mul(3_600_000),
        1 => timestamp.div_euclid(DAY).checked_mul(DAY),
        2 => i64::try_from(
            (i128::from(timestamp) + i128::from(3 * DAY)).div_euclid(i128::from(WEEK)) * i128::from(WEEK)
                - i128::from(3 * DAY),
        )
        .ok(),
        _ => chrono::DateTime::<Utc>::from_timestamp_millis(timestamp).and_then(|date| {
            chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|date| date.and_utc().timestamp_millis())
        }),
    }
    .ok_or(DiscussError::ReportAggregationOverflow)
}

fn usage_key(
    turn: &DiscussTurnRecord,
    model: Option<&str>,
    group: DiscussUsageGroupBy,
) -> Result<GroupKey, DiscussError> {
    Ok(match group {
        DiscussUsageGroupBy::Session => GroupKey::Session(turn.discuss_session_id.clone()),
        DiscussUsageGroupBy::Provider => GroupKey::Provider(turn.provider_name.clone()),
        DiscussUsageGroupBy::Model => GroupKey::Model(model.map(str::to_owned)),
        DiscussUsageGroupBy::UtcHour => GroupKey::Time(bucket(turn.turn_started_at_ms, 0)?),
        DiscussUsageGroupBy::UtcDay => GroupKey::Time(bucket(turn.turn_started_at_ms, 1)?),
        DiscussUsageGroupBy::UtcWeek => GroupKey::Time(bucket(turn.turn_started_at_ms, 2)?),
        DiscussUsageGroupBy::UtcMonth => GroupKey::Time(bucket(turn.turn_started_at_ms, 3)?),
    })
}

fn add_count(target: &mut u64, amount: u64) -> Result<(), DiscussError> {
    *target = target
        .checked_add(amount)
        .ok_or(DiscussError::ReportAggregationOverflow)?;
    Ok(())
}

fn add_optional(target: &mut Option<u64>, value: Option<u64>) -> Result<(), DiscussError> {
    if let Some(value) = value {
        *target = Some(
            target
                .unwrap_or(0)
                .checked_add(value)
                .ok_or(DiscussError::ReportAggregationOverflow)?,
        );
    }
    Ok(())
}

fn decimal(value: &str) -> Result<Decimal, DiscussError> {
    Decimal::from_str_exact(value).map_err(|_| DiscussError::ReportAggregationOverflow)
}

fn add_decimal(target: &mut Option<Decimal>, value: Decimal) -> Result<(), DiscussError> {
    *target = Some(
        target
            .unwrap_or(Decimal::ZERO)
            .checked_add(value)
            .ok_or(DiscussError::ReportAggregationOverflow)?,
    );
    Ok(())
}

#[derive(Default)]
struct Aggregate {
    row: DiscussUsageReportRow,
    calculated: Option<Decimal>,
    per_currency: BTreeMap<String, Decimal>,
    latest: Option<(i64, i64, DiscussHarnessCost)>,
}

impl Aggregate {
    fn record(
        &mut self,
        turn: &DiscussTurnRecord,
        entries: &[&DiscussModelUsageEntry],
        model_group: bool,
        multi: bool,
        filtered: bool,
    ) -> Result<(), DiscussError> {
        let coverage = &mut self.row.coverage;
        add_count(&mut coverage.turns_total, 1)?;
        if turn.usage_is_incomplete {
            add_count(&mut coverage.turns_usage_incomplete, 1)?;
        }
        if turn.cost_is_partial {
            add_count(&mut coverage.turns_harness_cost_partial, 1)?;
        }
        if model_group && multi {
            add_count(&mut coverage.turns_excluded_multi_model, 1)?;
        }
        let mut has_tokens = false;
        let mut has_cost = false;
        let mut output = None;
        for entry in entries {
            let tokens = &mut self.row.tokens;
            add_optional(&mut tokens.input_tokens, entry.input_tokens)?;
            add_optional(&mut tokens.output_tokens, entry.output_tokens)?;
            add_optional(&mut tokens.thought_tokens, entry.thought_tokens)?;
            add_optional(&mut tokens.cached_read_tokens, entry.cached_read_tokens)?;
            add_optional(&mut tokens.cached_write_tokens, entry.cached_write_tokens)?;
            add_optional(&mut tokens.total_tokens, entry.total_tokens)?;
            add_optional(&mut output, entry.output_tokens)?;
            has_tokens |= entry.input_tokens.is_some()
                || entry.output_tokens.is_some()
                || entry.thought_tokens.is_some()
                || entry.cached_read_tokens.is_some()
                || entry.cached_write_tokens.is_some()
                || entry.total_tokens.is_some();
            if let Some(cost) = &entry.calculated_cost_usd {
                add_decimal(&mut self.calculated, decimal(cost)?)?;
                has_cost = true;
            }
        }
        if has_tokens {
            add_count(&mut coverage.turns_with_token_evidence, 1)?;
        }
        if has_cost {
            add_count(&mut coverage.turns_with_calculated_cost, 1)?;
        }
        if !turn.usage_is_incomplete
            && entries
                .iter()
                .all(|entry| !entry.usage_is_incomplete && entry.output_tokens.is_some())
            && turn.completion_kind == DiscussCompletionKind::Completed
            && !((model_group || filtered) && multi)
            && let (Some(output), Some(duration)) = (
                output,
                turn.end_to_end_ms
                    .and_then(|v| u64::try_from(v).ok())
                    .filter(|v| *v > 0),
            )
        {
            add_count(&mut coverage.turns_with_throughput, 1)?;
            add_count(&mut self.row.tokens.throughput_output_tokens, output)?;
            add_count(&mut self.row.tokens.throughput_end_to_end_ms, duration)?;
        }
        if let (Some(amount), Some(currency), Some(kind)) = (
            &turn.harness_cost_amount,
            &turn.harness_cost_currency,
            turn.harness_cost_kind,
        ) {
            match kind {
                DiscussHarnessCostKind::PerTurn if !turn.cost_is_partial && !(model_group && multi) => {
                    let total = self
                        .per_currency
                        .entry(currency.clone())
                        .or_insert(Decimal::ZERO);
                    *total = total
                        .checked_add(decimal(amount)?)
                        .ok_or(DiscussError::ReportAggregationOverflow)?;
                }
                DiscussHarnessCostKind::SessionCumulative if !model_group => {
                    let order = (turn.turn_started_at_ms, turn.turn_ordinal);
                    if self
                        .latest
                        .as_ref()
                        .is_none_or(|(time, ordinal, _)| order > (*time, *ordinal))
                    {
                        self.latest = Some((
                            order.0,
                            order.1,
                            DiscussHarnessCost {
                                amount: amount.clone(),
                                currency: currency.clone(),
                                is_partial: turn.cost_is_partial,
                            },
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish(mut self, key: GroupKey) -> Result<DiscussUsageReportRow, DiscussError> {
        self.row.key = key.projection();
        self.row.calculated_cost_usd = self.calculated.map(|value| value.to_string());
        self.row.harness_per_turn_cost = self
            .per_currency
            .into_iter()
            .map(|(currency, value)| (currency, value.to_string()))
            .collect();
        if matches!(key, GroupKey::Session(_)) {
            self.row.harness_session_cost_latest = self.latest.map(|(_, _, cost)| cost);
        } else {
            self.row.harness_session_cost_reason = Some("Only available for session groups".to_owned());
        }
        let tokens = &mut self.row.tokens;
        if tokens.throughput_end_to_end_ms > 0 {
            let numerator = u128::from(tokens.throughput_output_tokens) * 1_000_000_000;
            let denominator = u128::from(tokens.throughput_end_to_end_ms);
            let mut scaled = numerator / denominator;
            let remainder = numerator % denominator;
            if remainder * 2 > denominator || (remainder * 2 == denominator && scaled % 2 == 1) {
                scaled += 1;
            }
            let scaled = i128::try_from(scaled).map_err(|_| DiscussError::ReportAggregationOverflow)?;
            let rate =
                Decimal::try_from_i128_with_scale(scaled, 6).map_err(|_| DiscussError::ReportAggregationOverflow)?;
            tokens.end_to_end_turn_tokens_per_second = Some(format!("{rate:.6}"));
        }
        Ok(self.row)
    }
}

impl Actor {
    pub(super) async fn usage_report(
        &self,
        from: Option<i64>,
        to: Option<i64>,
        provider: Option<String>,
        model: Option<String>,
        group: DiscussUsageGroupBy,
        spend: bool,
    ) -> Result<DiscussResponse, DiscussError> {
        let (from, to) = range(from, to)?;
        let conn = self.persistence.acquire_connection().await?;
        let (turns, entries) = select_usage_range(&conn, from, to).await?;
        let mut by_turn: HashMap<(&str, i64), Vec<&DiscussModelUsageEntry>> = HashMap::new();
        for entry in &entries {
            by_turn
                .entry((&entry.discuss_session_id, entry.turn_ordinal))
                .or_default()
                .push(entry);
        }
        let mut groups: BTreeMap<GroupKey, Aggregate> = BTreeMap::new();
        for turn in &turns {
            if provider.as_ref().is_some_and(|p| p != &turn.provider_name) {
                continue;
            }
            let all = by_turn
                .get(&(turn.discuss_session_id.as_str(), turn.turn_ordinal))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let selected: Vec<_> = all
                .iter()
                .copied()
                .filter(|entry| {
                    model
                        .as_ref()
                        .is_none_or(|id| entry.model_id.as_ref() == Some(id))
                })
                .collect();
            if model.is_some() && selected.is_empty() {
                continue;
            }
            let multi = all.len() > 1;
            if group == DiscussUsageGroupBy::Model {
                if selected.is_empty() {
                    groups
                        .entry(usage_key(turn, None, group)?)
                        .or_default()
                        .record(turn, &[], true, false, false)?;
                }
                for entry in selected {
                    let key = usage_key(turn, entry.model_id.as_deref(), group)?;
                    groups
                        .entry(key)
                        .or_default()
                        .record(turn, &[entry], true, multi, model.is_some())?;
                }
            } else {
                let key = usage_key(turn, None, group)?;
                groups
                    .entry(key)
                    .or_default()
                    .record(turn, &selected, false, multi, model.is_some())?;
            }
        }
        let truncated = groups.len() > LIMIT;
        let rows = groups
            .into_iter()
            .take(LIMIT)
            .map(|(key, value)| value.finish(key))
            .collect::<Result<_, _>>()?;
        let report = DiscussUsageReport { rows, truncated };
        Ok(if spend {
            DiscussResponse::SpendReport { report }
        } else {
            DiscussResponse::TokenUsageReport { report }
        })
    }

    pub(super) async fn allowance_report(
        &self,
        from: Option<i64>,
        to: Option<i64>,
        provider: Option<String>,
        group: DiscussAllowanceGroupBy,
    ) -> Result<DiscussResponse, DiscussError> {
        let (from, to) = range(from, to)?;
        let conn = self.persistence.acquire_connection().await?;
        let observations = select_allowance_range(&conn, from, to).await?;
        let mut groups: BTreeMap<GroupKey, Vec<DiscussAllowanceSample>> = BTreeMap::new();
        for observation in observations {
            let projection: DiscussAllowanceProjection =
                serde_json::from_str(&observation.entry_json).map_err(|error| DiscussError::Persistence {
                    code: "allowance_decode".to_owned(),
                    message: error.to_string(),
                })?;
            if provider
                .as_ref()
                .is_some_and(|p| p != &observation.provider && p != &projection.display_provider)
            {
                continue;
            }
            let key = match group {
                DiscussAllowanceGroupBy::Provider => GroupKey::Provider(observation.provider.clone()),
                DiscussAllowanceGroupBy::Account => {
                    GroupKey::Account(observation.provider.clone(), observation.account_key.clone())
                }
                DiscussAllowanceGroupBy::UtcHour => GroupKey::Time(bucket(observation.observed_at_ms, 0)?),
                DiscussAllowanceGroupBy::UtcDay => GroupKey::Time(bucket(observation.observed_at_ms, 1)?),
                DiscussAllowanceGroupBy::UtcWeek => GroupKey::Time(bucket(observation.observed_at_ms, 2)?),
                DiscussAllowanceGroupBy::UtcMonth => GroupKey::Time(bucket(observation.observed_at_ms, 3)?),
            };
            groups.entry(key).or_default().push(DiscussAllowanceSample {
                display_provider: projection.display_provider.clone(),
                provider: observation.provider,
                account: observation.account_key,
                observed_at_ms: observation.observed_at_ms,
                fetched_at_ms: observation.fetched_at_ms,
                stale_since_ms: observation.stale_since_ms,
                error_class: observation.error_class,
                projection,
            });
        }
        let truncated = groups.len() > LIMIT;
        let rows = groups
            .into_iter()
            .take(LIMIT)
            .map(|(key, observations)| DiscussAllowanceReportRow {
                key: key.projection(),
                observation_count: observations.len() as u64,
                observations,
            })
            .collect();
        Ok(DiscussResponse::AllowanceReport {
            report: DiscussAllowanceReport { rows, truncated },
        })
    }

    pub(super) fn current_allowance_response(&self) -> Result<DiscussResponse, DiscussError> {
        let entries = self.current_allowance()?.to_vec();
        Ok(DiscussResponse::CurrentAllowance { entries })
    }
}

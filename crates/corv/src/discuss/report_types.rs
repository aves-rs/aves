use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use specta::Type;
/// Terminal outcome of an accounted turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussCompletionKind {
    /// The turn completed normally.
    Completed,
    /// The turn was cancelled.
    Cancelled,
    /// The turn failed.
    Failed,
}

/// Scope of a cost amount reported by a harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussHarnessCostKind {
    /// Cost for this turn alone.
    PerTurn,
    /// Cumulative session cost.
    SessionCumulative,
}

/// Source of the captured usage projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussUsageEvidenceSource {
    /// Standard prompt response.
    #[serde(rename = "prompt_response")]
    Response,
    /// Usage metadata on a prompt response.
    #[serde(rename = "prompt_response_meta")]
    ResponseMeta,
    /// Provider prompt-complete notification.
    #[serde(rename = "prompt_complete")]
    Complete,
}

/// One terminal turn's timing, occupancy, and harness cost evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussTurnRecord {
    pub discuss_session_id: String,
    pub turn_ordinal: i64,
    pub provider_name: String,
    pub model_id: Option<String>,
    pub turn_started_at_ms: i64,
    pub turn_completed_at_ms: Option<i64>,
    pub end_to_end_ms: Option<i64>,
    pub completion_kind: DiscussCompletionKind,
    pub stop_reason: Option<String>,
    pub context_used_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub harness_cost_amount: Option<String>,
    pub harness_cost_currency: Option<String>,
    pub harness_cost_kind: Option<DiscussHarnessCostKind>,
    pub usage_is_incomplete: bool,
    pub cost_is_partial: bool,
    pub evidence_source: DiscussUsageEvidenceSource,
    /// Bounded usage-only projection, never arbitrary provider metadata.
    pub evidence_json: Option<String>,
    pub created_at_ms: i64,
}

/// Token evidence and exact calculated cost attributed to one model, if known.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussModelUsageEntry {
    pub discuss_session_id: String,
    pub turn_ordinal: i64,
    pub provider_name: String,
    pub turn_started_at_ms: i64,
    pub model_id: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub thought_tokens: Option<u64>,
    pub cached_read_tokens: Option<u64>,
    pub cached_write_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub calculated_cost_usd: Option<String>,
    pub incomplete_categories: i64,
    pub usage_is_incomplete: bool,
    pub cost_is_partial: bool,
    pub pricing_basis: Option<String>,
    pub price_snapshot_json: Option<String>,
}

/// Dimensions available to usage and spend reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussUsageGroupBy {
    Session,
    Provider,
    Model,
    UtcHour,
    UtcDay,
    UtcWeek,
    UtcMonth,
}

/// Dimensions available to allowance reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscussAllowanceGroupBy {
    Provider,
    Account,
    UtcHour,
    UtcDay,
    UtcWeek,
    UtcMonth,
}

/// Report group identity; absent fields are not grouping dimensions.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussReportKey {
    pub session_id: Option<String>,
    pub provider: Option<String>,
    pub model_id: Option<String>,
    pub account: Option<String>,
    pub bucket_start_ms: Option<i64>,
}

/// Persisted evidence coverage for a report group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussReportCoverage {
    pub turns_total: u64,
    pub turns_with_token_evidence: u64,
    pub turns_with_calculated_cost: u64,
    pub turns_with_throughput: u64,
    pub turns_usage_incomplete: u64,
    pub turns_harness_cost_partial: u64,
    pub turns_excluded_multi_model: u64,
}

/// Nullable token sums and weighted end-to-end throughput.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussTokenSums {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub thought_tokens: Option<u64>,
    pub cached_read_tokens: Option<u64>,
    pub cached_write_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub throughput_output_tokens: u64,
    pub throughput_end_to_end_ms: u64,
    pub end_to_end_turn_tokens_per_second: Option<String>,
}

/// A cumulative harness cost observation, never summed with per-turn costs.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussHarnessCost {
    pub amount: String,
    pub currency: String,
    pub is_partial: bool,
}

/// A usage or spend report group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussUsageReportRow {
    pub key: DiscussReportKey,
    pub coverage: DiscussReportCoverage,
    pub tokens: DiscussTokenSums,
    pub calculated_cost_usd: Option<String>,
    pub harness_per_turn_cost: BTreeMap<String, String>,
    pub harness_session_cost_latest: Option<DiscussHarnessCost>,
    pub harness_session_cost_reason: Option<String>,
}

/// Bounded deterministic report output.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussUsageReport {
    pub rows: Vec<DiscussUsageReportRow>,
    pub truncated: bool,
}

/// An upstream allowance window, retaining its original units.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceWindow {
    pub used_percent: f64,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<String>,
    pub window_minutes: Option<i64>,
    pub window_kind: Option<String>,
    pub used_count: Option<f64>,
    pub total_count: Option<f64>,
}

/// A named or unnamed additional allowance window.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceExtraWindow {
    pub title: Option<String>,
    pub id: Option<String>,
    pub window: Option<DiscussAllowanceWindow>,
}

/// Account-wide and extra allowance windows.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceWindows {
    pub primary: Option<DiscussAllowanceWindow>,
    pub secondary: Option<DiscussAllowanceWindow>,
    pub tertiary: Option<DiscussAllowanceWindow>,
    pub extra_rate_windows: Option<Vec<DiscussAllowanceExtraWindow>>,
}

/// A pool amount in the upstream minor units and exponent.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceAmount {
    pub minor: i64,
    pub exponent: u8,
    pub unit: String,
}

/// An upstream balance or credit pool.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowancePool {
    pub id: String,
    pub label: String,
    pub funding: String,
    pub basis: String,
    pub remaining: Option<DiscussAllowanceAmount>,
    pub total: Option<DiscussAllowanceAmount>,
    pub spendable: Option<bool>,
    pub resets_at: Option<String>,
    pub expires_at: Option<String>,
    pub unlimited: Option<bool>,
}

/// One kind of saved reset credit.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceResetKind {
    pub kind: String,
    pub available_count: u32,
    pub soonest_expires_at: Option<String>,
}

/// Saved reset inventory and upstream credit expiry timestamps.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceSavedResets {
    pub available_count: u32,
    pub soonest_expires_at: Option<String>,
    pub credit_expiries: Vec<String>,
    pub kinds: Vec<DiscussAllowanceResetKind>,
}

/// Provider-supplied account display metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceAccountInfo {
    pub email: Option<String>,
    pub org_name: Option<String>,
    pub plan_type: Option<String>,
    pub subscription_renews_at: Option<String>,
    pub subscription_ends_at: Option<String>,
}

/// Reason and start time of a stale allowance reading.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceStale {
    pub since: String,
    pub class: Option<String>,
}

/// Normalized provider observation shared by historical and current reads.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceProjection {
    pub provider: String,
    pub display_provider: String,
    pub api_provider: Option<String>,
    pub account: Option<String>,
    pub account_info: Option<DiscussAllowanceAccountInfo>,
    pub fetched_at: Option<String>,
    pub stale: Option<DiscussAllowanceStale>,
    pub error: Option<String>,
    pub error_class: Option<String>,
    pub windows: Option<DiscussAllowanceWindows>,
    pub pools: Option<Vec<DiscussAllowancePool>>,
    pub saved_resets: Option<DiscussAllowanceSavedResets>,
}

/// A historical allowance sample and its normalized provider projection.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceSample {
    pub provider: String,
    pub display_provider: String,
    pub account: Option<String>,
    pub observed_at_ms: i64,
    pub fetched_at_ms: Option<i64>,
    pub stale_since_ms: Option<i64>,
    pub error_class: Option<String>,
    pub projection: DiscussAllowanceProjection,
}

/// A historical allowance group with its sampled observations.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceReportRow {
    pub key: DiscussReportKey,
    pub observation_count: u64,
    pub observations: Vec<DiscussAllowanceSample>,
}

/// Bounded historical allowance output.
#[derive(Clone, Debug, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscussAllowanceReport {
    pub rows: Vec<DiscussAllowanceReportRow>,
    pub truncated: bool,
}

use std::time::Instant;

use agent_client_protocol::v1::schema::PromptResponse;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use super::database::{PersistenceJob, enqueue_persistence_job};
use super::plugins::{BillingCategorySemantics, DiscussAgentPlugin};
use super::pricing::{PriceIndex, PriceSnapshot};
use super::types::{
    DiscussCompletionKind, DiscussHarnessCostKind, DiscussModelUsageEntry, DiscussTurnRecord,
    DiscussUsageEvidenceSource,
};

/// One model's observed per-turn token counts.
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct ModelUsage {
    pub model_id: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub thought_tokens: Option<u64>,
    pub cached_read_tokens: Option<u64>,
    pub cached_write_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// Bounded, normalized evidence; raw provider metadata is never retained.
#[derive(Clone, Debug)]
pub(super) struct DiscussUsageEvidence {
    pub source: DiscussUsageEvidenceSource,
    pub models: Vec<ModelUsage>,
    pub context_used_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub cost: Option<(String, String, DiscussHarnessCostKind)>,
    pub usage_is_incomplete: bool,
    pub cost_is_partial: bool,
    pub turn_correlatable: bool,
}

/// The raw notification or typed prompt response supplied to a plugin.
pub(super) enum UsageCaptureEvent<'a> {
    Notification { method: &'a str, params: &'a Value },
    PromptResponse(&'a PromptResponse),
}

/// Standard ACP capture shared by every provider.
pub(super) fn capture_standard_usage(event: &UsageCaptureEvent<'_>) -> Option<DiscussUsageEvidence> {
    let mut evidence = DiscussUsageEvidence {
        source: DiscussUsageEvidenceSource::Response,
        models: Vec::new(),
        context_used_tokens: None,
        context_window_tokens: None,
        cost: None,
        usage_is_incomplete: false,
        cost_is_partial: false,
        turn_correlatable: false,
    };
    match event {
        UsageCaptureEvent::PromptResponse(response) => {
            let usage = response.usage.as_ref()?;
            evidence.models.push(ModelUsage {
                model_id: None,
                input_tokens: Some(usage.input_tokens),
                output_tokens: Some(usage.output_tokens),
                thought_tokens: usage.thought_tokens,
                cached_read_tokens: usage.cached_read_tokens,
                cached_write_tokens: usage.cached_write_tokens,
                total_tokens: Some(usage.total_tokens),
            });
            evidence.turn_correlatable = true;
        }
        UsageCaptureEvent::Notification { method, params } => {
            if *method != "session/update" {
                return None;
            }
            let update = params.get("update")?;
            if update.get("sessionUpdate")?.as_str()? != "usage_update" {
                return None;
            }
            let (Some(used), Some(size)) = (
                update.get("used").and_then(Value::as_u64),
                update.get("size").and_then(Value::as_u64),
            ) else {
                tracing::debug!("Malformed ACP usage occupancy");
                return None;
            };
            evidence.context_used_tokens = Some(used);
            evidence.context_window_tokens = Some(size);
            if let Some(cost) = update.get("cost").filter(|cost| !cost.is_null()) {
                if let (Some(amount), Some(currency)) = (
                    cost.get("amount").and_then(Value::as_number),
                    cost.get("currency").and_then(Value::as_str),
                ) {
                    evidence.cost = Some((
                        amount.to_string(),
                        currency.to_owned(),
                        DiscussHarnessCostKind::SessionCumulative,
                    ));
                } else {
                    tracing::debug!("Malformed ACP usage cost");
                }
            }
        }
    }
    Some(evidence)
}

struct ActiveTurn {
    started: Instant,
    turn: DiscussTurnRecord,
    evidence: Vec<DiscussUsageEvidence>,
    pricing: Option<(PriceSnapshot, BillingCategorySemantics, Option<&'static str>)>,
}

/// Worker-local turn accounting with one retained late-evidence slot.
pub(super) struct UsageTurnAccumulator {
    session_id: String,
    provider: String,
    next_ordinal: i64,
    occupancy: (Option<u64>, Option<u64>),
    active: Option<ActiveTurn>,
    last: Option<ActiveTurn>,
    pricing: Option<(PriceIndex, BillingCategorySemantics, Option<&'static str>)>,
}

impl UsageTurnAccumulator {
    pub(super) fn new(session_id: String, provider: String, next_ordinal: i64) -> Self {
        Self {
            session_id,
            provider,
            next_ordinal,
            occupancy: (None, None),
            active: None,
            last: None,
            pricing: None,
        }
    }

    /// Attach the shared catalog and source-verified provider billing rules.
    pub(super) fn set_pricing(&mut self, index: PriceIndex, plugin: &dyn DiscussAgentPlugin) {
        self.pricing = Some((index, plugin.billing_semantics(), plugin.models_dev_provider_id()));
    }

    pub(super) fn dispatch(&mut self, model_id: Option<String>) {
        self.last = None;
        let now = chrono::Utc::now().timestamp_millis();
        let turn = DiscussTurnRecord {
            discuss_session_id: self.session_id.clone(),
            turn_ordinal: self.next_ordinal,
            provider_name: self.provider.clone(),
            model_id,
            turn_started_at_ms: now,
            turn_completed_at_ms: None,
            end_to_end_ms: None,
            completion_kind: DiscussCompletionKind::Completed,
            stop_reason: None,
            context_used_tokens: self.occupancy.0,
            context_window_tokens: self.occupancy.1,
            harness_cost_amount: None,
            harness_cost_currency: None,
            harness_cost_kind: None,
            usage_is_incomplete: false,
            cost_is_partial: false,
            evidence_source: DiscussUsageEvidenceSource::Response,
            evidence_json: None,
            created_at_ms: now,
        };
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.active = Some(ActiveTurn {
            started: Instant::now(),
            turn,
            evidence: Vec::new(),
            pricing: None,
        });
    }

    pub(super) async fn capture(&mut self, evidence: DiscussUsageEvidence, queue: &mpsc::Sender<PersistenceJob>) {
        if evidence.context_used_tokens.is_some() {
            self.occupancy.0 = evidence.context_used_tokens;
        }
        if evidence.context_window_tokens.is_some() {
            self.occupancy.1 = evidence.context_window_tokens;
        }
        if let Some(active) = &mut self.active {
            active.evidence.push(evidence);
        } else if evidence.turn_correlatable {
            if let Some(last) = &mut self.last {
                last.evidence.push(evidence);
                let (turn, entries) = assemble(last);
                enqueue_persistence_job(queue, PersistenceJob::UpdateTurnUsage { turn, entries }).await;
            } else {
                tracing::debug!("Dropping uncorrelated late usage evidence");
            }
        }
    }

    pub(super) async fn finish(
        &mut self,
        kind: DiscussCompletionKind,
        stop_reason: Option<String>,
        queue: &mpsc::Sender<PersistenceJob>,
    ) {
        let Some(mut active) = self.active.take() else {
            return;
        };
        active.turn.turn_completed_at_ms = Some(chrono::Utc::now().timestamp_millis());
        active.turn.end_to_end_ms = (kind == DiscussCompletionKind::Completed)
            .then(|| i64::try_from(active.started.elapsed().as_millis()).unwrap_or(i64::MAX));
        active.turn.completion_kind = kind;
        active.turn.stop_reason = stop_reason;
        active.turn.context_used_tokens = self.occupancy.0;
        active.turn.context_window_tokens = self.occupancy.1;
        active.pricing = self
            .pricing
            .as_ref()
            .map(|(index, semantics, provider)| (index.snapshot(), *semantics, *provider));
        let (turn, entries) = assemble(&active);
        enqueue_persistence_job(queue, PersistenceJob::UsageTurn { turn, entries }).await;
        self.last = Some(active);
    }
}

fn priority(source: DiscussUsageEvidenceSource) -> u8 {
    match source {
        DiscussUsageEvidenceSource::ResponseMeta => 3,
        DiscussUsageEvidenceSource::Complete => 2,
        DiscussUsageEvidenceSource::Response => 1,
    }
}

fn assemble(active: &ActiveTurn) -> (DiscussTurnRecord, Vec<DiscussModelUsageEntry>) {
    let mut turn = active.turn.clone();
    let mut sources: Vec<_> = active.evidence.iter().rev().collect();
    sources.sort_by_key(|item| std::cmp::Reverse(priority(item.source)));
    if let Some(first) = sources.first() {
        turn.evidence_source = first.source;
    }
    let mut models: Vec<ModelUsage> = Vec::new();
    for evidence in sources {
        turn.usage_is_incomplete |= evidence.usage_is_incomplete;
        turn.cost_is_partial |= evidence.cost_is_partial;
        if turn.harness_cost_amount.is_none()
            && let Some((amount, currency, kind)) = &evidence.cost
        {
            turn.harness_cost_amount = Some(amount.clone());
            turn.harness_cost_currency = Some(currency.clone());
            turn.harness_cost_kind = Some(*kind);
        }
        for model in &evidence.models {
            let mut model = model.clone();
            if model.model_id.is_none() {
                model.model_id = turn.model_id.clone();
            }
            if let Some(existing) = models
                .iter_mut()
                .find(|item| item.model_id == model.model_id)
            {
                existing.input_tokens = existing.input_tokens.or(model.input_tokens);
                existing.output_tokens = existing.output_tokens.or(model.output_tokens);
                existing.thought_tokens = existing.thought_tokens.or(model.thought_tokens);
                existing.cached_read_tokens = existing.cached_read_tokens.or(model.cached_read_tokens);
                existing.cached_write_tokens = existing.cached_write_tokens.or(model.cached_write_tokens);
                existing.total_tokens = existing.total_tokens.or(model.total_tokens);
            } else {
                models.push(model);
            }
        }
    }
    if !models.is_empty() || turn.harness_cost_amount.is_some() {
        turn.evidence_json = Some(serde_json::json!({"models": models, "cost_amount": turn.harness_cost_amount, "cost_currency": turn.harness_cost_currency, "cost_kind": turn.harness_cost_kind}).to_string());
    }
    let entries = models
        .into_iter()
        .map(|model| {
            let mut entry = DiscussModelUsageEntry {
                discuss_session_id: turn.discuss_session_id.clone(),
                turn_ordinal: turn.turn_ordinal,
                provider_name: turn.provider_name.clone(),
                turn_started_at_ms: turn.turn_started_at_ms,
                model_id: model.model_id,
                input_tokens: model.input_tokens,
                output_tokens: model.output_tokens,
                thought_tokens: model.thought_tokens,
                cached_read_tokens: model.cached_read_tokens,
                cached_write_tokens: model.cached_write_tokens,
                total_tokens: model.total_tokens,
                calculated_cost_usd: None,
                incomplete_categories: 0,
                usage_is_incomplete: turn.usage_is_incomplete,
                cost_is_partial: turn.cost_is_partial,
                pricing_basis: None,
                price_snapshot_json: None,
            };
            if let Some((snapshot, semantics, provider)) = &active.pricing {
                snapshot.price(&mut entry, *semantics, *provider, turn.context_used_tokens);
            }
            entry
        })
        .collect();
    (turn, entries)
}

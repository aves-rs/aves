use std::collections::{HashMap, HashSet};
use std::time::Duration;

use quota_core::model::{ProviderUsage, windows};
use quota_core::{Registry, config::QuotaConfig};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::database::persist_allowance_observation;
use super::storage::{DiscussAllowanceObservation, DiscussPersistence};

const CAPACITY: usize = 8;
const WINDOW_MS: i64 = 15 * 60 * 1000;
type Key = (String, Option<String>);

/// Actor-owned receiver and writer sender for one registry lifetime.
pub(super) struct AllowanceService {
    pub(super) observations: mpsc::Receiver<Vec<ProviderUsage>>,
    pub(super) writer: mpsc::Sender<Vec<ProviderUsage>>,
    registry_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
}

/// Start a single read-only registry and its bounded observation writer.
pub(super) fn spawn(persistence: DiscussPersistence, cancel: CancellationToken) -> AllowanceService {
    let (observation_tx, observations) = mpsc::channel(CAPACITY);
    let (writer, writer_rx) = mpsc::channel(CAPACITY);
    let registry_task = tokio::spawn(async move {
        let registry = Registry::with_defaults(QuotaConfig::default(), None);
        registry
            .refresh_loop_observed(cancel, move |entries| {
                let _ = observation_tx.try_send(entries);
                std::future::ready(())
            })
            .await;
    });
    let writer_task = tokio::spawn(write_observations(persistence, writer_rx));
    AllowanceService {
        observations,
        writer,
        registry_task,
        writer_task,
    }
}

impl AllowanceService {
    /// Cancel externally, then drain registry output and finish pending writes.
    pub(super) async fn stop(mut self) {
        if let Err(error) = self.registry_task.await {
            tracing::warn!(?error, "Allowance registry task failed");
        }
        while let Some(snapshot) = self.observations.recv().await {
            if self.writer.send(snapshot).await.is_err() {
                break;
            }
        }
        drop(self.writer);
        if let Err(error) = self.writer_task.await {
            tracing::warn!(?error, "Allowance writer task failed");
        }
    }
}

/// Display-only association; registry identity is unchanged.
pub(super) fn display_provider(provider: &str) -> &str {
    match provider {
        "claude" => "claude-acp",
        "codex" => "codex-acp",
        "grok" => "grok-build",
        "antigravity" => "antigravity-acp",
        "opencode" => "opencode",
        other => other,
    }
}

struct Sample {
    entry: ProviderUsage,
    observed_at_ms: i64,
    window: i64,
    pending: bool,
}

async fn write_observations(persistence: DiscussPersistence, mut receiver: mpsc::Receiver<Vec<ProviderUsage>>) {
    let mut samples: HashMap<Key, Sample> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            snapshot = receiver.recv() => match snapshot {
                Some(snapshot) => observe(&persistence, &mut samples, snapshot).await,
                None => break,
            },
            _ = ticker.tick() => flush_expired(&persistence, &mut samples, chrono::Utc::now().timestamp_millis()).await,
        }
    }
    for sample in samples.values_mut() {
        flush(&persistence, sample).await;
    }
}

async fn observe(persistence: &DiscussPersistence, samples: &mut HashMap<Key, Sample>, snapshot: Vec<ProviderUsage>) {
    let now = chrono::Utc::now().timestamp_millis();
    let mut seen = HashSet::new();
    for entry in snapshot {
        let key = (entry.provider.clone(), entry.account.clone());
        seen.insert(key.clone());
        let next_window = now.div_euclid(WINDOW_MS);
        if let Some(previous) = samples.get_mut(&key) {
            let transition = previous.entry.error_class != entry.error_class
                || reset_crossed(&previous.entry, previous.observed_at_ms, now);
            if previous.window != next_window || transition {
                flush(persistence, previous).await;
            }
            previous.entry = entry;
            previous.observed_at_ms = now;
            previous.window = next_window;
            previous.pending = true;
            if transition {
                flush(persistence, previous).await;
            }
        } else {
            let mut sample = Sample {
                entry,
                observed_at_ms: now,
                window: next_window,
                pending: true,
            };
            flush(persistence, &mut sample).await;
            samples.insert(key, sample);
        }
    }
    let missing: Vec<_> = samples
        .keys()
        .filter(|key| !seen.contains(*key))
        .cloned()
        .collect();
    for key in missing {
        if let Some(mut sample) = samples.remove(&key) {
            flush(persistence, &mut sample).await;
            persist_disappearance(persistence, &sample.entry, now).await;
        }
    }
}

async fn persist_disappearance(persistence: &DiscussPersistence, entry: &ProviderUsage, now: i64) {
    let entry_json = match serde_json::to_string(entry) {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(?error, "Could not serialize absent allowance account");
            return;
        }
    };
    let observation = DiscussAllowanceObservation {
        provider: entry.provider.clone(),
        account_key: entry.account.clone(),
        fetched_at_ms: entry.fetched_at.as_deref().and_then(timestamp),
        observed_at_ms: now,
        stale_since_ms: entry
            .stale
            .as_ref()
            .and_then(|stale| timestamp(&stale.since)),
        error_class: Some("account_absent".to_owned()),
        entry_json,
        created_at_ms: now,
    };
    if let Err(error) = persist_allowance_observation(persistence, &observation).await {
        tracing::warn!(?error, "Could not persist absent allowance account");
    }
}

fn reset_crossed(entry: &ProviderUsage, previous: i64, now: i64) -> bool {
    entry
        .usage
        .iter()
        .flat_map(windows)
        .filter_map(|window| window.resets_at.as_deref())
        .filter_map(timestamp)
        .any(|reset| previous < reset && reset <= now)
}

fn timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.timestamp_millis())
}

async fn flush_expired(persistence: &DiscussPersistence, samples: &mut HashMap<Key, Sample>, now: i64) {
    let current = now.div_euclid(WINDOW_MS);
    for sample in samples
        .values_mut()
        .filter(|sample| sample.window < current)
    {
        flush(persistence, sample).await;
    }
}

async fn flush(persistence: &DiscussPersistence, sample: &mut Sample) {
    if !sample.pending {
        return;
    }
    let entry = &sample.entry;
    let entry_json = match serde_json::to_string(entry) {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(?error, "Could not serialize allowance observation");
            return;
        }
    };
    let observation = DiscussAllowanceObservation {
        provider: entry.provider.clone(),
        account_key: entry.account.clone(),
        fetched_at_ms: entry.fetched_at.as_deref().and_then(timestamp),
        observed_at_ms: sample.observed_at_ms,
        stale_since_ms: entry
            .stale
            .as_ref()
            .and_then(|stale| timestamp(&stale.since)),
        error_class: entry.error_class.clone(),
        entry_json,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    match persist_allowance_observation(persistence, &observation).await {
        Ok(()) => sample.pending = false,
        Err(error) => tracing::warn!(?error, "Could not persist allowance observation"),
    }
}

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::database::persist_allowance_observation;
use super::report_types::{DiscussAllowanceProjection, DiscussAllowanceWindow, DiscussAllowanceWindows};
use super::storage::{DiscussAllowanceObservation, DiscussPersistence};

const CAPACITY: usize = 8;
const WINDOW_MS: i64 = 15 * 60 * 1000;
type Key = (String, Option<String>);

/// Actor-owned writer sender for one server lifetime.
pub(super) struct AllowanceService {
    pub(super) writer: mpsc::Sender<Vec<DiscussAllowanceProjection>>,
    writer_task: JoinHandle<()>,
}

/// Start the bounded observation writer for host-supplied snapshots.
pub(super) fn spawn(persistence: DiscussPersistence) -> AllowanceService {
    let (writer, writer_rx) = mpsc::channel(CAPACITY);
    let writer_task = tokio::spawn(write_observations(persistence, writer_rx));
    AllowanceService { writer, writer_task }
}

impl AllowanceService {
    /// Close the writer and finish pending writes.
    pub(super) async fn stop(self) {
        drop(self.writer);
        if let Err(error) = self.writer_task.await {
            tracing::warn!(?error, "Allowance writer task failed");
        }
    }
}

struct Sample {
    entry: DiscussAllowanceProjection,
    observed_at_ms: i64,
    window: i64,
    pending: bool,
}

async fn write_observations(
    persistence: DiscussPersistence,
    mut receiver: mpsc::Receiver<Vec<DiscussAllowanceProjection>>,
) {
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

async fn observe(
    persistence: &DiscussPersistence,
    samples: &mut HashMap<Key, Sample>,
    snapshot: Vec<DiscussAllowanceProjection>,
) {
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

async fn persist_disappearance(persistence: &DiscussPersistence, entry: &DiscussAllowanceProjection, now: i64) {
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

fn reset_crossed(entry: &DiscussAllowanceProjection, previous: i64, now: i64) -> bool {
    entry
        .windows
        .iter()
        .flat_map(windows)
        .filter_map(|window| window.resets_at.as_deref())
        .filter_map(timestamp)
        .any(|reset| previous < reset && reset <= now)
}

/// Every populated rate window, including named extra windows.
fn windows(windows: &DiscussAllowanceWindows) -> impl Iterator<Item = &DiscussAllowanceWindow> {
    [&windows.primary, &windows.secondary, &windows.tertiary]
        .into_iter()
        .flatten()
        .chain(
            windows
                .extra_rate_windows
                .iter()
                .flatten()
                .filter_map(|extra| extra.window.as_ref()),
        )
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

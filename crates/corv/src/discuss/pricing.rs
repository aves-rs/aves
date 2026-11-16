use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::database::persist_models_catalog;
use super::plugins::{
    BILLING_CACHED_READ, BILLING_CACHED_WRITE, BILLING_INPUT, BILLING_OUTPUT, BILLING_THOUGHT, BILLING_TOTAL,
    BillingCategorySemantics,
};
use super::storage::{DiscussModelsCatalog, DiscussPersistence, load_models_catalog};
use super::types::DiscussModelUsageEntry;

const BASIS: &str = "base tier; tiered/batch/long-context rates not applied";
const DAY: Duration = Duration::from_secs(86_400);
const CATALOG_URL: &str = "https://models.dev/api.json";
const MAX_CATALOG_BYTES: usize = 32 * 1024 * 1024;

type Catalog = HashMap<(String, String), ModelCost>;

#[derive(Deserialize)]
struct ProviderData {
    models: HashMap<String, ModelData>,
}

#[derive(Deserialize)]
struct ModelData {
    cost: Option<RawCost>,
}

#[derive(Deserialize)]
struct RawCost {
    input: Box<RawValue>,
    output: Box<RawValue>,
    cache_read: Option<Box<RawValue>>,
    cache_write: Option<Box<RawValue>>,
}

struct Price {
    text: String,
    value: Decimal,
}

struct ModelCost {
    input: Price,
    output: Price,
    cache_read: Option<Price>,
    cache_write: Option<Price>,
}

#[derive(Serialize, Deserialize, Default)]
struct CacheValidators {
    etag: Option<String>,
    last_modified: Option<String>,
}

fn parse_price(raw: &RawValue) -> Option<Price> {
    let text = raw.get();
    let value = Decimal::from_str(text)
        .or_else(|_| Decimal::from_scientific(text))
        .ok()?;
    Some(Price {
        text: text.to_owned(),
        value,
    })
}

fn parse_catalog(body: &str) -> Result<Catalog, serde_json::Error> {
    let providers: HashMap<String, ProviderData> = serde_json::from_str(body)?;
    let mut catalog = HashMap::new();
    for (provider, data) in providers {
        for (model, data) in data.models {
            let Some(cost) = data.cost else { continue };
            let parsed = (|| {
                Some(ModelCost {
                    input: parse_price(&cost.input)?,
                    output: parse_price(&cost.output)?,
                    cache_read: optional_price(cost.cache_read.as_deref())?,
                    cache_write: optional_price(cost.cache_write.as_deref())?,
                })
            })();
            if let Some(cost) = parsed {
                catalog.insert((provider.clone(), model), cost);
            } else {
                tracing::debug!(%provider, %model, "Invalid model price");
            }
        }
    }
    Ok(catalog)
}

fn optional_price(raw: Option<&RawValue>) -> Option<Option<Price>> {
    match raw {
        Some(raw) => Some(Some(parse_price(raw)?)),
        None => Some(None),
    }
}

struct PriceCatalog {
    catalog: Catalog,
    fetched_at_ms: i64,
}

/// Shared last-good catalog, read synchronously by session workers.
#[derive(Clone, Default)]
pub(super) struct PriceIndex(Arc<RwLock<Option<Arc<PriceCatalog>>>>);

/// An immutable catalog version retained by a finalized turn.
#[derive(Clone)]
pub(super) struct PriceSnapshot(Option<Arc<PriceCatalog>>);

impl PriceIndex {
    /// Capture the current catalog version without copying its contents.
    pub(super) fn snapshot(&self) -> PriceSnapshot {
        PriceSnapshot(self.0.read().ok().and_then(|catalog| catalog.clone()))
    }
}

impl PriceSnapshot {
    /// Price one model using only an exact provider/model catalog match.
    pub(super) fn price(
        &self,
        entry: &mut DiscussModelUsageEntry,
        semantics: BillingCategorySemantics,
        provider: Option<&str>,
    ) {
        let missing = missing_categories(entry, semantics);
        entry.incomplete_categories = i64::from(missing);
        if !semantics.declared() || missing != 0 {
            return;
        }
        let Some(model) = entry.model_id.as_deref() else {
            return;
        };
        let pair = model
            .split_once('/')
            .or_else(|| provider.map(|provider| (provider, model)));
        let Some((provider, model)) = pair.filter(|(provider, model)| !provider.is_empty() && !model.is_empty()) else {
            return;
        };
        let Some(catalog) = self.0.as_ref() else {
            return;
        };
        let Some(cost) = catalog
            .catalog
            .get(&(provider.to_owned(), model.to_owned()))
        else {
            return;
        };
        let (amount, missing) = calculate_cost(entry, semantics, cost);
        entry.incomplete_categories |= i64::from(missing);
        if let Some(amount) = amount {
            entry.calculated_cost_usd = Some(amount.to_string());
            entry.pricing_basis = Some(BASIS.to_owned());
            entry.price_snapshot_json = Some(
                serde_json::json!({
                    "provider": provider, "model": model, "fetched_at_ms": catalog.fetched_at_ms,
                    "input": cost.input.text, "output": cost.output.text,
                    "cache_read": cost.cache_read.as_ref().map(|price| &price.text),
                    "cache_write": cost.cache_write.as_ref().map(|price| &price.text),
                })
                .to_string(),
            );
        }
    }
}

fn missing_categories(entry: &DiscussModelUsageEntry, semantics: BillingCategorySemantics) -> u8 {
    [
        (BILLING_INPUT, entry.input_tokens),
        (BILLING_OUTPUT, entry.output_tokens),
        (BILLING_THOUGHT, entry.thought_tokens),
        (BILLING_CACHED_READ, entry.cached_read_tokens),
        (BILLING_CACHED_WRITE, entry.cached_write_tokens),
        (BILLING_TOTAL, entry.total_tokens),
    ]
    .into_iter()
    .fold(0, |missing, (bit, value)| {
        missing
            | if value.is_none() {
                semantics.guaranteed_categories & bit
            } else {
                0
            }
    })
}

fn calculate_cost(
    entry: &DiscussModelUsageEntry,
    semantics: BillingCategorySemantics,
    cost: &ModelCost,
) -> (Option<Decimal>, u8) {
    let mut missing = missing_categories(entry, semantics);
    if entry.input_tokens.is_none() {
        missing |= BILLING_INPUT;
    }
    if entry.output_tokens.is_none() {
        missing |= BILLING_OUTPUT;
    }
    if entry.cached_read_tokens.is_some_and(|count| count > 0) && cost.cache_read.is_none() {
        missing |= BILLING_CACHED_READ;
    }
    if entry.cached_write_tokens.is_some_and(|count| count > 0) && cost.cache_write.is_none() {
        missing |= BILLING_CACHED_WRITE;
    }
    if missing != 0 {
        return (None, missing);
    }
    let mut input = entry.input_tokens.unwrap_or_default();
    if semantics.input_includes_cache == Some(true) {
        input = input
            .saturating_sub(entry.cached_read_tokens.unwrap_or_default())
            .saturating_sub(entry.cached_write_tokens.unwrap_or_default());
    }
    let total = (|| {
        let mut value = charge(input, &cost.input)?.checked_add(charge(entry.output_tokens?, &cost.output)?)?;
        if let Some(price) = &cost.cache_read {
            value = value.checked_add(charge(entry.cached_read_tokens.unwrap_or_default(), price)?)?;
        }
        if let Some(price) = &cost.cache_write {
            value = value.checked_add(charge(entry.cached_write_tokens.unwrap_or_default(), price)?)?;
        }
        value.checked_div(Decimal::from(1_000_000u64))
    })();
    (total, 0)
}

fn charge(tokens: u64, rate: &Price) -> Option<Decimal> {
    rate.value.checked_mul(Decimal::from(tokens))
}

/// Start the catalog service; the shared index remains empty until a catalog loads.
pub(super) fn spawn(persistence: DiscussPersistence, cancel: CancellationToken, index: PriceIndex) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut persisted = match persistence.acquire_connection().await {
            Ok(conn) => match load_models_catalog(&conn).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(?error, "Could not load models catalog");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(?error, "Could not open models catalog");
                None
            }
        };
        if let Some(row) = &persisted {
            match parse_catalog(&row.body) {
                Ok(catalog) => set_index(&index, catalog, row.fetched_at_ms),
                Err(error) => {
                    tracing::warn!(?error, "Invalid stored models catalog");
                    persisted = None;
                }
            }
        }
        let client = match Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(?error, "Could not create models catalog client");
                return;
            }
        };
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = refresh(&client, &persistence, &index, &mut persisted) => {}
            }
            tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(DAY) => {} }
        }
    })
}

async fn refresh(
    client: &Client,
    persistence: &DiscussPersistence,
    index: &PriceIndex,
    row: &mut Option<DiscussModelsCatalog>,
) {
    let metadata = row
        .as_ref()
        .and_then(|row| serde_json::from_str::<CacheValidators>(&row.metadata_json).ok())
        .unwrap_or_default();
    let mut request = client.get(CATALOG_URL);
    if let Some(etag) = &metadata.etag {
        request = request.header(IF_NONE_MATCH, etag);
    }
    if let Some(last_modified) = &metadata.last_modified {
        request = request.header(IF_MODIFIED_SINCE, last_modified);
    }
    let mut response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(?error, "Models catalog refresh failed; retaining last good catalog");
            return;
        }
    };
    let status = response.status();
    let validators = CacheValidators {
        etag: response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        last_modified: response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    };
    if status == StatusCode::NOT_MODIFIED {
        let validators = CacheValidators {
            etag: validators.etag.or(metadata.etag),
            last_modified: validators.last_modified.or(metadata.last_modified),
        };
        if let (Some(existing), Ok(metadata_json)) = (row.as_ref(), serde_json::to_string(&validators)) {
            let mut updated = existing.clone();
            updated.metadata_json = metadata_json;
            match persist_models_catalog(persistence, &updated).await {
                Ok(()) => *row = Some(updated),
                Err(error) => tracing::warn!(?error, "Could not persist catalog validators"),
            }
        }
        return;
    }
    if status != StatusCode::OK {
        tracing::warn!(%status, "Models catalog refresh failed; retaining last good catalog");
        return;
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if chunk.len() <= MAX_CATALOG_BYTES - body.len() => body.extend_from_slice(&chunk),
            Ok(Some(_)) => {
                tracing::warn!("Models catalog exceeds size limit; retaining last good catalog");
                return;
            }
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(?error, "Models catalog download failed; retaining last good catalog");
                return;
            }
        }
    }
    save_download(persistence, index, row, body, validators).await;
}

async fn save_download(
    persistence: &DiscussPersistence,
    index: &PriceIndex,
    row: &mut Option<DiscussModelsCatalog>,
    body: Vec<u8>,
    validators: CacheValidators,
) {
    let (Ok(body), Ok(metadata_json)) = (String::from_utf8(body), serde_json::to_string(&validators)) else {
        tracing::warn!("Could not encode models catalog");
        return;
    };
    let catalog = match parse_catalog(&body) {
        Ok(catalog) => catalog,
        Err(error) => {
            tracing::warn!(?error, "Invalid models catalog; retaining last good catalog");
            return;
        }
    };
    let updated = DiscussModelsCatalog {
        body,
        metadata_json,
        fetched_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    match persist_models_catalog(persistence, &updated).await {
        Ok(()) => {
            set_index(index, catalog, updated.fetched_at_ms);
            *row = Some(updated);
        }
        Err(error) => tracing::warn!(?error, "Could not persist models catalog"),
    }
}

fn set_index(index: &PriceIndex, catalog: Catalog, fetched_at_ms: i64) {
    if let Ok(mut guard) = index.0.write() {
        *guard = Some(Arc::new(PriceCatalog { catalog, fetched_at_ms }));
    }
}

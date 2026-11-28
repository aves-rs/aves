use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

use super::assets::atomic_write;
use super::database::persist_models_catalog;
use super::plugins::{
    BILLING_CACHED_READ, BILLING_CACHED_WRITE, BILLING_INPUT, BILLING_OUTPUT, BILLING_THOUGHT, BILLING_TOTAL,
    BillingCategorySemantics,
};
use super::report_types::DiscussModelUsageEntry;
use super::storage::{DiscussModelsCatalog, DiscussPersistence, load_models_catalog};

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
    experimental: Option<Box<RawValue>>,
    release_date: Option<String>,
}

#[derive(Deserialize)]
struct RawCost {
    input: Box<RawValue>,
    output: Box<RawValue>,
    cache_read: Option<Box<RawValue>>,
    cache_write: Option<Box<RawValue>>,
    tiers: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawTier {
    input: Box<RawValue>,
    output: Box<RawValue>,
    cache_read: Option<Box<RawValue>>,
    cache_write: Option<Box<RawValue>>,
    tier: RawTierBound,
}

#[derive(Deserialize)]
struct RawTierBound {
    #[serde(rename = "type")]
    kind: String,
    size: u64,
}

#[derive(Deserialize)]
struct RawExperimental {
    modes: Option<HashMap<String, RawMode>>,
}

#[derive(Deserialize)]
struct RawMode {
    cost: Option<RawCost>,
}

struct Price {
    text: String,
    value: Decimal,
}

struct Rates {
    input: Price,
    output: Price,
    cache_read: Option<Price>,
    cache_write: Option<Price>,
}

/// Rates charged once the turn's context exceeds `above` tokens.
struct ContextTier {
    above: u64,
    rates: Rates,
}

struct ModelCost {
    base: Rates,
    tiers: Vec<ContextTier>,
    mode: Option<String>,
    /// models.dev release date (ISO), used to resolve a family alias to its newest model.
    release_date: Option<String>,
}

impl ModelCost {
    /// Select the highest context tier the turn exceeds; unknown context uses base rates.
    fn rates_for(&self, context_tokens: Option<u64>) -> (&Rates, Option<u64>) {
        context_tokens
            .and_then(|used| self.tiers.iter().rev().find(|tier| used > tier.above))
            .map_or((&self.base, None), |tier| (&tier.rates, Some(tier.above)))
    }

    fn basis(&self, tier: Option<u64>, context_tokens: Option<u64>) -> String {
        let mut basis = match &self.mode {
            Some(mode) => format!("models.dev {mode} mode rates"),
            None => "models.dev standard rates".to_owned(),
        };
        match (tier, context_tokens) {
            (Some(above), _) => basis.push_str(&format!("; context tier above {above} tokens")),
            (None, None) if !self.tiers.is_empty() => basis.push_str("; context unknown, context tiers not applied"),
            _ => {}
        }
        basis.push_str("; batch rates not applied");
        basis
    }
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

fn parse_rates(
    input: &RawValue,
    output: &RawValue,
    cache_read: Option<&RawValue>,
    cache_write: Option<&RawValue>,
) -> Option<Rates> {
    Some(Rates {
        input: parse_price(input)?,
        output: parse_price(output)?,
        cache_read: optional_price(cache_read)?,
        cache_write: optional_price(cache_write)?,
    })
}

fn parse_cost(cost: &RawCost, mode: Option<&str>, release_date: Option<&str>) -> Option<ModelCost> {
    let base = parse_rates(
        &cost.input,
        &cost.output,
        cost.cache_read.as_deref(),
        cost.cache_write.as_deref(),
    )?;
    let mut tiers: Vec<ContextTier> = cost
        .tiers
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<RawTier>>(raw.get()).ok())
        .unwrap_or_default()
        .iter()
        .filter(|tier| tier.tier.kind == "context")
        .filter_map(|tier| {
            Some(ContextTier {
                above: tier.tier.size,
                rates: parse_rates(
                    &tier.input,
                    &tier.output,
                    tier.cache_read.as_deref(),
                    tier.cache_write.as_deref(),
                )?,
            })
        })
        .collect();
    tiers.sort_by_key(|tier| tier.above);
    Some(ModelCost {
        base,
        tiers,
        mode: mode.map(str::to_owned),
        release_date: release_date.map(str::to_owned),
    })
}

/// Index exact models first, then each priced mode as `<model>-<mode>` unless an exact entry owns that name.
fn parse_catalog(body: &str) -> Result<Catalog, serde_json::Error> {
    let providers: HashMap<String, ProviderData> = serde_json::from_str(body)?;
    let mut catalog = HashMap::new();
    let mut modes = Vec::new();
    for (provider, data) in providers {
        for (model, data) in data.models {
            if let Some(cost) = &data.cost {
                match parse_cost(cost, None, data.release_date.as_deref()) {
                    Some(cost) => {
                        catalog.insert((provider.clone(), model.clone()), cost);
                    }
                    None => tracing::debug!(%provider, %model, "Invalid model price"),
                }
            }
            let Some(experimental) = data
                .experimental
                .as_deref()
                .and_then(|raw| serde_json::from_str::<RawExperimental>(raw.get()).ok())
            else {
                continue;
            };
            for (mode, raw) in experimental.modes.unwrap_or_default() {
                if let Some(cost) = raw
                    .cost
                    .as_ref()
                    .and_then(|cost| parse_cost(cost, Some(&mode), data.release_date.as_deref()))
                {
                    modes.push(((provider.clone(), format!("{model}-{mode}")), cost));
                }
            }
        }
    }
    for (key, cost) in modes {
        catalog.entry(key).or_insert(cost);
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
    /// Price one model from its exact provider/model entry, or a `<model>-<mode>` mode entry, at the turn's context tier.
    /// A provider prefix is first mapped through `aliases`; a bare family alias such as `opus` resolves to the
    /// provider's newest released model carrying that name.
    pub(super) fn price(
        &self,
        entry: &mut DiscussModelUsageEntry,
        semantics: BillingCategorySemantics,
        provider: Option<&str>,
        aliases: &[(&str, &str)],
        context_tokens: Option<u64>,
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
        let provider = aliases
            .iter()
            .find(|(from, _)| *from == provider)
            .map_or(provider, |(_, to)| *to);
        let Some(catalog) = self.0.as_ref() else {
            return;
        };
        let (model, cost, alias) = match catalog
            .catalog
            .get(&(provider.to_owned(), model.to_owned()))
        {
            Some(cost) => (model, cost, None),
            None => match resolve_family_alias(&catalog.catalog, provider, model) {
                Some((resolved, cost)) => (resolved, cost, Some(model)),
                None => return,
            },
        };
        let (rates, tier) = cost.rates_for(context_tokens);
        let (amount, missing) = calculate_cost(entry, semantics, rates);
        entry.incomplete_categories |= i64::from(missing);
        if let Some(amount) = amount {
            entry.calculated_cost_usd = Some(amount.to_string());
            let mut basis = cost.basis(tier, context_tokens);
            if let Some(alias) = alias {
                basis.push_str(&format!("; alias {alias} resolved to newest {model}"));
            }
            entry.pricing_basis = Some(basis);
            entry.price_snapshot_json = Some(
                serde_json::json!({
                    "provider": provider, "model": model, "fetched_at_ms": catalog.fetched_at_ms,
                    "mode": cost.mode, "context_tokens": context_tokens, "context_tier_above": tier,
                    "input": rates.input.text, "output": rates.output.text,
                    "cache_read": rates.cache_read.as_ref().map(|price| &price.text),
                    "cache_write": rates.cache_write.as_ref().map(|price| &price.text),
                })
                .to_string(),
            );
        }
    }
}

/// Resolve a single-word family alias (`opus`) to the provider's newest standard-rate model whose ID contains it.
fn resolve_family_alias<'a>(catalog: &'a Catalog, provider: &str, alias: &str) -> Option<(&'a str, &'a ModelCost)> {
    if alias.is_empty() || alias.contains('-') {
        return None;
    }
    catalog
        .iter()
        .filter(|((owner, id), cost)| {
            owner == provider && cost.mode.is_none() && id.split('-').any(|part| part == alias)
        })
        .max_by(|((_, a_id), a), ((_, b_id), b)| (&a.release_date, a_id).cmp(&(&b.release_date, b_id)))
        .map(|((_, id), cost)| (id.as_str(), cost))
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
    cost: &Rates,
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
        let mut output = entry.output_tokens?;
        if semantics.output_excludes_thought {
            output = output.checked_add(entry.thought_tokens.unwrap_or_default())?;
        }
        let mut value = charge(input, &cost.input)?.checked_add(charge(output, &cost.output)?)?;
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
pub(super) fn spawn(
    persistence: DiscussPersistence,
    cancel: CancellationToken,
    index: PriceIndex,
    cache_root: Option<PathBuf>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let cache_file = cache_root.map(|root| root.join("models.dev/catalog.json"));
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
                Ok(catalog) => {
                    set_index(&index, catalog, row.fetched_at_ms);
                    publish_catalog(cache_file.as_deref(), &row.body).await;
                }
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
                _ = refresh(&client, &persistence, &index, &mut persisted, cache_file.as_deref()) => {}
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
    cache_file: Option<&Path>,
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
    save_download(persistence, index, row, body, validators, cache_file).await;
}

async fn save_download(
    persistence: &DiscussPersistence,
    index: &PriceIndex,
    row: &mut Option<DiscussModelsCatalog>,
    body: Vec<u8>,
    validators: CacheValidators,
    cache_file: Option<&Path>,
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
            publish_catalog(cache_file, &updated.body).await;
            *row = Some(updated);
        }
        Err(error) => tracing::warn!(?error, "Could not persist models catalog"),
    }
}

/// Publish a disposable copy without coupling it to accounting persistence.
async fn publish_catalog(path: Option<&Path>, body: &str) {
    let Some(path) = path else { return };
    let (path, body) = (path.to_owned(), body.as_bytes().to_vec());
    match tokio::task::spawn_blocking(move || atomic_write(&path, &body)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(?error, "Could not write models catalog cache"),
        Err(error) => tracing::warn!(?error, "Models catalog cache writer stopped"),
    }
}

fn set_index(index: &PriceIndex, catalog: Catalog, fetched_at_ms: i64) {
    if let Ok(mut guard) = index.0.write() {
        *guard = Some(Arc::new(PriceCatalog { catalog, fetched_at_ms }));
    }
}

#[cfg(test)]
mod cache_tests {
    use super::publish_catalog;

    #[tokio::test]
    async fn catalog_copy_is_created_and_replaced_with_raw_json() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("models.dev/catalog.json");
        let first = r#"{"openai":{"models":{}}}"#;
        let second = r#"{"anthropic":{"models":{}}}"#;
        publish_catalog(Some(&file), first).await;
        assert_eq!(std::fs::read_to_string(&file).unwrap(), first);
        publish_catalog(Some(&file), second).await;
        assert_eq!(std::fs::read_to_string(&file).unwrap(), second);
    }
}

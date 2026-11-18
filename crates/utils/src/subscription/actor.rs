//! Provider allowance observation through insula's `quota-core`, recorded into Discuss.

use std::env::home_dir;
use std::sync::Arc;

use quota_core::Registry;
use quota_core::config::QuotaConfig;
use quota_core::credential_source::CredentialSource;
use quota_core::model::{Amount, Pool, PoolBasis, PoolFunding, ProviderUsage, RateWindow, Usage};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use aves::bus::BusRpcContext;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;
use corv::{
    DiscussAllowanceAccountInfo, DiscussAllowanceAmount, DiscussAllowanceExtraWindow, DiscussAllowancePool,
    DiscussAllowanceProjection, DiscussAllowanceResetKind, DiscussAllowanceSavedResets, DiscussAllowanceStale,
    DiscussAllowanceWindow, DiscussAllowanceWindows, DiscussError, RecordAllowanceInput, dispatch_record_allowance,
};

use super::messages::{SubscriptionRequest, SubscriptionRequestPayload, SubscriptionResponse};
use super::plugins::CliCredentials;

/// Subscription actor owning the running allowance observation, if any.
pub(super) struct Actor {
    receiver: mpsc::Receiver<SubscriptionRequest>,
    broadcast_tx: mpsc::Sender<SubscriptionResponse>,
    root_context: BusRpcContext,
    running: Option<CancellationToken>,
}

impl Actor {
    pub(super) fn new(
        receiver: mpsc::Receiver<SubscriptionRequest>,
        broadcast_tx: mpsc::Sender<SubscriptionResponse>,
        root_context: BusRpcContext,
    ) -> Self {
        Self {
            receiver,
            broadcast_tx,
            root_context,
            running: None,
        }
    }

    async fn handle_message(&mut self, request: SubscriptionRequest) {
        let SubscriptionRequest { payload, reply_to } = request;
        let response = match payload {
            SubscriptionRequestPayload::StartServer => {
                self.start_server();
                SubscriptionResponse::ServerStarted
            }
            SubscriptionRequestPayload::StopServer => {
                self.stop_server();
                SubscriptionResponse::ServerStopped
            }
        };
        let _ = reply_to.send(Ok(response.clone()));
        let _ = self.broadcast_tx.send(response).await;
    }

    /// Start one registry lifetime unless one is already running.
    fn start_server(&mut self) {
        if self.running.is_some() {
            return;
        }
        let cancel = CancellationToken::new();
        observe(self.root_context.clone(), cancel.clone());
        self.running = Some(cancel);
    }

    fn stop_server(&mut self) {
        if let Some(cancel) = self.running.take() {
            cancel.cancel();
        }
    }
}

/// Run the registry until `cancel` fires and forward each snapshot to Discuss.
fn observe(context: BusRpcContext, cancel: CancellationToken) {
    let (sender, mut receiver) = mpsc::channel::<Vec<ProviderUsage>>(DEFAULT_BUFFER_SIZE);

    tokio::spawn(async move {
        let credentials = home_dir().map(|home| Arc::new(CliCredentials::new(home)) as Arc<dyn CredentialSource>);
        let registry = Registry::with_defaults(QuotaConfig::default(), credentials);
        registry
            .refresh_loop_observed(cancel, move |entries| {
                let _ = sender.try_send(entries);
                std::future::ready(())
            })
            .await;
    });

    tokio::spawn(async move {
        while let Some(entries) = receiver.recv().await {
            let entries = entries.iter().map(projection).collect();
            match dispatch_record_allowance(&context, RecordAllowanceInput { entries }).await {
                Ok(_) | Err(DiscussError::AccountingUnavailable) => {}
                Err(error) => tracing::warn!(%error, "Could not record allowance snapshot"),
            }
        }
    });
}

/// Discuss provider name for a registry provider name, for display only.
fn display_provider(provider: &str) -> &str {
    match provider {
        "claude" => "claude-acp",
        "codex" => "codex-acp",
        "grok" => "grok-build",
        "antigravity" => "antigravity-acp",
        "opencode" => "opencode",
        "opencodego" => "opencode",
        "opencode-go" => "opencode",
        other => other,
    }
}

fn projection(entry: &ProviderUsage) -> DiscussAllowanceProjection {
    DiscussAllowanceProjection {
        provider: entry.provider.clone(),
        display_provider: display_provider(&entry.provider).to_owned(),
        api_provider: entry.api_provider.clone(),
        account: entry.account.clone(),
        account_info: entry
            .account_info
            .as_ref()
            .map(|info| DiscussAllowanceAccountInfo {
                email: info.email.clone(),
                org_name: info.org_name.clone(),
                plan_type: info.plan_type.clone(),
                subscription_renews_at: info.subscription_renews_at.clone(),
                subscription_ends_at: info.subscription_ends_at.clone(),
            }),
        fetched_at: entry.fetched_at.clone(),
        stale: entry.stale.as_ref().map(|stale| DiscussAllowanceStale {
            since: stale.since.clone(),
            class: stale.class.clone(),
        }),
        error: entry.error.clone(),
        error_class: entry.error_class.clone(),
        windows: entry.usage.as_ref().map(windows),
        pools: entry
            .spend
            .as_ref()
            .map(|pools| pools.iter().map(pool).collect()),
        saved_resets: entry
            .saved_resets
            .as_ref()
            .map(|resets| DiscussAllowanceSavedResets {
                available_count: resets.available_count,
                soonest_expires_at: resets.soonest_expires_at.clone(),
                credit_expiries: resets
                    .credits
                    .iter()
                    .map(|credit| credit.expires_at.clone())
                    .collect(),
                kinds: resets
                    .kinds
                    .iter()
                    .map(|kind| DiscussAllowanceResetKind {
                        kind: kind.kind.clone(),
                        available_count: kind.available_count,
                        soonest_expires_at: kind.soonest_expires_at.clone(),
                    })
                    .collect(),
            }),
    }
}

fn window(window: &RateWindow) -> DiscussAllowanceWindow {
    DiscussAllowanceWindow {
        used_percent: window.used_percent,
        raw_used_percent: window.raw_used_percent,
        resets_at: window.resets_at.clone(),
        window_minutes: window.window_minutes,
        window_kind: window.window_kind.clone(),
        used_count: window.used_count,
        total_count: window.total_count,
    }
}

fn windows(usage: &Usage) -> DiscussAllowanceWindows {
    DiscussAllowanceWindows {
        primary: usage.primary.as_ref().map(window),
        secondary: usage.secondary.as_ref().map(window),
        tertiary: usage.tertiary.as_ref().map(window),
        extra_rate_windows: usage.extra_rate_windows.as_ref().map(|windows| {
            windows
                .iter()
                .map(|extra| DiscussAllowanceExtraWindow {
                    title: extra.title.clone(),
                    id: extra.id.clone(),
                    window: extra.window.as_ref().map(window),
                })
                .collect()
        }),
    }
}

fn amount(amount: &Amount) -> DiscussAllowanceAmount {
    DiscussAllowanceAmount {
        minor: amount.minor,
        exponent: amount.exponent,
        unit: amount.unit.clone(),
    }
}

fn pool(pool: &Pool) -> DiscussAllowancePool {
    let funding = match pool.funding {
        PoolFunding::Granted => "granted",
        PoolFunding::Purchased => "purchased",
        PoolFunding::Subscription => "subscription",
        PoolFunding::Unknown => "unknown",
    };
    let basis = match pool.basis {
        PoolBasis::Reported => "reported",
        PoolBasis::Derived => "derived",
        PoolBasis::Unstated => "unstated",
    };
    DiscussAllowancePool {
        id: pool.id.clone(),
        label: pool.label.clone(),
        funding: funding.to_owned(),
        basis: basis.to_owned(),
        remaining: pool.remaining.as_ref().map(amount),
        total: pool.total.as_ref().map(amount),
        spendable: pool.spendable,
        resets_at: pool.resets_at.clone(),
        expires_at: pool.expires_at.clone(),
        unlimited: pool.unlimited,
    }
}

/// Runs the sequential actor loop; channel closure stops observation and terminates the actor.
pub(super) async fn run_actor(mut actor: Actor) {
    while let Some(request) = actor.receiver.recv().await {
        actor.handle_message(request).await;
    }
    actor.stop_server();
}

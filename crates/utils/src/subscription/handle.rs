use tokio::sync::mpsc;

use aves::bus::{BusRpcContext, EventBus};
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, run_actor};
use super::domain::{SubscriptionDomain, SubscriptionGeneratedHandler};
use super::messages::{SubscriptionBroadcast, SubscriptionRequest};

/// Construct the Subscription actor and its generated routing loop.
pub(super) fn new<Root>(
    bus: &mut EventBus<Root>,
    root_context: BusRpcContext,
) -> SubscriptionGeneratedHandler<Root, SubscriptionBroadcast>
where
    Root: Clone + Send + 'static,
    SubscriptionBroadcast: Into<Root> + Clone + Send + 'static,
{
    let channels = bus.register::<SubscriptionDomain, SubscriptionRequest, SubscriptionBroadcast>();
    let (actor_tasks, actor_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);

    let (broadcast_tx, actor_broadcast_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);

    tokio::spawn(run_actor(Actor::new(actor_rx, broadcast_tx, root_context)));

    SubscriptionGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        actor_broadcast_rx,
    )
}

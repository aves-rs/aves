use tokio::sync::mpsc;

use aves::bus::{BusRpcContext, EventBus};
use aves::runtime::ServicePublisher;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, run_actor};
use super::domain::{
    SubscriptionDomain, SubscriptionGeneratedHandler, SubscriptionSudoService, SubscriptionSudoServiceImpl,
};
use super::messages::{SubscriptionBroadcast, SubscriptionRequest};

/// Construct the Subscription actor and its generated routing loop.
pub(super) fn new<Root>(
    bus: &mut EventBus<Root>,
    root_context: BusRpcContext,
    publisher: ServicePublisher<dyn SubscriptionSudoService>,
) -> SubscriptionGeneratedHandler<Root, SubscriptionBroadcast>
where
    Root: Clone + Send + 'static,
    SubscriptionBroadcast: Into<Root> + Clone + Send + 'static,
{
    let channels = bus.register::<SubscriptionDomain, SubscriptionRequest, SubscriptionBroadcast>();
    let (actor_tasks, actor_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);

    let (broadcast_tx, actor_broadcast_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (service, service_rx) = SubscriptionSudoServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    tokio::spawn(run_actor(Actor::new(actor_rx, broadcast_tx, root_context)));
    assert!(
        publisher.publish(service).is_ok(),
        "Subscription service must publish once"
    );

    SubscriptionGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        actor_broadcast_rx,
        service_rx,
    )
}

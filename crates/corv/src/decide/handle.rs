use tokio::sync::mpsc;

use aves::bus::EventBus;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, run_actor};
use super::domain::{
    DecideDomain, DecideGeneratedHandler, DecideKernelServices, DecideNodeLifecycleServiceImpl,
    DecideRuntimeServiceImpl,
};
use super::messages::{DecideBroadcast, DecideRequest};
use super::nodes::decide_node_registrations;

/// Construct the Decide actor and its generated routing loop.
pub(super) async fn new<Root>(
    bus: &mut EventBus<Root>,
    services: DecideKernelServices,
) -> DecideGeneratedHandler<Root, DecideBroadcast>
where
    Root: Clone + Send + 'static,
    DecideBroadcast: Into<Root> + Clone + Send + 'static,
{
    let DecideKernelServices {
        runtime_service,
        node_lifecycle_service,
        node_registration_supply,
    } = services;

    let channels = bus.register::<DecideDomain, DecideRequest, DecideBroadcast>();
    let (actor_tasks, actor_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (runtime_impl, runtime_rx) = DecideRuntimeServiceImpl::channel(DEFAULT_BUFFER_SIZE);
    let (node_lifecycle_impl, node_lifecycle_rx) = DecideNodeLifecycleServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    for registration in decide_node_registrations(node_lifecycle_impl.clone()) {
        node_registration_supply.push(registration);
    }

    let actor = Actor::new(actor_rx, super::plugins::for_provider);
    tokio::spawn(run_actor(actor));

    let _ = runtime_service.publish(runtime_impl);
    let _ = node_lifecycle_service.publish(node_lifecycle_impl);

    DecideGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        runtime_rx,
        node_lifecycle_rx,
    )
}

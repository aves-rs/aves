use std::path::PathBuf;

use tokio::sync::mpsc;

use aves::bus::{BusRpcContext, EventBus};
use aves::flow::NodeRegistrationSupply;

use super::actor::{Actor, run_actor};
use super::domain::{MessagingDomain, MessagingGeneratedHandler, MessagingKernelServices, MessagingRuntimeServiceImpl};
use super::messages::{MessagingBroadcast, MessagingRequest};

const REQUEST_BUFFER_SIZE: usize = 64;

/// Constructs Tyto's actor and generated bus handler.
pub(super) async fn new<Root>(
    bus: &mut EventBus<Root>,
    config_dir: PathBuf,
    node_registration_supply: NodeRegistrationSupply,
    services: MessagingKernelServices,
    context: BusRpcContext,
) -> MessagingGeneratedHandler<Root, MessagingBroadcast>
where
    Root: Clone + Send + 'static,
    MessagingBroadcast: Into<Root> + Clone + Send + 'static,
{
    let channels = bus.register::<MessagingDomain, MessagingRequest, MessagingBroadcast>();

    let (actor_tasks, actor_rx) = mpsc::channel(REQUEST_BUFFER_SIZE);
    let (broadcast_tx, broadcast_rx) = mpsc::channel(REQUEST_BUFFER_SIZE);
    let (runtime_impl, runtime_rx) = MessagingRuntimeServiceImpl::channel(REQUEST_BUFFER_SIZE);

    let actor = Actor::new(
        actor_rx,
        broadcast_tx,
        config_dir,
        node_registration_supply,
        services.flows,
        context,
    )
    .await
    .expect("Tyto message store opens");
    tokio::spawn(run_actor(actor));

    let _ = services.runtime.publish(runtime_impl);

    MessagingGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        broadcast_rx,
        runtime_rx,
    )
}

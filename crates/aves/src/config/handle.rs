use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::bus::EventBus;
use crate::runtime::ServicePublisher;
use crate::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, run_actor};
use super::domain::{
    ConfigDomain, ConfigGeneratedHandler, ConfigRegistrationSudoService, ConfigRegistrationSudoServiceImpl,
};
use super::messages::{ConfigBroadcast, ConfigRequest};

/// Registers Config's bus marker and starts its single file-owning actor.
pub(super) async fn new<Root>(
    bus: &mut EventBus<Root>,
    directory: PathBuf,
    registration: ServicePublisher<dyn ConfigRegistrationSudoService>,
) -> ConfigGeneratedHandler<Root, ConfigBroadcast>
where
    Root: Clone + Send + 'static,
    ConfigBroadcast: Into<Root> + Clone + Send + 'static,
{
    let channels = bus.register::<ConfigDomain, ConfigRequest, ConfigBroadcast>();

    let (actor_tasks, actor_tasks_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (actor_broadcast_tx, actor_broadcast_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (service, service_rx) = ConfigRegistrationSudoServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    let actor =
        Actor::new(directory, actor_tasks_rx, actor_broadcast_tx).expect("Config file must be readable at boot");
    tokio::spawn(run_actor(actor));

    assert!(
        registration.publish(service).is_ok(),
        "Config registration service must publish once"
    );

    ConfigGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        actor_broadcast_rx,
        service_rx,
    )
}

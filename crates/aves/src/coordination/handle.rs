use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::bus::EventBus;
use crate::machine::MachinePeerService;
use crate::runtime::{ServicePublisher, ServiceReader};
use crate::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, run_actor};
use super::domain::{
    CoordinationDomain, CoordinationGeneratedHandler, CoordinationLifecycleServiceImpl, CoordinationSigningService,
    CoordinationSigningServiceImpl,
};
use super::messages::{CoordinationBroadcast, CoordinationRequest};

/// Owns coordination bus routing and keeps its generated handler running.
pub(super) struct Handle<Root>
where
    Root: Clone + Send + 'static,
    CoordinationBroadcast: Into<Root>,
{
    generated_handler: CoordinationGeneratedHandler<Root, CoordinationBroadcast>,
    lifecycle_service: std::sync::Arc<CoordinationLifecycleServiceImpl>,
}

impl<Root> Handle<Root>
where
    Root: Clone + Send + 'static,
    CoordinationBroadcast: Into<Root>,
{
    /// Registers the bus domain and starts its request-owning actor.
    pub(super) async fn new(
        bus: &mut EventBus<Root>,
        config_dir: PathBuf,
        sync_services: ServiceReader<dyn MachinePeerService>,
        signing_service: ServicePublisher<dyn CoordinationSigningService>,
    ) -> Self {
        let admission = bus.admission_registry();
        let channels = bus.register::<CoordinationDomain, CoordinationRequest, CoordinationBroadcast>();
        let (actor_tasks, actor_tasks_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (broadcasts, broadcast_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (service, service_rx) = CoordinationSigningServiceImpl::channel(DEFAULT_BUFFER_SIZE);
        let (lifecycle_service, lifecycle_rx) = CoordinationLifecycleServiceImpl::channel(DEFAULT_BUFFER_SIZE);
        assert!(
            signing_service.publish(service).is_ok(),
            "coordination signing service must publish once"
        );
        let mut actor = Actor::new(actor_tasks_rx, config_dir, None, admission, broadcasts).await;
        actor.set_peer_services_reader(sync_services);
        tokio::spawn(run_actor(actor));

        Self {
            lifecycle_service,
            generated_handler: CoordinationGeneratedHandler::new(
                channels.requests_rx,
                actor_tasks,
                channels.broadcasts_tx,
                broadcast_rx,
                service_rx,
                lifecycle_rx,
            ),
        }
    }

    /// Routes public requests until the bus or actor channels close.
    pub(super) async fn run(self) {
        let _lifecycle_service = self.lifecycle_service;
        self.generated_handler.run().await;
    }
}

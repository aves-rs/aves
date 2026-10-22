use tokio::sync::mpsc;

use crate::bus::EventBus;
use crate::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, ActorDependencies, run_actor};
use super::domain::{
    FlowControlServiceImpl, FlowDomain, FlowGeneratedHandler, FlowKernelServices, FlowTriggerSourceServiceImpl,
};
use super::messages::{FlowBroadcast, FlowRequest};
use super::types::FlowBootOptions;

/// Construct Flow routing and initialize its actor with boot retention settings.
pub(super) async fn new<Root>(
    bus: &mut EventBus<Root>,
    options: FlowBootOptions,
    services: FlowKernelServices,
) -> FlowGeneratedHandler<Root, FlowBroadcast>
where
    Root: Clone + Send + 'static,
    FlowBroadcast: Into<Root> + Clone + Send + 'static,
{
    let channels = bus.register::<FlowDomain, FlowRequest, FlowBroadcast>();
    let (actor_tasks, actor_tasks_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (actor_broadcast_tx, actor_broadcast_rx) = mpsc::channel::<FlowBroadcast>(DEFAULT_BUFFER_SIZE);
    let (service_impl, service_rx) = FlowControlServiceImpl::channel(DEFAULT_BUFFER_SIZE);
    let (trigger_source_impl, trigger_source_rx) = FlowTriggerSourceServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    assert!(
        !options.data_root.as_os_str().is_empty(),
        "Flow data root must not be empty"
    );

    let actor = Actor::new(
        actor_tasks_rx,
        ActorDependencies {
            trigger_registry: services.trigger_registry,
            broadcast_tx: actor_broadcast_tx,
            auth_service: services.auth_service,
            mcp_bus_service: Some(services.mcp_bus_service),
            bus_context: services.bus_context,
            data_root: options.data_root,
            database: options.database,
            node_registrations: options.node_registrations,
            node_registration_supply: options.node_registration_supply,
            workspace_source_service: services.workspace_source_service,
            sync_services: services.sync_services,
            mobility_service: services.mobility_service,
            retention_completed_runs: options.retention_completed_runs,
            #[cfg(test)]
            native_fixture: None,
        },
    )
    .await
    .expect("Flow durable state must load during domain construction");
    tokio::spawn(run_actor(actor));

    assert!(
        services.service.publish(service_impl).is_ok(),
        "Flow service must publish once"
    );
    assert!(
        services
            .trigger_source_service
            .publish(trigger_source_impl)
            .is_ok(),
        "Flow trigger source service must publish once"
    );

    FlowGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        actor_broadcast_rx,
        service_rx,
        trigger_source_rx,
    )
}

use tokio::sync::mpsc;

use aves::bus::EventBus;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::DiscussKernelServices;
use super::actor::{Actor, run_actor};
use super::agent_nodes::agent_node_registration;
use super::domain::{DiscussAgentNodeLifecycleServiceImpl, DiscussGeneratedHandler};
use super::messages::{DiscussBroadcast, DiscussRequest};
use super::types::{ActorInboundPorts, WorkspaceCopySourcePort};

/// Construct the Discuss actor and its generated routing loop.
pub(super) async fn new<Root>(
    driver: &mut EventBus<Root>,
    services: DiscussKernelServices,
) -> DiscussGeneratedHandler<Root, DiscussBroadcast>
where
    Root: Clone + Send + 'static,
    DiscussBroadcast: Into<Root> + Clone + Send + 'static,
{
    let DiscussKernelServices {
        boot,
        streaming_service,
        provider_config_service,
        workspace_copy_source_port: workspace_copy_source_publisher,
        agent_node_lifecycle_service,
        agent_node_streaming,
        agent_node_copy_sources,
        node_registration_supply,
        bus_service,
        auth_service,
        root_context,
    } = services;

    let (actor_tasks, actor_tasks_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (workspace_copy_source_port, workspace_copy_source_rx) = WorkspaceCopySourcePort::channel(DEFAULT_BUFFER_SIZE);

    let channels = driver.register::<super::DiscussDomain, DiscussRequest, DiscussBroadcast>();
    let (streaming_service_impl, streaming_service_rx) =
        super::DiscussStreamingServiceImpl::channel(DEFAULT_BUFFER_SIZE);
    let (provider_config_service_impl, provider_config_service_rx) =
        super::DiscussProviderConfigSudoServiceImpl::channel(DEFAULT_BUFFER_SIZE);
    let (agent_node_lifecycle_impl, agent_node_lifecycle_rx) =
        DiscussAgentNodeLifecycleServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    node_registration_supply.push(agent_node_registration(
        agent_node_lifecycle_impl.clone(),
        agent_node_streaming,
        agent_node_copy_sources,
    ));

    let (broadcast_tx, actor_broadcast_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);

    let actor = Actor::new(
        actor_tasks_rx,
        ActorInboundPorts {
            workspace_copy_source_rx,
        },
        root_context,
        bus_service,
        auth_service,
        broadcast_tx,
        boot,
    )
    .await;
    tokio::spawn(run_actor(actor));

    let _ = streaming_service.publish(streaming_service_impl);
    let _ = provider_config_service.publish(provider_config_service_impl);
    let _ = agent_node_lifecycle_service.publish(agent_node_lifecycle_impl);

    assert!(
        workspace_copy_source_publisher
            .publish(workspace_copy_source_port)
            .is_ok(),
        "Discuss workspace copy source port must publish once"
    );

    DiscussGeneratedHandler::new(
        channels.requests_rx,
        actor_tasks,
        channels.broadcasts_tx,
        actor_broadcast_rx,
        streaming_service_rx,
        provider_config_service_rx,
        agent_node_lifecycle_rx,
    )
}

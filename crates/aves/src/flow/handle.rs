use std::sync::Arc;
use tokio::sync::mpsc;

use crate::auth::{AuthCapability, AuthProjection, PairingOwner};
use crate::bus::{BusRpcContext, EventBus};
use crate::machine::MachinePeerServices;
use crate::utils::buffers::DEFAULT_BUFFER_SIZE;

use super::actor::{Actor, ActorDependencies, run_actor};
use super::database::FlowStore;
use super::default_workspace::DefaultWorkspace;
use super::domain::{
    FlowControlServiceImpl, FlowDomain, FlowGeneratedHandler, FlowKernelServices, FlowTriggerSourceServiceImpl,
};
use super::hook::FlowAppWorkspaceHook;
use super::messages::{FlowBroadcast, FlowRequest, FlowRequestPayload};
use super::mobility::RuntimeFlowMobilityService;
use super::registration::FlowNodeRegistry;
use super::snapshot::ExecutionSnapshotStore;
use super::triggers::TriggerRegistry;
use super::types::{FlowBootOptions, FlowError, WorkspaceError};

/// Construct Flow routing and initialize its actor with boot retention settings.
pub(super) async fn new<Root>(
    bus: &mut EventBus<Root>,
    options: FlowBootOptions,
    services: FlowKernelServices,
    trigger_registry: TriggerRegistry,
    bus_context: BusRpcContext,
    pairing_owner: PairingOwner,
    machine: &MachinePeerServices,
) -> FlowGeneratedHandler<Root, FlowBroadcast>
where
    Root: Clone + Send + 'static,
    FlowBroadcast: Into<Root> + Clone + Send + 'static,
{
    let database_path = options.data_root.with_extension("db");
    assert!(
        !options.data_root.as_os_str().is_empty(),
        "Flow data root must not be empty"
    );

    let channels = bus.register::<FlowDomain, FlowRequest, FlowBroadcast>();
    let (actor_tasks, actor_tasks_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (actor_broadcast_tx, actor_broadcast_rx) = mpsc::channel::<FlowBroadcast>(DEFAULT_BUFFER_SIZE);
    let (service_impl, service_rx) = FlowControlServiceImpl::channel(DEFAULT_BUFFER_SIZE);
    let (trigger_source_impl, trigger_source_rx) = FlowTriggerSourceServiceImpl::channel(DEFAULT_BUFFER_SIZE);

    let mut registry = FlowNodeRegistry::new();
    for registration in options
        .node_registrations
        .into_iter()
        .chain(options.node_registration_supply.drain())
    {
        registry
            .register(registration)
            .expect("Flow node registrations must be valid");
    }
    let node_registry = Arc::new(registry);

    // TODO: remove the below switch when we move the default hook implementation out
    // of this crate and into sluice application; see comment in domain.rs for reasoning
    let application_workspace = options.app_workspace_hook.is_some();
    let workspace_hook: Arc<dyn FlowAppWorkspaceHook> = match options.app_workspace_hook {
        Some(workspace) => workspace,
        None => Arc::new(
            DefaultWorkspace::new(
                options.data_root,
                machine,
                node_registry.clone(),
                services.workspace_source_service,
                FlowStore::new(options.database.clone()),
                ExecutionSnapshotStore::new(options.database.clone()),
            )
            .await
            .expect("Flow default workspace must initialize"),
        ),
    };

    let mobility_service = Arc::new(RuntimeFlowMobilityService {
        connectivity: machine.connectivity.clone(),
        peer_service: services.sync_services.clone(),
        auth_service: services.auth_service.clone(),
        pairing_owner,
    });

    let actor = Actor::new(
        actor_tasks_rx,
        ActorDependencies {
            trigger_registry,
            broadcast_tx: actor_broadcast_tx,
            auth_service: services.auth_service,
            mcp_bus_service: Some(services.mcp_bus_service),
            bus_context,
            // TODO: simplify the below when we don't have the default workspace setup
            database_path,
            database: options.database,
            node_registry,
            sync_services: services.sync_services,
            mobility_service,
            retention_completed_runs: options.retention_completed_runs,
            workspace_hook,
            application_workspace,
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

/// Binds mobility imports to the authenticated peer and limits cloud blob pulls.
pub(super) fn ensure_caller_has_capabilities(
    payload: &FlowRequestPayload,
    projection: &AuthProjection,
) -> Result<(), FlowError> {
    if let FlowRequestPayload::ImportMobilitySnapshot { source_peer_key, .. } = payload
        && source_peer_key != &projection.issuer
    {
        return Err(FlowError::MobilityPeer {
            message: "Flow mobility source peer does not match authenticated issuer".to_string(),
        });
    }
    if !matches!(payload, FlowRequestPayload::DownloadWorkspaceBlob { .. }) {
        return Ok(());
    }
    if projection
        .cap
        .iter()
        .any(|capability| matches!(capability, AuthCapability::PullCloudBlobs | AuthCapability::AllBus))
    {
        Ok(())
    } else {
        Err(FlowError::Workspace(WorkspaceError::CapabilityDenied))
    }
}

use crate::bus::{BusRpcContext, EventBus};
use crate::config::ConfigDirectory;
use crate::machine::MachinePeerService;
use crate::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, dispatch_lifecycle_request,
};
use crate::utils::errors::SharedInfraError;

use super::handle::Handle;
use super::messages::{CoordinationBroadcast, CoordinationRequest, CoordinationRequestPayload, CoordinationResponse};
use super::types::CoordinationError;

/// Marker for application-neutral chain coordination bus requests.
pub struct CoordinationDomain;

aves::domain! {
    marker CoordinationDomain;

    request CoordinationRequestPayload => CoordinationResponse
    error = CoordinationError
    transport = SharedInfraError
    target = "coordination";

    outbound CoordinationBroadcast => CoordinationBroadcast {
        event => event,
    }
}

pub(super) use __Coordination_generated_routing::CoordinationGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for CoordinationDomain
where
    Root: Clone + Send + 'static,
    CoordinationBroadcast: Into<Root>,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'a, 'b, Root>(facade: &'a mut ConstructionFacade<'b, EventBus<Root>>) -> DomainFuture<'a>
        where
            Root: Clone + Send + 'static,
            CoordinationBroadcast: Into<Root>,
        {
            let config_dir = facade
                .input::<ConfigDirectory>()
                .expect("Config directory must be supplied")
                .0
                .clone();
            let sync_services = facade.reader::<dyn MachinePeerService>();
            let future = Handle::new(facade.bus_mut(), config_dir, sync_services);
            Box::pin(async move {
                let handle = future.await;
                tokio::spawn(handle.run());
            })
        }

        DomainCallbacks {
            new: new_domain::<Root>,
            start: |context| Box::pin(CoordinationDomain::start(context)),
            stop: |context| Box::pin(CoordinationDomain::stop(context)),
        }
    }
}

impl CoordinationDomain {
    /// Start the coordination server through its typed lifecycle request.
    async fn start(context: BusRpcContext) {
        let result =
            dispatch_lifecycle_request::<CoordinationDomain, _, _, _>(context, |reply_to| CoordinationRequest {
                reply_to,
                payload: CoordinationRequestPayload::StartServer,
            })
            .await;
        if !matches!(&result, Some(Ok(CoordinationResponse::ServerStarted))) {
            tracing::error!(?result, "coordination server start was not acknowledged");
        }
    }

    /// Stop the coordination server through its typed lifecycle request.
    async fn stop(context: BusRpcContext) {
        let result =
            dispatch_lifecycle_request::<CoordinationDomain, _, _, _>(context, |reply_to| CoordinationRequest {
                reply_to,
                payload: CoordinationRequestPayload::StopServer,
            })
            .await;
        if !matches!(&result, Some(Ok(CoordinationResponse::ServerStopped))) {
            tracing::error!(?result, "coordination server stop was not acknowledged");
        }
    }
}

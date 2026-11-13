use std::sync::Arc;

use crate::auth::AuthAdmissionRegistry;
use crate::bus::{BusRpcContext, EventBus};
use crate::config::ConfigRegistrationSudoService;
use crate::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, dispatch_lifecycle_request,
};

use super::handle;
use super::messages::{MachineBroadcast, MachineRequest, MachineRequestPayload, MachineResponse};
use super::peering::{MachinePeerService, MachinePeerServices};
use super::types::{MachineBootOptions, MachineError};

/// Machine domain marker.
pub struct MachineDomain;

aves::domain! {
    marker MachineDomain;

    request MachineRequestPayload => MachineResponse
    error = MachineError
    transport = MachineError
    target = "machine";

    outbound MachineBroadcast => MachineBroadcast {
        event => event,
    }
}

pub(super) use __Machine_generated_routing::MachineGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for MachineDomain
where
    Root: Clone + Send + 'static,
    MachineBroadcast: Into<Root>,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn construct<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            MachineBroadcast: Into<Root>,
        {
            let registry = facade
                .input::<AuthAdmissionRegistry>()
                .cloned()
                .or_else(|| facade.bus_mut().admission_registry())
                .expect("Machine requires an Auth admission registry");
            let local_issuer = registry
                .current_issuers()
                .into_iter()
                .next()
                .expect("Auth admission registry must contain its local issuer")
                .issuer;
            let options = facade
                .input::<MachineBootOptions>()
                .cloned()
                .unwrap_or_else(|| MachineBootOptions::new(local_issuer.clone()));
            assert!(
                options.machine == local_issuer,
                "Machine boot identity must match the Auth admission registry"
            );

            let config = facade.reader::<dyn ConfigRegistrationSudoService>();
            let context = facade.root_context().clone();

            let peer_services = facade.input::<MachinePeerServices>().cloned();
            let publisher = facade.publisher::<dyn MachinePeerService>();

            if let Some(services) = &peer_services {
                assert!(
                    publisher.publish(Arc::new(services.clone())).is_ok(),
                    "Machine peer services must publish once"
                );
            }
            let future = handle::construct(facade.bus_mut(), options, config, context, registry, peer_services);

            Box::pin(future)
        }

        DomainCallbacks {
            new: construct::<Root>,
            start: |context| Box::pin(MachineDomain::start(context)),
            stop: |context| Box::pin(MachineDomain::stop(context)),
        }
    }
}

impl MachineDomain {
    async fn start(context: BusRpcContext) {
        let result = dispatch_lifecycle_request::<MachineDomain, _, _, _>(context, |reply_to| MachineRequest {
            reply_to,
            payload: MachineRequestPayload::StartServer,
        })
        .await;
        if !matches!(&result, Some(Ok(MachineResponse::ServerStarted))) {
            tracing::error!(?result, "machine server start was not acknowledged");
        }
    }

    async fn stop(context: BusRpcContext) {
        let result = dispatch_lifecycle_request::<MachineDomain, _, _, _>(context, |reply_to| MachineRequest {
            reply_to,
            payload: MachineRequestPayload::StopServer,
        })
        .await;
        if !matches!(&result, Some(Ok(MachineResponse::ServerStopped))) {
            tracing::error!(?result, "machine server stop was not acknowledged");
        }
    }
}

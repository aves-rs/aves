use aves::bus::{BusRpcContext, EventBus};
use aves::flow::NodeRegistrationSupply;
use aves::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher,
    dispatch_lifecycle_request,
};
use aves::utils::errors::SharedInfraError;

use super::handle::new as construct_decide_domain;
use super::messages::{
    CancelNodeInput, DecideBroadcast, DecidePrivilegedCancelNode, DecidePrivilegedRemoveCredential,
    DecidePrivilegedStartNode, DecidePrivilegedSupplyCredential, DecidePrivilegedTickNode, DecideRequest,
    DecideRequestPayload, DecideSudoServicePrivilegeMapping, NodeStartedOutput, NodeTickedOutput,
    RemoveCredentialInput, StartNodeInput, SuccessOutput, SupplyCredentialInput, TickNodeInput,
};
use super::types::DecideError;

/// Decide lifecycle marker; runtime state belongs to the domain handle.
pub struct DecideDomain;

/// Services and inputs used to construct the Decide domain.
pub(super) struct DecideKernelServices {
    pub(super) runtime_service: ServicePublisher<dyn DecideRuntimeService>,
    pub(super) node_lifecycle_service: ServicePublisher<dyn DecideNodeLifecycleService>,
    pub(super) node_registration_supply: NodeRegistrationSupply,
}

aves::domain! {
    marker DecideDomain;

    request DecideRequestPayload => DecideResponse
    error = DecideError
    transport = SharedInfraError
    target = "decide";

    pub service DecideRuntimeService {
        SupplyCredential => Success,
        RemoveCredential => Success,
    }

    pub service DecideNodeLifecycleService {
        StartNode => NodeStarted,
        TickNode => NodeTicked,
        CancelNode => Success,
    }
}

pub(super) use __Decide_generated_routing::DecideGeneratedHandler;

impl DecideDomain {
    async fn construct_and_run<Root>(bus: &mut EventBus<Root>, services: DecideKernelServices)
    where
        Root: Clone + Send + 'static,
        DecideBroadcast: Into<Root> + Clone + Send + 'static,
    {
        let handler = construct_decide_domain(bus, services).await;
        tokio::spawn(async move { handler.run().await });
    }
}

impl<Root> DomainDescriptorFactory<EventBus<Root>> for DecideDomain
where
    Root: Clone + Send + 'static,
    DecideBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            DecideBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let services = DecideKernelServices {
                runtime_service: facade.publisher(),
                node_lifecycle_service: facade.publisher(),
                node_registration_supply: facade
                    .input::<NodeRegistrationSupply>()
                    .cloned()
                    .expect("Decide node registration supply construction input"),
            };
            Box::pin(DecideDomain::construct_and_run(facade.bus_mut(), services))
        }

        DomainCallbacks {
            new: new_domain::<Root>,
            start: |context| Box::pin(lifecycle(context, DecideRequestPayload::StartServer)),
            stop: |context| Box::pin(lifecycle(context, DecideRequestPayload::StopServer)),
        }
    }
}

async fn lifecycle(context: BusRpcContext, payload: DecideRequestPayload) {
    let _ =
        dispatch_lifecycle_request::<DecideDomain, _, _, _>(context, |reply_to| DecideRequest { payload, reply_to })
            .await;
}

use aves::bus::{BusRpcContext, EventBus};
use aves::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, dispatch_lifecycle_request,
};
use aves::utils::errors::SharedInfraError;

use super::handle::new as construct_subscription_domain;
use super::messages::{
    SavedResetUsedOutput, SubscriptionBroadcast, SubscriptionPrivilegedUseSavedReset, SubscriptionRequest,
    SubscriptionRequestPayload, SubscriptionResponse, SubscriptionSudoServicePrivilegeMapping, UseSavedResetInput,
};
use super::types::SubscriptionError;

/// Subscription lifecycle marker; runtime state belongs to the domain actor.
pub struct SubscriptionDomain;

aves::domain! {
    marker SubscriptionDomain;

    request SubscriptionRequestPayload => SubscriptionResponse
    error = SubscriptionError
    transport = SharedInfraError
    target = "subscription";

    pub service SubscriptionSudoService {
        UseSavedReset => SavedResetUsed,
    }

    outbound SubscriptionResponse => SubscriptionBroadcast {
        SubscriptionResponse::ServerStarted => SubscriptionBroadcast::ServerStarted,
        SubscriptionResponse::ServerStopped => SubscriptionBroadcast::ServerStopped,
    }
}

pub(super) use __Subscription_generated_routing::SubscriptionGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for SubscriptionDomain
where
    Root: Clone + Send + 'static,
    SubscriptionBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            SubscriptionBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let root_context = facade.root_context().clone();
            let service = facade.publisher::<dyn SubscriptionSudoService>();
            let handler = construct_subscription_domain(facade.bus_mut(), root_context, service);
            Box::pin(async move {
                tokio::spawn(async move { handler.run().await });
            })
        }

        DomainCallbacks {
            new: new_domain::<Root>,
            start: |context| Box::pin(lifecycle(context, SubscriptionRequestPayload::StartServer)),
            stop: |context| Box::pin(lifecycle(context, SubscriptionRequestPayload::StopServer)),
        }
    }
}

async fn lifecycle(context: BusRpcContext, payload: SubscriptionRequestPayload) {
    let _ = dispatch_lifecycle_request::<SubscriptionDomain, _, _, _>(context, |reply_to| SubscriptionRequest {
        payload,
        reply_to,
    })
    .await;
}

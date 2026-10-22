use aves::bus::{BusRpcContext, EventBus};
use aves::config::ConfigDirectory;
use aves::flow::{FlowTriggerSourceService, NodeRegistrationSupply, TriggerRegistrationError, TriggerRegistry};
use aves::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher, ServiceReader,
    dispatch_lifecycle_request,
};

use super::handle::new as new_handle;
use super::messages::{
    MessagingBroadcast, MessagingPrivilegedRemoveCredential, MessagingPrivilegedSupplyCredential, MessagingRequest,
    MessagingRequestPayload, MessagingResponse, MessagingSudoServicePrivilegeMapping, RemoveCredentialInput,
    SuccessOutput, SupplyCredentialInput,
};
use super::types::{MessagingError, SubscriptionTriggerConfig};

pub(super) const SUBSCRIPTION_TRIGGER_ADAPTER_KIND: &str = "messaging";

/// Inbound channel runtime composed independently of the application's root broadcast.
pub struct MessagingDomain;

pub(super) struct MessagingKernelServices {
    pub(super) runtime: ServicePublisher<dyn MessagingRuntimeService>,
    pub(super) flows: ServiceReader<dyn FlowTriggerSourceService>,
}

aves::domain! {
    marker MessagingDomain;

    request MessagingRequestPayload => MessagingResponse
    error = MessagingError
    transport = MessagingError
    target = "messaging";

    pub service MessagingRuntimeService {
        SupplyCredential => Success,
        RemoveCredential => Success,
    }

    outbound MessagingResponse => MessagingBroadcast {
        SubscriptionTrigger => Trigger,
        ChannelMessage { body, metadata } => ChannelMessage { body, metadata },
    }
}

pub(super) use __Messaging_generated_routing::MessagingGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for MessagingDomain
where
    Root: Clone + Send + 'static,
    MessagingBroadcast: Into<Root> + Clone + Send + 'static,
{
    /// Builds the handler at construction and routes start/stop through Aves lifecycle requests.
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn construct<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            MessagingBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let context = facade.root_context().clone();
            let config_dir = facade
                .input::<ConfigDirectory>()
                .expect("ConfigDirectory is a construction input")
                .0
                .clone();

            let node_registration_supply = facade
                .input::<NodeRegistrationSupply>()
                .expect("NodeRegistrationSupply is a construction input")
                .clone();

            let services = MessagingKernelServices {
                runtime: facade.publisher(),
                flows: facade.reader(),
            };

            let bus = facade.bus_mut();
            Box::pin(async move {
                let handler = new_handle(bus, config_dir, node_registration_supply, services, context.clone()).await;
                tokio::spawn(async move { handler.run().await });
            })
        }
        DomainCallbacks {
            new: construct::<Root>,
            start: |context| Box::pin(lifecycle(context, MessagingRequestPayload::StartServer)),
            stop: |context| Box::pin(lifecycle(context, MessagingRequestPayload::StopServer)),
        }
    }
}

/// Dispatches a lifecycle payload through the normal Aves domain routing.
async fn lifecycle(context: BusRpcContext, payload: MessagingRequestPayload) {
    let _ = dispatch_lifecycle_request::<MessagingDomain, _, _, _>(context, |reply_to| MessagingRequest {
        payload,
        reply_to,
    })
    .await;
}

/// Registers Tyto-owned configuration validation without making Flow depend on Tyto.
pub fn messaging_trigger_registry() -> TriggerRegistry {
    let mut registry = TriggerRegistry::new();
    registry
        .register_configuration::<SubscriptionTriggerConfig, _>(SUBSCRIPTION_TRIGGER_ADAPTER_KIND, |configuration| {
            if configuration.connection_id.0.is_nil() {
                return Err(TriggerRegistrationError::InvalidConfiguration {
                    reason: "a non-nil connection UUID is required",
                });
            }
            Ok(())
        })
        .expect("subscription trigger configuration registers once");
    registry
}

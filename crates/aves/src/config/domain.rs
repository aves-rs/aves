use std::path::PathBuf;

use crate::bus::EventBus;
use crate::runtime::{ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher};

use super::handle;
use super::messages::{
    ConfigBroadcast, ConfigPrivilegedRegisterSection, ConfigPrivilegedSealRegistration, ConfigRequest,
    ConfigRequestPayload, ConfigSudoServicePrivilegeMapping, RegisterSectionInput, RegisteredOutput,
    SealRegistrationInput,
};
use super::types::ConfigError;

/// Generic Config domain marker.
pub struct ConfigDomain;

aves::domain! {
    marker ConfigDomain;

    request ConfigRequestPayload => ConfigResponse
    error = ConfigError
    transport = ConfigError
    target = "config";

    pub service ConfigRegistrationSudoService {
        RegisterSection => Registered,
        SealRegistration => Registered,
    }

    outbound ConfigBroadcast => ConfigBroadcast {
        ConfigChanged { section, value } => ConfigChanged { section, value },
    }
}

pub(super) use __Config_generated_routing::ConfigGeneratedHandler;

/// Directory handed to the runtime by the embedding application.
#[derive(Clone)]
pub struct ConfigDirectory(pub PathBuf);

impl<Root> DomainDescriptorFactory<EventBus<Root>> for ConfigDomain
where
    Root: Clone + Send + 'static,
    ConfigBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn construct<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            ConfigBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let directory = facade
                .input::<ConfigDirectory>()
                .expect("Config directory must be supplied")
                .0
                .clone();
            let registration: ServicePublisher<dyn ConfigRegistrationSudoService> = facade.publisher();
            let future = handle::new(facade.bus_mut(), directory, registration);
            Box::pin(async move {
                let handler = future.await;
                tokio::spawn(async move { handler.run().await });
            })
        }
        DomainCallbacks {
            new: construct::<Root>,
            start: |_| Box::pin(async {}),
            stop: |_| Box::pin(async {}),
        }
    }
}

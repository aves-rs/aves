use std::path::PathBuf;
use std::sync::Arc;

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

/// Resolved application directories supplied at boot and shared with domains.
#[derive(Clone, Debug)]
pub struct BootDirectories {
    /// Directory containing application configuration.
    pub config_dir: PathBuf,
    /// Directory containing durable application data.
    pub data_dir: PathBuf,
    /// Directory containing regenerable application data.
    pub cache_dir: PathBuf,
}

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
            let directories = facade
                .input::<BootDirectories>()
                .expect("Config boot directories must be supplied")
                .clone();
            let directory = directories.config_dir.clone();
            let directories_publisher = facade.publisher::<BootDirectories>();
            let registration: ServicePublisher<dyn ConfigRegistrationSudoService> = facade.publisher();
            let future = handle::new(facade.bus_mut(), directory, registration);
            Box::pin(async move {
                let handler = future.await;
                directories_publisher
                    .publish(Arc::new(directories))
                    .expect("Config boot directories must be published once");
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

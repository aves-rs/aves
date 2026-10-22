use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::auth::{AuthBootOptions, AvesAuthError, ConstructionResult};
use crate::bus::BusRpcContext;

/// Typed construction inputs shared by independently owned runtime domains.
#[derive(Default)]
pub struct ConstructionInputs {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ConstructionInputs {
    /// Creates an empty construction-input registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one uniquely typed input and returns the registry for chaining.
    pub fn with<Input>(mut self, input: Input) -> Self
    where
        Input: Send + Sync + 'static,
    {
        self.values.insert(TypeId::of::<Input>(), Box::new(input));
        self
    }

    pub(crate) fn get<Input>(&self) -> Option<&Input>
    where
        Input: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<Input>())
            .and_then(|value| value.downcast_ref())
    }

    pub(crate) fn insert<Input>(&mut self, input: Input)
    where
        Input: Send + Sync + 'static,
    {
        self.values.insert(TypeId::of::<Input>(), Box::new(input));
    }
}

/// Construction-only view used by a domain descriptor factory.
pub struct ConstructionFacade<'bus, Bus: ?Sized> {
    bus: &'bus mut Bus,
    services: &'bus mut ServiceSlotRegistry,
    inputs: &'bus (dyn Any + Send + Sync),
    auth_options: Option<&'bus AuthBootOptions>,
    auth_construction: Option<&'bus mut Option<Result<ConstructionResult, AvesAuthError>>>,
    root_context: Option<BusRpcContext>,
}

impl<'bus, Bus: ?Sized> ConstructionFacade<'bus, Bus> {
    pub(super) fn new_with_auth(
        bus: &'bus mut Bus,
        services: &'bus mut ServiceSlotRegistry,
        inputs: &'bus (dyn Any + Send + Sync),
        auth_options: &'bus AuthBootOptions,
        auth_construction: &'bus mut Option<Result<ConstructionResult, AvesAuthError>>,
    ) -> Self {
        Self {
            bus,
            services,
            inputs,
            auth_options: Some(auth_options),
            auth_construction: Some(auth_construction),
            root_context: None,
        }
    }

    pub(super) fn new_with_root_context(
        bus: &'bus mut Bus,
        services: &'bus mut ServiceSlotRegistry,
        inputs: &'bus (dyn Any + Send + Sync),
        root_context: BusRpcContext,
    ) -> Self {
        Self {
            bus,
            services,
            inputs,
            auth_options: None,
            auth_construction: None,
            root_context: Some(root_context),
        }
    }

    /// Returns the authenticated context issued by the owning runtime.
    pub fn root_context(&self) -> &BusRpcContext {
        self.root_context
            .as_ref()
            .expect("root context is available for runtime domain construction")
    }

    /// Returns the mutable bus registration reference for domain setup.
    pub fn bus_mut(&mut self) -> &mut Bus {
        self.bus
    }

    /// Returns the authentication boot options for the owning runtime.
    pub fn auth_boot_options(&self) -> Option<&AuthBootOptions> {
        self.auth_options
    }

    /// Returns the result of the authentication construction for the owning runtime.
    pub fn set_auth_construction(&mut self, result: Result<ConstructionResult, AvesAuthError>) {
        *self
            .auth_construction
            .as_deref_mut()
            .expect("Auth construction handoff is available only for built-in Auth") = Some(result);
    }

    /// Returns an explicitly supplied construction input for a domain.
    pub fn input<Input>(&self) -> Option<&Input>
    where
        Input: Send + Sync + 'static,
    {
        self.inputs
            .downcast_ref::<ConstructionInputs>()
            .and_then(ConstructionInputs::get)
            .or_else(|| self.inputs.downcast_ref::<Input>())
    }

    /// Creates the one-shot publisher for a statically selected service slot.
    pub fn publisher<Service>(&mut self) -> ServicePublisher<Service>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        self.services.publisher()
    }

    /// Creates the late-bound reader for a statically selected service slot.
    pub fn reader<Service>(&mut self) -> ServiceReader<Service>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        self.services.reader()
    }
}

/// Registry of permanent, late-bound slots keyed by service trait type.
pub(super) struct ServiceSlotRegistry {
    slots: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ServiceSlotRegistry {
    /// Creates an empty registry whose slots live for the owning runtime.
    pub fn new() -> Self {
        Self { slots: HashMap::new() }
    }

    /// Returns the permanent slot associated with a service trait type.
    fn slot_or_insert<Service>(&mut self) -> Arc<OnceLock<Arc<Service>>>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        let slot = self
            .slots
            .entry(TypeId::of::<Service>())
            .or_insert_with(|| Box::new(Arc::new(OnceLock::<Arc<Service>>::new())));

        slot.downcast_ref::<Arc<OnceLock<Arc<Service>>>>()
            .expect("service slot type changed for an existing service key")
            .clone()
    }

    /// Creates the one-shot publisher for a service trait slot.
    pub(super) fn publisher<Service>(&mut self) -> ServicePublisher<Service>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        ServicePublisher {
            slot: self.slot_or_insert(),
        }
    }

    /// Creates the late-bound reader for a service trait slot.
    pub(super) fn reader<Service>(&mut self) -> ServiceReader<Service>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        ServiceReader {
            slot: self.slot_or_insert(),
        }
    }

    pub(super) fn declared_reader<Service>(&self) -> ServiceReader<Service>
    where
        Service: ?Sized + Send + Sync + 'static,
    {
        let slot = self
            .slots
            .get(&TypeId::of::<Service>())
            .and_then(|slot| slot.downcast_ref::<Arc<OnceLock<Arc<Service>>>>())
            .expect("runtime service slot must be declared during domain construction")
            .clone();
        ServiceReader { slot }
    }
}

/// Publisher for a typed service slot.
pub struct ServicePublisher<Service: ?Sized + Send + Sync + 'static> {
    slot: Arc<OnceLock<Arc<Service>>>,
}

impl<Service: ?Sized + Send + Sync + 'static> ServicePublisher<Service> {
    /// Publishes the generated service exactly once.
    pub fn publish(&self, service: Arc<Service>) -> Result<(), Arc<Service>> {
        self.slot.set(service)
    }
}

/// Reader for a typed, late-bound service slot.
pub struct ServiceReader<Service: ?Sized + Send + Sync + 'static> {
    slot: Arc<OnceLock<Arc<Service>>>,
}

impl<Service: ?Sized + Send + Sync + 'static> ServiceReader<Service> {
    /// Reads the published service, if its provider has published it.
    pub fn get(&self) -> Option<Arc<Service>> {
        self.slot.get().cloned()
    }

    /// Creates a pre-published reader for isolated downstream tests.
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_published_test_service(service: Arc<Service>) -> Self {
        let slot = Arc::new(OnceLock::new());
        assert!(
            slot.set(service).is_ok(),
            "new test service slot must not already be published"
        );
        Self { slot }
    }
}

impl<Service: ?Sized + Send + Sync + 'static> Clone for ServiceReader<Service> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

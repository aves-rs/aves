use std::future::Future;
use std::pin::Pin;

use tokio::sync::oneshot;

use crate::bus::{BusRpcContext, OpaqueRequestEnvelope};

use super::context::ConstructionFacade;

/// Erased asynchronous callback future used by a domain descriptor.
pub type DomainFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Erased lifecycle callbacks for one installed domain marker.
pub struct DomainCallbacks<Bus: ?Sized> {
    /// Constructs the marker's filtered services and invokes its `new` method.
    pub new: for<'facade, 'bus> fn(&'facade mut ConstructionFacade<'bus, Bus>) -> DomainFuture<'facade>,
    /// Invokes the marker's static `start` method.
    pub start: fn(BusRpcContext) -> DomainFuture<'static>,
    /// Invokes the marker's static `stop` method.
    pub stop: fn(BusRpcContext) -> DomainFuture<'static>,
}

/// Narrow integration contract implemented by an empty domain marker.
///
/// The implementation supplies erased callbacks which delegate to the
/// marker's existing runtime/domain implementation.  It is deliberately a
/// construction contract rather than another lifecycle API.
pub trait DomainDescriptorFactory<Bus: ?Sized>: Send + Sync + 'static {
    /// Returns the marker's type-erased lifecycle callbacks.
    fn callbacks() -> DomainCallbacks<Bus>;
}

/// Type-erased runtime descriptor for one installed domain.
pub struct DomainDescriptor<Bus: ?Sized> {
    name: &'static str,
    callbacks: DomainCallbacks<Bus>,
}

impl<Bus: ?Sized> DomainDescriptor<Bus> {
    /// Creates a descriptor from marker-owned lifecycle callbacks.
    pub const fn from_callbacks(name: &'static str, callbacks: DomainCallbacks<Bus>) -> Self {
        Self { name, callbacks }
    }

    /// Creates a descriptor for an installed empty domain marker.
    pub fn for_domain<Domain>(name: &'static str) -> Self
    where
        Domain: DomainDescriptorFactory<Bus>,
    {
        Self::from_callbacks(name, Domain::callbacks())
    }

    /// Returns the marker name recorded by the installation macro.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn construct<'facade, 'bus>(
        &self,
        facade: &'facade mut ConstructionFacade<'bus, Bus>,
    ) -> DomainFuture<'facade> {
        (self.callbacks.new)(facade)
    }

    pub(crate) fn start(&self, context: BusRpcContext) -> DomainFuture<'static> {
        (self.callbacks.start)(context)
    }

    pub(crate) fn stop(&self, context: BusRpcContext) -> DomainFuture<'static> {
        (self.callbacks.stop)(context)
    }
}

/// Sends one lifecycle request through the normal bus and awaits its typed reply.
pub async fn dispatch_lifecycle_request<Marker, Request, Response, DomainError>(
    context: BusRpcContext,
    request: impl FnOnce(oneshot::Sender<Result<Response, DomainError>>) -> Request,
) -> Option<Result<Response, DomainError>>
where
    Marker: 'static,
    Request: Send + 'static,
    Response: Send + 'static,
    DomainError: Send + 'static,
{
    let (reply_to, reply) = oneshot::channel();
    if context
        .bus_sender()
        .expect("authenticated lifecycle context")
        .send(OpaqueRequestEnvelope::new::<Marker, _>(
            context.jwt().expect("authenticated lifecycle context"),
            request(reply_to),
        ))
        .await
        .is_err()
    {
        return None;
    }
    reply.await.ok()
}

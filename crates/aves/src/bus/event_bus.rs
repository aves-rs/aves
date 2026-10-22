use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::auth::{
    AuthAdmissionRegistry, AuthProjection, AuthSudoService, AvesAuthError, Jwt, messages::VerifyJwtInput,
};

const REQUEST_BUFFER_CAPACITY: usize = 64;
const BROADCAST_BUFFER_CAPACITY: usize = 1000;

type AdmissionRejection = Box<dyn FnOnce(Box<dyn Any + Send>) + Send>;

/// Opaque request envelope owned by the generic bus.
///
/// The envelope contains only the marker-derived [`TypeId`] and the complete
/// concrete request erased behind `Any`. The request itself retains its
/// payload, typed reply channel, response/error types, and handler context.
/// No caller-provided route, raw body, string, numeric identity, or serialized
/// transport metadata can enter the envelope.
pub struct OpaqueRequestEnvelope {
    target: TypeId,
    jwt: Option<Jwt>,
    projection: Option<AuthProjection>,
    rejection: Option<AdmissionRejection>,
    body: Box<dyn Any + Send>,
}

impl OpaqueRequestEnvelope {
    /// Constructs an envelope using only the generic marker's type identity.
    pub fn new<Marker, Request>(jwt: Jwt, request: Request) -> Self
    where
        Marker: 'static,
        Request: Send + 'static,
    {
        Self {
            target: TypeId::of::<Marker>(),
            jwt: Some(jwt),
            projection: None,
            rejection: None,
            body: Box::new(request),
        }
    }

    /// Constructs an envelope with an opaque callback for admission rejection.
    pub fn new_with_rejection<Marker, Request>(
        jwt: Jwt,
        request: Request,
        rejection: impl FnOnce(Box<dyn Any + Send>) + Send + 'static,
    ) -> Self
    where
        Marker: 'static,
        Request: Send + 'static,
    {
        Self {
            target: TypeId::of::<Marker>(),
            jwt: Some(jwt),
            projection: None,
            rejection: Some(Box::new(rejection)),
            body: Box::new(request),
        }
    }

    /// Returns the marker-derived routing identity.
    pub(crate) fn target(&self) -> TypeId {
        self.target
    }

    async fn admit(mut self, auth: Option<&AuthAdmissionConfiguration>) -> Option<Self> {
        let jwt = self.jwt.take()?;
        let Some(configuration) = auth else {
            self.reject(AvesAuthError::Internal("Auth admission is unavailable".to_owned()));
            return None;
        };
        let verified = match configuration
            .verifier
            .verify_jwt(VerifyJwtInput {
                jwt,
                accepted_issuers: configuration.registry.current_issuers(),
                audience: configuration.audience,
            })
            .await
        {
            Ok(verified) => verified,
            Err(error) => {
                self.reject(error);
                return None;
            }
        };
        self.projection = Some(verified.projection);
        Some(self)
    }

    fn reject(&mut self, error: AvesAuthError) {
        if let Some(rejection) = self.rejection.take() {
            rejection(Box::new(error));
        }
    }

    /// Recovers the request and the projection produced by bus admission.
    ///
    /// The raw JWT is consumed before a request reaches a domain adapter.
    pub(crate) fn downcast_with_projection<Request>(self) -> Result<(Request, AuthProjection), Box<Self>>
    where
        Request: Send + 'static,
    {
        let Self {
            target,
            jwt,
            projection,
            rejection: _,
            body,
        } = self;
        drop(jwt);
        let Some(projection) = projection else {
            return Err(Box::new(Self {
                target,
                jwt: None,
                projection: None,
                rejection: None,
                body,
            }));
        };
        match body.downcast::<Request>() {
            Ok(request) => Ok((*request, projection)),
            Err(body) => Err(Box::new(Self {
                target,
                jwt: None,
                projection: Some(projection),
                rejection: None,
                body,
            })),
        }
    }

    /// Returns the marker-derived routing identity for test support.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn test_target(&self) -> TypeId {
        self.target()
    }

    /// Recovers a concrete request for test support.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn test_downcast<Request>(self) -> Result<Request, Self>
    where
        Request: Send + 'static,
    {
        let Self { target, body, .. } = self;
        match body.downcast::<Request>() {
            Ok(request) => Ok(*request),
            Err(body) => Err(Self {
                target,
                jwt: None,
                projection: None,
                rejection: None,
                body,
            }),
        }
    }
}

impl fmt::Debug for OpaqueRequestEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueRequestEnvelope")
            .field("target", &self.target)
            .field("body", &"<opaque>")
            .finish()
    }
}

/// Typed publisher for the root broadcast channel.
pub struct BroadcastAdapter<B, Root>
where
    B: Into<Root> + Clone + Send + 'static,
    Root: Clone + Send + 'static,
{
    sender: broadcast::Sender<Root>,
    _marker: std::marker::PhantomData<B>,
}

impl<B, Root> BroadcastAdapter<B, Root>
where
    B: Into<Root> + Clone + Send + 'static,
    Root: Clone + Send + 'static,
{
    pub fn new(sender: broadcast::Sender<Root>) -> Self {
        Self {
            sender,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn send(&self, event: B) -> Result<usize, broadcast::error::SendError<Root>> {
        self.sender.send(event.into())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Root> {
        self.sender.subscribe()
    }
}

impl<B, Root> Clone for BroadcastAdapter<B, Root>
where
    B: Into<Root> + Clone + Send + 'static,
    Root: Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

/// The channels produced when a marker is registered with the bus.
pub struct DomainChannels<Request, Broadcast, Root>
where
    Request: Send + 'static,
    Broadcast: Into<Root> + Clone + Send + 'static,
    Root: Clone + Send + 'static,
{
    pub broadcasts_tx: BroadcastAdapter<Broadcast, Root>,
    pub broadcasts_rx: broadcast::Receiver<Root>,
    pub requests_tx: mpsc::Sender<OpaqueRequestEnvelope>,
    pub requests_rx: mpsc::Receiver<(Request, AuthProjection)>,
}

#[derive(Clone)]
pub(crate) struct AuthAdmissionConfiguration {
    pub(crate) registry: AuthAdmissionRegistry,
    pub(crate) verifier: Arc<dyn AuthSudoService>,
    pub(crate) audience: uuid::Uuid,
}

/// A marker-keyed, type-erased request and root-broadcast bus.
pub struct EventBus<Root>
where
    Root: Clone + Send + 'static,
{
    input_sender: mpsc::Sender<OpaqueRequestEnvelope>,
    adapters: Arc<RwLock<HashMap<TypeId, mpsc::Sender<OpaqueRequestEnvelope>>>>,
    auth_configuration: Arc<RwLock<Option<AuthAdmissionConfiguration>>>,
    broadcast_sender: broadcast::Sender<Root>,
    routing_shutdown: Option<oneshot::Sender<()>>,
    routing_task: Option<tokio::task::JoinHandle<()>>,
}

impl<Root> Default for EventBus<Root>
where
    Root: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Root> EventBus<Root>
where
    Root: Clone + Send + 'static,
{
    pub fn new() -> Self {
        let (input_sender, mut input_receiver) = mpsc::channel::<OpaqueRequestEnvelope>(REQUEST_BUFFER_CAPACITY);

        let auth_configuration = Arc::new(RwLock::new(None::<AuthAdmissionConfiguration>));
        let routing_auth = Arc::clone(&auth_configuration);

        let adapters: Arc<RwLock<HashMap<TypeId, mpsc::Sender<OpaqueRequestEnvelope>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let routing_adapters = Arc::clone(&adapters);

        let (routing_shutdown, mut shutdown_receiver) = oneshot::channel();

        let routing_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => break,
                    envelope = input_receiver.recv() => {
                        let Some(envelope) = envelope else { break };
                        let auth = routing_auth
                            .read()
                            .expect("auth registry lock poisoned")
                            .clone();
                        let Some(envelope) = envelope.admit(auth.as_ref()).await else { continue };
                        let target = envelope.target();
                        let adapter = routing_adapters
                            .read()
                            .expect("event bus registry lock poisoned")
                            .get(&target)
                            .cloned()
                            .unwrap_or_else(|| panic!("unregistered domain marker: {target:?}"));
                        tokio::select! {
                            _ = &mut shutdown_receiver => break,
                            result = adapter.send(envelope) => {
                                result.expect("registered domain mailbox unexpectedly closed");
                            }
                        }
                    }
                }
            }
        });

        let (broadcast_sender, _) = broadcast::channel(BROADCAST_BUFFER_CAPACITY);
        Self {
            input_sender,
            adapters,
            auth_configuration,
            broadcast_sender,
            routing_shutdown: Some(routing_shutdown),
            routing_task: Some(routing_task),
        }
    }

    pub(crate) fn install_auth(&mut self, configuration: AuthAdmissionConfiguration) {
        let mut auth = self
            .auth_configuration
            .write()
            .expect("auth registry lock poisoned");
        assert!(
            auth.replace(configuration).is_none(),
            "Auth admission is already installed"
        );
    }

    /// Registers a marker and returns its opaque input sender plus typed mailbox.
    pub fn register<Marker, Request, Broadcast>(&mut self) -> DomainChannels<Request, Broadcast, Root>
    where
        Marker: 'static,
        Request: Send + 'static,
        Broadcast: Into<Root> + Clone + Send + 'static,
    {
        let (adapter_tx, mut adapter_rx) = mpsc::channel(REQUEST_BUFFER_CAPACITY);
        let (request_tx, request_rx) = mpsc::channel(REQUEST_BUFFER_CAPACITY);
        let marker = TypeId::of::<Marker>();
        let mut registry = self
            .adapters
            .write()
            .expect("event bus registry lock poisoned");
        assert!(
            registry.insert(marker, adapter_tx).is_none(),
            "domain marker already registered"
        );
        drop(registry);

        tokio::spawn(async move {
            while let Some(envelope) = adapter_rx.recv().await {
                match envelope.downcast_with_projection::<Request>() {
                    Ok((request, projection)) => {
                        if request_tx.send((request, projection)).await.is_err() {
                            break;
                        }
                    }
                    Err(envelope) => {
                        panic!("opaque request body does not match its marker-derived route: {envelope:?}");
                    }
                }
            }
        });

        DomainChannels {
            broadcasts_tx: BroadcastAdapter::new(self.broadcast_sender.clone()),
            broadcasts_rx: self.broadcast_sender.subscribe(),
            requests_tx: self.input_sender.clone(),
            requests_rx: request_rx,
        }
    }

    pub fn broadcast_sender(&self) -> broadcast::Sender<Root> {
        self.broadcast_sender.clone()
    }

    /// Returns the single opaque ingress shared by all registered domains.
    pub fn input_sender(&self) -> mpsc::Sender<OpaqueRequestEnvelope> {
        self.input_sender.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Root> {
        self.broadcast_sender.subscribe()
    }

    pub(crate) fn into_routing_parts(
        self,
    ) -> (
        mpsc::Sender<OpaqueRequestEnvelope>,
        broadcast::Sender<Root>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        (
            self.input_sender,
            self.broadcast_sender,
            self.routing_shutdown
                .expect("event bus routing shutdown missing"),
            self.routing_task.expect("event bus routing task missing"),
        )
    }

    pub(crate) fn abort_routing(&mut self) {
        if let Some(shutdown) = self.routing_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.routing_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FirstMarker;
    struct SecondMarker;
    struct FirstRequest;
    struct CompleteRequest {
        payload: u8,
        reply_to: std::sync::mpsc::Sender<u8>,
    }

    #[test]
    fn test_downcast_recovers_the_complete_request_without_projection() {
        let (reply_to, reply_receiver) = std::sync::mpsc::channel();
        let envelope = OpaqueRequestEnvelope::new::<FirstMarker, _>(
            Jwt::from_opaque("test-jwt".to_owned()),
            CompleteRequest { payload: 7, reply_to },
        );
        let request = envelope
            .test_downcast::<CompleteRequest>()
            .expect("request should downcast to its original concrete type");

        assert_eq!(request.payload, 7);
        request
            .reply_to
            .send(11)
            .expect("reply channel remains intact");
        assert_eq!(reply_receiver.recv().expect("reply should arrive"), 11);
    }

    #[test]
    fn route_identity_is_derived_only_from_the_marker() {
        let request = CompleteRequest {
            payload: 7,
            reply_to: std::sync::mpsc::channel().0,
        };
        let envelope = OpaqueRequestEnvelope::new::<FirstMarker, _>(Jwt::from_opaque("test-jwt".to_owned()), request);

        assert_eq!(envelope.target(), TypeId::of::<FirstMarker>());
        assert_ne!(envelope.target(), TypeId::of::<SecondMarker>());
    }

    #[tokio::test]
    async fn admission_requires_auth_configuration() {
        let envelope =
            OpaqueRequestEnvelope::new::<FirstMarker, _>(Jwt::from_opaque("test-jwt".to_owned()), FirstRequest);

        assert!(envelope.admit(None).await.is_none());
    }

    #[test]
    fn debug_output_keeps_the_erased_request_opaque() {
        let debug_for = |payload| {
            let envelope = OpaqueRequestEnvelope::new::<FirstMarker, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                CompleteRequest {
                    payload,
                    reply_to: std::sync::mpsc::channel().0,
                },
            );
            format!("{envelope:?}")
        };
        let debug = debug_for(99);

        assert!(debug.contains("OpaqueRequestEnvelope"));
        assert!(debug.contains("<opaque>"));
        assert_eq!(debug, debug_for(1));
    }

    #[tokio::test]
    async fn duplicate_marker_registration_is_rejected() {
        let mut bus = EventBus::<u8>::new();
        let _ = bus.register::<FirstMarker, CompleteRequest, u8>();

        let duplicate = std::panic::AssertUnwindSafe(|| {
            bus.register::<FirstMarker, CompleteRequest, u8>();
        });
        assert!(std::panic::catch_unwind(duplicate).is_err());
    }
}

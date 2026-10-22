use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

use crate::auth::Jwt;
use crate::runtime::ServicePublisher;

use super::PeerConnectivity;
use super::control::{BusControlError, BusSudoService, TransportBindings, start_control};
use super::event_bus::{EventBus, OpaqueRequestEnvelope};
use super::messages::BusRpcContext;

/// Handle for in-process bus ingress and bus-owned listener lifecycle.
#[must_use = "BusHandle owns the bus ingress and must be retained"]
pub(crate) struct Handle<Broadcast>
where
    Broadcast: Clone + Send + 'static,
{
    input_sender: mpsc::Sender<OpaqueRequestEnvelope>,
    broadcast_sender: broadcast::Sender<Broadcast>,
    routing_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    routing_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    control_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    control_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    root_jwt: Jwt,
}

impl<Broadcast> Handle<Broadcast>
where
    Broadcast: Clone + Send + 'static,
{
    /// Creates the bus ingress, publishes the private listener-control service,
    /// and starts the single actor that owns both listener registries.
    pub(crate) async fn new(
        mut driver: EventBus<Broadcast>,
        bindings: TransportBindings,
        publisher: ServicePublisher<dyn BusSudoService>,
        root_jwt: Jwt,
        peer_connectivity: PeerConnectivity,
    ) -> Result<Self, BusControlError> {
        let input_sender = driver.input_sender();
        let broadcast_sender = driver.broadcast_sender();
        let (control_shutdown, _control_requests, control_task) =
            match start_control(input_sender.clone(), bindings, publisher, peer_connectivity) {
                Ok(handles) => handles,
                Err(error) => {
                    driver.abort_routing();
                    return Err(error);
                }
            };
        let (_, _, routing_shutdown, routing_task) = driver.into_routing_parts();

        Ok(Self {
            input_sender,
            broadcast_sender,
            routing_shutdown: Mutex::new(Some(routing_shutdown)),
            routing_task: Mutex::new(Some(routing_task)),
            control_shutdown: Mutex::new(Some(control_shutdown)),
            control_task: Mutex::new(Some(control_task)),
            root_jwt,
        })
    }

    /// Subscribes to root broadcasts from an embedding host.
    pub(crate) fn subscribe_broadcasts(&self) -> broadcast::Receiver<Broadcast> {
        self.broadcast_sender.subscribe()
    }

    /// Creates the server-issued normal RPC context.
    pub(crate) fn rpc_context(&self) -> BusRpcContext {
        BusRpcContext::authenticated(self.input_sender.clone(), self.root_jwt.clone())
    }

    /// Returns the generic marker-keyed ingress shared with generated dispatchers.
    pub(crate) fn input_sender(&self) -> mpsc::Sender<OpaqueRequestEnvelope> {
        self.input_sender.clone()
    }

    /// Gracefully closes every listener and then joins the control actor.
    pub(crate) async fn shutdown(&self) -> Result<(), BusControlError> {
        let routing_send_error = self
            .routing_shutdown
            .lock()
            .await
            .take()
            .and_then(|shutdown| shutdown.send(()).err())
            .map(|_| BusControlError::ChannelSend("bus routing task is closed".to_string()));
        let control_send_error = self
            .control_shutdown
            .lock()
            .await
            .take()
            .and_then(|shutdown| shutdown.send(()).err())
            .map(|_| BusControlError::ChannelSend("control actor is closed".to_string()));
        let routing_join_error = if let Some(task) = self.routing_task.lock().await.take() {
            task.await
                .map_err(|source| BusControlError::Join {
                    protocol: "bus routing",
                    source,
                })
                .err()
        } else {
            None
        };
        let control_join_error = if let Some(task) = self.control_task.lock().await.take() {
            task.await
                .map_err(|source| BusControlError::Join {
                    protocol: "bus control",
                    source,
                })
                .err()
        } else {
            None
        };

        routing_send_error
            .or(control_send_error)
            .or(routing_join_error)
            .or(control_join_error)
            .map_or(Ok(()), Err)
    }
}

impl<Broadcast> Drop for Handle<Broadcast>
where
    Broadcast: Clone + Send + 'static,
{
    fn drop(&mut self) {
        let routing_shutdown_sent = self
            .routing_shutdown
            .try_lock()
            .ok()
            .and_then(|mut shutdown| shutdown.take())
            .map(|shutdown| shutdown.send(()).is_ok())
            .unwrap_or(false);
        let shutdown_sent = self
            .control_shutdown
            .try_lock()
            .ok()
            .and_then(|mut shutdown| shutdown.take())
            .map(|shutdown| shutdown.send(()).is_ok())
            .unwrap_or(false);
        if !routing_shutdown_sent
            && let Ok(task) = self.routing_task.try_lock()
            && let Some(task) = task.as_ref()
            && !task.is_finished()
        {
            task.abort();
        }
        if !shutdown_sent
            && let Ok(task) = self.control_task.try_lock()
            && let Some(task) = task.as_ref()
            && !task.is_finished()
        {
            task.abort();
        }
    }
}

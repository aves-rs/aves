use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use uuid::Uuid;

use aves::bus::BusRpcContext;
use aves::flow::{FlowTriggerRecord, FlowTriggerSourceService, LifecycleCorrelation, NodeRegistrationSupply};
use aves::runtime::ServiceReader;
use aves::utils::uuid::SpectaUuid;

use super::channels::{
    CHANNEL_MESSAGE_BUFFER_SIZE, InboundEvent, MessagingPlugin, MessagingPluginHandle, MessagingTask, for_kind,
};
use super::database::MessageStore;
use super::messages::{MessagingRequest, MessagingRequestPayload, MessagingResponse};
use super::registrations::{NodeCommand, TytoNodeHandle, message_node_registration, permission_node_registration};
use super::types::{
    ConnectionTransport, MessagingCredentials, MessagingError, MessagingInteraction, MessagingSendReceipt,
    NormalizedMessage, PersistencePolicy, SubscriptionTriggerConfig,
};

pub(super) type Subscribers = Arc<Mutex<HashMap<Uuid, mpsc::Sender<NormalizedMessage>>>>;

pub(super) struct ActorInbound {
    pub(super) connection_id: SpectaUuid,
    pub(super) event: InboundEvent,
}

pub(super) struct ActorInteraction {
    pub(super) connection_id: SpectaUuid,
    pub(super) interaction: MessagingInteraction,
}

pub(super) struct SendCompletion {
    pub(super) reply_to: oneshot::Sender<Result<MessagingResponse, MessagingError>>,
    pub(super) connection_id: SpectaUuid,
    pub(super) body: String,
    pub(super) metadata: Option<String>,
    pub(super) retention_seconds: i64,
    pub(super) result: Result<MessagingSendReceipt, MessagingError>,
    pub(super) persistence: PersistencePolicy,
}

pub(super) struct PermSendCompleted {
    pub(super) request_id: String,
    pub(super) connection_id: SpectaUuid,
    pub(super) retention_seconds: i64,
    pub(super) body: String,
    pub(super) metadata: Option<String>,
    pub(super) result: Result<String, MessagingError>,
}

pub(super) struct EditCompleted {
    pub(super) request_id: String,
    pub(super) result: Result<(), MessagingError>,
}

pub(super) enum NodeEvent {
    Command(NodeCommand),
    MessageSendCompleted(LifecycleCorrelation, Result<MessagingSendReceipt, MessagingError>),
    PermissionSendCompleted(PermSendCompleted),
    EditCompleted(EditCompleted),
}

pub(super) enum ProviderIngress {
    Message(ActorInbound),
    Interaction(ActorInteraction),
}

pub(super) struct NodePending {
    pub(super) connection_id: SpectaUuid,
    pub(super) retention_seconds: i64,
    pub(super) body: String,
    pub(super) metadata: Option<String>,
    pub(super) result: Option<Result<MessagingSendReceipt, MessagingError>>,
}

pub(super) struct Listener {
    pub(super) trigger: FlowTriggerRecord,
    pub(super) configuration: SubscriptionTriggerConfig,
    pub(super) task: JoinHandle<()>,
    pub(super) filter_task: MessagingTask,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
        self.filter_task.abort();
    }
}

pub(super) struct Inflow {
    pub(super) connection_id: SpectaUuid,
    pub(super) retention_seconds: i64,
    pub(super) plugin: MessagingPluginHandle,
    pub(super) transport: ConnectionTransport,
    pub(super) credentials: MessagingCredentials,
    pub(super) credential_generation: u64,
    pub(super) subscribers: Subscribers,
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) producer: Arc<StdMutex<ProducerState>>,
}

impl Drop for Inflow {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        let mut producer = self.producer.lock().expect("inflow task state");
        producer.stopping = true;
        if let Some(handle) = &producer.handle {
            handle.abort();
        }
    }
}

#[derive(Default)]
pub(super) struct ProducerState {
    pub(super) handle: Option<AbortHandle>,
    pub(super) stopping: bool,
}

pub(super) struct Actor {
    pub(super) receiver: mpsc::Receiver<MessagingRequest>,
    pub(super) broadcast_tx: mpsc::Sender<MessagingResponse>,
    pub(super) config_dir: PathBuf,
    pub(super) store: Option<MessageStore>,
    pub(super) node_tx: mpsc::Sender<NodeEvent>,
    pub(super) node_rx: mpsc::Receiver<NodeEvent>,
    pub(super) node_pending: HashMap<LifecycleCorrelation, NodePending>,
    pub(super) ingress_tx: mpsc::Sender<ProviderIngress>,
    pub(super) ingress_rx: mpsc::Receiver<ProviderIngress>,
    pub(super) send_tx: mpsc::Sender<SendCompletion>,
    pub(super) send_rx: mpsc::Receiver<SendCompletion>,
    pub(super) credentials: HashMap<SpectaUuid, MessagingCredentials>,
    pub(super) credential_generation: HashMap<SpectaUuid, u64>,
    pub(super) flows: ServiceReader<dyn FlowTriggerSourceService>,
    pub(super) knowledge_context: BusRpcContext,
    pub(super) listeners: HashMap<Uuid, Listener>,
    pub(super) inflows: HashMap<SpectaUuid, Inflow>,
    pub(super) running: bool,
}

impl Actor {
    /// Constructs the actor and registers its Flow node kinds.
    pub(super) async fn new(
        receiver: mpsc::Receiver<MessagingRequest>,
        broadcast_tx: mpsc::Sender<MessagingResponse>,
        config_dir: PathBuf,
        node_registration_supply: NodeRegistrationSupply,
        flows: ServiceReader<dyn FlowTriggerSourceService>,
        knowledge_context: BusRpcContext,
    ) -> Result<Self, MessagingError> {
        let store = Some(MessageStore::open(&config_dir).await?);
        let (ingress_tx, ingress_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);
        let (send_tx, send_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);
        let (node_tx, node_rx) = mpsc::channel(64);
        let node_handle = Arc::new(TytoNodeHandle { tx: node_tx.clone() });

        node_registration_supply.push(message_node_registration(node_handle.clone()));
        node_registration_supply.push(permission_node_registration(node_handle));

        Ok(Self {
            receiver,
            broadcast_tx,
            config_dir,
            store,
            node_tx,
            node_rx,
            node_pending: HashMap::new(),
            ingress_tx,
            ingress_rx,
            send_tx,
            send_rx,
            credentials: HashMap::new(),
            credential_generation: HashMap::new(),
            flows,
            knowledge_context,
            listeners: HashMap::new(),
            inflows: HashMap::new(),
            running: false,
        })
    }

    /// Routes a public request and replies immediately unless Send is handed off.
    async fn handle_message(&mut self, request: MessagingRequest) {
        let result = match request.payload {
            MessagingRequestPayload::StartServer => self.start_server().await,
            MessagingRequestPayload::StopServer => self.stop_server().await,
            MessagingRequestPayload::SupplyCredential {
                credential_id,
                credentials,
            } => {
                self.credentials.insert(credential_id, credentials);
                *self.credential_generation.entry(credential_id).or_default() += 1;
                self.reconcile().await
            }
            MessagingRequestPayload::RemoveCredential { credential_id } => {
                self.credentials.remove(&credential_id);
                self.credential_generation.remove(&credential_id);
                self.reconcile().await
            }
            MessagingRequestPayload::ListConnections => self.list_connections().await,
            MessagingRequestPayload::CreateConnection { input } => self.create_connection(input).await,
            MessagingRequestPayload::UpdateConnection { id, input } => self.update_connection(id, input).await,
            MessagingRequestPayload::SetConnectionEnabled { connection_id, enabled } => {
                self.set_connection_enabled(connection_id, enabled).await
            }
            MessagingRequestPayload::DeleteConnection { connection_id } => self.delete_connection(connection_id).await,
            MessagingRequestPayload::List => Ok(self.list_subscriptions()),
            MessagingRequestPayload::Send { sender, message } => {
                self.start_send(request.reply_to, sender, message).await;
                return;
            }
            MessagingRequestPayload::SendMetadataSchema { listener_type } => {
                Ok(MessagingResponse::SendMetadataSchema {
                    schema: Value::from(for_kind(listener_type).send_metadata_schema()),
                })
            }
        };
        let _ = request.reply_to.send(result);
    }

    /// Reconciles listeners before starting the periodic maintenance worker.
    pub(super) async fn start_server(&mut self) -> Result<MessagingResponse, MessagingError> {
        self.running = true;
        self.reconcile().await
    }

    /// Selects the plugin used only while reconciling inbound listeners.
    pub(super) fn reconciliation_plugin(transport: &ConnectionTransport) -> Box<dyn MessagingPlugin> {
        #[cfg(not(test))]
        {
            for_kind(transport.provider())
        }
        #[cfg(test)]
        {
            let _ = transport;
            tests::pending_channel()
        }
    }

    /// Returns the message store, opening it if necessary.
    pub(super) async fn store(&mut self) -> Result<&MessageStore, MessagingError> {
        if self.store.is_none() {
            self.store = Some(MessageStore::open(&self.config_dir).await?);
        }
        Ok(self.store.as_ref().expect("opened"))
    }

    /// Returns a parsed connection UUID, or an error if the ID is invalid.
    pub(super) fn connection_uuid(id: &str) -> Result<SpectaUuid, MessagingError> {
        Uuid::parse_str(id)
            .map(SpectaUuid)
            .map_err(|error| MessagingError::ConnectionNotFound {
                connection_id: error.to_string(),
            })
    }

    /// Stops owned tasks and releases the store after maintenance.
    pub(super) async fn stop_server(&mut self) -> Result<MessagingResponse, MessagingError> {
        self.running = false;
        self.listeners.clear();
        self.inflows.clear();
        if let Some(store) = self.store.take() {
            store.maintain().await;
        }
        Ok(MessagingResponse::Success)
    }

    async fn handle_node_event(&mut self, event: NodeEvent) {
        match event {
            NodeEvent::Command(command) => self.handle_node_command(command).await,
            NodeEvent::MessageSendCompleted(correlation, result) => {
                if let Some(pending) = self.node_pending.get_mut(&correlation) {
                    pending.result = Some(result);
                }
            }
            NodeEvent::PermissionSendCompleted(completion) => self.handle_permission_send_completion(completion).await,
            NodeEvent::EditCompleted(completion) => self.handle_edit_completion(completion).await,
        }
    }

    async fn handle_provider_ingress(&mut self, event: ProviderIngress) {
        match event {
            ProviderIngress::Message(inbound) => self.handle_inbound_delivery(inbound).await,
            ProviderIngress::Interaction(interaction) => self.handle_interaction(interaction).await,
        }
    }
}

/// Runs the sequential actor loop; channel closure terminates the actor.
pub(super) async fn run_actor(mut actor: Actor) {
    let maintenance_period = Duration::from_secs(300);
    let mut maintenance = interval_at(Instant::now() + maintenance_period, maintenance_period);
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            request = actor.receiver.recv() => {
                let Some(request) = request else { break; };
                let was_running = actor.running;
                actor.handle_message(request).await;
                if !was_running && actor.running {
                    maintenance = interval_at(Instant::now() + maintenance_period, maintenance_period);
                    maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
                }
            }

            _ = maintenance.tick(), if actor.running => {
                actor.handle_worker_tick().await;
            }

            inbound = actor.ingress_rx.recv() => {
                let Some(event) = inbound else { break; };
                actor.handle_provider_ingress(event).await;
            }

            command = actor.node_rx.recv() => {
                let Some(command) = command else { break; };
                actor.handle_node_event(command).await;
            }

            completion = actor.send_rx.recv() => {
                let Some(completion) = completion else { break; };
                actor.handle_send_completion(completion).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;

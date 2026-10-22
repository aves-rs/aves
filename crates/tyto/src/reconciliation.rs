use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::{from_str, from_value, json, to_value};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{select, spawn};
use uuid::Uuid;

use aves::bus::BusRpcContext;
use aves::flow::{FireSourceTriggerInput, FlowTriggerRecord, FlowTriggerSourceService, ListSourceTriggersInput};
use aves::runtime::ServiceReader;
use aves::utils::{retry::RetryBackoff, uuid::SpectaUuid};

use super::actor::{Actor, ActorInbound, ActorInteraction, Inflow, Listener, ProducerState, ProviderIngress};
use super::channels::{
    CHANNEL_MESSAGE_BUFFER_SIZE, InboundEvent, MessagingPluginHandle, MessagingSubscription, MessagingTask,
};
use super::messages::MessagingResponse;
use super::types::{
    ConnectionTransport, MessagingCredentials, MessagingError, MessagingInteraction, NormalizedMessage,
    PersistencePolicy, SubscriptionTriggerConfig,
};

impl Actor {
    /// Reconciles authenticated inflows before optionally admitting Flow listeners.
    pub(super) async fn reconcile(&mut self) -> Result<MessagingResponse, MessagingError> {
        if !self.running {
            return Ok(MessagingResponse::Success);
        }

        let desired = self.desired_connections().await?;
        let stale = self.reconcile_inflows(&desired);
        if let Some(triggers) = self.trigger_inventory().await {
            let desired = self.desired_listeners(triggers);
            self.remove_stale_listeners(&desired, &stale).await;
            self.attach_listeners(desired).await;
        }
        self.start_inflows();
        Ok(MessagingResponse::Success)
    }

    async fn desired_connections(
        &mut self,
    ) -> Result<HashMap<SpectaUuid, (ConnectionTransport, MessagingCredentials, u64, i64)>, MessagingError> {
        let connections = self.store().await?.list_connections().await?;
        let mut desired = HashMap::new();
        for connection in connections
            .into_iter()
            .filter(|connection| connection.enabled)
        {
            let connection_id = match Uuid::parse_str(&connection.id) {
                Ok(id) => SpectaUuid(id),
                Err(error) => {
                    tracing::warn!(%error, connection_id = %connection.id, "Invalid messaging connection identifier");
                    continue;
                }
            };
            let transport = match from_str::<ConnectionTransport>(&connection.transport_settings) {
                Ok(transport) => transport,
                Err(error) => {
                    tracing::warn!(%error, %connection_id, "Invalid messaging connection transport settings");
                    continue;
                }
            };
            let credential_id = match Uuid::parse_str(&connection.credential_id) {
                Ok(id) => SpectaUuid(id),
                Err(error) => {
                    tracing::warn!(%error, %connection_id, "Invalid messaging credential identifier");
                    continue;
                }
            };
            let Some(credentials) = self.credentials.get(&credential_id).cloned() else {
                tracing::warn!(%connection_id, %credential_id, "Messaging credential is missing");
                continue;
            };
            let generation = self
                .credential_generation
                .get(&credential_id)
                .copied()
                .unwrap_or_default();
            desired.insert(
                connection_id,
                (transport, credentials, generation, connection.retention_seconds),
            );
        }
        Ok(desired)
    }

    fn reconcile_inflows(
        &mut self,
        desired: &HashMap<SpectaUuid, (ConnectionTransport, MessagingCredentials, u64, i64)>,
    ) -> Vec<SpectaUuid> {
        let stale: Vec<_> = self
            .inflows
            .iter()
            .filter_map(|(id, inflow)| {
                let Some((transport, _, generation, _)) = desired.get(id) else {
                    return Some(*id);
                };
                (inflow.credential_generation != *generation
                    || to_value(&inflow.transport).ok() != to_value(transport).ok())
                .then_some(*id)
            })
            .collect();
        for id in &stale {
            self.inflows.remove(id);
        }
        for (connection_id, (transport, credentials, generation, retention_seconds)) in desired {
            if let Some(inflow) = self.inflows.get_mut(connection_id) {
                inflow.retention_seconds = *retention_seconds;
                continue;
            }
            let plugin = Actor::reconciliation_plugin(transport);
            self.inflows.insert(
                *connection_id,
                Inflow {
                    connection_id: *connection_id,
                    retention_seconds: *retention_seconds,
                    plugin: Arc::new(Mutex::new(plugin)),
                    transport: transport.clone(),
                    credentials: credentials.clone(),
                    credential_generation: *generation,
                    subscribers: Arc::new(Mutex::new(HashMap::new())),
                    task: None,
                    producer: Arc::new(StdMutex::new(ProducerState::default())),
                },
            );
        }
        stale
    }

    async fn trigger_inventory(&self) -> Option<Vec<FlowTriggerRecord>> {
        let Some(flows) = self.flows.get() else {
            tracing::warn!("Flow service unavailable; preserving existing messaging listeners");
            return None;
        };
        match flows
            .list_source_triggers(ListSourceTriggersInput {
                adapter_kind: "subscription".to_owned(),
            })
            .await
        {
            Ok(listed) => Some(listed.triggers),
            Err(error) => {
                tracing::warn!(%error, "Could not list subscription Flow triggers; preserving existing listeners");
                None
            }
        }
    }

    fn desired_listeners(
        &self,
        triggers: Vec<FlowTriggerRecord>,
    ) -> HashMap<Uuid, (FlowTriggerRecord, SubscriptionTriggerConfig)> {
        let mut desired = HashMap::new();
        for trigger in triggers {
            let configuration = match from_value::<SubscriptionTriggerConfig>(trigger.adapter_config.clone()) {
                Ok(configuration) => configuration,
                Err(error) => {
                    tracing::warn!(%error, trigger_id = %trigger.trigger_id, "Invalid messaging trigger configuration");
                    continue;
                }
            };
            if !self.inflows.contains_key(&configuration.connection_id) {
                tracing::warn!(trigger_id = %trigger.trigger_id, connection_id = %configuration.connection_id, "Messaging connection has no live inflow");
                continue;
            }
            desired.insert(trigger.trigger_id, (trigger, configuration));
        }
        desired
    }

    async fn remove_stale_listeners(
        &mut self,
        desired: &HashMap<Uuid, (FlowTriggerRecord, SubscriptionTriggerConfig)>,
        stale: &[SpectaUuid],
    ) {
        let removed: Vec<_> = self
            .listeners
            .iter()
            .filter_map(|(id, listener)| {
                let changed = desired.get(id).is_some_and(|(trigger, configuration)| {
                    listener.configuration != *configuration
                        || listener.trigger.flow_id != trigger.flow_id
                        || listener.trigger.revision != trigger.revision
                        || listener.trigger.enabled != trigger.enabled
                });
                (!desired.contains_key(id)
                    || changed
                    || stale.contains(&listener.configuration.connection_id)
                    || !self
                        .inflows
                        .contains_key(&listener.configuration.connection_id)
                    || listener.task.is_finished())
                .then_some(*id)
            })
            .collect();

        for id in removed {
            if let Some(listener) = self.listeners.remove(&id)
                && let Some(inflow) = self.inflows.get(&listener.configuration.connection_id)
            {
                inflow.subscribers.lock().await.remove(&id);
            }
        }
    }

    async fn attach_listeners(&mut self, desired: HashMap<Uuid, (FlowTriggerRecord, SubscriptionTriggerConfig)>) {
        for (id, (trigger, configuration)) in desired {
            if self.listeners.contains_key(&id) {
                continue;
            }

            let inflow = self
                .inflows
                .get(&configuration.connection_id)
                .expect("desired trigger has live inflow");

            let (input_tx, input_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);
            let (output_tx, output_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);

            let mut filter_plugin = Actor::reconciliation_plugin(&inflow.transport);
            let filter_task = match filter_plugin
                .run_filters(MessagingSubscription::from(&configuration), input_rx, output_tx)
                .await
            {
                Ok(task) => task,
                Err(error) => {
                    tracing::warn!(trigger_id = %id, %error, "Messaging subscription configuration is invalid");
                    continue;
                }
            };
            inflow.subscribers.lock().await.insert(id, input_tx);

            let task = spawn(Self::dispatch_messages(
                id,
                configuration.clone(),
                output_rx,
                self.flows.clone(),
                self.broadcast_tx.clone(),
                self.knowledge_context.clone(),
            ));

            self.listeners.insert(
                id,
                Listener {
                    trigger,
                    configuration,
                    task,
                    filter_task,
                },
            );
        }
    }

    fn start_inflows(&mut self) {
        for inflow in self.inflows.values_mut() {
            if inflow.task.as_ref().is_some_and(JoinHandle::is_finished) {
                inflow.task.take();
            }
            if inflow.task.is_none() {
                inflow.task = Some(spawn(Self::run_inflow(
                    inflow.plugin.clone(),
                    inflow.transport.clone(),
                    inflow.credentials.clone(),
                    inflow.producer.clone(),
                    inflow.connection_id,
                    self.ingress_tx.clone(),
                )));
            }
        }
    }

    async fn run_inflow(
        plugin: MessagingPluginHandle,
        transport: ConnectionTransport,
        credentials: MessagingCredentials,
        active: Arc<StdMutex<ProducerState>>,
        connection_id: SpectaUuid,
        ingress_tx: mpsc::Sender<ProviderIngress>,
    ) {
        let settings = transport.messaging_config();
        let mut backoff = RetryBackoff::new(Duration::from_secs(10), Duration::from_secs(60));

        loop {
            let connected = plugin.lock().await.connect(&settings, &credentials).await;
            if let Err(error) = connected {
                tracing::warn!(%error, "Messaging inflow connection failed; retrying");
                sleep(backoff.next_delay()).await;
                continue;
            }

            let (event_tx, mut event_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);
            let (inter_tx, mut inter_rx) = mpsc::channel(CHANNEL_MESSAGE_BUFFER_SIZE);

            let listen_result = plugin.lock().await.listen(event_tx, inter_tx).await;
            let mut task = match listen_result {
                Ok(task) => task,
                Err(error) => {
                    tracing::warn!(%error, "Messaging inflow could not start");
                    sleep(backoff.next_delay()).await;
                    continue;
                }
            };

            if !Self::register_producer(&active, &task) {
                task.abort();
                return;
            }

            let retry = Self::pump_inflow(
                &mut task,
                &mut event_rx,
                &mut inter_rx,
                connection_id,
                &ingress_tx,
                &mut backoff,
            )
            .await;
            if !retry {
                return;
            }

            active.lock().expect("inflow task state").handle = None;
            sleep(backoff.next_delay()).await;
        }
    }

    /// Records the provider task so inflow teardown can abort it safely.
    fn register_producer(active: &Arc<StdMutex<ProducerState>>, task: &MessagingTask) -> bool {
        let mut state = active.lock().expect("inflow task state");
        if state.stopping {
            return false;
        }
        state.handle = Some(task.abort_handle());
        true
    }

    /// Forwards provider queues and resets retry state after an inbound event.
    async fn pump_inflow(
        task: &mut MessagingTask,
        events: &mut mpsc::Receiver<InboundEvent>,
        interactions: &mut mpsc::Receiver<MessagingInteraction>,
        connection_id: SpectaUuid,
        ingress_tx: &mpsc::Sender<ProviderIngress>,
        backoff: &mut RetryBackoff,
    ) -> bool {
        loop {
            select! {
                result = &mut *task => {
                    if let Ok(Err(error)) = result {
                        tracing::warn!(%error, "Messaging inflow will reconnect");
                    }
                    break;
                }
                event = events.recv() => {
                    let Some(event) = event else { task.abort(); return false; };
                    backoff.reset();
                    if ingress_tx.send(ProviderIngress::Message(ActorInbound { connection_id, event })).await.is_err() { task.abort(); return false; }
                }
                interaction = interactions.recv() => {
                    let Some(interaction) = interaction else { task.abort(); return false; };
                    if ingress_tx.send(ProviderIngress::Interaction(ActorInteraction { connection_id, interaction })).await.is_err() { task.abort(); return false; }
                }
            }
        }
        true
    }

    async fn dispatch_messages(
        trigger_id: Uuid,
        configuration: SubscriptionTriggerConfig,
        mut rx: mpsc::Receiver<NormalizedMessage>,
        flows: ServiceReader<dyn FlowTriggerSourceService>,
        broadcast_tx: mpsc::Sender<MessagingResponse>,
        knowledge_context: BusRpcContext,
    ) {
        while let Some(message) = rx.recv().await {
            if configuration.persistence != PersistencePolicy::None {
                Self::spawn_knowledge_handoff(
                    knowledge_context.clone(),
                    configuration.persistence.clone(),
                    trigger_id.to_string(),
                    message.clone(),
                );
            }
            let pulse = json!({
                "source": "subscription",
                "trigger_id": trigger_id,
                "connection_id": configuration.connection_id,
                "body": message.body,
                "metadata": message.metadata,
                "received_at": Utc::now().to_rfc3339(),
            });
            if broadcast_tx
                .send(MessagingResponse::ChannelMessage {
                    body: message.body,
                    metadata: message.metadata,
                })
                .await
                .is_err()
            {
                return;
            }
            let Some(service) = flows.get() else {
                tracing::warn!(%trigger_id, "Flow source service unavailable");
                continue;
            };
            match service
                .fire_source_trigger(FireSourceTriggerInput { trigger_id, pulse })
                .await
            {
                Ok(_) => {
                    let _ = broadcast_tx
                        .send(MessagingResponse::SubscriptionTrigger)
                        .await;
                }
                Err(error) => tracing::warn!(%trigger_id, %error, "Subscription Flow trigger rejected"),
            }
        }
    }
}

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use uuid::Uuid;
use zeroize::Zeroizing;

use aves::auth::Jwt;
use aves::bus::BusRpcContext;
use aves::flow::{
    FireSourceTriggerInput, FlowError, FlowId, FlowTriggerRecord, FlowTriggerSourceService, ListSourceTriggersInput,
    NodeRegistrationSupply, SourceTriggersListedOutput, StartedOutput,
};
use aves::runtime::ServiceReader;
use aves::utils::uuid::SpectaUuid;

use super::super::channels::{InboundEvent, MessagingPlugin, MessagingSubscription, MessagingTask};
use super::super::database::MessagingConnection;
use super::super::messages::{ListenerType, MessagingRequest, MessagingRequestPayload, MessagingResponse};
use super::super::types::{
    ConnectionTransport, EmailConfig, MessagingConfig, MessagingCredentials, MessagingError, MessagingInteraction,
    MessagingSendReceipt, MessagingSenderConfig, MessagingSubscriptionError, NormalizedMessage, PersistencePolicy,
    SubscriptionTriggerConfig, TelegramConfig, TelegramSenderConfig,
};

use super::Actor;

struct Sources(Mutex<Vec<FlowTriggerRecord>>);

fn loggable_subscription_error(error: &MessagingError) -> String {
    match error {
        MessagingError::ChannelConnectionFailed { listener_type, .. } => {
            format!("channel connection failed for {listener_type}")
        }
        _ => "messaging runtime error".to_owned(),
    }
}

#[async_trait::async_trait]
impl FlowTriggerSourceService for Sources {
    async fn list_source_triggers(
        &self,
        input: ListSourceTriggersInput,
    ) -> Result<SourceTriggersListedOutput, FlowError> {
        assert_eq!(input.adapter_kind, "subscription");
        Ok(SourceTriggersListedOutput {
            triggers: self.0.lock().unwrap().clone(),
        })
    }

    async fn fire_source_trigger(&self, _input: FireSourceTriggerInput) -> Result<StartedOutput, FlowError> {
        Err(FlowError::TriggerNotFound)
    }
}

pub(super) struct PendingChannel {
    connect_attempts: usize,
}

pub(super) fn pending_channel() -> Box<dyn MessagingPlugin> {
    Box::new(PendingChannel { connect_attempts: 0 })
}

#[async_trait::async_trait]
impl MessagingPlugin for PendingChannel {
    async fn connect(
        &mut self,
        settings: &MessagingConfig,
        credentials: &MessagingCredentials,
    ) -> Result<(), MessagingError> {
        self.connect_attempts += 1;
        if matches!(credentials, MessagingCredentials::Telegram { token } if token.as_str().starts_with("fail-") && self.connect_attempts <= token.trim_start_matches("fail-").parse::<usize>().unwrap_or(1))
        {
            return Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Telegram,
                message: "temporary connection failure".to_owned(),
            });
        }
        if settings.telegram.is_some() && matches!(credentials, MessagingCredentials::Telegram { .. }) {
            Ok(())
        } else {
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Telegram,
                message: "Telegram settings or credentials are missing".to_owned(),
            })
        }
    }

    async fn send(
        &mut self,
        _sender: &MessagingSenderConfig,
        _message: &NormalizedMessage,
    ) -> Result<MessagingSendReceipt, MessagingError> {
        if _message.body == "block-send" {
            std::future::pending().await
        }
        Ok(MessagingSendReceipt::ProviderMessageId {
            message_id: "test".to_owned(),
        })
    }

    fn send_metadata_schema(&self) -> schemars::Schema {
        schemars::schema_for!(NormalizedMessage)
    }

    async fn listen(
        &mut self,
        _tx: mpsc::Sender<InboundEvent>,
        _interaction_tx: mpsc::Sender<MessagingInteraction>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        Ok(tokio::spawn(std::future::pending()))
    }

    async fn send_interactive(
        &mut self,
        _sender: &MessagingSenderConfig,
        _request_id: SpectaUuid,
        _request_text: &str,
    ) -> Result<String, MessagingError> {
        Err(MessagingError::InteractiveUnsupported {
            provider: ListenerType::Telegram,
        })
    }

    async fn edit_interactive(
        &mut self,
        _sender: &MessagingSenderConfig,
        _provider_message_id: &str,
        _request_text: &str,
        _outcome_label: &str,
    ) -> Result<(), MessagingError> {
        Err(MessagingError::InteractiveUnsupported {
            provider: ListenerType::Telegram,
        })
    }

    async fn run_filters(
        &mut self,
        subscription: MessagingSubscription,
        _rx: mpsc::Receiver<NormalizedMessage>,
        _tx: mpsc::Sender<NormalizedMessage>,
    ) -> Result<MessagingTask, MessagingSubscriptionError> {
        if subscription
            .subscribed_channels
            .as_ref()
            .is_some_and(|channels| {
                channels
                    .iter()
                    .any(|channel| channel.parse::<i64>().is_err())
            })
        {
            return Err(MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Telegram,
                message: "invalid Telegram chat id".to_owned(),
            });
        }
        Ok(tokio::spawn(async move {
            let _ = (_rx, _tx);
            std::future::pending().await
        }))
    }
}

fn trigger() -> FlowTriggerRecord {
    let connection_id = Uuid::new_v4();
    FlowTriggerRecord {
        trigger_id: Uuid::new_v4(),
        flow_id: FlowId(Uuid::new_v4()),
        revision: 1,
        adapter_kind: "subscription".to_owned(),
        adapter_config: serde_json::json!({
            "connection_id": connection_id,
        }),
        state: serde_json::json!({}),
        enabled: true,
        next_fire_at: None,
        last_fired_at: None,
    }
}

async fn actor_for(sources: Arc<Sources>) -> Actor {
    let (_, receiver) = mpsc::channel(16);
    let (broadcasts, _) = mpsc::channel(16);
    let (bus_tx, _) = mpsc::channel(16);
    let knowledge_context = BusRpcContext::authenticated(bus_tx, Jwt::from_opaque("test".to_owned()));
    let config_dir = tempfile::tempdir().unwrap().keep();
    let mut actor = Actor::new(
        receiver,
        broadcasts,
        config_dir.clone(),
        NodeRegistrationSupply::default(),
        ServiceReader::from_published_test_service(sources.clone()),
        knowledge_context,
    )
    .await
    .unwrap();
    let mut seeded = HashSet::new();
    for trigger in sources.0.lock().unwrap().iter() {
        let config: SubscriptionTriggerConfig = serde_json::from_value(trigger.adapter_config.clone()).unwrap();
        if seeded.insert(config.connection_id) {
            actor
                .store()
                .await
                .unwrap()
                .create_connection(&MessagingConnection {
                    id: config.connection_id.to_string(),
                    display_name: "test Telegram".to_owned(),
                    provider_type: "Telegram".to_owned(),
                    credential_id: config.connection_id.to_string(),
                    transport_settings: serde_json::to_string(
                        &ConnectionTransport::Telegram(TelegramConfig::default()),
                    )
                    .unwrap(),
                    enabled: true,
                    retention_seconds: 3600,
                })
                .await
                .unwrap();
        }
        actor.credentials.insert(
            config.connection_id,
            MessagingCredentials::Telegram {
                token: Zeroizing::new("test-token".to_owned()),
            },
        );
    }
    actor
}

async fn request(actor: &mut Actor, payload: MessagingRequestPayload) -> Result<MessagingResponse, MessagingError> {
    let (reply_to, reply) = tokio::sync::oneshot::channel();
    actor
        .handle_message(MessagingRequest { payload, reply_to })
        .await;
    reply.await.unwrap()
}

#[tokio::test]
async fn reconciliation_rebuilds_same_uuid_credential_and_only_affected_key() {
    let first = trigger();
    let second = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![first.clone(), second.clone()])));
    let mut actor = actor_for(sources.clone()).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    let first_task = actor.listeners[&first.trigger_id].task.id();
    let second_task = actor.listeners[&second.trigger_id].task.id();
    request(
        &mut actor,
        MessagingRequestPayload::SupplyCredential {
            credential_id: serde_json::from_value(first.adapter_config["connection_id"].clone()).unwrap(),
            credentials: MessagingCredentials::Telegram {
                token: Zeroizing::new("test-token".to_owned()),
            },
        },
    )
    .await
    .unwrap();
    assert_ne!(actor.listeners[&first.trigger_id].task.id(), first_task);
    assert_eq!(actor.listeners[&second.trigger_id].task.id(), second_task);
    sources
        .0
        .lock()
        .unwrap()
        .retain(|row| row.trigger_id == second.trigger_id);
    actor.reconcile().await.unwrap();
    assert_eq!(actor.listeners.len(), 1);
    assert!(actor.listeners.contains_key(&second.trigger_id));
}

#[tokio::test]
async fn stop_and_live_config_updates_preserve_flow_definitions_without_restarting_stopped_listeners() {
    let sources = Arc::new(Sources(Mutex::new(vec![trigger()])));
    let mut actor = actor_for(sources.clone()).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    let original_task = actor.listeners.values().next().unwrap().task.id();
    actor.reconcile().await.unwrap();
    assert_eq!(actor.listeners.values().next().unwrap().task.id(), original_task);
    request(&mut actor, MessagingRequestPayload::StopServer)
        .await
        .unwrap();
    assert!(actor.listeners.is_empty());
    assert_eq!(sources.0.lock().unwrap().len(), 1);
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert_eq!(actor.listeners.len(), 1);
}

#[tokio::test]
async fn invalid_transport_json_is_skipped_without_blocking_reconcile() {
    let row = trigger();
    let healthy = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![row.clone(), healthy.clone()])));
    let mut actor = actor_for(sources.clone()).await;
    let config: SubscriptionTriggerConfig = serde_json::from_value(row.adapter_config.clone()).unwrap();
    let mut connection = actor
        .store()
        .await
        .unwrap()
        .get_connection(&config.connection_id.to_string())
        .await
        .unwrap()
        .unwrap();
    connection.transport_settings = "not-json".to_owned();
    actor
        .store()
        .await
        .unwrap()
        .update_connection(&connection)
        .await
        .unwrap();
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert!(!actor.listeners.contains_key(&row.trigger_id));
    assert!(actor.listeners.contains_key(&healthy.trigger_id));
}

#[tokio::test]
async fn disabled_connection_does_not_start_an_inflow_or_subscriber() {
    let row = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![row.clone()])));
    let mut actor = actor_for(sources).await;
    let config: SubscriptionTriggerConfig = serde_json::from_value(row.adapter_config.clone()).unwrap();
    actor
        .store()
        .await
        .unwrap()
        .set_enabled(&config.connection_id.to_string(), false)
        .await
        .unwrap();
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert!(actor.inflows.is_empty());
    assert!(actor.listeners.is_empty());
}

#[tokio::test]
async fn missing_credential_does_not_block_healthy_listener() {
    let missing = trigger();
    let healthy = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![missing.clone(), healthy.clone()])));
    let mut actor = actor_for(sources).await;
    let missing_config: SubscriptionTriggerConfig = serde_json::from_value(missing.adapter_config.clone()).unwrap();
    actor.credentials.remove(&missing_config.connection_id);
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert!(actor.listeners.contains_key(&healthy.trigger_id));
    assert!(!actor.listeners.contains_key(&missing.trigger_id));
}

#[tokio::test(start_paused = true)]
async fn connection_failure_is_retried_by_inflow_after_reconcile_succeeds() {
    let row = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![row.clone()])));
    let mut actor = actor_for(sources).await;
    let config: SubscriptionTriggerConfig = serde_json::from_value(row.adapter_config.clone()).unwrap();
    actor.credentials.insert(
        config.connection_id,
        MessagingCredentials::Telegram {
            token: Zeroizing::new("fail-1".to_owned()),
        },
    );
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert!(actor.listeners.contains_key(&row.trigger_id));
    let inflow = &actor.inflows[&actor.listeners[&row.trigger_id].configuration.connection_id];
    let mut handle_is_set = false;
    for _ in 0..20 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        if inflow.producer.lock().unwrap().handle.is_some() {
            handle_is_set = true;
            break;
        }
    }
    assert!(handle_is_set, "inflow did not establish a handle after retries");
}

#[tokio::test]
async fn shared_inflow_retains_surviving_subscriber_after_removal() {
    let first = trigger();
    let second = trigger();
    let mut second_config = first.adapter_config.clone();
    second_config["connection_id"] = first.adapter_config["connection_id"].clone();
    let mut second = second;
    second.adapter_config = second_config;
    let sources = Arc::new(Sources(Mutex::new(vec![first.clone(), second.clone()])));
    let mut actor = actor_for(sources.clone()).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    let key = actor.listeners[&first.trigger_id]
        .configuration
        .connection_id;
    sources
        .0
        .lock()
        .unwrap()
        .retain(|trigger| trigger.trigger_id == second.trigger_id);
    actor.reconcile().await.unwrap();
    assert!(actor.listeners.contains_key(&second.trigger_id));
    assert_eq!(actor.inflows[&key].subscribers.lock().await.len(), 1);
    assert!(
        actor.inflows[&key]
            .subscribers
            .lock()
            .await
            .contains_key(&second.trigger_id)
    );
    actor.inflows[&key].subscribers.lock().await[&second.trigger_id]
        .send(NormalizedMessage {
            body: "survivor".to_owned(),
            metadata: serde_json::json!({}),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn attaches_subscriber_while_shared_inflow_is_live() {
    let first = trigger();
    let mut second = trigger();
    second.adapter_config["connection_id"] = first.adapter_config["connection_id"].clone();
    let sources = Arc::new(Sources(Mutex::new(vec![first.clone()])));
    let mut actor = actor_for(sources.clone()).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    let key = actor.listeners[&first.trigger_id]
        .configuration
        .connection_id;
    for _ in 0..10 {
        if actor.inflows[&key]
            .producer
            .lock()
            .unwrap()
            .handle
            .is_some()
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        actor.inflows[&key]
            .producer
            .lock()
            .unwrap()
            .handle
            .is_some()
    );
    sources.0.lock().unwrap().push(second.clone());
    tokio::time::timeout(Duration::from_millis(100), actor.reconcile())
        .await
        .expect("reconciliation must not wait for the producer receive loop")
        .unwrap();
    let subscribers = actor.inflows[&key].subscribers.lock().await;
    assert!(subscribers.contains_key(&first.trigger_id));
    assert!(subscribers.contains_key(&second.trigger_id));
}

#[tokio::test]
async fn changed_listener_policy_rebinds_without_disturbing_shared_peer() {
    let first = trigger();
    let mut second = trigger();
    second.adapter_config["connection_id"] = first.adapter_config["connection_id"].clone();
    let sources = Arc::new(Sources(Mutex::new(vec![first.clone(), second.clone()])));
    let mut actor = actor_for(sources.clone()).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    let peer_task = actor.listeners[&second.trigger_id].task.id();
    sources
        .0
        .lock()
        .unwrap()
        .iter_mut()
        .find(|row| row.trigger_id == first.trigger_id)
        .unwrap()
        .adapter_config["allowed_senders"] = serde_json::json!(["trusted"]);
    actor.reconcile().await.unwrap();
    assert_eq!(
        actor.listeners[&first.trigger_id]
            .configuration
            .allowed_senders,
        vec!["trusted".to_owned()]
    );
    assert_eq!(actor.listeners[&second.trigger_id].task.id(), peer_task);
    let key = actor.listeners[&second.trigger_id]
        .configuration
        .connection_id;
    let subscribers = actor.inflows[&key].subscribers.lock().await;
    assert!(subscribers.contains_key(&first.trigger_id));
    assert!(subscribers.contains_key(&second.trigger_id));
}

#[tokio::test]
async fn invalid_provider_subscription_does_not_block_healthy_inflow() {
    let mut invalid = trigger();
    invalid.adapter_config["subscribed_channels"] = serde_json::json!(["not-a-chat-id"]);
    let healthy = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![invalid.clone(), healthy.clone()])));
    let mut actor = actor_for(sources).await;
    request(&mut actor, MessagingRequestPayload::StartServer)
        .await
        .unwrap();
    assert!(!actor.listeners.contains_key(&invalid.trigger_id));
    assert!(actor.listeners.contains_key(&healthy.trigger_id));
    assert!(
        actor.inflows.contains_key(
            &actor.listeners[&healthy.trigger_id]
                .configuration
                .connection_id
        )
    );
}

#[tokio::test]
async fn pending_send_does_not_block_another_actor_request() {
    let row = trigger();
    let sources = Arc::new(Sources(Mutex::new(vec![row.clone()])));
    let mut actor = actor_for(sources).await;
    let config: SubscriptionTriggerConfig = serde_json::from_value(row.adapter_config.clone()).unwrap();
    let (reply_to, mut reply) = tokio::sync::oneshot::channel();
    actor
        .handle_message(MessagingRequest {
            payload: MessagingRequestPayload::Send {
                sender: MessagingSenderConfig::Telegram(TelegramSenderConfig {
                    connection_id: config.connection_id,
                    chat_id: "chat".to_owned(),
                    persistence: PersistencePolicy::None,
                }),
                message: NormalizedMessage {
                    body: "block-send".to_owned(),
                    metadata: serde_json::json!({}),
                },
            },
            reply_to,
        })
        .await;
    assert!(matches!(
        reply.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        request(&mut actor, MessagingRequestPayload::List)
            .await
            .unwrap(),
        MessagingResponse::Subscriptions { .. }
    ));
}

#[test]
fn loggable_subscription_error_channel_connection_failed_omits_message() {
    let error = MessagingError::ChannelConnectionFailed {
        listener_type: ListenerType::Email,
        message: "secret-password".to_owned(),
    };
    assert_eq!(
        loggable_subscription_error(&error),
        "channel connection failed for Email"
    );
}

#[test]
fn configuration_serialization_and_debug_output_never_expose_injected_credentials() {
    let configuration = MessagingConfig {
        email: Some(EmailConfig {
            username: Some(Zeroizing::new("private-user".to_owned())),
            password: Some(Zeroizing::new("private-password".to_owned())),
            imap_server: Some("imap.example.com".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let json = serde_json::to_value(&configuration).unwrap();
    assert!(json["email"].get("username").is_none());
    assert!(json["email"].get("password").is_none());
    assert_eq!(json["email"]["imap_server"], "imap.example.com");
    let debug = format!("{configuration:?}");
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-password"));
}

#[test]
fn configuration_rejects_secret_fields_and_trigger_configuration_round_trips_references_and_access_lists() {
    assert!(serde_json::from_value::<EmailConfig>(serde_json::json!({"password": "not-config"})).is_err());
    let configuration = SubscriptionTriggerConfig {
        connection_id: SpectaUuid(Uuid::new_v4()),
        subscribed_channels: None,
        allowed_senders: vec!["trusted@example.com".to_owned()],
        persistence: PersistencePolicy::default(),
    };
    let value = serde_json::to_value(&configuration).unwrap();
    let restored = serde_json::from_value::<SubscriptionTriggerConfig>(value.clone()).unwrap();
    assert_eq!(restored, configuration);
    let mut invalid = value;
    invalid["subscribed_channels"] = serde_json::json!([]);
    assert!(serde_json::from_value::<SubscriptionTriggerConfig>(invalid).is_err());
}

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use chrono::Utc;
use serde_json::{Map, Value, from_str, to_string};
use tokio::sync::Mutex;
use tokio::task::spawn;
use tokio::time::{sleep, timeout};

use aves::bus::BusRpcContext;
use aves::utils::{
    retry::{RetryBackoff, retry_with_backoff},
    uuid::SpectaUuid,
};
use kea::{
    CreateKnowledgeInput, CreateKnowledgeRequest, UpsertDataInput, UpsertDataRequest, dispatch_create_knowledge,
    dispatch_upsert_data,
};

use super::actor::{Actor, ActorInbound, SendCompletion};
use super::channels::{InboundEvent, for_kind};
use super::messages::MessagingResponse;
use super::types::{
    MessagingConfig, MessagingCredentials, MessagingError, MessagingSendError, MessagingSendReceipt,
    MessagingSenderConfig, NormalizedMessage, PersistencePolicy, persistence_identifier,
};

impl Actor {
    /// Sends one message, retrying only provider-classified transient failures.
    pub(super) async fn send_message(
        sender: MessagingSenderConfig,
        message: NormalizedMessage,
        settings: MessagingConfig,
        credentials: MessagingCredentials,
    ) -> Result<MessagingSendReceipt, MessagingError> {
        let (listener_type, _, _) = sender.connection_info();
        let mut plugin = for_kind(listener_type);
        plugin.connect(&settings, &credentials).await?;
        let plugin = Mutex::new(plugin);
        let retry_after = StdMutex::new(None);
        let plugin = &plugin;
        let sender = &sender;
        let message = &message;
        let retry_after = &retry_after;
        retry_with_backoff(
            3,
            RetryBackoff::new(Duration::from_secs(10), Duration::from_secs(60)),
            || {
                let delay = retry_after.lock().expect("send retry-after state").take();
                async move {
                    if let Some(delay) = delay {
                        sleep(Duration::from_secs(delay)).await;
                    }
                    plugin.lock().await.send(sender, message).await
                }
            },
            |error| match error {
                MessagingError::Send(MessagingSendError::Retryable {
                    retry_after_seconds, ..
                }) => {
                    *retry_after.lock().expect("send retry-after state") = *retry_after_seconds;
                    true
                }
                _ => false,
            },
        )
        .await
    }

    /// Persists the raw event before fan-out; persistence failure never blocks delivery.
    pub(super) async fn handle_inbound_delivery(&mut self, inbound: ActorInbound) {
        let ActorInbound { connection_id, event } = inbound;
        self.persist_inbound(&connection_id, &event).await;
        self.fan_out_inbound(connection_id, event.normalized).await;
    }

    async fn persist_inbound(&mut self, connection_id: &SpectaUuid, event: &InboundEvent) {
        let retention_seconds = self.inbound_retention(connection_id).await;
        let now = Utc::now().timestamp();
        let expires_at = now.saturating_add(retention_seconds);
        let body = event
            .normalized
            .as_ref()
            .map(|message| message.body.as_str());
        let metadata = event
            .normalized
            .as_ref()
            .and_then(|message| match to_string(&message.metadata) {
                Ok(metadata) => Some(metadata),
                Err(error) => {
                    tracing::warn!(%error, %connection_id, "Could not serialize inbound message metadata");
                    None
                }
            });
        let channel_id = event
            .normalized
            .as_ref()
            .and_then(|message| message.metadata.get("channel_id"))
            .and_then(Value::as_str);
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .insert_inbound(
                        &event.raw_payload,
                        body,
                        metadata.as_deref(),
                        &connection_id.to_string(),
                        channel_id,
                        now,
                        retention_seconds,
                        expires_at,
                    )
                    .await
                {
                    tracing::warn!(%error, %connection_id, "Could not persist inbound message; continuing delivery");
                }
            }
            Err(error) => {
                tracing::warn!(%error, %connection_id, "Could not open inbound message store; continuing delivery")
            }
        }
    }

    async fn inbound_retention(&mut self, connection_id: &SpectaUuid) -> i64 {
        if let Some(inflow) = self.inflows.get(connection_id) {
            return inflow.retention_seconds;
        }
        match self.store().await {
            Ok(store) => match store.get_connection(&connection_id.to_string()).await {
                Ok(Some(connection)) => connection.retention_seconds,
                Ok(None) => {
                    tracing::warn!(%connection_id, "Inbound message connection is missing; using zero retention");
                    0
                }
                Err(error) => {
                    tracing::warn!(%error, %connection_id, "Could not load inbound message retention; using zero retention");
                    0
                }
            },
            Err(error) => {
                tracing::warn!(%error, %connection_id, "Could not open inbound message store; using zero retention");
                0
            }
        }
    }

    async fn fan_out_inbound(&mut self, connection_id: SpectaUuid, normalized: Option<NormalizedMessage>) {
        let Some(normalized) = normalized else { return };
        let Some(inflow) = self.inflows.get(&connection_id) else {
            return;
        };
        let subscribers: Vec<_> = inflow
            .subscribers
            .lock()
            .await
            .iter()
            .map(|(trigger_id, sender)| (*trigger_id, sender.clone()))
            .collect();
        let mut closed = Vec::new();
        for (trigger_id, sender) in subscribers {
            if sender.is_closed() {
                tracing::warn!(%trigger_id, %connection_id, "Removing closed messaging subscriber");
                closed.push(trigger_id);
            } else {
                let message = normalized.clone();
                spawn(async move {
                    if let Err(error) = sender.send(message).await {
                        tracing::debug!(%error, %trigger_id, "Messaging subscriber closed before delivery");
                    }
                });
            }
        }
        let mut subscribers = inflow.subscribers.lock().await;
        for trigger_id in closed {
            subscribers.remove(&trigger_id);
        }
    }

    /// Completes an accepted send even when its best-effort history write fails.
    pub(super) async fn handle_send_completion(&mut self, completion: SendCompletion) {
        let SendCompletion {
            reply_to,
            connection_id,
            body,
            metadata,
            retention_seconds,
            result,
            persistence,
        } = completion;
        match result {
            Ok(receipt) => {
                self.persist_accepted_send(
                    &connection_id,
                    &body,
                    metadata.as_deref(),
                    &receipt,
                    retention_seconds,
                    [
                        "Could not serialize accepted-send receipt; continuing delivery",
                        "Could not persist accepted outbound message; continuing delivery",
                        "Could not open outbound message store; continuing delivery",
                    ],
                )
                .await;
                if persistence != PersistencePolicy::None {
                    Self::spawn_knowledge_handoff(
                        self.knowledge_context.clone(),
                        persistence,
                        "outbound".to_owned(),
                        NormalizedMessage {
                            body,
                            metadata: metadata
                                .as_deref()
                                .and_then(|value| from_str(value).ok())
                                .unwrap_or(Value::Null),
                        },
                    );
                }
                let _ = reply_to.send(Ok(MessagingResponse::Send { receipt }));
            }
            Err(error) => {
                let _ = reply_to.send(Err(error));
            }
        }
    }

    /// Writes accepted-send history with caller-provided warning messages.
    pub(super) async fn persist_accepted_send(
        &mut self,
        connection_id: &SpectaUuid,
        body: &str,
        metadata: Option<&str>,
        receipt: &MessagingSendReceipt,
        retention_seconds: i64,
        warnings: [&'static str; 3],
    ) {
        let now = Utc::now().timestamp();
        let expires_at = now.saturating_add(retention_seconds);
        let receipt_json = match to_string(receipt) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, %connection_id, "{}", warnings[0]);
                return;
            }
        };
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .insert_accepted_send(
                        body,
                        metadata,
                        &receipt_json,
                        &connection_id.to_string(),
                        now,
                        retention_seconds,
                        expires_at,
                    )
                    .await
                {
                    tracing::warn!(%error, %connection_id, "{}", warnings[1]);
                }
            }
            Err(error) => tracing::warn!(%error, %connection_id, "{}", warnings[2]),
        }
    }

    /// Dispatches optional persistence asynchronously; it never gates accepted delivery.
    pub(super) fn spawn_knowledge_handoff(
        context: BusRpcContext,
        policy: PersistencePolicy,
        scope: String,
        message: NormalizedMessage,
    ) {
        spawn(async move {
            let (scoped_id, note_filename) = persistence_identifier(&scope);
            let request = async {
                match policy {
                    PersistencePolicy::None => Ok(()),
                    PersistencePolicy::Data { namespace } => {
                        let mut value = Map::new();
                        value.insert("body".to_owned(), Value::String(message.body));
                        value.insert("metadata".to_owned(), message.metadata);
                        dispatch_upsert_data(
                            &context,
                            UpsertDataInput {
                                request: UpsertDataRequest {
                                    key: format!("{namespace}/{scoped_id}"),
                                    value,
                                },
                            },
                        )
                        .await
                        .map(|_| ())
                    }
                    PersistencePolicy::Note { directory } => {
                        let backend_id = format!("{}/{}", directory.trim_end_matches('/'), note_filename);
                        let title = note_filename.trim_end_matches(".md").to_owned();
                        let frontmatter = match message.metadata {
                            Value::Object(metadata) => metadata,
                            _ => {
                                tracing::warn!(%scope, "Knowledge Note metadata must be a JSON object");
                                return Ok(());
                            }
                        };
                        dispatch_create_knowledge(
                            &context,
                            CreateKnowledgeInput {
                                request: CreateKnowledgeRequest {
                                    backend_id: Some(backend_id),
                                    title,
                                    content: Some(message.body),
                                    input_file: None,
                                    content_type: from_str("\"Markdown\"")
                                        .expect("KnowledgeContentType::Markdown is a valid serialized value"),
                                    frontmatter,
                                    tags: Vec::new(),
                                },
                            },
                        )
                        .await
                        .map(|_| ())
                    }
                }
            };
            match timeout(Duration::from_secs(30), request).await {
                Ok(Err(error)) => tracing::warn!(%error, %scope, "Optional Knowledge persistence handoff failed"),
                Err(_) => tracing::warn!(%scope, "Knowledge persistence handoff timed out"),
                Ok(Ok(())) => {}
            }
        });
    }
}

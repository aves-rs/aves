use chrono::Utc;
use serde_json::{Value, from_str, to_string};
use std::time::Duration;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

use aves::flow::{
    DeleteFlowTriggerInput, ListFlowTriggersInput, ListFlowsInput, dispatch_delete_flow_trigger,
    dispatch_list_flow_triggers, dispatch_list_flows,
};
use aves::utils::uuid::SpectaUuid;

use super::actor::Actor;
use super::database::MessagingConnection;
use super::messages::{ListenerType, MessagingResponse, SubscriptionRow};
use super::types::{
    ConnectionRecord, ConnectionTransport, EditConnection, MessagingError, NewConnection, RetentionDuration,
};

impl Actor {
    pub(super) async fn create_connection(
        &mut self,
        input: NewConnection,
    ) -> Result<MessagingResponse, MessagingError> {
        Self::validate_connection_provider(input.provider, &input.transport)?;
        let record = ConnectionRecord {
            id: SpectaUuid(Uuid::new_v4()),
            display_name: input.display_name,
            provider: input.provider,
            credential_id: input.credential_id,
            transport: input.transport,
            enabled: input.enabled,
            retention: input.retention,
        };
        self.store()
            .await?
            .create_connection(&Self::record_to_connection(&record))
            .await?;
        let _ = self.reconcile().await;
        Ok(MessagingResponse::Connection { connection: record })
    }

    pub(super) async fn start_test_connection(
        &self,
        reply_to: oneshot::Sender<Result<MessagingResponse, MessagingError>>,
        transport: ConnectionTransport,
        credential_id: SpectaUuid,
    ) {
        let listener_type = transport.provider();
        let Some(credentials) = self.credentials.get(&credential_id).cloned() else {
            let _ = reply_to.send(Err(MessagingError::ChannelConnectionFailed {
                listener_type,
                message: "configured credential is not available".to_owned(),
            }));
            return;
        };
        let settings = transport.messaging_config();
        let mut plugin = Self::reconciliation_plugin(&transport);
        spawn(async move {
            let result = match timeout(Duration::from_secs(60), plugin.verify_remote(&settings, &credentials)).await {
                Ok(Ok(())) => Ok(MessagingResponse::Success),
                Ok(Err(MessagingError::ChannelConnectionFailed { message, .. }))
                    if message.to_ascii_lowercase().contains("timeout")
                        || message.to_ascii_lowercase().contains("timed out") =>
                {
                    Err(MessagingError::ChannelConnectionFailed {
                        listener_type,
                        message: format!("connection verification timed out: {message}"),
                    })
                }
                Ok(Err(error)) => Err(error),
                Err(_) => Err(MessagingError::ChannelConnectionFailed {
                    listener_type,
                    message: "connection verification timed out after 60 seconds".to_owned(),
                }),
            };
            let _ = reply_to.send(result);
        });
    }

    pub(super) async fn list_connections(&mut self) -> Result<MessagingResponse, MessagingError> {
        let connections = self.store().await?.list_connections().await?;
        let connections = connections
            .iter()
            .map(Self::connection_to_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MessagingResponse::Connections { connections })
    }

    pub(super) fn list_subscriptions(&self) -> MessagingResponse {
        let subscriptions = self
            .listeners
            .values()
            .filter_map(|listener| {
                let connection_id = listener.configuration.connection_id;
                let inflow = self.inflows.get(&connection_id)?;
                Some(SubscriptionRow {
                    id: SpectaUuid(listener.trigger.trigger_id),
                    listener_type: inflow.transport.provider(),
                    subscribed_channels: listener.configuration.subscribed_channels.clone(),
                    flow_id: SpectaUuid(listener.trigger.flow_id.0),
                })
            })
            .collect();
        MessagingResponse::Subscriptions { subscriptions }
    }

    pub(super) async fn update_connection(
        &mut self,
        id: SpectaUuid,
        input: EditConnection,
    ) -> Result<MessagingResponse, MessagingError> {
        Self::validate_connection_provider(input.provider, &input.transport)?;
        let connection = MessagingConnection {
            id: id.to_string(),
            display_name: input.display_name,
            provider_type: input.provider.to_string(),
            credential_id: input.credential_id.to_string(),
            transport_settings: to_string(&input.transport)?,
            enabled: false,
            retention_seconds: input.retention.seconds as i64,
        };
        self.store().await?.update_connection(&connection).await?;
        let stored = self
            .store()
            .await?
            .get_connection(&id.to_string())
            .await?
            .ok_or_else(|| MessagingError::ConnectionNotFound {
                connection_id: id.to_string(),
            })?;
        let _ = self.reconcile().await;
        Ok(MessagingResponse::Connection {
            connection: Self::connection_to_record(&stored)?,
        })
    }

    pub(super) async fn set_connection_enabled(
        &mut self,
        connection_id: SpectaUuid,
        enabled: bool,
    ) -> Result<MessagingResponse, MessagingError> {
        self.store()
            .await?
            .set_enabled(&connection_id.to_string(), enabled)
            .await?;
        let _ = self.reconcile().await;
        Ok(MessagingResponse::Success)
    }

    pub(super) async fn delete_connection(
        &mut self,
        connection_id: SpectaUuid,
    ) -> Result<MessagingResponse, MessagingError> {
        let id = connection_id.to_string();
        self.remove_connection_triggers(&id).await;
        self.store()
            .await?
            .mark_deleted_for_connection(&id, Utc::now().timestamp())
            .await?;
        self.store().await?.delete_connection(&id).await?;
        let _ = self.reconcile().await;
        Ok(MessagingResponse::Success)
    }

    async fn remove_connection_triggers(&self, connection_id: &str) {
        match dispatch_list_flows(&self.knowledge_context, ListFlowsInput {}).await {
            Ok(output) => {
                for flow in output.flows {
                    match dispatch_list_flow_triggers(
                        &self.knowledge_context,
                        ListFlowTriggersInput { flow_id: flow.flow_id },
                    )
                    .await
                    {
                        Ok(output) => {
                            for trigger in output.triggers {
                                if trigger.adapter_kind == "subscription"
                                    && trigger
                                        .adapter_config
                                        .get("connection_id")
                                        .and_then(Value::as_str)
                                        == Some(connection_id)
                                    && let Err(error) = dispatch_delete_flow_trigger(
                                        &self.knowledge_context,
                                        DeleteFlowTriggerInput {
                                            trigger_id: trigger.trigger_id,
                                        },
                                    )
                                    .await
                                {
                                    tracing::warn!(%error, trigger_id = %trigger.trigger_id, "Failed to remove subscription trigger for deleted connection");
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, flow_id = %flow.flow_id, "Failed to list Flow triggers during connection deletion")
                        }
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "Failed to list Flows during connection deletion"),
        }
    }

    fn connection_to_record(conn: &MessagingConnection) -> Result<ConnectionRecord, MessagingError> {
        let provider =
            ListenerType::from_storage(&conn.provider_type).ok_or_else(|| MessagingError::Serialization {
                message: "invalid messaging provider type".to_owned(),
            })?;
        let transport = from_str(&conn.transport_settings)?;
        let id = Uuid::parse_str(&conn.id)
            .map(SpectaUuid)
            .map_err(|_| MessagingError::ConnectionNotFound {
                connection_id: conn.id.clone(),
            })?;
        let credential_id = Uuid::parse_str(&conn.credential_id)
            .map(SpectaUuid)
            .map_err(|_| MessagingError::ConnectionNotFound {
                connection_id: conn.id.clone(),
            })?;
        let retention_seconds =
            u64::try_from(conn.retention_seconds).map_err(|_| MessagingError::InvalidRetention {
                reason: "stored duration is invalid".to_owned(),
            })?;
        Ok(ConnectionRecord {
            id,
            display_name: conn.display_name.clone(),
            provider,
            credential_id,
            transport,
            enabled: conn.enabled,
            retention: RetentionDuration::new(retention_seconds)?,
        })
    }

    fn record_to_connection(record: &ConnectionRecord) -> MessagingConnection {
        MessagingConnection {
            id: record.id.to_string(),
            display_name: record.display_name.clone(),
            provider_type: record.provider.to_string(),
            credential_id: record.credential_id.to_string(),
            transport_settings: to_string(&record.transport).expect("connection transport serializes"),
            enabled: record.enabled,
            retention_seconds: i64::try_from(record.retention.seconds).expect("validated retention fits storage"),
        }
    }

    fn validate_connection_provider(
        provider: ListenerType,
        transport: &ConnectionTransport,
    ) -> Result<(), MessagingError> {
        if provider == transport.provider() {
            Ok(())
        } else {
            Err(MessagingError::InboundConfigurationInvalid {
                provider,
                reason: "provider does not match transport".to_owned(),
            })
        }
    }
}

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::Utc;
use serde_json::{Value, from_str, to_string, to_value};
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn;
use uuid::Uuid;

use aves::flow::{BoundOutputs, LifecycleCorrelation, NodeLifecycleDispatchError, NodeLifecycleResult};
use aves::utils::uuid::SpectaUuid;

use super::actor::{Actor, NodeEvent, PermSendCompleted};
use super::channels::for_kind;
use super::database::PermissionRequest;
use super::flow_nodes::NodeSetupError;
use super::messages::ListenerType;
use super::types::{
    ConnectionTransport, MessagingConfig, MessagingCredentials, MessagingError, MessagingSenderConfig,
    PermissionDuration, PermissionNodeConfig,
};

struct PreparedPermissionNode {
    connection_id: SpectaUuid,
    retention_seconds: i64,
    request_id: Uuid,
    request: PermissionRequest,
    sender: MessagingSenderConfig,
    provider: ListenerType,
    settings: MessagingConfig,
    credentials: MessagingCredentials,
    body: String,
    metadata: Option<String>,
}

impl Actor {
    pub(super) async fn start_permission_node(
        &mut self,
        correlation: LifecycleCorrelation,
        config: PermissionNodeConfig,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        let prepared = match self.prepare_permission_node(&correlation, &config).await {
            Ok(prepared) => prepared,
            Err(error) => return Self::reply_failed(reply, error.to_string()),
        };
        let PreparedPermissionNode {
            connection_id,
            retention_seconds,
            request_id,
            request,
            sender,
            provider,
            settings,
            credentials,
            body,
            metadata,
        } = prepared;
        let write = match self.store().await {
            Ok(store) => store
                .insert_prepared(&request)
                .await
                .map_err(NodeSetupError::Store),
            Err(error) => Err(NodeSetupError::Messaging(error)),
        };
        if let Err(error) = write {
            return Self::reply_failed(reply, error.to_string());
        }
        spawn(Self::run_interactive_send(
            sender,
            provider,
            settings,
            credentials,
            SpectaUuid(request_id),
            body.clone(),
            connection_id,
            retention_seconds,
            body,
            metadata,
            self.node_tx.clone(),
        ));
        let _ = reply.send(Ok(NodeLifecycleResult::Running));
    }

    pub(super) async fn handle_permission_send_completion(&mut self, completion: PermSendCompleted) {
        let PermSendCompleted {
            request_id,
            connection_id,
            retention_seconds,
            body,
            metadata,
            result,
        } = completion;
        match result {
            Ok(provider_message_id) => {
                self.attach_permission_receipt(&request_id, &provider_message_id)
                    .await;
                self.persist_permission_acceptance(
                    &connection_id,
                    &body,
                    metadata.as_deref(),
                    retention_seconds,
                    &provider_message_id,
                )
                .await;
            }
            Err(error) => self.fail_permission_send(&request_id, error).await,
        }
    }

    async fn prepare_permission_node(
        &mut self,
        correlation: &LifecycleCorrelation,
        config: &PermissionNodeConfig,
    ) -> Result<PreparedPermissionNode, NodeSetupError> {
        if config.provider == ListenerType::Email {
            return Err(NodeSetupError::EmailNotSupported);
        }
        let connection = self
            .store()
            .await?
            .get_connection(&config.connection_id.to_string())
            .await?;
        let connection = connection.ok_or(NodeSetupError::UnknownConnection)?;
        let now = Utc::now().timestamp();
        let expires_at = PermissionDuration::new(config.duration_seconds)?.expires_at(now)?;
        let transport = from_str::<ConnectionTransport>(&connection.transport_settings)?;
        let provider = ListenerType::from_storage(&connection.provider_type).ok_or(NodeSetupError::InvalidProvider)?;
        if provider != config.provider {
            return Err(NodeSetupError::ProviderMismatch);
        }
        let connection_id = SpectaUuid(Uuid::parse_str(&connection.id)?);
        let sender = MessagingSenderConfig::from_destination(provider, connection_id, &config.destination);
        let credential_id = SpectaUuid::from_str(&connection.credential_id)?;
        let credentials = self
            .credentials
            .get(&credential_id)
            .cloned()
            .ok_or(NodeSetupError::CredentialUnavailable)?;
        let request_id = Uuid::new_v4();
        let metadata = to_string(&config.metadata).ok();
        let request = PermissionRequest {
            request_id: request_id.to_string(),
            connection_id: connection.id,
            provider_message_id: None,
            destination: config.destination.to_string(),
            request_text: config.body.clone(),
            expires_at,
            flow_execution_id: correlation.flow_execution_id.to_string(),
            flow_node_id: correlation.flow_node_id.to_string(),
            status: "prepared".to_owned(),
            provider_receipt: None,
            decided_at: None,
            outstanding_edit: false,
        };
        Ok(PreparedPermissionNode {
            connection_id,
            retention_seconds: connection.retention_seconds,
            request_id,
            request,
            sender,
            provider,
            settings: transport.messaging_config(),
            credentials,
            body: config.body.clone(),
            metadata,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_interactive_send(
        sender: MessagingSenderConfig,
        provider: ListenerType,
        settings: MessagingConfig,
        credentials: MessagingCredentials,
        request_id: SpectaUuid,
        request_text: String,
        connection_id: SpectaUuid,
        retention_seconds: i64,
        body: String,
        metadata: Option<String>,
        done: mpsc::Sender<NodeEvent>,
    ) {
        let mut plugin = for_kind(provider);
        let result = match plugin.connect(&settings, &credentials).await {
            Ok(()) => {
                plugin
                    .send_interactive(&sender, request_id, &request_text)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = done
            .send(NodeEvent::PermissionSendCompleted(PermSendCompleted {
                request_id: request_id.to_string(),
                connection_id,
                retention_seconds,
                body,
                metadata,
                result,
            }))
            .await
        {
            tracing::warn!(%error, "Could not deliver PermissionNode send completion to actor");
        }
    }

    async fn persist_permission_acceptance(
        &mut self,
        connection_id: &SpectaUuid,
        body: &str,
        metadata: Option<&str>,
        retention_seconds: i64,
        provider_message_id: &str,
    ) {
        let now = Utc::now().timestamp();
        let expires_at = now.saturating_add(retention_seconds);
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .insert_accepted_send(
                        body,
                        metadata,
                        provider_message_id,
                        &connection_id.to_string(),
                        now,
                        retention_seconds,
                        expires_at,
                    )
                    .await
                {
                    tracing::warn!(%error, %connection_id, "Could not persist accepted PermissionNode send; continuing");
                }
            }
            Err(error) => {
                tracing::warn!(%error, %connection_id, "Could not open store for accepted PermissionNode send")
            }
        }
    }

    async fn attach_permission_receipt(&mut self, request_id: &str, provider_message_id: &str) {
        let marked = match self.store().await {
            Ok(store) => match store
                .mark_pending(request_id, provider_message_id, provider_message_id)
                .await
            {
                Ok(marked) => marked,
                Err(error) => {
                    tracing::warn!(%error, %request_id, "Could not mark PermissionNode request pending");
                    false
                }
            },
            Err(error) => {
                tracing::warn!(%error, %request_id, "Could not open store to mark PermissionNode pending");
                false
            }
        };
        if marked {
            return;
        }
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .attach_provider_message(request_id, provider_message_id, provider_message_id)
                    .await
                {
                    tracing::warn!(%error, %request_id, "Could not attach PermissionNode provider receipt");
                }
                match store.get_permission_request(request_id).await {
                    Ok(Some(row)) if row.outstanding_edit => {
                        let label = match row.status.as_str() {
                            "approved" => Some("Approved"),
                            "denied" => Some("Denied"),
                            "expired" => Some("Expired"),
                            "cancelled" => Some("Cancelled"),
                            _ => None,
                        };
                        if let Some(label) = label {
                            self.edit_finalized(row, label).await;
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, %request_id, "Could not reload PermissionNode request after receipt")
                    }
                }
            }
            Err(error) => tracing::warn!(%error, %request_id, "Could not open store to attach PermissionNode receipt"),
        }
    }

    async fn fail_permission_send(&mut self, request_id: &str, error: MessagingError) {
        tracing::warn!(%error, %request_id, "PermissionNode interactive send failed");
        let now = Utc::now().timestamp();
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .finalize_atomic(request_id, "failed", now, false)
                    .await
                {
                    tracing::warn!(%error, %request_id, "Could not persist failed PermissionNode request");
                }
            }
            Err(error) => {
                tracing::warn!(%error, %request_id, "Could not open store to persist failed PermissionNode request")
            }
        }
    }

    pub(super) async fn tick_permission_node(
        &mut self,
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        let row = match self.store().await {
            Ok(store) => {
                store
                    .get_by_correlation(
                        &correlation.flow_execution_id.to_string(),
                        &correlation.flow_node_id.to_string(),
                    )
                    .await
            }
            Err(error) => {
                Self::reply_failed(reply, error.to_string());
                return;
            }
        };
        match row {
            Ok(None) => Self::reply_failed(reply, "permission request not found"),
            Err(error) => Self::reply_failed(reply, error.to_string()),
            Ok(Some(row)) => match row.status.as_str() {
                "approved" => {
                    let outputs = BoundOutputs::new(BTreeMap::from([
                        ("approved".to_owned(), Value::Bool(true)),
                        (
                            "provider_receipt".to_owned(),
                            to_value(row.provider_receipt).unwrap_or(Value::Null),
                        ),
                        ("decided_at".to_owned(), to_value(row.decided_at).unwrap_or(Value::Null)),
                    ]));
                    let _ = reply.send(Ok(NodeLifecycleResult::Succeeded {
                        outputs,
                        state: Value::Null,
                    }));
                }
                "denied" | "expired" | "cancelled" | "deleted" | "failed" => {
                    Self::reply_failed(reply, format!("permission {}", row.status));
                }
                _ => {
                    let _ = reply.send(Ok(NodeLifecycleResult::Running));
                }
            },
        }
    }

    pub(super) async fn cancel_permission_node(
        &mut self,
        correlation: LifecycleCorrelation,
        reply: oneshot::Sender<Result<NodeLifecycleResult, NodeLifecycleDispatchError>>,
    ) {
        let row = match self.store().await {
            Ok(store) => {
                store
                    .get_by_correlation(
                        &correlation.flow_execution_id.to_string(),
                        &correlation.flow_node_id.to_string(),
                    )
                    .await
            }
            Err(error) => {
                tracing::warn!(%error, "Could not load PermissionNode for cancellation");
                let _ = reply.send(Ok(NodeLifecycleResult::Cancelled));
                return;
            }
        };
        if let Ok(Some(row)) = row
            && matches!(row.status.as_str(), "prepared" | "pending")
        {
            self.finalize_permission_cancel(&row).await;
            if let Some(provider_message_id) = row.provider_message_id.clone() {
                self.launch_cancel_edit(row, provider_message_id).await;
            }
        }
        let _ = reply.send(Ok(NodeLifecycleResult::Cancelled));
    }
}

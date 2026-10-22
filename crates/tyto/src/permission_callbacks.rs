use std::str::FromStr;

use chrono::Utc;
use serde_json::{Value, from_str};
use tokio::sync::mpsc;
use tokio::task::spawn;

use aves::utils::uuid::SpectaUuid;

use super::actor::{Actor, ActorInteraction, EditCompleted, NodeEvent};
use super::channels::for_kind;
use super::database::PermissionRequest;
use super::messages::ListenerType;
use super::types::{
    ConnectionTransport, InteractionAction, MessagingConfig, MessagingCredentials, MessagingError,
    MessagingInteraction, MessagingSenderConfig,
};

impl Actor {
    pub(super) async fn handle_interaction(&mut self, event: ActorInteraction) {
        let ActorInteraction {
            connection_id,
            interaction,
        } = event;
        let now = Utc::now().timestamp();
        let request_id = interaction.request_id.to_string();
        let request = self.load_callback_request(&request_id).await;
        let (callback_connection_id, retention_seconds) = self
            .callback_retention(request.as_ref(), &connection_id)
            .await;
        self.persist_permission_callback(
            &interaction,
            &request_id,
            &callback_connection_id,
            retention_seconds,
            now,
        )
        .await;

        let Some(request) = request else { return };
        if !matches!(request.status.as_str(), "prepared" | "pending") {
            return;
        }
        let (status, label) = if now > request.expires_at {
            ("expired", "Expired")
        } else {
            match interaction.action {
                InteractionAction::Approve => ("approved", "Approved"),
                InteractionAction::Deny => ("denied", "Denied"),
                InteractionAction::Unknown => return,
            }
        };
        match self.store().await {
            Ok(store) => match store
                .finalize_atomic(&request.request_id, status, now, true)
                .await
            {
                Ok(true) => self.edit_finalized(request, label).await,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, request_id = %request.request_id, "Could not finalize permission callback decision")
                }
            },
            Err(error) => {
                tracing::warn!(%error, request_id = %request.request_id, "Could not open store to finalize permission callback decision")
            }
        }
    }

    async fn load_callback_request(&mut self, request_id: &str) -> Option<PermissionRequest> {
        match self.store().await {
            Ok(store) => match store.get_permission_request(request_id).await {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(%error, %request_id, "Could not load PermissionNode request for callback arbitration");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(%error, %request_id, "Could not open store for callback arbitration");
                None
            }
        }
    }

    async fn callback_retention(
        &mut self,
        request: Option<&PermissionRequest>,
        ingress_connection: &SpectaUuid,
    ) -> (Option<String>, i64) {
        let connection_id = request
            .map(|request| request.connection_id.clone())
            .unwrap_or_else(|| ingress_connection.to_string());
        let warnings = match request {
            Some(_) => (
                "Permission callback connection is missing; using one-hour retention",
                "Could not load Permission callback retention; using one-hour retention",
                "Could not open store for callback retention; using one-hour retention",
            ),
            None => (
                "Unknown callback connection; using one-hour retention",
                "Could not load callback retention; using one-hour retention",
                "Could not open store for callback retention; using one-hour retention",
            ),
        };
        let retention = match self.store().await {
            Ok(store) => match store.get_connection(&connection_id).await {
                Ok(Some(connection)) => connection.retention_seconds,
                Ok(None) => {
                    tracing::warn!(%connection_id, "{}", warnings.0);
                    3_600
                }
                Err(error) => {
                    tracing::warn!(%error, %connection_id, "{}", warnings.1);
                    3_600
                }
            },
            Err(error) => {
                tracing::warn!(%error, %connection_id, "{}", warnings.2);
                3_600
            }
        };
        (Some(connection_id), retention)
    }

    async fn persist_permission_callback(
        &mut self,
        interaction: &MessagingInteraction,
        request_id: &str,
        connection_id: &Option<String>,
        retention_seconds: i64,
        now: i64,
    ) {
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .insert_callback(
                        &interaction.raw_payload,
                        Some(request_id),
                        Some(interaction.action.as_str()),
                        Some(now),
                        connection_id.as_deref(),
                        now,
                        retention_seconds,
                        now.saturating_add(retention_seconds),
                    )
                    .await
                {
                    tracing::warn!(%error, %request_id, "Could not persist permission callback; continuing arbitration");
                }
            }
            Err(error) => {
                tracing::warn!(%error, %request_id, "Could not open store to persist permission callback; continuing arbitration")
            }
        }
    }

    /// Expires requests, retries provider edits, then prunes retained history.
    pub(super) async fn handle_worker_tick(&mut self) {
        let now = Utc::now().timestamp();
        self.expire_permission_requests(now).await;
        self.retry_permission_edits().await;
        self.prune_messaging_history(now).await;
    }

    async fn expire_permission_requests(&mut self, now: i64) {
        let rows = match self.store().await {
            Ok(store) => match store.scan_unfinished(now).await {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(%error, "Could not scan unfinished PermissionNode requests");
                    return;
                }
            },
            Err(error) => {
                tracing::warn!(%error, "Could not open store to scan unfinished PermissionNode requests");
                return;
            }
        };
        for row in rows {
            if !matches!(row.status.as_str(), "prepared" | "pending") || now < row.expires_at {
                continue;
            }
            let finalized = match self.store().await {
                Ok(store) => match store
                    .finalize_atomic(&row.request_id, "expired", now, true)
                    .await
                {
                    Ok(finalized) => finalized,
                    Err(error) => {
                        tracing::warn!(%error, request_id = %row.request_id, "Could not expire PermissionNode request");
                        false
                    }
                },
                Err(error) => {
                    tracing::warn!(%error, request_id = %row.request_id, "Could not open store to expire PermissionNode request");
                    false
                }
            };
            if finalized {
                self.edit_finalized(row, "Expired").await;
            }
        }
    }

    async fn retry_permission_edits(&mut self) {
        let rows = match self.store().await {
            Ok(store) => match store.scan_outstanding_edits().await {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(%error, "Could not scan outstanding PermissionNode edits");
                    return;
                }
            },
            Err(error) => {
                tracing::warn!(%error, "Could not open store to scan outstanding PermissionNode edits");
                return;
            }
        };
        for row in rows {
            let label = match row.status.as_str() {
                "approved" => "Approved",
                "denied" => "Denied",
                "expired" => "Expired",
                "cancelled" => "Cancelled",
                "deleted" => "Deleted",
                _ => "Expired",
            };
            self.edit_finalized(row, label).await;
        }
    }

    async fn prune_messaging_history(&mut self, now: i64) {
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store.prune_expired(now).await {
                    tracing::warn!(%error, "Could not prune expired messaging records");
                }
            }
            Err(error) => tracing::warn!(%error, "Could not open store to prune expired messaging records"),
        }
    }

    /// Clears the retry flag only after the provider edit succeeds.
    pub(super) async fn handle_edit_completion(&mut self, completion: EditCompleted) {
        let EditCompleted { request_id, result } = completion;
        match result {
            Ok(()) => match self.store().await {
                Ok(store) => {
                    if let Err(error) = store.clear_outstanding_edit(&request_id).await {
                        tracing::warn!(%error, %request_id, "Could not clear PermissionNode outstanding edit flag");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, %request_id, "Could not open store to clear PermissionNode edit flag")
                }
            },
            Err(error) => {
                tracing::warn!(%error, %request_id, "PermissionNode provider edit failed; retry remains pending")
            }
        }
    }

    pub(super) async fn finalize_permission_cancel(&mut self, row: &PermissionRequest) {
        let now = Utc::now().timestamp();
        match self.store().await {
            Ok(store) => {
                if let Err(error) = store
                    .finalize_atomic(&row.request_id, "cancelled", now, true)
                    .await
                {
                    tracing::warn!(%error, request_id = %row.request_id, "Could not finalize PermissionNode cancellation");
                }
            }
            Err(error) => {
                tracing::warn!(%error, request_id = %row.request_id, "Could not open store to finalize PermissionNode cancellation")
            }
        }
    }

    pub(super) async fn launch_cancel_edit(&mut self, row: PermissionRequest, provider_message_id: String) {
        match self.prepare_cancel_edit(&row).await {
            Ok((sender, provider, settings, credentials)) => {
                spawn(Self::run_provider_edit(
                    sender,
                    provider,
                    settings,
                    credentials,
                    provider_message_id,
                    row.request_text,
                    "Cancelled".to_owned(),
                    row.request_id,
                    self.node_tx.clone(),
                ));
            }
            Err(error) => tracing::warn!(%error, "Could not prepare PermissionNode provider edit"),
        }
    }

    pub(super) async fn edit_finalized(&mut self, row: PermissionRequest, outcome_label: &str) {
        let Some(provider_message_id) = row.provider_message_id.clone() else {
            tracing::warn!(request_id = %row.request_id, "Finalized PermissionNode request has no provider message to edit");
            return;
        };
        match self.prepare_provider_edit(&row).await {
            Ok((sender, provider, settings, credentials)) => {
                spawn(Self::run_provider_edit(
                    sender,
                    provider,
                    settings,
                    credentials,
                    provider_message_id,
                    row.request_text,
                    outcome_label.to_owned(),
                    row.request_id,
                    self.node_tx.clone(),
                ));
            }
            Err(error) => {
                tracing::warn!(%error, request_id = %row.request_id, "Could not prepare finalized PermissionNode provider edit")
            }
        }
    }

    async fn prepare_provider_edit(
        &mut self,
        row: &PermissionRequest,
    ) -> Result<
        (
            MessagingSenderConfig,
            ListenerType,
            MessagingConfig,
            MessagingCredentials,
        ),
        MessagingError,
    > {
        let conn = self
            .store()
            .await?
            .get_connection(&row.connection_id)
            .await?
            .ok_or_else(|| MessagingError::ConnectionNotFound {
                connection_id: row.connection_id.clone(),
            })?;
        let provider =
            ListenerType::from_storage(&conn.provider_type).ok_or_else(|| MessagingError::Serialization {
                message: "invalid messaging provider type".to_owned(),
            })?;
        let transport: ConnectionTransport = from_str(&conn.transport_settings)?;
        let destination: Value = from_str(&row.destination)?;
        let connection_id = Self::connection_uuid(&row.connection_id)?;
        let sender = MessagingSenderConfig::from_destination(provider, connection_id, &destination);
        let credential_id =
            SpectaUuid::from_str(&conn.credential_id).map_err(|error| MessagingError::ConnectionNotFound {
                connection_id: error.to_string(),
            })?;
        let credentials = self
            .credentials
            .get(&credential_id)
            .cloned()
            .ok_or_else(|| MessagingError::ChannelConnectionFailed {
                listener_type: provider,
                message: "configured credential is not available".to_owned(),
            })?;
        Ok((sender, provider, transport.messaging_config(), credentials))
    }

    async fn prepare_cancel_edit(
        &mut self,
        row: &PermissionRequest,
    ) -> Result<
        (
            MessagingSenderConfig,
            ListenerType,
            MessagingConfig,
            MessagingCredentials,
        ),
        MessagingError,
    > {
        let conn = self
            .store()
            .await?
            .get_connection(&row.connection_id)
            .await?
            .ok_or_else(|| MessagingError::ConnectionNotFound {
                connection_id: row.connection_id.clone(),
            })?;
        let provider =
            ListenerType::from_storage(&conn.provider_type).ok_or_else(|| MessagingError::Serialization {
                message: "invalid messaging provider type".to_owned(),
            })?;
        let transport: ConnectionTransport = from_str(&conn.transport_settings)?;
        let credential_id =
            SpectaUuid::from_str(&conn.credential_id).map_err(|error| MessagingError::ConnectionNotFound {
                connection_id: error.to_string(),
            })?;
        let credentials = self
            .credentials
            .get(&credential_id)
            .cloned()
            .ok_or_else(|| MessagingError::ChannelConnectionFailed {
                listener_type: provider,
                message: "configured credential is not available".to_owned(),
            })?;
        let destination: Value = from_str(&row.destination)?;
        let connection_id = Self::connection_uuid(&conn.id)?;
        let sender = MessagingSenderConfig::from_destination(provider, connection_id, &destination);
        Ok((sender, provider, transport.messaging_config(), credentials))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_provider_edit(
        sender: MessagingSenderConfig,
        provider: ListenerType,
        settings: MessagingConfig,
        credentials: MessagingCredentials,
        provider_message_id: String,
        request_text: String,
        outcome_label: String,
        request_id: String,
        done: mpsc::Sender<NodeEvent>,
    ) {
        let mut plugin = for_kind(provider);
        let result = match plugin.connect(&settings, &credentials).await {
            Ok(()) => {
                plugin
                    .edit_interactive(&sender, &provider_message_id, &request_text, &outcome_label)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = done
            .send(NodeEvent::EditCompleted(EditCompleted { request_id, result }))
            .await
        {
            tracing::warn!(%error, "Could not deliver PermissionNode edit completion to actor");
        }
    }
}

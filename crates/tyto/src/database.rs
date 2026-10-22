use std::path::{Path, PathBuf};

use thiserror::Error;
use turso::{Connection, Database, Error as TursoError, Row};

use aves::utils::database::{
    DatabaseError, MigrationRunner, connect_database, maintain_database, open_database_at_path,
};

use super::migrations::MIGRATIONS;

const LIST_CONNECTIONS_SQL: &str = include_str!("queries/list_connections.sql");
const GET_CONNECTION_SQL: &str = include_str!("queries/get_connection.sql");
const CREATE_CONNECTION_SQL: &str = include_str!("queries/create_connection.sql");
const UPDATE_CONNECTION_SQL: &str = include_str!("queries/update_connection.sql");
const SET_CONNECTION_ENABLED_SQL: &str = include_str!("queries/set_connection_enabled.sql");
const DELETE_CONNECTION_SQL: &str = include_str!("queries/delete_connection.sql");
const INSERT_PREPARED_SQL: &str = include_str!("queries/insert_prepared.sql");
const MARK_PENDING_SQL: &str = include_str!("queries/mark_pending.sql");
const ATTACH_PROVIDER_MESSAGE_SQL: &str = include_str!("queries/attach_provider_message.sql");
const FINALIZE_ATOMIC_SQL: &str = include_str!("queries/finalize_atomic.sql");
const CLEAR_OUTSTANDING_EDIT_SQL: &str = include_str!("queries/clear_outstanding_edit.sql");
const GET_PERMISSION_REQUEST_SQL: &str = include_str!("queries/get_permission_request.sql");
const GET_BY_CORRELATION_SQL: &str = include_str!("queries/get_by_correlation.sql");
const SCAN_UNFINISHED_SQL: &str = include_str!("queries/scan_unfinished.sql");
const SCAN_OUTSTANDING_EDITS_SQL: &str = include_str!("queries/scan_outstanding_edits.sql");
const MARK_DELETED_FOR_CONNECTION_SQL: &str = include_str!("queries/mark_deleted_for_connection.sql");
const INSERT_INBOUND_SQL: &str = include_str!("queries/insert_inbound.sql");
const INSERT_CALLBACK_SQL: &str = include_str!("queries/insert_callback.sql");
const INSERT_ACCEPTED_SEND_SQL: &str = include_str!("queries/insert_accepted_send.sql");
const PRUNE_EXPIRED_SQL: &str = include_str!("queries/prune_expired.sql");
const PRUNE_CALLBACKS_SQL: &str = include_str!("queries/prune_callbacks.sql");
const PRUNE_ACCEPTED_SENDS_SQL: &str = include_str!("queries/prune_accepted_sends.sql");

/// Typed errors raised by the Tyto message store.
#[derive(Debug, Error)]
pub(super) enum StoreError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Turso(#[from] TursoError),
}

/// A persisted provider connection configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MessagingConnection {
    pub(super) id: String,
    pub(super) display_name: String,
    pub(super) provider_type: String,
    pub(super) credential_id: String,
    pub(super) transport_settings: String,
    pub(super) enabled: bool,
    pub(super) retention_seconds: i64,
}

/// A durable permission request, including its finalization/edit state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PermissionRequest {
    pub(super) request_id: String,
    pub(super) connection_id: String,
    pub(super) provider_message_id: Option<String>,
    pub(super) destination: String,
    pub(super) request_text: String,
    pub(super) expires_at: i64,
    pub(super) flow_execution_id: String,
    pub(super) flow_node_id: String,
    pub(super) status: String,
    pub(super) provider_receipt: Option<String>,
    pub(super) decided_at: Option<i64>,
    pub(super) outstanding_edit: bool,
}

/// Tyto's durable messaging database handle.
pub(super) struct MessageStore {
    database: Database,
    path: PathBuf,
}

impl MessageStore {
    /// Opens and migrates `config_dir/messages.db`.
    pub(super) async fn open(config_dir: &Path) -> Result<Self, StoreError> {
        let path = config_dir.join("messages.db");
        let database = open_database_at_path(path.clone()).await?;
        MigrationRunner::new("__peitho_migrations", MIGRATIONS)
            .apply(&database)
            .await?;
        Ok(Self { database, path })
    }

    async fn connection(&self) -> Result<Connection, StoreError> {
        Ok(connect_database(&self.database).await?)
    }

    /// Performs Aves database maintenance for the message store.
    pub(super) async fn maintain(&self) {
        maintain_database(&self.database, &self.path).await;
    }

    /// Lists all configured messaging connections.
    pub(super) async fn list_connections(&self) -> Result<Vec<MessagingConnection>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection.query(LIST_CONNECTIONS_SQL, ()).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(Self::read_connection(&row)?);
        }
        Ok(result)
    }

    /// Loads one connection by its immutable identifier.
    pub(super) async fn get_connection(&self, id: &str) -> Result<Option<MessagingConnection>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection.query(GET_CONNECTION_SQL, (id,)).await?;
        Ok(rows
            .next()
            .await?
            .as_ref()
            .map(Self::read_connection)
            .transpose()?)
    }

    /// Inserts a new connection configuration.
    pub(super) async fn create_connection(&self, value: &MessagingConnection) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                CREATE_CONNECTION_SQL,
                (
                    value.id.as_str(),
                    value.display_name.as_str(),
                    value.provider_type.as_str(),
                    value.credential_id.as_str(),
                    value.transport_settings.as_str(),
                    i64::from(value.enabled),
                    value.retention_seconds,
                ),
            )
            .await?;
        Ok(())
    }

    /// Updates editable connection settings while preserving its identifier and enabled state.
    pub(super) async fn update_connection(&self, value: &MessagingConnection) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                UPDATE_CONNECTION_SQL,
                (
                    value.id.as_str(),
                    value.display_name.as_str(),
                    value.provider_type.as_str(),
                    value.credential_id.as_str(),
                    value.transport_settings.as_str(),
                    value.retention_seconds,
                ),
            )
            .await?;
        Ok(())
    }

    /// Sets a connection's inbound-listener enabled state.
    pub(super) async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(SET_CONNECTION_ENABLED_SQL, (id, i64::from(enabled)))
            .await?;
        Ok(())
    }

    /// Deletes a connection configuration without deleting retained history.
    pub(super) async fn delete_connection(&self, id: &str) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection.execute(DELETE_CONNECTION_SQL, (id,)).await?;
        Ok(())
    }

    /// Persists a permission request before provider delivery.
    pub(super) async fn insert_prepared(&self, request: &PermissionRequest) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                INSERT_PREPARED_SQL,
                (
                    request.request_id.as_str(),
                    request.connection_id.as_str(),
                    request.destination.as_str(),
                    request.request_text.as_str(),
                    request.expires_at,
                    request.flow_execution_id.as_str(),
                    request.flow_node_id.as_str(),
                ),
            )
            .await?;
        Ok(())
    }

    /// Marks a prepared request pending; returns false if another durable transition already won.
    pub(super) async fn mark_pending(
        &self,
        request_id: &str,
        message_id: &str,
        receipt: &str,
    ) -> Result<bool, StoreError> {
        let connection = self.connection().await?;
        Ok(connection
            .execute(MARK_PENDING_SQL, (request_id, message_id, receipt))
            .await?
            == 1)
    }

    /// Attaches a provider message receipt without changing the request status.
    pub(super) async fn attach_provider_message(
        &self,
        request_id: &str,
        provider_message_id: &str,
        provider_receipt: &str,
    ) -> Result<bool, StoreError> {
        let connection = self.connection().await?;
        Ok(connection
            .execute(
                ATTACH_PROVIDER_MESSAGE_SQL,
                (request_id, provider_message_id, provider_receipt),
            )
            .await?
            == 1)
    }

    /// Atomically performs the first transition out of prepared/pending.
    pub(super) async fn finalize_atomic(
        &self,
        request_id: &str,
        status: &str,
        decided_at: i64,
        outstanding_edit: bool,
    ) -> Result<bool, StoreError> {
        let connection = self.connection().await?;
        Ok(connection
            .execute(
                FINALIZE_ATOMIC_SQL,
                (request_id, status, decided_at, i64::from(outstanding_edit)),
            )
            .await?
            == 1)
    }

    /// Clears the retry flag after a provider message edit succeeds.
    pub(super) async fn clear_outstanding_edit(&self, request_id: &str) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(CLEAR_OUTSTANDING_EDIT_SQL, (request_id,))
            .await?;
        Ok(())
    }

    /// Loads a permission request by its pre-send identity.
    pub(super) async fn get_permission_request(
        &self,
        request_id: &str,
    ) -> Result<Option<PermissionRequest>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection
            .query(GET_PERMISSION_REQUEST_SQL, (request_id,))
            .await?;
        Ok(rows
            .next()
            .await?
            .as_ref()
            .map(Self::read_permission_request)
            .transpose()?)
    }

    /// Loads one permission request by its Flow execution and node correlation.
    pub(super) async fn get_by_correlation(
        &self,
        flow_execution_id: &str,
        flow_node_id: &str,
    ) -> Result<Option<PermissionRequest>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection
            .query(GET_BY_CORRELATION_SQL, (flow_execution_id, flow_node_id))
            .await?;
        Ok(rows
            .next()
            .await?
            .as_ref()
            .map(Self::read_permission_request)
            .transpose()?)
    }

    /// Scans expired unfinished requests and requests whose provider edit remains outstanding.
    pub(super) async fn scan_unfinished(&self, now: i64) -> Result<Vec<PermissionRequest>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection.query(SCAN_UNFINISHED_SQL, (now,)).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(Self::read_permission_request(&row)?);
        }
        Ok(result)
    }

    /// Scans finalized requests whose provider edit must be retried.
    pub(super) async fn scan_outstanding_edits(&self) -> Result<Vec<PermissionRequest>, StoreError> {
        let connection = self.connection().await?;
        let mut rows = connection.query(SCAN_OUTSTANDING_EDITS_SQL, ()).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(Self::read_permission_request(&row)?);
        }
        Ok(result)
    }

    /// Finalizes every unfinished request for a deleted connection without editing provider messages.
    pub(super) async fn mark_deleted_for_connection(&self, id: &str, now: i64) -> Result<u64, StoreError> {
        let connection = self.connection().await?;
        Ok(connection
            .execute(MARK_DELETED_FOR_CONNECTION_SQL, (id, now))
            .await?)
    }

    /// Inserts a raw inbound provider delivery and optional normalized projection.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn insert_inbound(
        &self,
        raw: &[u8],
        body: Option<&str>,
        metadata: Option<&str>,
        connection_id: &str,
        channel_id: Option<&str>,
        received_at: i64,
        retention_seconds: i64,
        expires_at: i64,
    ) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                INSERT_INBOUND_SQL,
                (
                    raw,
                    body,
                    metadata,
                    connection_id,
                    channel_id,
                    received_at,
                    retention_seconds,
                    expires_at,
                ),
            )
            .await?;
        Ok(())
    }

    /// Inserts a raw permission callback independently of its arbitration outcome.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn insert_callback(
        &self,
        raw: &str,
        request_id: Option<&str>,
        action: Option<&str>,
        decided_at: Option<i64>,
        connection_id: Option<&str>,
        received_at: i64,
        retention_seconds: i64,
        expires_at: i64,
    ) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                INSERT_CALLBACK_SQL,
                (
                    raw,
                    request_id,
                    action,
                    decided_at,
                    connection_id,
                    received_at,
                    retention_seconds,
                    expires_at,
                ),
            )
            .await?;
        Ok(())
    }

    /// Inserts an accepted outbound send; raw payload is deliberately SQL NULL.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn insert_accepted_send(
        &self,
        body: &str,
        metadata: Option<&str>,
        receipt: &str,
        connection_id: &str,
        received_at: i64,
        retention_seconds: i64,
        expires_at: i64,
    ) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        connection
            .execute(
                INSERT_ACCEPTED_SEND_SQL,
                (
                    body,
                    metadata,
                    receipt,
                    connection_id,
                    received_at,
                    retention_seconds,
                    expires_at,
                ),
            )
            .await?;
        Ok(())
    }

    /// Prunes only inbound, callback, and accepted-send history under live/snapshot retention rules.
    pub(super) async fn prune_expired(&self, now: i64) -> Result<(), StoreError> {
        let connection = self.connection().await?;
        for query in [PRUNE_EXPIRED_SQL, PRUNE_CALLBACKS_SQL, PRUNE_ACCEPTED_SENDS_SQL] {
            connection.execute(query, (now,)).await?;
        }
        Ok(())
    }

    fn read_connection(row: &Row) -> Result<MessagingConnection, TursoError> {
        Ok(MessagingConnection {
            id: row.get(0)?,
            display_name: row.get(1)?,
            provider_type: row.get(2)?,
            credential_id: row.get(3)?,
            transport_settings: row.get(4)?,
            enabled: row.get::<i64>(5)? != 0,
            retention_seconds: row.get(6)?,
        })
    }

    fn read_permission_request(row: &Row) -> Result<PermissionRequest, TursoError> {
        Ok(PermissionRequest {
            request_id: row.get(0)?,
            connection_id: row.get(1)?,
            provider_message_id: row.get(2)?,
            destination: row.get(3)?,
            request_text: row.get(4)?,
            expires_at: row.get(5)?,
            flow_execution_id: row.get(6)?,
            flow_node_id: row.get(7)?,
            status: row.get(8)?,
            provider_receipt: row.get(9)?,
            decided_at: row.get(10)?,
            outstanding_edit: row.get::<i64>(11)? != 0,
        })
    }
}

//! Durable application-owned coordination recovery data.

use std::path::Path;

use serde::{Deserialize, Serialize};
use turso::{Connection, Database};

use crate::migrations::coordination::MIGRATIONS;
use crate::utils::database::{MigrationRunner, connect_database, maintain_database, open_database_at_path};

use super::types::{CoordinationError, DomainId};

const CLEAR_DECIDED: &str = include_str!("queries/clear_decided.sql");
const CLEAR_PENDING: &str = include_str!("queries/clear_pending.sql");
const CLEAR_PROPOSALS: &str = include_str!("queries/clear_proposals.sql");
const INSERT_DECIDED: &str = include_str!("queries/insert_decided.sql");
const INSERT_PENDING: &str = include_str!("queries/insert_pending.sql");
const INSERT_PROPOSAL: &str = include_str!("queries/insert_proposal.sql");
const INSERT_RETIRED: &str = include_str!("queries/insert_retired.sql");
const LOAD_DECIDED: &str = include_str!("queries/load_decided.sql");
const LOAD_PENDING: &str = include_str!("queries/load_pending.sql");
const LOAD_PROPOSALS: &str = include_str!("queries/load_proposals.sql");
const LOAD_RETIRED: &str = include_str!("queries/load_retired.sql");
const LOAD_SNAPSHOT: &str = include_str!("queries/load_snapshot.sql");
const SAVE_SNAPSHOT: &str = include_str!("queries/save_snapshot.sql");

/// Serialized ABCI recovery unit; JSON representation belongs to the ABCI and network owners.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RecoveryState {
    pub(super) state_json: String,
    pub(super) pending: Vec<PendingRow>,
    pub(super) decided: Vec<DecidedRow>,
    pub(super) proposals: Vec<ProposalRow>,
}

/// A signed, admitted transaction in admission order.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingRow {
    pub(super) event_json: String,
    pub(super) body_json: String,
}

/// Decided application bytes and the network-encoded extended certificate.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DecidedRow {
    pub(super) height: u64,
    pub(super) value_bytes: Vec<u8>,
    pub(super) certificate_json: String,
}

/// An application-owned proposal and its retained replay data.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ProposalRow {
    pub(super) height: u64,
    pub(super) value_id: Vec<u8>,
    pub(super) payload_json: String,
}

/// Coordination's independently migrated database.
pub(super) struct CoordinationStore {
    database: Database,
}

impl CoordinationStore {
    /// Opens the coordination database in the supplied configuration directory.
    pub(super) async fn open(config_dir: &Path) -> Result<Self, CoordinationError> {
        let path = config_dir.join("coordination.db");
        let database = open_database_at_path(&path)
            .await
            .map_err(CoordinationError::Database)?;
        MigrationRunner::new("__coordination_migrations", MIGRATIONS)
            .apply(&database)
            .await
            .map_err(CoordinationError::Database)?;
        maintain_database(&database, &path).await;
        Ok(Self { database })
    }

    /// Loads the recovery unit, if one has been saved.
    pub(super) async fn load(&self) -> Result<Option<RecoveryState>, CoordinationError> {
        let mut connection = connect_database(&self.database)
            .await
            .map_err(CoordinationError::Database)?;
        let transaction = connection.transaction().await.map_err(storage_error)?;
        let mut rows = transaction
            .query(LOAD_SNAPSHOT, ())
            .await
            .map_err(storage_error)?;
        let Some(row) = rows.next().await.map_err(storage_error)? else {
            drop(rows);
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let state_json: String = row.get(0).map_err(storage_error)?;
        drop(rows);
        let mut pending = Vec::new();
        let mut rows = transaction
            .query(LOAD_PENDING, ())
            .await
            .map_err(storage_error)?;
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            pending.push(PendingRow {
                event_json: row.get(0).map_err(storage_error)?,
                body_json: row.get(1).map_err(storage_error)?,
            });
        }
        drop(rows);
        let mut decided = Vec::new();
        let mut rows = transaction
            .query(LOAD_DECIDED, ())
            .await
            .map_err(storage_error)?;
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            decided.push(DecidedRow {
                height: decode_height(row.get(0).map_err(storage_error)?)?,
                value_bytes: row.get(1).map_err(storage_error)?,
                certificate_json: row.get(2).map_err(storage_error)?,
            });
        }
        drop(rows);
        let mut proposals = Vec::new();
        let mut rows = transaction
            .query(LOAD_PROPOSALS, ())
            .await
            .map_err(storage_error)?;
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            proposals.push(ProposalRow {
                height: decode_height(row.get(0).map_err(storage_error)?)?,
                value_id: row.get(1).map_err(storage_error)?,
                payload_json: row.get(2).map_err(storage_error)?,
            });
        }
        drop(rows);
        transaction.commit().await.map_err(storage_error)?;
        let state = RecoveryState {
            state_json,
            pending,
            decided,
            proposals,
        };
        state.validate_json()?;
        Ok(Some(state))
    }

    /// Atomically replaces projection, pending queue, decided history and retained proposals.
    pub(super) async fn save(&self, state: &RecoveryState) -> Result<(), CoordinationError> {
        state.validate_json()?;
        let mut connection = connect_database(&self.database)
            .await
            .map_err(CoordinationError::Database)?;
        let transaction = connection.transaction().await.map_err(storage_error)?;
        Self::write_active(&transaction, state).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(())
    }

    /// Retains an immutable predecessor and replaces the active unit in one transaction.
    pub(super) async fn save_successor(
        &self,
        predecessor: DomainId,
        previous: &RecoveryState,
        successor: &RecoveryState,
    ) -> Result<(), CoordinationError> {
        previous.validate_json()?;
        successor.validate_json()?;
        let encoded = serde_json::to_string(previous).map_err(|_| CoordinationError::Encoding)?;
        let mut connection = connect_database(&self.database)
            .await
            .map_err(CoordinationError::Database)?;
        let transaction = connection.transaction().await.map_err(storage_error)?;
        transaction
            .execute(INSERT_RETIRED, (predecessor.as_bytes().to_vec(), encoded))
            .await
            .map_err(storage_error)?;
        if Self::read_retired(&transaction, predecessor)
            .await?
            .as_ref()
            != Some(previous)
        {
            return Err(CoordinationError::Encoding);
        }
        Self::write_active(&transaction, successor).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(())
    }

    /// Loads a retired recovery unit by its chain identity.
    pub(super) async fn load_retired(&self, domain: DomainId) -> Result<Option<RecoveryState>, CoordinationError> {
        let connection = connect_database(&self.database)
            .await
            .map_err(CoordinationError::Database)?;
        Self::read_retired(&connection, domain).await
    }

    async fn read_retired(
        connection: &Connection,
        domain: DomainId,
    ) -> Result<Option<RecoveryState>, CoordinationError> {
        let mut rows = connection
            .query(LOAD_RETIRED, (domain.as_bytes().to_vec(),))
            .await
            .map_err(storage_error)?;
        let Some(row) = rows.next().await.map_err(storage_error)? else {
            return Ok(None);
        };
        let json: String = row.get(0).map_err(storage_error)?;
        let state: RecoveryState = serde_json::from_str(&json).map_err(|_| CoordinationError::Encoding)?;
        state.validate_json()?;
        Ok(Some(state))
    }

    async fn write_active(transaction: &Connection, state: &RecoveryState) -> Result<(), CoordinationError> {
        transaction
            .execute(SAVE_SNAPSHOT, (state.state_json.clone(),))
            .await
            .map_err(storage_error)?;
        transaction
            .execute(CLEAR_PENDING, ())
            .await
            .map_err(storage_error)?;
        transaction
            .execute(CLEAR_DECIDED, ())
            .await
            .map_err(storage_error)?;
        transaction
            .execute(CLEAR_PROPOSALS, ())
            .await
            .map_err(storage_error)?;
        for (index, item) in state.pending.iter().enumerate() {
            let sequence = i64::try_from(index).map_err(|_| CoordinationError::PendingSequenceOverflow)?;
            transaction
                .execute(
                    INSERT_PENDING,
                    (sequence, item.event_json.clone(), item.body_json.clone()),
                )
                .await
                .map_err(storage_error)?;
        }
        for item in &state.decided {
            transaction
                .execute(
                    INSERT_DECIDED,
                    (
                        item.height.to_be_bytes().to_vec(),
                        item.value_bytes.clone(),
                        item.certificate_json.clone(),
                    ),
                )
                .await
                .map_err(storage_error)?;
        }
        for item in &state.proposals {
            transaction
                .execute(
                    INSERT_PROPOSAL,
                    (
                        item.height.to_be_bytes().to_vec(),
                        item.value_id.clone(),
                        item.payload_json.clone(),
                    ),
                )
                .await
                .map_err(storage_error)?;
        }
        Ok(())
    }
}

impl RecoveryState {
    fn validate_json(&self) -> Result<(), CoordinationError> {
        let values = std::iter::once(&self.state_json)
            .chain(
                self.pending
                    .iter()
                    .flat_map(|row| [&row.event_json, &row.body_json]),
            )
            .chain(self.decided.iter().map(|row| &row.certificate_json))
            .chain(self.proposals.iter().map(|row| &row.payload_json));
        for value in values {
            serde_json::from_str::<serde_json::Value>(value).map_err(|_| CoordinationError::Encoding)?;
        }
        Ok(())
    }
}

fn decode_height(bytes: Vec<u8>) -> Result<u64, CoordinationError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| CoordinationError::Encoding)?;
    Ok(u64::from_be_bytes(bytes))
}

fn storage_error(error: turso::Error) -> CoordinationError {
    CoordinationError::Storage(error)
}

#[cfg(test)]
mod tests {
    use super::{CoordinationStore, DecidedRow, DomainId, PendingRow, ProposalRow, RecoveryState};

    fn recovery(marker: u8) -> RecoveryState {
        RecoveryState {
            state_json: marker.to_string(),
            pending: vec![PendingRow {
                event_json: "{\"event\":1}".into(),
                body_json: "[1,2]".into(),
            }],
            decided: vec![DecidedRow {
                height: 1,
                value_bytes: vec![marker, 255],
                certificate_json: "{}".into(),
            }],
            proposals: vec![ProposalRow {
                height: 2,
                value_id: vec![marker],
                payload_json: "[]".into(),
            }],
        }
    }

    #[tokio::test]
    async fn successor_and_full_retired_state_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let domain = DomainId::from_bytes([1; 32]);
        let previous = recovery(1);
        let successor = recovery(2);
        let store = CoordinationStore::open(dir.path()).await.unwrap();
        assert!(store.load_retired(domain).await.unwrap().is_none());
        store.save(&previous).await.unwrap();
        store
            .save_successor(domain, &previous, &successor)
            .await
            .unwrap();
        drop(store);
        let store = CoordinationStore::open(dir.path()).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(successor));
        assert_eq!(store.load_retired(domain).await.unwrap(), Some(previous));
    }

    #[tokio::test]
    async fn retired_state_is_immutable_and_identical_retry_is_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let domain = DomainId::from_bytes([2; 32]);
        let previous = recovery(1);
        let successor = recovery(2);
        let store = CoordinationStore::open(dir.path()).await.unwrap();
        store
            .save_successor(domain, &previous, &successor)
            .await
            .unwrap();
        store
            .save_successor(domain, &previous, &successor)
            .await
            .unwrap();
        assert!(
            store
                .save_successor(domain, &recovery(3), &recovery(4))
                .await
                .is_err()
        );
        assert_eq!(store.load_retired(domain).await.unwrap(), Some(previous));
        assert_eq!(store.load().await.unwrap(), Some(successor));
    }

    #[tokio::test]
    async fn failed_successor_write_rolls_back_retirement_and_active_state() {
        let dir = tempfile::tempdir().unwrap();
        let domain = DomainId::from_bytes([3; 32]);
        let previous = recovery(1);
        let mut invalid = recovery(2);
        invalid.decided.push(DecidedRow {
            height: 1,
            value_bytes: vec![9],
            certificate_json: "{}".into(),
        });
        let store = CoordinationStore::open(dir.path()).await.unwrap();
        store.save(&previous).await.unwrap();
        assert!(
            store
                .save_successor(domain, &previous, &invalid)
                .await
                .is_err()
        );
        drop(store);
        let store = CoordinationStore::open(dir.path()).await.unwrap();
        assert!(store.load_retired(domain).await.unwrap().is_none());
        assert_eq!(store.load().await.unwrap(), Some(previous));
    }
}

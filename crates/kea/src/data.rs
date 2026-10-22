use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use turso::Database;

use aves::utils::database::{
    DatabaseConcurrency, MigrationRunner, connect_database, maintain_database, open_database_at_path_with_concurrency,
};

use super::migrations::MIGRATIONS;
use super::types::{DataList, DataObject, DataSummary, KnowledgeError, ListDataRequest, UpsertDataRequest};

pub(super) struct DataStore {
    database: Arc<Database>,
    path: PathBuf,
}

impl DataStore {
    pub(super) async fn maintain_db(&self) {
        maintain_database(&self.database, &self.path).await;
    }

    pub(super) async fn open(config_dir: &Path) -> Result<Self, KnowledgeError> {
        let concurrency = if cfg!(test) {
            DatabaseConcurrency::SingleProcess
        } else {
            DatabaseConcurrency::MultiProcessWal
        };
        let path = config_dir.join("knowledge.db");
        let database = open_database_at_path_with_concurrency(&path, concurrency)
            .await
            .map_err(|error| storage_error("open knowledge data", error))?;
        MigrationRunner::new("__knowledge_migrations", MIGRATIONS)
            .apply(&database)
            .await
            .map_err(|error| storage_error("migrate knowledge data", error))?;
        maintain_database(&database, &path).await;
        Ok(Self {
            database: Arc::new(database),
            path,
        })
    }

    pub(super) async fn upsert(&self, request: UpsertDataRequest) -> Result<DataObject, KnowledgeError> {
        let updated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
        let value_json = serde_json::to_string(&request.value).map_err(|error| storage_error("encode data", error))?;
        let connection = connect_database(&self.database)
            .await
            .map_err(|error| storage_error("connect to knowledge data", error))?;
        connection
            .execute(
                include_str!("queries/upsert_data.sql"),
                (request.key.as_str(), value_json.as_str(), updated_at.as_str()),
            )
            .await
            .map_err(|error| storage_error("upsert data", error))?;
        Ok(DataObject {
            key: request.key,
            value: request.value,
            updated_at,
        })
    }

    pub(super) async fn list(&self, request: ListDataRequest) -> Result<DataList, KnowledgeError> {
        let since = request.since.as_deref().map(parse_bound).transpose()?;
        let until = request.until.as_deref().map(parse_bound).transpose()?;
        let connection = connect_database(&self.database)
            .await
            .map_err(|error| storage_error("connect to knowledge data", error))?;
        let mut rows = connection
            .query(
                include_str!("queries/list_data.sql"),
                (
                    since.as_deref(),
                    until.as_deref(),
                    i64::from(request.limit.min(200)),
                    i64::from(request.offset),
                ),
            )
            .await
            .map_err(|error| storage_error("list data", error))?;
        let mut entries = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| storage_error("read data list", error))?
        {
            entries.push(DataSummary {
                key: row
                    .get(0)
                    .map_err(|error| storage_error("decode data list", error))?,
                updated_at: row
                    .get(1)
                    .map_err(|error| storage_error("decode data list", error))?,
            });
        }
        Ok(DataList { entries })
    }

    pub(super) async fn get(&self, key: &str) -> Result<DataObject, KnowledgeError> {
        let connection = connect_database(&self.database)
            .await
            .map_err(|error| storage_error("connect to knowledge data", error))?;
        let mut rows = connection
            .query(include_str!("queries/get_data.sql"), (key,))
            .await
            .map_err(|error| storage_error("get data", error))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|error| storage_error("read data", error))?
        else {
            return Err(KnowledgeError::DataNotFound { key: key.to_owned() });
        };
        let value_json: String = row
            .get(0)
            .map_err(|error| storage_error("decode data", error))?;
        let updated_at: String = row
            .get(1)
            .map_err(|error| storage_error("decode data", error))?;
        let value = serde_json::from_str(&value_json).map_err(|error| storage_error("decode data", error))?;
        Ok(DataObject {
            key: key.to_owned(),
            value,
            updated_at,
        })
    }

    pub(super) async fn delete(&self, key: &str) -> Result<(), KnowledgeError> {
        let connection = connect_database(&self.database)
            .await
            .map_err(|error| storage_error("connect to knowledge data", error))?;
        let affected = connection
            .execute(include_str!("queries/delete_data.sql"), (key,))
            .await
            .map_err(|error| storage_error("delete data", error))?;
        if affected == 0 {
            return Err(KnowledgeError::DataNotFound { key: key.to_owned() });
        }
        Ok(())
    }
}

fn parse_bound(value: &str) -> Result<String, KnowledgeError> {
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Micros, true)
        })
        .map_err(|_| KnowledgeError::InvalidDataDate {
            value: value.to_owned(),
        })
}

fn storage_error(operation: &'static str, error: impl std::fmt::Display) -> KnowledgeError {
    KnowledgeError::DataStorage {
        operation: operation.to_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::*;

    fn object(value: Value) -> serde_json::Map<String, Value> {
        serde_json::from_value(value).expect("JSON object")
    }

    #[tokio::test]
    async fn data_is_exact_case_sensitive_and_upsert_replaces_the_whole_object() {
        let root = tempdir().expect("tempdir");
        let store = DataStore::open(root.path()).await.expect("open data store");
        store
            .upsert(UpsertDataRequest {
                key: "Alpha".to_owned(),
                value: object(json!({"old": 1})),
            })
            .await
            .expect("upsert first key");
        store
            .upsert(UpsertDataRequest {
                key: "alpha".to_owned(),
                value: object(json!({"lower": true})),
            })
            .await
            .expect("upsert second key");
        let replaced = store
            .upsert(UpsertDataRequest {
                key: "Alpha".to_owned(),
                value: object(json!({"new": 2})),
            })
            .await
            .expect("replace value");
        assert_eq!(replaced.value, object(json!({"new": 2})));
        assert_eq!(
            store.get("Alpha").await.expect("uppercase key").value,
            object(json!({"new": 2}))
        );
        assert_eq!(
            store.get("alpha").await.expect("lowercase key").value,
            object(json!({"lower": true}))
        );
        assert!(matches!(
            store.get("ALPHA").await,
            Err(KnowledgeError::DataNotFound { .. })
        ));
        store.delete("Alpha").await.expect("delete exact key");
        assert!(matches!(
            store.delete("Alpha").await,
            Err(KnowledgeError::DataNotFound { .. })
        ));
        assert!(store.get("alpha").await.is_ok());
    }

    #[tokio::test]
    async fn data_list_filters_only_by_write_time_and_paginates_keys_without_values() {
        let root = tempdir().expect("tempdir");
        let store = DataStore::open(root.path()).await.expect("open data store");
        let first = store
            .upsert(UpsertDataRequest {
                key: "a".to_owned(),
                value: object(json!({"n": 1})),
            })
            .await
            .expect("first key");
        store
            .upsert(UpsertDataRequest {
                key: "b".to_owned(),
                value: object(json!({"n": 2})),
            })
            .await
            .expect("second key");
        let all = store
            .list(ListDataRequest {
                since: Some(first.updated_at.clone()),
                until: None,
                limit: 10,
                offset: 0,
            })
            .await
            .expect("full listing");
        let page = store
            .list(ListDataRequest {
                since: Some(first.updated_at),
                until: None,
                limit: 1,
                offset: 1,
            })
            .await
            .expect("list second page");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(all.entries.len(), 2);
        assert_eq!(page.entries, all.entries[1..]);
        assert!(matches!(
            store
                .list(ListDataRequest {
                    since: Some("not a date".to_owned()),
                    until: None,
                    limit: 10,
                    offset: 0,
                })
                .await,
            Err(KnowledgeError::InvalidDataDate { .. })
        ));
    }

    #[tokio::test]
    async fn migration_drops_retired_note_tables_without_touching_new_data_on_reopen() {
        let root = tempdir().expect("tempdir");
        let legacy = open_database_at_path_with_concurrency(
            &root.path().join("knowledge.db"),
            DatabaseConcurrency::SingleProcess,
        )
        .await
        .expect("legacy database");
        MigrationRunner::new("__knowledge_migrations", &MIGRATIONS[..1])
            .apply(&legacy)
            .await
            .expect("legacy migration");
        let connection = connect_database(&legacy).await.expect("legacy connection");
        connection
            .execute(
                "INSERT INTO knowledge (id, title, content, content_type, kind, source_type, content_hash) VALUES ('old', 'old', 'retired note', 'markdown', 'note', 'manual', 'hash')",
                (),
            )
            .await
            .expect("legacy note");
        drop(connection);
        drop(legacy);

        let store = DataStore::open(root.path()).await.expect("open data store");
        store
            .upsert(UpsertDataRequest {
                key: "persist".to_owned(),
                value: object(json!({"ok": true})),
            })
            .await
            .expect("store object");
        let connection = connect_database(&store.database).await.expect("connect");
        let mut rows = connection.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('knowledge', 'knowledge_chunks', 'knowledge_data') ORDER BY name",
            (),
        ).await.expect("inspect schema");
        let row = rows.next().await.expect("read schema").expect("data table");
        assert_eq!(row.get::<String>(0).expect("table name"), "knowledge_data");
        assert!(rows.next().await.expect("read remaining schema").is_none());
        drop(rows);
        drop(connection);
        drop(store);
        let reopened = DataStore::open(root.path())
            .await
            .expect("reopen migrated data store");
        assert_eq!(
            reopened
                .get("persist")
                .await
                .expect("persisted object")
                .value,
            object(json!({"ok": true}))
        );
    }
}

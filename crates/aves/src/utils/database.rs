use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use thiserror::Error;
use turso::{Builder, Connection, Database, Error as TursoError};

const BUSY_TIMEOUT_MS: u64 = 30_000;
const MINIMUM_FREE_BYTES: i64 = 64 * 1024 * 1024;
const LEDGER_TABLE: &str = "{table}";

/// Selects the coordination mode for a local database.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseConcurrency {
    /// Use the ordinary single-process WAL implementation.
    SingleProcess,
    /// Enable experimental multiprocess coordination on Linux only.
    MultiProcessWal,
}

#[cfg(target_os = "linux")]
impl DatabaseConcurrency {
    fn multiprocess_wal_enabled(self) -> bool {
        matches!(self, Self::MultiProcessWal)
    }
}

/// An embedded, ordered database migration owned by a registering package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

/// Reusable database open, connection, and migration lifecycle.
pub struct MigrationRunner<'m> {
    bookkeeping_table: &'static str,
    migrations: &'m [Migration],
}

impl<'m> MigrationRunner<'m> {
    /// Creates a runner for one database-owned migration history table.
    pub const fn new(bookkeeping_table: &'static str, migrations: &'m [Migration]) -> Self {
        Self {
            bookkeeping_table,
            migrations,
        }
    }

    /// Applies every not-yet-recorded migration in registration order.
    pub async fn apply(&self, database: &Database) -> Result<usize, DatabaseError> {
        let mut connection = connect_database(database).await?;
        let create_sql = ledger_sql(
            include_str!("queries/database_ledger_create.sql"),
            self.bookkeeping_table,
        );
        let exists_sql = ledger_sql(
            include_str!("queries/database_ledger_exists.sql"),
            self.bookkeeping_table,
        );
        let insert_sql = ledger_sql(
            include_str!("queries/database_ledger_insert.sql"),
            self.bookkeeping_table,
        );
        connection.execute(&create_sql, ()).await?;
        let mut applied = 0;
        for migration in self.migrations {
            if apply_migration(&mut connection, &exists_sql, &insert_sql, migration).await? {
                applied += 1;
            }
        }
        Ok(applied)
    }
}

async fn apply_migration(
    connection: &mut Connection,
    exists_sql: &str,
    insert_sql: &str,
    migration: &Migration,
) -> Result<bool, DatabaseError> {
    let mut rows = connection.query(exists_sql, (migration.version,)).await?;
    if rows.next().await?.is_some() {
        return Ok(false);
    }
    let transaction = connection.transaction().await?;
    transaction
        .execute_batch(migration.sql)
        .await
        .map_err(|source| DatabaseError::Migration {
            version: migration.version,
            name: migration.name,
            source,
        })?;
    transaction
        .execute(insert_sql, (migration.version, migration.name))
        .await?;
    transaction.commit().await?;
    Ok(true)
}

fn ledger_sql(template: &str, table: &str) -> String {
    template.replace(LEDGER_TABLE, table)
}

/// Typed failures from Aves-owned database lifecycle operations.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("database path must have a parent directory: {path}")]
    MissingParentDirectory { path: PathBuf },
    #[error("database path is not valid UTF-8: {path}")]
    InvalidPath { path: PathBuf },
    #[error("failed to create database directory {path}: {source}")]
    CreateDirectory { path: PathBuf, source: IoError },
    #[error("failed to open database {path}: {source}")]
    OpenDatabase { path: PathBuf, source: TursoError },
    #[error("database operation failed: {0}")]
    Database(#[from] TursoError),
    #[error("failed applying migration {version} ({name}): {source}")]
    Migration {
        version: i64,
        name: &'static str,
        source: TursoError,
    },
}

/// Opens a local Turso database at an explicit path and prepares its directory.
pub async fn open_database_at_path(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
    open_database_at_path_with_concurrency(path, DatabaseConcurrency::MultiProcessWal).await
}

/// Opens a local Turso database with an explicit process-concurrency policy.
pub async fn open_database_at_path_with_concurrency(
    path: impl AsRef<Path>,
    _concurrency: DatabaseConcurrency,
) -> Result<Database, DatabaseError> {
    let path = path.as_ref().to_path_buf();
    let Some(parent) = path.parent() else {
        return Err(DatabaseError::MissingParentDirectory { path });
    };
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|source| DatabaseError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    let path_string = path
        .to_str()
        .ok_or_else(|| DatabaseError::InvalidPath { path: path.clone() })?;
    let builder = Builder::new_local(path_string);
    #[cfg(target_os = "linux")]
    let builder = builder.experimental_multiprocess_wal(_concurrency.multiprocess_wal_enabled());
    builder
        .build()
        .await
        .map_err(|source| DatabaseError::OpenDatabase { path, source })
}

/// Opens a configured connection with the shared SQLite safety pragmas.
pub async fn connect_database(database: &Database) -> Result<Connection, DatabaseError> {
    let connection = database.connect()?;
    apply_connection_setup(&connection).await?;
    Ok(connection)
}

/// Applies the shared SQLite connection safety pragmas.
async fn apply_connection_setup(connection: &Connection) -> Result<(), DatabaseError> {
    connection.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))?;
    connection
        .execute(include_str!("queries/database_foreign_keys.sql"), ())
        .await?;
    Ok(())
}

#[derive(Debug, Error)]
enum MaintenanceError {
    #[error(transparent)]
    DatabaseLifecycle(#[from] DatabaseError),
    #[error(transparent)]
    Database(#[from] TursoError),
    #[error(transparent)]
    Filesystem(#[from] IoError),
    #[error("database maintenance query returned no result: {0}")]
    MissingResult(&'static str),
    #[error("checkpoint did not complete (status {0}); maintenance deferred")]
    CheckpointIncomplete(i64),
}

/// Checkpoints a Aves database WAL and reclaims substantial free space without blocking lifecycle transitions.
pub async fn maintain_database(database: &Database, path: &Path) {
    let started = Instant::now();
    match maintain(database, path).await {
        Ok((before, after, vacuumed)) => tracing::info!(
            database = %path.display(),
            database_bytes_before = before.0,
            wal_bytes_before = before.1,
            database_bytes_after = after.0,
            wal_bytes_after = after.1,
            vacuumed,
            elapsed_ms = started.elapsed().as_millis(),
            "database maintenance completed"
        ),
        Err(error) => tracing::warn!(
            database = %path.display(),
            %error,
            elapsed_ms = started.elapsed().as_millis(),
            "database maintenance deferred"
        ),
    }
}

async fn maintain(database: &Database, path: &Path) -> Result<((u64, u64), (u64, u64), bool), MaintenanceError> {
    let before = sizes(path).await?;
    let connection = connect_database(database).await?;
    connection.busy_timeout(Duration::ZERO)?;
    checkpoint(&connection).await?;
    let page_count = scalar(
        &connection,
        include_str!("queries/database_page_count.sql"),
        "PRAGMA page_count",
    )
    .await?;
    let free_pages = scalar(
        &connection,
        include_str!("queries/database_freelist_count.sql"),
        "PRAGMA freelist_count",
    )
    .await?;
    let page_size = scalar(
        &connection,
        include_str!("queries/database_page_size.sql"),
        "PRAGMA page_size",
    )
    .await?;
    let vacuumed = should_vacuum(page_count, free_pages, page_size);
    if vacuumed {
        connection
            .execute(include_str!("queries/database_vacuum.sql"), ())
            .await?;
        checkpoint(&connection).await?;
    }
    Ok((before, sizes(path).await?, vacuumed))
}

fn should_vacuum(page_count: i64, free_pages: i64, page_size: i64) -> bool {
    page_count > 0
        && page_size > 0
        && free_pages > 0
        && free_pages <= page_count
        && free_pages.saturating_mul(page_size) >= MINIMUM_FREE_BYTES
        && free_pages.saturating_mul(5) >= page_count
}

async fn scalar(connection: &Connection, sql: &'static str, diagnostic: &'static str) -> Result<i64, MaintenanceError> {
    let mut rows = connection.query(sql, ()).await?;
    let row = rows
        .next()
        .await?
        .ok_or(MaintenanceError::MissingResult(diagnostic))?;
    let value = row.get::<i64>(0)?;
    while rows.next().await?.is_some() {}
    Ok(value)
}

async fn checkpoint(connection: &Connection) -> Result<(), MaintenanceError> {
    let mut rows = connection
        .query(include_str!("queries/database_checkpoint.sql"), ())
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or(MaintenanceError::MissingResult("wal_checkpoint"))?;
    let status = row.get::<i64>(0)?;
    checkpoint_status(status)?;
    let frames = row.get::<i64>(1)?;
    let checkpointed = row.get::<i64>(2)?;
    while rows.next().await?.is_some() {}
    tracing::debug!(frames, checkpointed, "database WAL checkpoint completed");
    Ok(())
}

fn checkpoint_status(status: i64) -> Result<(), MaintenanceError> {
    if status == 0 {
        Ok(())
    } else {
        Err(MaintenanceError::CheckpointIncomplete(status))
    }
}

async fn sizes(path: &Path) -> Result<(u64, u64), MaintenanceError> {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    Ok((file_size(path).await?, file_size(Path::new(&wal)).await?))
}

async fn file_size(path: &Path) -> Result<u64, IoError> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;

    use tempfile::tempdir;

    #[test]
    fn vacuum_requires_both_absolute_and_relative_free_space() {
        assert!(should_vacuum(80_000, 16_384, 4096));
        assert!(!should_vacuum(100_000, 16_384, 4096));
        assert!(!should_vacuum(100, 90, 4096));
        assert!(!should_vacuum(0, 0, 4096));
    }

    #[test]
    fn vacuum_thresholds_are_inclusive_and_invalid_counts_are_rejected() {
        assert!(should_vacuum(2560, 512, 131_072));
        assert!(!should_vacuum(2560, 512, 131_071));
        assert!(!should_vacuum(100, 101, MINIMUM_FREE_BYTES));
        assert!(!should_vacuum(-1, 1, MINIMUM_FREE_BYTES));
        assert!(!should_vacuum(100, 1, 0));
        assert!(!should_vacuum(100, 0, MINIMUM_FREE_BYTES));
        assert!(should_vacuum(i64::MAX, i64::MAX, i64::MAX));
    }

    #[test]
    fn ledger_templates_substitute_trusted_table_and_leave_values_bound() {
        let sql = ledger_sql(include_str!("queries/database_ledger_insert.sql"), "owner_history");
        assert!(sql.starts_with("INSERT INTO owner_history"));
        assert!(sql.contains("VALUES (?1, ?2)"));
        assert!(!sql.contains(LEDGER_TABLE));
    }

    #[test]
    fn checkpoint_status_accepts_complete_and_reports_busy_status() {
        assert!(checkpoint_status(0).is_ok());
        assert!(matches!(
            checkpoint_status(1),
            Err(MaintenanceError::CheckpointIncomplete(1))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn multiprocess_mode_is_enabled_only_for_its_policy_variant() {
        assert!(DatabaseConcurrency::MultiProcessWal.multiprocess_wal_enabled());
        assert!(!DatabaseConcurrency::SingleProcess.multiprocess_wal_enabled());
    }

    #[tokio::test]
    async fn runner_applies_registration_order_idempotently_and_keeps_prior_commits() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("runner.db");
        let database = open_database_at_path_with_concurrency(&path, DatabaseConcurrency::SingleProcess)
            .await
            .unwrap();
        let migrations = [
            Migration {
                version: 9,
                name: "first",
                sql: "CREATE TABLE ordering (value INTEGER); INSERT INTO ordering VALUES (9);",
            },
            Migration {
                version: 2,
                name: "second",
                sql: "INSERT INTO ordering VALUES (2);",
            },
            Migration {
                version: 3,
                name: "third",
                sql: "INSERT INTO ordering VALUES (3);",
            },
        ];
        let runner = MigrationRunner::new("test_ledger", &migrations);
        assert_eq!(runner.apply(&database).await.unwrap(), 3);
        assert_eq!(runner.apply(&database).await.unwrap(), 0);
        let connection = database.connect().unwrap();
        let mut rows = connection
            .query("SELECT value FROM ordering", ())
            .await
            .unwrap();
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            values.push(row.get::<i64>(0).unwrap());
        }
        assert_eq!(values, [9, 2, 3]);
    }

    #[tokio::test]
    async fn runner_with_no_migrations_creates_empty_ledger() {
        let directory = tempdir().unwrap();
        let database = open_database_at_path_with_concurrency(
            directory.path().join("empty.db"),
            DatabaseConcurrency::SingleProcess,
        )
        .await
        .unwrap();
        assert_eq!(
            MigrationRunner::new("empty_ledger", &[])
                .apply(&database)
                .await
                .unwrap(),
            0
        );
        let connection = database.connect().unwrap();
        let mut rows = connection
            .query("SELECT COUNT(*) FROM empty_ledger", ())
            .await
            .unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 0);
    }

    #[tokio::test]
    async fn failed_migration_rolls_back_its_changes_but_preserves_earlier_ledger_rows() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("rollback.db");
        let database = open_database_at_path_with_concurrency(&path, DatabaseConcurrency::SingleProcess)
            .await
            .unwrap();
        let migrations = [
            Migration {
                version: 1,
                name: "committed",
                sql: "CREATE TABLE committed_rows (value INTEGER); INSERT INTO committed_rows VALUES (1);",
            },
            Migration {
                version: 2,
                name: "failed",
                sql: "CREATE TABLE rolled_back (value INTEGER); INSERT INTO absent_table VALUES (2);",
            },
            Migration {
                version: 3,
                name: "later",
                sql: "CREATE TABLE later_rows (value INTEGER);",
            },
        ];
        let runner = MigrationRunner::new("test_ledger", &migrations);
        assert!(matches!(
            runner.apply(&database).await,
            Err(DatabaseError::Migration {
                version: 2,
                name: "failed",
                ..
            })
        ));
        let connection = database.connect().unwrap();
        let mut rows = connection
            .query("SELECT version FROM test_ledger ORDER BY version", ())
            .await
            .unwrap();
        assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 1);
        assert!(rows.next().await.unwrap().is_none());
        let mut absent = connection
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('rolled_back', 'later_rows')",
                (),
            )
            .await
            .unwrap();
        assert!(absent.next().await.unwrap().is_none());
        assert!(
            runner
                .apply(&database)
                .await
                .unwrap_err()
                .to_string()
                .contains("failed")
        );
    }

    #[tokio::test]
    async fn connection_setup_enables_foreign_key_enforcement() {
        let directory = tempdir().unwrap();
        let database =
            open_database_at_path_with_concurrency(directory.path().join("fk.db"), DatabaseConcurrency::SingleProcess)
                .await
                .unwrap();
        let connection = connect_database(&database).await.unwrap();
        assert_eq!(
            scalar(&connection, "PRAGMA busy_timeout", "busy_timeout")
                .await
                .unwrap(),
            BUSY_TIMEOUT_MS as i64
        );
        connection
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();
        connection
            .execute("CREATE TABLE child (parent_id INTEGER REFERENCES parent(id))", ())
            .await
            .unwrap();
        assert!(
            connection
                .execute("INSERT INTO child VALUES (7)", ())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn open_preserves_path_validation_and_directory_creation_errors() {
        assert!(matches!(
            open_database_at_path_with_concurrency("", DatabaseConcurrency::SingleProcess).await,
            Err(DatabaseError::MissingParentDirectory { .. })
        ));
        let directory = tempdir().unwrap();
        let file = directory.path().join("not_directory");
        tokio::fs::write(&file, b"x").await.unwrap();
        assert!(matches!(
            open_database_at_path_with_concurrency(file.join("db"), DatabaseConcurrency::SingleProcess).await,
            Err(DatabaseError::CreateDirectory { .. })
        ));
        #[cfg(unix)]
        {
            let mut invalid = directory.path().as_os_str().to_os_string();
            invalid.push(std::ffi::OsString::from_vec(b"/db\xff".to_vec()));
            assert!(matches!(
                open_database_at_path_with_concurrency(PathBuf::from(invalid), DatabaseConcurrency::SingleProcess)
                    .await,
                Err(DatabaseError::InvalidPath { .. })
            ));
        }
    }

    #[tokio::test]
    async fn scalar_preserves_diagnostic_and_drains_rows() {
        let directory = tempdir().unwrap();
        let database = open_database_at_path_with_concurrency(
            directory.path().join("scalar.db"),
            DatabaseConcurrency::SingleProcess,
        )
        .await
        .unwrap();
        let connection = connect_database(&database).await.unwrap();
        assert!(matches!(
            scalar(&connection, "SELECT 1 WHERE 0", "PRAGMA page_count").await,
            Err(MaintenanceError::MissingResult("PRAGMA page_count"))
        ));
        assert!(matches!(
            scalar(&connection, "SELECT 'wrong'", "diagnostic").await,
            Err(MaintenanceError::Database(_))
        ));
        assert_eq!(
            scalar(&connection, "SELECT 5 UNION ALL SELECT 6", "multi")
                .await
                .unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn checkpoint_projects_status_and_drains_result_rows() {
        let directory = tempdir().unwrap();
        let database = open_database_at_path_with_concurrency(
            directory.path().join("checkpoint.db"),
            DatabaseConcurrency::SingleProcess,
        )
        .await
        .unwrap();
        let connection = database.connect().unwrap();
        assert!(matches!(checkpoint(&connection).await, Ok(())));
    }

    #[tokio::test]
    async fn file_sizes_handle_missing_files_non_utf8_paths_and_other_io_errors() {
        let directory = tempdir().unwrap();
        let absent = directory.path().join("missing");
        assert_eq!(file_size(&absent).await.unwrap(), 0);
        let file = directory.path().join("db");
        tokio::fs::write(&file, b"abc").await.unwrap();
        assert_eq!(sizes(&file).await.unwrap(), (3, 0));
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let loop_path = directory.path().join("loop");
            symlink(&loop_path, &loop_path).unwrap();
            assert!(file_size(&loop_path).await.is_err());
            let mut non_utf8_file = directory.path().as_os_str().to_os_string();
            non_utf8_file.push(std::ffi::OsString::from_vec(b"/db\xff".to_vec()));
            let mut non_utf8_wal = non_utf8_file.clone();
            non_utf8_wal.push("-wal");
            tokio::fs::write(&non_utf8_file, b"bytes").await.unwrap();
            tokio::fs::write(&non_utf8_wal, b"waldata").await.unwrap();
            assert_eq!(sizes(Path::new(&non_utf8_file)).await.unwrap(), (5, 7));
        }
    }

    #[tokio::test]
    async fn maintenance_preserves_rows_and_repeated_runs_skip_vacuum() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flows.db");
        let database = open_database_at_path_with_concurrency(&path, DatabaseConcurrency::SingleProcess)
            .await
            .unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE preserved (value TEXT)", ())
            .await
            .unwrap();
        connection
            .execute("INSERT INTO preserved VALUES ('retained')", ())
            .await
            .unwrap();
        drop(connection);

        assert!(!maintain(&database, &path).await.unwrap().2);
        assert!(!maintain(&database, &path).await.unwrap().2);
        let connection = database.connect().unwrap();
        let mut rows = connection
            .query("SELECT value FROM preserved", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "retained"
        );
    }
}

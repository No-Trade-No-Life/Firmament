//! The control plane: datasets and their declared schemas, the cold catalog,
//! flush bookkeeping, write tokens, the audit trail, syncers and Linkit state.
//!
//! The data itself never lives here. A dataset owns one hot `SQLite` database
//! (see [`crate::hot`]) and an append-only tree of Parquet objects in the cold
//! store; this database only remembers what exists.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::cooling::{Cadence, CoolingSpec};
use crate::crypto::{Cipher, CipherError};
use crate::hot::HotStore;
use crate::schema::{self, TableSpec};

const SCHEMA_VERSION: &str = "4";

/// One dataset: one declared table, its cooling calendar and a cold catalog.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Dataset {
    /// Slug identifying the dataset.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Free-form description.
    pub description: String,
    /// When the hot window rolls over, in which calendar.
    pub cooling: CoolingSpec,
    /// Sequence number of the last committed flush batch.
    pub last_batch: i64,
    /// Creation timestamp.
    pub created_at: i64,
    /// The one table the dataset declares.
    pub table: TableSpec,
}

/// One immutable Parquet file in the cold tree.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColdFile {
    /// Owning dataset.
    pub dataset_id: String,
    /// Flush batch that produced the file.
    pub batch: i64,
    /// Dataset-relative object path, also the local mirror path.
    pub path: String,
    /// Partition key (`day=2026-10-01`).
    pub partition: String,
    /// Rows in the file.
    pub rows: i64,
    /// Object size in bytes.
    pub bytes: u64,
    /// SHA-256 of the object.
    pub sha256: String,
    /// Oldest timestamp in the file.
    pub min_time: String,
    /// Newest timestamp in the file.
    pub max_time: String,
    /// When the file became part of the catalog.
    pub created_at: i64,
}

/// Cold storage totals of a dataset.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ColdSummary {
    /// Files catalogued for the dataset.
    pub files: i64,
    /// Bytes catalogued for the dataset.
    pub bytes: u64,
    /// Oldest timestamp in cold storage.
    pub earliest_time: Option<String>,
    /// Newest timestamp in cold storage.
    pub latest_time: Option<String>,
}

/// One cooling pass over a dataset.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Flush {
    /// Flush id.
    pub id: String,
    /// Owning dataset.
    pub dataset_id: String,
    /// Batch sequence number, shared with the cold files it produced.
    pub batch: i64,
    /// What started it: `schedule`, `manual` or `backfill`.
    pub trigger: String,
    /// `running`, `completed` or `failed`.
    pub status: String,
    /// Files written.
    pub files: i64,
    /// Rows written.
    pub rows: i64,
    /// Bytes written.
    pub bytes: i64,
    /// Failure message, empty on success.
    pub message: String,
    /// Start timestamp.
    pub created_at: i64,
    /// End timestamp.
    pub finished_at: Option<i64>,
}

/// A user's mirror subscription of one dataset.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Syncer {
    /// Syncer id.
    pub id: String,
    /// Owning user.
    pub owner_id: String,
    /// Display name.
    pub name: String,
    /// Mirrored dataset.
    pub dataset_id: String,
    /// Last flush batch this mirror saw.
    pub last_batch: i64,
    /// Creation timestamp.
    pub created_at: i64,
    /// Last successful mirror timestamp.
    pub last_synced_at: Option<i64>,
}

/// Linkit delivery state of one user.
#[derive(Clone, Debug, Serialize)]
pub struct LinkitStatus {
    /// Whether a bot is provisioned.
    pub configured: bool,
    /// Provisioned bot id.
    pub bot_id: Option<String>,
    /// Linkit username the bot talks to.
    pub recipient_username: Option<String>,
    /// Last delivery attempt.
    pub last_attempt_at: Option<i64>,
    /// Last successful delivery.
    pub last_success_at: Option<i64>,
    /// Last failure.
    pub last_error: Option<String>,
}

/// Secret half of the Linkit connection.
#[derive(Clone, Debug)]
pub struct SecretLinkitSettings {
    /// Bot id.
    pub bot_id: String,
    /// Decrypted bot token.
    pub bot_token: String,
    /// Recipient username.
    pub username: String,
}

/// A write token; only its hash is stored.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WriteToken {
    /// Token id.
    pub id: String,
    /// Publisher name, used as the audit attribution.
    pub name: String,
    /// Creation timestamp.
    pub created_at: i64,
    /// Last use.
    pub last_used_at: Option<i64>,
}

/// A freshly created write token, with its one-time plaintext secret.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WriteTokenCreated {
    /// Token id.
    pub id: String,
    /// Publisher name.
    pub name: String,
    /// Plaintext secret; shown once.
    pub secret: String,
    /// Creation timestamp.
    pub created_at: i64,
}

/// One row of the ingest audit trail.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IngestEvent {
    /// Event id.
    pub id: String,
    /// Dataset written to.
    pub dataset_id: String,
    /// Rows upserted.
    pub upserted: i64,
    /// Rows deleted.
    pub deleted: i64,
    /// Token id used.
    pub token_id: String,
    /// Token name used, denormalised so revoked tokens stay attributable.
    pub token_name: String,
    /// When it happened.
    pub created_at: i64,
}

/// Cold storage coordinates.
pub const META_COLD_BUCKET: &str = "cold_bucket";
/// Cold storage region key.
pub const META_COLD_REGION: &str = "cold_region";
/// Default cold bucket.
pub const DEFAULT_COLD_BUCKET: &str = "firmament-ntnl";
/// Default cold region.
pub const DEFAULT_COLD_REGION: &str = "ap-northeast-1";

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("database lock is poisoned")]
    Poisoned,
    #[error("secret encryption failed")]
    Cipher(#[from] CipherError),
    #[error("dataset state failed: {0}")]
    Files(#[source] std::io::Error),
    #[error("hot storage failed: {0}")]
    Hot(#[from] crate::hot::HotError),
    #[error("{0}")]
    Conflict(String),
}

/// The control-plane database plus the hot store it owns.
#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
    cipher: Cipher,
    database_path: Arc<PathBuf>,
    hot: HotStore,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Database {
    /// Opens Firmament's state directory: the control-plane database, the hot
    /// databases and the credential key.
    ///
    /// # Errors
    ///
    /// Returns an error when the database, its schema, the hot store or the
    /// secret cipher cannot be initialized.
    pub fn open(state_directory: &Path) -> Result<Self, DatabaseError> {
        let cipher = Cipher::load_or_create(state_directory)?;
        let database_path = state_directory.join("default.sqlite3");
        let connection = Connection::open(&database_path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS app_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);",
        )?;
        drop_legacy_linkit_settings(&connection)?;
        migrate(&connection)?;
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                spec TEXT NOT NULL,
                cadence TEXT NOT NULL,
                timezone TEXT NOT NULL,
                last_batch INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS flushes (
                id TEXT PRIMARY KEY NOT NULL,
                dataset_id TEXT NOT NULL REFERENCES datasets(id),
                batch INTEGER NOT NULL,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed')),
                files INTEGER NOT NULL DEFAULT 0,
                rows INTEGER NOT NULL DEFAULT 0,
                bytes INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                finished_at INTEGER,
                UNIQUE(dataset_id, batch)
            );
            CREATE INDEX IF NOT EXISTS flushes_dataset_id_idx ON flushes(dataset_id, batch DESC);
            CREATE TABLE IF NOT EXISTS cold_files (
                dataset_id TEXT NOT NULL REFERENCES datasets(id),
                batch INTEGER NOT NULL,
                path TEXT NOT NULL,
                partition TEXT NOT NULL,
                rows INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                min_time TEXT NOT NULL,
                max_time TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (dataset_id, path)
            );
            CREATE INDEX IF NOT EXISTS cold_files_batch_idx ON cold_files(dataset_id, batch);
            CREATE TABLE IF NOT EXISTS syncers (
                id TEXT PRIMARY KEY NOT NULL,
                owner_id TEXT NOT NULL,
                name TEXT NOT NULL,
                dataset_id TEXT NOT NULL REFERENCES datasets(id),
                last_batch INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                last_synced_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS syncers_owner_id_idx ON syncers(owner_id);
            CREATE TABLE IF NOT EXISTS linkit_settings (
                owner_id TEXT PRIMARY KEY NOT NULL,
                bot_id TEXT NOT NULL,
                bot_token_ciphertext TEXT NOT NULL,
                username TEXT NOT NULL,
                last_attempt_at INTEGER,
                last_success_at INTEGER,
                last_error TEXT,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS write_tokens (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                secret_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS ingest_events (
                id TEXT PRIMARY KEY NOT NULL,
                dataset_id TEXT NOT NULL,
                upserted INTEGER NOT NULL,
                deleted INTEGER NOT NULL,
                token_id TEXT NOT NULL,
                token_name TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ingest_events_created_at_idx ON ingest_events(created_at);
            ",
        )?;
        let hot = HotStore::new(state_directory.join("hot"))?;
        let database = Self {
            connection: Arc::new(Mutex::new(connection)),
            cipher,
            database_path: Arc::new(database_path),
            hot,
        };
        database.put_meta_if_missing(META_COLD_BUCKET, DEFAULT_COLD_BUCKET)?;
        database.put_meta_if_missing(META_COLD_REGION, DEFAULT_COLD_REGION)?;
        database.reapply_schemas()?;
        Ok(database)
    }

    /// The control-plane `SQLite` file.
    pub fn database_path(&self) -> PathBuf {
        self.database_path.as_ref().clone()
    }

    /// The hot store backing every dataset.
    pub fn hot(&self) -> &HotStore {
        &self.hot
    }

    pub fn root_user_id(&self) -> Result<Option<String>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT value FROM app_meta WHERE key = 'root_user_id'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn initialize_root_user(&self, user_id: &str) -> Result<bool, DatabaseError> {
        let changed = self.connection()?.execute(
            "INSERT INTO app_meta(key, value) VALUES ('root_user_id', ?1) ON CONFLICT(key) DO NOTHING",
            [user_id],
        )?;
        Ok(changed == 1)
    }

    /// Creates a dataset and its hot database.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema is invalid, the id is taken or the hot
    /// database cannot be created. A half-created dataset is rolled back.
    pub fn create_dataset(
        &self,
        id: &str,
        name: &str,
        description: &str,
        cooling: &CoolingSpec,
        table: &TableSpec,
    ) -> Result<Dataset, DatabaseError> {
        schema::validate(table).map_err(DatabaseError::Conflict)?;
        let spec = serde_json::to_string(table).map_err(|error| {
            DatabaseError::Files(std::io::Error::other(format!(
                "schema encoding failed: {error}"
            )))
        })?;
        let created_at = Utc::now().timestamp();
        let inserted = self.connection()?.execute(
            "INSERT INTO datasets(id, name, description, spec, cadence, timezone, last_batch, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7) ON CONFLICT(id) DO NOTHING",
            params![id, name, description, spec, cooling.cadence.as_str(), cooling.timezone, created_at],
        )?;
        if inserted == 0 {
            return Err(DatabaseError::Conflict(format!(
                "dataset {id} already exists"
            )));
        }
        if let Err(error) = self.hot.create(id, table) {
            let _ = self
                .connection()?
                .execute("DELETE FROM datasets WHERE id = ?1", [id]);
            return Err(DatabaseError::Hot(error));
        }
        Ok(Dataset {
            id: id.to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            cooling: cooling.clone(),
            last_batch: 0,
            created_at,
            table: table.clone(),
        })
    }

    pub fn list_datasets(&self) -> Result<Vec<Dataset>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, name, description, spec, cadence, timezone, last_batch, created_at FROM datasets ORDER BY created_at ASC, id ASC",
        )?;
        let datasets = statement
            .query_map([], dataset_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(datasets)
    }

    pub fn dataset(&self, id: &str) -> Result<Option<Dataset>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, name, description, spec, cadence, timezone, last_batch, created_at FROM datasets WHERE id = ?1",
                [id],
                dataset_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    /// Starts a cooling pass, allocating the next batch number.
    ///
    /// # Errors
    ///
    /// Returns an error when another pass on the same dataset is still running.
    pub fn begin_flush(&self, dataset_id: &str, trigger: &str) -> Result<Flush, DatabaseError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let running: Option<i64> = transaction
            .query_row(
                "SELECT batch FROM flushes WHERE dataset_id = ?1 AND status = 'running' LIMIT 1",
                [dataset_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(batch) = running {
            return Err(DatabaseError::Conflict(format!(
                "batch {batch} is still running"
            )));
        }
        transaction.execute(
            "UPDATE datasets SET last_batch = last_batch + 1 WHERE id = ?1",
            [dataset_id],
        )?;
        let batch: i64 = transaction.query_row(
            "SELECT last_batch FROM datasets WHERE id = ?1",
            [dataset_id],
            |row| row.get(0),
        )?;
        let flush = Flush {
            id: Uuid::new_v4().to_string(),
            dataset_id: dataset_id.to_owned(),
            batch,
            trigger: trigger.to_owned(),
            status: "running".to_owned(),
            files: 0,
            rows: 0,
            bytes: 0,
            message: String::new(),
            created_at: Utc::now().timestamp(),
            finished_at: None,
        };
        transaction.execute(
            "INSERT INTO flushes(id, dataset_id, batch, trigger, status, files, rows, bytes, message, created_at) VALUES (?1, ?2, ?3, ?4, 'running', 0, 0, 0, '', ?5)",
            params![flush.id, flush.dataset_id, flush.batch, flush.trigger, flush.created_at],
        )?;
        transaction.commit()?;
        Ok(flush)
    }

    /// Records the files of a finished batch; this is the commit point of a
    /// cooling pass.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog rows cannot be written.
    pub fn record_batch(
        &self,
        dataset_id: &str,
        batch: i64,
        files: &[ColdFile],
    ) -> Result<(), DatabaseError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for file in files {
            transaction.execute(
                "INSERT INTO cold_files(dataset_id, batch, path, partition, rows, bytes, sha256, min_time, max_time, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) ON CONFLICT(dataset_id, path) DO UPDATE SET rows = excluded.rows, bytes = excluded.bytes, sha256 = excluded.sha256, min_time = excluded.min_time, max_time = excluded.max_time",
                params![
                    dataset_id,
                    batch,
                    file.path,
                    file.partition,
                    file.rows,
                    file.bytes as i64,
                    file.sha256,
                    file.min_time,
                    file.max_time,
                    file.created_at
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Finishes a cooling pass.
    ///
    /// # Errors
    ///
    /// Returns an error when the row cannot be updated.
    pub fn finish_flush(
        &self,
        id: &str,
        status: &str,
        files: i64,
        rows: i64,
        bytes: i64,
        message: &str,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "UPDATE flushes SET status = ?2, files = ?3, rows = ?4, bytes = ?5, message = ?6, finished_at = ?7 WHERE id = ?1",
            params![id, status, files, rows, bytes, message, Utc::now().timestamp()],
        )?;
        Ok(())
    }

    pub fn latest_flush(&self, dataset_id: &str) -> Result<Option<Flush>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, dataset_id, batch, trigger, status, files, rows, bytes, message, created_at, finished_at FROM flushes WHERE dataset_id = ?1 ORDER BY batch DESC LIMIT 1",
                [dataset_id],
                flush_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn list_flushes(&self, dataset_id: &str, limit: i64) -> Result<Vec<Flush>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, dataset_id, batch, trigger, status, files, rows, bytes, message, created_at, finished_at FROM flushes WHERE dataset_id = ?1 ORDER BY batch DESC LIMIT ?2",
        )?;
        statement
            .query_map(params![dataset_id, limit], flush_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    /// Lists cold files, optionally only those newer than a mirrored batch.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog cannot be read.
    pub fn cold_files(
        &self,
        dataset_id: &str,
        since_batch: i64,
    ) -> Result<Vec<ColdFile>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT dataset_id, batch, path, partition, rows, bytes, sha256, min_time, max_time, created_at FROM cold_files WHERE dataset_id = ?1 AND batch > ?2 ORDER BY batch ASC, path ASC",
        )?;
        statement
            .query_map(params![dataset_id, since_batch], cold_file_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    /// Looks up one catalogued cold file; anything else is not downloadable.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog cannot be read.
    pub fn cold_file(
        &self,
        dataset_id: &str,
        path: &str,
    ) -> Result<Option<ColdFile>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT dataset_id, batch, path, partition, rows, bytes, sha256, min_time, max_time, created_at FROM cold_files WHERE dataset_id = ?1 AND path = ?2",
                params![dataset_id, path],
                cold_file_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn list_syncers(&self, owner_id: Option<&str>) -> Result<Vec<Syncer>, DatabaseError> {
        let connection = self.connection()?;
        let columns = "SELECT id, owner_id, name, dataset_id, last_batch, created_at, last_synced_at FROM syncers";
        match owner_id {
            Some(owner_id) => {
                let mut statement = connection.prepare(&format!(
                    "{columns} WHERE owner_id = ?1 ORDER BY created_at DESC, rowid DESC"
                ))?;
                statement
                    .query_map([owner_id], syncer_from_row)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DatabaseError::Sqlite)
            }
            None => {
                let mut statement = connection
                    .prepare(&format!("{columns} ORDER BY created_at DESC, rowid DESC"))?;
                statement
                    .query_map([], syncer_from_row)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DatabaseError::Sqlite)
            }
        }
    }

    pub fn get_syncer(&self, id: &str) -> Result<Option<Syncer>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, owner_id, name, dataset_id, last_batch, created_at, last_synced_at FROM syncers WHERE id = ?1",
                [id],
                syncer_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn create_syncer(
        &self,
        owner_id: &str,
        name: &str,
        dataset_id: &str,
    ) -> Result<Syncer, DatabaseError> {
        let syncer = Syncer {
            id: Uuid::new_v4().to_string(),
            owner_id: owner_id.to_owned(),
            name: name.to_owned(),
            dataset_id: dataset_id.to_owned(),
            last_batch: 0,
            created_at: Utc::now().timestamp(),
            last_synced_at: None,
        };
        self.connection()?.execute(
            "INSERT INTO syncers(id, owner_id, name, dataset_id, last_batch, created_at, last_synced_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
            params![syncer.id, syncer.owner_id, syncer.name, syncer.dataset_id, syncer.last_batch, syncer.created_at],
        )?;
        Ok(syncer)
    }

    pub fn delete_syncer(&self, id: &str) -> Result<bool, DatabaseError> {
        let changed = self
            .connection()?
            .execute("DELETE FROM syncers WHERE id = ?1", [id])?;
        Ok(changed == 1)
    }

    /// Records a mirrored batch on a syncer.
    ///
    /// # Errors
    ///
    /// Returns an error when the row cannot be updated.
    pub fn mark_syncer_synced(
        &self,
        id: &str,
        timestamp: i64,
        batch: i64,
    ) -> Result<Option<Syncer>, DatabaseError> {
        self.connection()?.execute(
            "UPDATE syncers SET last_synced_at = ?2, last_batch = MAX(last_batch, ?3) WHERE id = ?1",
            params![id, timestamp, batch],
        )?;
        self.get_syncer(id)
    }

    pub fn linkit_status(&self, owner_id: &str) -> Result<LinkitStatus, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT bot_id, username, last_attempt_at, last_success_at, last_error FROM linkit_settings WHERE owner_id = ?1",
                [owner_id],
                |row| {
                    Ok(LinkitStatus {
                        configured: true,
                        bot_id: Some(row.get(0)?),
                        recipient_username: Some(row.get(1)?),
                        last_attempt_at: row.get(2)?,
                        last_success_at: row.get(3)?,
                        last_error: row.get(4)?,
                    })
                },
            )
            .optional()
            .map(|stored| {
                stored.unwrap_or(LinkitStatus {
                    configured: false,
                    bot_id: None,
                    recipient_username: None,
                    last_attempt_at: None,
                    last_success_at: None,
                    last_error: None,
                })
            })
            .map_err(DatabaseError::Sqlite)
    }

    pub fn linkit_connection(
        &self,
        owner_id: &str,
    ) -> Result<Option<SecretLinkitSettings>, DatabaseError> {
        let row = self
            .connection()?
            .query_row(
                "SELECT bot_id, bot_token_ciphertext, username FROM linkit_settings WHERE owner_id = ?1",
                [owner_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(bot_id, ciphertext, username)| {
            Ok(SecretLinkitSettings {
                bot_id,
                bot_token: self.cipher.decrypt(&ciphertext)?,
                username,
            })
        })
        .transpose()
    }

    /// Stores the provisioned Bot and refreshes the stored username, keeping
    /// the delivery status untouched.
    pub fn save_linkit_bot(
        &self,
        owner_id: &str,
        bot_id: &str,
        bot_token: &str,
        username: &str,
    ) -> Result<(), DatabaseError> {
        let updated_at = Utc::now().timestamp();
        let ciphertext = self.cipher.encrypt(bot_token)?;
        self.connection()?.execute(
            "INSERT INTO linkit_settings(owner_id, bot_id, bot_token_ciphertext, username, updated_at) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(owner_id) DO UPDATE SET bot_id = excluded.bot_id, bot_token_ciphertext = excluded.bot_token_ciphertext, username = excluded.username, updated_at = excluded.updated_at",
            params![owner_id, bot_id, ciphertext, username, updated_at],
        )?;
        Ok(())
    }

    pub fn record_linkit_delivery(
        &self,
        owner_id: &str,
        error: Option<&str>,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "UPDATE linkit_settings SET last_attempt_at = ?2, last_error = ?3, last_success_at = CASE WHEN ?3 IS NULL THEN ?2 ELSE last_success_at END WHERE owner_id = ?1",
            params![owner_id, Utc::now().timestamp(), error],
        )?;
        Ok(())
    }

    pub fn meta_value(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        self.connection()?
            .query_row("SELECT value FROM app_meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn put_meta_value(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "INSERT INTO app_meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn create_write_token(
        &self,
        name: &str,
        secret_hash: &str,
    ) -> Result<WriteToken, DatabaseError> {
        let token = WriteToken {
            id: Uuid::new_v4().to_string(),
            name: name.to_owned(),
            created_at: Utc::now().timestamp(),
            last_used_at: None,
        };
        self.connection()?.execute(
            "INSERT INTO write_tokens(id, name, secret_hash, created_at, last_used_at) VALUES (?1, ?2, ?3, ?4, NULL)",
            params![token.id, token.name, secret_hash, token.created_at],
        )?;
        Ok(token)
    }

    pub fn list_write_tokens(&self) -> Result<Vec<WriteToken>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, name, created_at, last_used_at FROM write_tokens ORDER BY created_at DESC, rowid DESC",
        )?;
        statement
            .query_map([], write_token_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn write_token_by_secret_hash(
        &self,
        secret_hash: &str,
    ) -> Result<Option<WriteToken>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, name, created_at, last_used_at FROM write_tokens WHERE secret_hash = ?1",
                [secret_hash],
                write_token_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn touch_write_token(&self, id: &str, timestamp: i64) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "UPDATE write_tokens SET last_used_at = ?2 WHERE id = ?1",
            params![id, timestamp],
        )?;
        Ok(())
    }

    pub fn delete_write_token(&self, id: &str) -> Result<bool, DatabaseError> {
        let changed = self
            .connection()?
            .execute("DELETE FROM write_tokens WHERE id = ?1", [id])?;
        Ok(changed == 1)
    }

    /// Records one ingest in the audit trail; the token name is denormalized so
    /// revoked tokens keep their attribution.
    ///
    /// # Errors
    ///
    /// Returns an error when the event cannot be written.
    pub fn record_ingest_event(
        &self,
        dataset_id: &str,
        upserted: i64,
        deleted: i64,
        token_id: &str,
        token_name: &str,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "INSERT INTO ingest_events(id, dataset_id, upserted, deleted, token_id, token_name, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![Uuid::new_v4().to_string(), dataset_id, upserted, deleted, token_id, token_name, Utc::now().timestamp()],
        )?;
        Ok(())
    }

    pub fn list_ingest_events(&self, limit: i64) -> Result<Vec<IngestEvent>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, dataset_id, upserted, deleted, token_id, token_name, created_at FROM ingest_events ORDER BY created_at DESC, rowid DESC LIMIT ?1",
        )?;
        statement
            .query_map([limit], ingest_event_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    /// Summarises the cold tree of a dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog cannot be read.
    pub fn cold_summary(&self, dataset_id: &str) -> Result<ColdSummary, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(bytes), 0), MIN(min_time), MAX(max_time) FROM cold_files WHERE dataset_id = ?1",
                [dataset_id],
                |row| {
                    Ok(ColdSummary {
                        files: row.get(0)?,
                        bytes: row.get::<_, i64>(1)?.max(0) as u64,
                        earliest_time: row.get(2)?,
                        latest_time: row.get(3)?,
                    })
                },
            )
            .map_err(DatabaseError::Sqlite)
    }

    fn reapply_schemas(&self) -> Result<(), DatabaseError> {
        for dataset in self.list_datasets()? {
            self.hot.apply_schema(&dataset.id, &dataset.table)?;
        }
        Ok(())
    }

    fn put_meta_if_missing(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "INSERT INTO app_meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
            params![key, value],
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, DatabaseError> {
        self.connection.lock().map_err(|_| DatabaseError::Poisoned)
    }
}

fn dataset_from_row(row: &Row<'_>) -> rusqlite::Result<Dataset> {
    let spec: String = row.get(3)?;
    let table = serde_json::from_str(&spec).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(error.to_string())),
        )
    })?;
    let cadence: String = row.get(4)?;
    Ok(Dataset {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        table,
        cooling: CoolingSpec {
            cadence: Cadence::parse(&cadence).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::other(format!("unknown cadence {cadence}"))),
                )
            })?,
            timezone: row.get(5)?,
        },
        last_batch: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn syncer_from_row(row: &Row<'_>) -> rusqlite::Result<Syncer> {
    Ok(Syncer {
        id: row.get(0)?,
        owner_id: row.get(1)?,
        name: row.get(2)?,
        dataset_id: row.get(3)?,
        last_batch: row.get(4)?,
        created_at: row.get(5)?,
        last_synced_at: row.get(6)?,
    })
}

fn write_token_from_row(row: &Row<'_>) -> rusqlite::Result<WriteToken> {
    Ok(WriteToken {
        id: row.get(0)?,
        name: row.get(1)?,
        created_at: row.get(2)?,
        last_used_at: row.get(3)?,
    })
}

fn ingest_event_from_row(row: &Row<'_>) -> rusqlite::Result<IngestEvent> {
    Ok(IngestEvent {
        id: row.get(0)?,
        dataset_id: row.get(1)?,
        upserted: row.get(2)?,
        deleted: row.get(3)?,
        token_id: row.get(4)?,
        token_name: row.get(5)?,
        created_at: row.get(6)?,
    })
}

fn flush_from_row(row: &Row<'_>) -> rusqlite::Result<Flush> {
    Ok(Flush {
        id: row.get(0)?,
        dataset_id: row.get(1)?,
        batch: row.get(2)?,
        trigger: row.get(3)?,
        status: row.get(4)?,
        files: row.get(5)?,
        rows: row.get(6)?,
        bytes: row.get(7)?,
        message: row.get(8)?,
        created_at: row.get(9)?,
        finished_at: row.get(10)?,
    })
}

fn cold_file_from_row(row: &Row<'_>) -> rusqlite::Result<ColdFile> {
    Ok(ColdFile {
        dataset_id: row.get(0)?,
        batch: row.get(1)?,
        path: row.get(2)?,
        partition: row.get(3)?,
        rows: row.get(4)?,
        bytes: row.get::<_, i64>(5)?.max(0) as u64,
        sha256: row.get(6)?,
        min_time: row.get(7)?,
        max_time: row.get(8)?,
        created_at: row.get(9)?,
    })
}

// The data model changed shape with the hot/cold rewrite: a dataset used to be
// a directory of arbitrary files and is now a declared schema. Datasets,
// syncers and the old cold catalog are dropped rather than migrated; tokens and
// the Linkit connection survive because their meaning did not change.
fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    let version: Option<String> = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if version.as_deref() != Some(SCHEMA_VERSION) {
        connection.execute_batch(
            "DROP TABLE IF EXISTS archive_jobs;
             DROP TABLE IF EXISTS publish_events;
             DROP TABLE IF EXISTS cold_files;
             DROP TABLE IF EXISTS syncers;
             DROP TABLE IF EXISTS datasets;",
        )?;
        connection.execute(
            "INSERT INTO app_meta(key, value) VALUES ('schema_version', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [SCHEMA_VERSION],
        )?;
    }
    Ok(())
}

// The pre-ensure table stored a manually configured Bot. The connection is now
// provisioned automatically and is rebuilt by the next `ensure` call, so the
// legacy table is dropped instead of migrated.
fn drop_legacy_linkit_settings(connection: &Connection) -> rusqlite::Result<()> {
    let legacy = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('linkit_settings') WHERE name = 'recipient_username')",
        [],
        |row| row.get::<_, i64>(0).map(|exists| exists != 0),
    )?;
    if legacy {
        connection.execute_batch("DROP TABLE linkit_settings;")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::Database;
    use crate::cooling::{Cadence, CoolingSpec};
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec};

    fn daily() -> CoolingSpec {
        CoolingSpec {
            cadence: Cadence::Daily,
            timezone: "Asia/Shanghai".to_owned(),
        }
    }

    fn column(name: &str, column_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            column_type,
            nullable: false,
        }
    }

    fn bars() -> TableSpec {
        TableSpec {
            primary_key: vec!["symbol".to_owned(), "updated_at".to_owned()],
            time_column: "updated_at".to_owned(),
            granularity: Granularity::Day,
            columns: vec![
                column("symbol", ColumnType::Text),
                column("updated_at", ColumnType::Timestamp),
                column("close", ColumnType::Real),
            ],
        }
    }

    #[test]
    fn datasets_carry_their_declared_schema() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        assert!(database.list_datasets().expect("list").is_empty());

        let dataset = database
            .create_dataset("bars", "Bars", "test", &daily(), &bars())
            .expect("dataset creates");
        assert_eq!(dataset.cooling.timezone, "Asia/Shanghai");
        assert_eq!(dataset.last_batch, 0);
        assert!(
            database
                .create_dataset("bars", "Bars", "test", &daily(), &bars())
                .is_err(),
            "ids are unique"
        );

        let stored = database.dataset("bars").expect("read").expect("exists");
        assert_eq!(stored.table.granularity, Granularity::Day);
    }

    #[test]
    fn batch_numbers_are_allocated_per_dataset_and_serialized() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        database
            .create_dataset("bars", "Bars", "", &daily(), &bars())
            .expect("dataset creates");

        let first = database.begin_flush("bars", "schedule").expect("first");
        assert_eq!(first.batch, 1);
        assert!(
            database.begin_flush("bars", "manual").is_err(),
            "one running pass per dataset"
        );
        database
            .finish_flush(&first.id, "completed", 1, 10, 100, "")
            .expect("finish");
        let second = database.begin_flush("bars", "manual").expect("second");
        assert_eq!(second.batch, 2);

        let flushes = database.list_flushes("bars", 10).expect("list");
        assert_eq!(flushes.len(), 2);
        assert_eq!(flushes[0].batch, 2);
        assert_eq!(flushes[0].status, "running");
        database
            .finish_flush(&second.id, "failed", 0, 0, 0, "boom")
            .expect("fail");
        assert_eq!(
            database
                .latest_flush("bars")
                .expect("latest")
                .expect("exists")
                .status,
            "failed"
        );
    }

    #[test]
    fn catalogue_supports_incremental_reads() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        database
            .create_dataset("bars", "Bars", "", &daily(), &bars())
            .expect("dataset creates");
        let file = super::ColdFile {
            dataset_id: "bars".to_owned(),
            batch: 1,
            path: "day=2026-10-01/000001-0000.parquet".to_owned(),
            partition: "day=2026-10-01".to_owned(),
            rows: 3,
            bytes: 42,
            sha256: "abc".to_owned(),
            min_time: "2026-10-01T00:00:00.000000Z".to_owned(),
            max_time: "2026-10-01T00:02:00.000000Z".to_owned(),
            created_at: 1,
        };
        database
            .record_batch("bars", 1, std::slice::from_ref(&file))
            .expect("records");

        assert_eq!(database.cold_files("bars", 0).expect("all").len(), 1);
        assert!(
            database
                .cold_files("bars", 1)
                .expect("incremental")
                .is_empty(),
            "since is exclusive"
        );
        assert_eq!(
            database
                .cold_file("bars", "day=2026-10-01/000001-0000.parquet")
                .expect("lookup")
                .expect("exists")
                .rows,
            3
        );
        assert!(
            database
                .cold_file("bars", "nope.parquet")
                .expect("lookup")
                .is_none(),
            "only catalogued paths resolve"
        );
    }

    #[test]
    fn invalid_schemas_are_rejected_before_the_hot_database_exists() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        let mut broken = bars();
        broken.primary_key.clear();
        assert!(
            database
                .create_dataset("bars", "Bars", "", &daily(), &broken)
                .is_err()
        );
        assert!(database.list_datasets().expect("list").is_empty());
        assert!(!state.path().join("hot/bars.sqlite3").exists());
    }

    #[test]
    fn opening_a_database_with_the_legacy_linkit_table_rebuilds_it() {
        let state = TempDir::new().expect("state directory");
        {
            let database = Database::open(state.path()).expect("database opens");
            database
                .connection()
                .expect("connection")
                .execute_batch(
                    "DROP TABLE linkit_settings;
                     CREATE TABLE linkit_settings (
                         owner_id TEXT PRIMARY KEY NOT NULL,
                         recipient_username TEXT NOT NULL,
                         bot_token_ciphertext TEXT NOT NULL,
                         updated_at INTEGER NOT NULL
                     );
                     INSERT INTO linkit_settings(owner_id, recipient_username, bot_token_ciphertext, updated_at)
                     VALUES ('owner', 'alice', 'legacy-ciphertext', 0);",
                )
                .expect("legacy table");
        }

        let database = Database::open(state.path()).expect("database reopens");
        database
            .save_linkit_bot("owner", "bot-1", "sk-token", "alice")
            .expect("connection saves");
        let status = database.linkit_status("owner").expect("status");
        assert!(status.configured);
        assert_eq!(status.bot_id.as_deref(), Some("bot-1"));
        assert_eq!(status.recipient_username.as_deref(), Some("alice"));
    }
}

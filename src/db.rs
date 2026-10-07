use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::crypto::{Cipher, CipherError};

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
    cipher: Cipher,
    database_path: Arc<PathBuf>,
    datasets_directory: Arc<PathBuf>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Database").finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("database lock is poisoned")]
    Poisoned,
    #[error("secret encryption failed")]
    Cipher(#[from] CipherError),
    #[error("dataset directory operation failed")]
    Files(#[source] std::io::Error),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tier: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Syncer {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub dataset_id: String,
    pub subdir: String,
    pub created_at: i64,
    pub last_synced_at: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LinkitSettings {
    pub owner_id: String,
    pub recipient_username: String,
    pub configured: bool,
    pub updated_at: i64,
}

#[derive(Clone, Debug)]
pub struct SecretLinkitSettings {
    pub recipient_username: String,
    pub bot_token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColdFile {
    pub dataset_id: String,
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub source_path: String,
    pub archived_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArchiveJob {
    pub id: String,
    pub dataset_id: String,
    pub status: String,
    pub total_files: i64,
    pub archived_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
    pub message: String,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ArchiveCounts {
    pub total_files: i64,
    pub archived_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
}

const DEMO_DATASET: &str = "demo";
const DEMO_DESCRIPTION: &str = "示例数据集：一段合成的行情样本，用来体验导出与同步流程。";
const DEMO_README: &str = include_str!("../samples/demo/README.md");
const DEMO_MARKET_CSV: &str = include_str!("../samples/demo/market-sample.csv");

pub const META_COLD_BUCKET: &str = "cold_bucket";
pub const META_COLD_REGION: &str = "cold_region";
pub const DEFAULT_COLD_BUCKET: &str = "firmament-ntnl";
pub const DEFAULT_COLD_REGION: &str = "ap-northeast-1";

impl Database {
    /// Opens Firmament's `SQLite` database and forces WAL mode before serving requests.
    ///
    /// # Errors
    ///
    /// Returns an error when the database, schema or secret cipher cannot be initialized.
    pub fn open(state_directory: &Path) -> Result<Self, DatabaseError> {
        let cipher = Cipher::load_or_create(state_directory)?;
        let database_path = state_directory.join("default.sqlite3");
        let datasets_directory = state_directory.join("datasets");
        let connection = Connection::open(&database_path)?;
        connection.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                tier TEXT NOT NULL CHECK(tier IN ('hot', 'cold')),
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS syncers (
                id TEXT PRIMARY KEY NOT NULL,
                owner_id TEXT NOT NULL,
                name TEXT NOT NULL,
                dataset_id TEXT NOT NULL REFERENCES datasets(id),
                subdir TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                last_synced_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS syncers_owner_id_idx ON syncers(owner_id);
            CREATE TABLE IF NOT EXISTS linkit_settings (
                owner_id TEXT PRIMARY KEY NOT NULL,
                recipient_username TEXT NOT NULL,
                bot_token_ciphertext TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cold_files (
                dataset_id TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                source_path TEXT NOT NULL,
                archived_at INTEGER NOT NULL,
                PRIMARY KEY (dataset_id, path)
            );
            CREATE TABLE IF NOT EXISTS archive_jobs (
                id TEXT PRIMARY KEY NOT NULL,
                dataset_id TEXT NOT NULL REFERENCES datasets(id),
                status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed')),
                total_files INTEGER NOT NULL DEFAULT 0,
                archived_files INTEGER NOT NULL DEFAULT 0,
                skipped_files INTEGER NOT NULL DEFAULT 0,
                failed_files INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                finished_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS archive_jobs_dataset_id_idx ON archive_jobs(dataset_id);
            ",
        )?;
        let database = Self {
            connection: Arc::new(Mutex::new(connection)),
            cipher,
            database_path: Arc::new(database_path),
            datasets_directory: Arc::new(datasets_directory),
        };
        database.seed_demo_dataset()?;
        database.put_meta_if_missing(META_COLD_BUCKET, DEFAULT_COLD_BUCKET)?;
        database.put_meta_if_missing(META_COLD_REGION, DEFAULT_COLD_REGION)?;
        Ok(database)
    }

    pub fn database_path(&self) -> PathBuf {
        self.database_path.as_ref().clone()
    }

    pub fn dataset_directory(&self, dataset_id: &str) -> PathBuf {
        self.datasets_directory.join(dataset_id)
    }

    fn seed_demo_dataset(&self) -> Result<(), DatabaseError> {
        let directory = self.dataset_directory(DEMO_DATASET);
        std::fs::create_dir_all(&directory).map_err(DatabaseError::Files)?;
        write_if_missing(&directory.join("README.md"), DEMO_README)?;
        write_if_missing(&directory.join("market-sample.csv"), DEMO_MARKET_CSV)?;
        self.connection()?.execute(
            "INSERT INTO datasets(id, name, description, tier, created_at) VALUES (?1, ?2, ?3, 'hot', ?4) ON CONFLICT(id) DO NOTHING",
            params![DEMO_DATASET, "Demo", DEMO_DESCRIPTION, Utc::now().timestamp()],
        )?;
        Ok(())
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

    pub fn list_datasets(&self) -> Result<Vec<Dataset>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT id, name, description, tier, created_at FROM datasets ORDER BY id")?;
        statement
            .query_map([], dataset_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn dataset(&self, id: &str) -> Result<Option<Dataset>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, name, description, tier, created_at FROM datasets WHERE id = ?1",
                [id],
                dataset_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn list_syncers(&self, owner_id: Option<&str>) -> Result<Vec<Syncer>, DatabaseError> {
        let connection = self.connection()?;
        let columns = "SELECT id, owner_id, name, dataset_id, subdir, created_at, last_synced_at FROM syncers";
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
                "SELECT id, owner_id, name, dataset_id, subdir, created_at, last_synced_at FROM syncers WHERE id = ?1",
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
        subdir: &str,
    ) -> Result<Syncer, DatabaseError> {
        let syncer = Syncer {
            id: Uuid::new_v4().to_string(),
            owner_id: owner_id.to_owned(),
            name: name.to_owned(),
            dataset_id: dataset_id.to_owned(),
            subdir: subdir.to_owned(),
            created_at: Utc::now().timestamp(),
            last_synced_at: None,
        };
        self.connection()?.execute(
            "INSERT INTO syncers(id, owner_id, name, dataset_id, subdir, created_at, last_synced_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
            params![syncer.id, syncer.owner_id, syncer.name, syncer.dataset_id, syncer.subdir, syncer.created_at],
        )?;
        Ok(syncer)
    }

    pub fn delete_syncer(&self, id: &str) -> Result<bool, DatabaseError> {
        let changed = self
            .connection()?
            .execute("DELETE FROM syncers WHERE id = ?1", [id])?;
        Ok(changed == 1)
    }

    pub fn mark_syncer_synced(
        &self,
        id: &str,
        timestamp: i64,
    ) -> Result<Option<Syncer>, DatabaseError> {
        self.connection()?.execute(
            "UPDATE syncers SET last_synced_at = ?2 WHERE id = ?1",
            params![id, timestamp],
        )?;
        self.get_syncer(id)
    }

    pub fn put_linkit_settings(
        &self,
        owner_id: &str,
        recipient_username: &str,
        bot_token: &str,
    ) -> Result<LinkitSettings, DatabaseError> {
        let updated_at = Utc::now().timestamp();
        let ciphertext = self.cipher.encrypt(bot_token)?;
        self.connection()?.execute(
            "INSERT INTO linkit_settings(owner_id, recipient_username, bot_token_ciphertext, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(owner_id) DO UPDATE SET recipient_username = excluded.recipient_username, bot_token_ciphertext = excluded.bot_token_ciphertext, updated_at = excluded.updated_at",
            params![owner_id, recipient_username, ciphertext, updated_at],
        )?;
        Ok(LinkitSettings {
            owner_id: owner_id.to_owned(),
            recipient_username: recipient_username.to_owned(),
            configured: true,
            updated_at,
        })
    }

    pub fn linkit_settings(&self, owner_id: &str) -> Result<Option<LinkitSettings>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT owner_id, recipient_username, updated_at FROM linkit_settings WHERE owner_id = ?1",
                [owner_id],
                |row| {
                    Ok(LinkitSettings {
                        owner_id: row.get(0)?,
                        recipient_username: row.get(1)?,
                        configured: true,
                        updated_at: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn secret_linkit_settings(
        &self,
        owner_id: &str,
    ) -> Result<Option<SecretLinkitSettings>, DatabaseError> {
        let row = self
            .connection()?
            .query_row(
                "SELECT recipient_username, bot_token_ciphertext FROM linkit_settings WHERE owner_id = ?1",
                [owner_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        row.map(|(recipient_username, ciphertext)| {
            Ok(SecretLinkitSettings {
                recipient_username,
                bot_token: self.cipher.decrypt(&ciphertext)?,
            })
        })
        .transpose()
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

    fn put_meta_if_missing(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "INSERT INTO app_meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn list_cold_files(&self, dataset_id: &str) -> Result<Vec<ColdFile>, DatabaseError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT dataset_id, path, size, sha256, source_path, archived_at FROM cold_files WHERE dataset_id = ?1 ORDER BY path",
        )?;
        statement
            .query_map([dataset_id], cold_file_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn cold_file(
        &self,
        dataset_id: &str,
        path: &str,
    ) -> Result<Option<ColdFile>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT dataset_id, path, size, sha256, source_path, archived_at FROM cold_files WHERE dataset_id = ?1 AND path = ?2",
                params![dataset_id, path],
                cold_file_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    pub fn upsert_cold_file(
        &self,
        dataset_id: &str,
        path: &str,
        size: u64,
        sha256: &str,
        source_path: &str,
        archived_at: i64,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "INSERT INTO cold_files(dataset_id, path, size, sha256, source_path, archived_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(dataset_id, path) DO UPDATE SET size = excluded.size, sha256 = excluded.sha256, source_path = excluded.source_path, archived_at = excluded.archived_at",
            params![dataset_id, path, size as i64, sha256, source_path, archived_at],
        )?;
        Ok(())
    }

    pub fn create_archive_job_if_idle(
        &self,
        dataset_id: &str,
    ) -> Result<Option<ArchiveJob>, DatabaseError> {
        let connection = self.connection()?;
        let running: Option<String> = connection
            .query_row(
                "SELECT id FROM archive_jobs WHERE dataset_id = ?1 AND status = 'running' LIMIT 1",
                [dataset_id],
                |row| row.get(0),
            )
            .optional()?;
        if running.is_some() {
            return Ok(None);
        }
        let job = ArchiveJob {
            id: Uuid::new_v4().to_string(),
            dataset_id: dataset_id.to_owned(),
            status: "running".to_owned(),
            total_files: 0,
            archived_files: 0,
            skipped_files: 0,
            failed_files: 0,
            message: String::new(),
            created_at: Utc::now().timestamp(),
            finished_at: None,
        };
        connection.execute(
            "INSERT INTO archive_jobs(id, dataset_id, status, total_files, archived_files, skipped_files, failed_files, message, created_at, finished_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
            params![job.id, job.dataset_id, job.status, job.total_files, job.archived_files, job.skipped_files, job.failed_files, job.message, job.created_at],
        )?;
        Ok(Some(job))
    }

    pub fn update_archive_job_progress(
        &self,
        id: &str,
        counts: ArchiveCounts,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "UPDATE archive_jobs SET total_files = ?2, archived_files = ?3, skipped_files = ?4, failed_files = ?5 WHERE id = ?1",
            params![id, counts.total_files, counts.archived_files, counts.skipped_files, counts.failed_files],
        )?;
        Ok(())
    }

    pub fn finish_archive_job(
        &self,
        id: &str,
        status: &str,
        counts: ArchiveCounts,
        message: &str,
    ) -> Result<(), DatabaseError> {
        self.connection()?.execute(
            "UPDATE archive_jobs SET status = ?2, total_files = ?3, archived_files = ?4, skipped_files = ?5, failed_files = ?6, message = ?7, finished_at = ?8 WHERE id = ?1",
            params![id, status, counts.total_files, counts.archived_files, counts.skipped_files, counts.failed_files, message, Utc::now().timestamp()],
        )?;
        Ok(())
    }

    pub fn latest_archive_job(
        &self,
        dataset_id: &str,
    ) -> Result<Option<ArchiveJob>, DatabaseError> {
        self.connection()?
            .query_row(
                "SELECT id, dataset_id, status, total_files, archived_files, skipped_files, failed_files, message, created_at, finished_at FROM archive_jobs WHERE dataset_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                [dataset_id],
                archive_job_from_row,
            )
            .optional()
            .map_err(DatabaseError::Sqlite)
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, DatabaseError> {
        self.connection.lock().map_err(|_| DatabaseError::Poisoned)
    }
}

fn dataset_from_row(row: &Row<'_>) -> rusqlite::Result<Dataset> {
    Ok(Dataset {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        tier: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn syncer_from_row(row: &Row<'_>) -> rusqlite::Result<Syncer> {
    Ok(Syncer {
        id: row.get(0)?,
        owner_id: row.get(1)?,
        name: row.get(2)?,
        dataset_id: row.get(3)?,
        subdir: row.get(4)?,
        created_at: row.get(5)?,
        last_synced_at: row.get(6)?,
    })
}

fn cold_file_from_row(row: &Row<'_>) -> rusqlite::Result<ColdFile> {
    Ok(ColdFile {
        dataset_id: row.get(0)?,
        path: row.get(1)?,
        size: row.get::<_, i64>(2)?.max(0) as u64,
        sha256: row.get(3)?,
        source_path: row.get(4)?,
        archived_at: row.get(5)?,
    })
}

fn archive_job_from_row(row: &Row<'_>) -> rusqlite::Result<ArchiveJob> {
    Ok(ArchiveJob {
        id: row.get(0)?,
        dataset_id: row.get(1)?,
        status: row.get(2)?,
        total_files: row.get(3)?,
        archived_files: row.get(4)?,
        skipped_files: row.get(5)?,
        failed_files: row.get(6)?,
        message: row.get(7)?,
        created_at: row.get(8)?,
        finished_at: row.get(9)?,
    })
}

fn write_if_missing(path: &Path, contents: &str) -> Result<(), DatabaseError> {
    if path.exists() {
        return Ok(());
    }
    std::fs::write(path, contents).map_err(DatabaseError::Files)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::Database;

    #[test]
    fn seeds_demo_dataset_and_initializes_root_user() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");

        let datasets = database.list_datasets().expect("datasets list");
        assert!(datasets.iter().any(|dataset| dataset.id == "demo"));
        assert!(
            database
                .dataset_directory("demo")
                .join("market-sample.csv")
                .exists()
        );

        assert!(database.root_user_id().expect("root user").is_none());
        database
            .initialize_root_user("user-1")
            .expect("root user initializes");
        assert_eq!(
            database.root_user_id().expect("root user").as_deref(),
            Some("user-1")
        );
    }

    #[test]
    fn syncers_round_trip() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");

        let syncer = database
            .create_syncer("user-1", "research", "demo", "data/")
            .expect("syncer creates");
        assert_eq!(
            database.list_syncers(Some("user-1")).expect("list").len(),
            1
        );
        assert!(database.get_syncer(&syncer.id).expect("get").is_some());
        let marked = database
            .mark_syncer_synced(&syncer.id, 42)
            .expect("mark synced")
            .expect("syncer exists");
        assert_eq!(marked.last_synced_at, Some(42));
        assert!(database.delete_syncer(&syncer.id).expect("delete"));
    }

    #[test]
    fn cold_files_and_archive_jobs_round_trip() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");

        assert_eq!(
            database
                .meta_value(super::META_COLD_BUCKET)
                .expect("meta")
                .as_deref(),
            Some(super::DEFAULT_COLD_BUCKET)
        );

        database
            .upsert_cold_file(
                "demo",
                "market-sample.parquet",
                128,
                "abc",
                "market-sample.csv",
                7,
            )
            .expect("cold file upserts");
        let cold = database
            .cold_file("demo", "market-sample.parquet")
            .expect("cold file")
            .expect("exists");
        assert_eq!(cold.size, 128);
        assert_eq!(database.list_cold_files("demo").expect("list").len(), 1);

        let job = database
            .create_archive_job_if_idle("demo")
            .expect("job creates")
            .expect("idle");
        assert!(
            database
                .create_archive_job_if_idle("demo")
                .expect("job checks")
                .is_none(),
            "a running job blocks new ones"
        );
        database
            .update_archive_job_progress(
                &job.id,
                super::ArchiveCounts {
                    total_files: 2,
                    archived_files: 1,
                    ..super::ArchiveCounts::default()
                },
            )
            .expect("progress");
        database
            .finish_archive_job(
                &job.id,
                "completed",
                super::ArchiveCounts {
                    total_files: 2,
                    archived_files: 1,
                    skipped_files: 1,
                    failed_files: 0,
                },
                "",
            )
            .expect("finish");
        let latest = database
            .latest_archive_job("demo")
            .expect("latest")
            .expect("exists");
        assert_eq!(latest.status, "completed");
        assert_eq!(latest.archived_files, 1);
        assert!(latest.finished_at.is_some());
    }
}

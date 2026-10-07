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

const DEMO_DATASET: &str = "demo";
const DEMO_DESCRIPTION: &str = "示例数据集：一段合成的行情样本，用来体验导出与同步流程。";
const DEMO_README: &str = include_str!("../samples/demo/README.md");
const DEMO_MARKET_CSV: &str = include_str!("../samples/demo/market-sample.csv");

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
            ",
        )?;
        let database = Self {
            connection: Arc::new(Mutex::new(connection)),
            cipher,
            database_path: Arc::new(database_path),
            datasets_directory: Arc::new(datasets_directory),
        };
        database.seed_demo_dataset()?;
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
}

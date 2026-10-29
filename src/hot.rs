//! The hot side of a dataset: one writable `SQLite` database per dataset.
//!
//! The hot database holds the window that is still mutable. Rows leave it the
//! moment they cool past the dataset's hot window: they are exported to cold
//! Parquet first, then deleted here. Dataset ids are validated identifiers, so
//! they are also safe path segments.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, params_from_iter};
use serde::Serialize;
use serde_json::{Map, Value as Json};

use crate::schema::{self, ColumnSpec, TableSpec, Value};

/// Per-table state of the hot window.
#[derive(Clone, Debug, Serialize)]
pub struct TableStat {
    /// Table name.
    pub table: String,
    /// Rows currently in the hot window.
    pub rows: i64,
    /// Rows old enough to be flushed on the next cooling pass.
    pub coolable_rows: i64,
    /// Oldest timestamp held in hot, if any.
    pub oldest_time: Option<String>,
    /// Newest timestamp held in hot, if any.
    pub newest_time: Option<String>,
}

/// What one ingest call changed.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct IngestCounts {
    /// Rows inserted or replaced.
    pub upserted: i64,
    /// Rows removed from the hot window.
    pub deleted: i64,
}

/// One row read back from the hot window.
#[derive(Clone, Debug)]
pub struct HotRow {
    /// Cell values in declared column order.
    pub values: Vec<Value>,
    /// Partition bucket the row belongs to in cold storage.
    pub bucket: String,
}

#[derive(Debug, thiserror::Error)]
pub enum HotError {
    #[error("hot database operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("dataset state directory failed: {0}")]
    Files(#[source] std::io::Error),
    #[error("hot database lock is poisoned")]
    Poisoned,
    #[error("{0}")]
    Invalid(String),
}

/// The writable half of every dataset.
#[derive(Clone)]
pub struct HotStore {
    directory: Arc<PathBuf>,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<Connection>>>>>,
}

impl std::fmt::Debug for HotStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("HotStore").finish_non_exhaustive()
    }
}

impl HotStore {
    /// Opens (or creates) the directory holding every hot database.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created.
    pub fn new(directory: PathBuf) -> Result<Self, HotError> {
        std::fs::create_dir_all(&directory).map_err(HotError::Files)?;
        Ok(Self {
            directory: Arc::new(directory),
            connections: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The `SQLite` file backing one dataset.
    pub fn database_path(&self, dataset_id: &str) -> PathBuf {
        self.directory.join(format!("{dataset_id}.sqlite3"))
    }

    /// Creates the hot database of a new dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when the file or its tables cannot be created.
    pub fn create(&self, dataset_id: &str, tables: &[TableSpec]) -> Result<(), HotError> {
        self.apply_schema(dataset_id, tables)
    }

    /// Creates any missing table of a dataset, leaving existing data untouched.
    ///
    /// # Errors
    ///
    /// Returns an error when the hot database cannot be opened or written.
    pub fn apply_schema(&self, dataset_id: &str, tables: &[TableSpec]) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        for statement in schema::create_statements(tables) {
            guard.execute_batch(&statement)?;
        }
        Ok(())
    }

    /// Runs one ingest batch: keyed upserts plus explicit keyed deletes.
    ///
    /// # Errors
    ///
    /// Returns an error when a row does not match the declared schema or the
    /// transaction cannot be committed.
    pub fn ingest(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        upsert: &[Map<String, Json>],
        delete: &[Map<String, Json>],
    ) -> Result<IngestCounts, HotError> {
        let key_columns = table.key_columns().map_err(HotError::Invalid)?;
        let mut rows = Vec::with_capacity(upsert.len());
        for row in upsert {
            let mut values = Vec::with_capacity(table.columns.len());
            for column in &table.columns {
                values.push(
                    schema::coerce(column, row.get(&column.name)).map_err(HotError::Invalid)?,
                );
            }
            rows.push(values);
        }
        let mut keys = Vec::with_capacity(delete.len());
        for row in delete {
            let mut values = Vec::with_capacity(key_columns.len());
            for column in &key_columns {
                let value =
                    schema::coerce(column, row.get(&column.name)).map_err(HotError::Invalid)?;
                values.push(value);
            }
            keys.push(values);
        }

        let connection = self.connection(dataset_id)?;
        let mut guard = lock(&connection)?;
        let transaction = guard.transaction()?;
        let mut counts = IngestCounts::default();
        if !rows.is_empty() {
            let names = table
                .columns
                .iter()
                .map(|column| format!("\"{}\"", column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = (1..=table.columns.len() + 1)
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let updates = table
                .columns
                .iter()
                .map(|column| format!("\"{}\" = excluded.\"{}\"", column.name, column.name))
                .collect::<Vec<_>>();
            let key = table
                .primary_key
                .iter()
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let statement = format!(
                "INSERT INTO \"{}\" (\"_flush\", {names}) VALUES ({placeholders}) ON CONFLICT ({key}) DO UPDATE SET {}, \"_flush\" = NULL",
                table.name,
                updates.join(", ")
            );
            let mut prepared = transaction.prepare(&statement)?;
            for values in rows {
                let mut parameters = Vec::with_capacity(values.len() + 1);
                parameters.push(Value::Null.to_sql());
                parameters.extend(values.iter().map(Value::to_sql));
                prepared.execute(params_from_iter(parameters.iter()))?;
                counts.upserted += 1;
            }
        }
        if !keys.is_empty() {
            let predicate = table
                .primary_key
                .iter()
                .enumerate()
                .map(|(index, name)| format!("\"{name}\" = ?{}", index + 1))
                .collect::<Vec<_>>()
                .join(" AND ");
            let statement = format!("DELETE FROM \"{}\" WHERE {predicate}", table.name);
            let mut prepared = transaction.prepare(&statement)?;
            for values in keys {
                if prepared.execute(params_from_iter(values.iter().map(Value::to_sql)))? > 0 {
                    counts.deleted += 1;
                }
            }
        }
        transaction.commit()?;
        Ok(counts)
    }

    /// Sums up the hot window of every table of a dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when a hot table cannot be read.
    pub fn stats(
        &self,
        dataset_id: &str,
        tables: &[TableSpec],
        boundaries: &HashMap<String, String>,
    ) -> Result<Vec<TableStat>, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let mut stats = Vec::with_capacity(tables.len());
        for table in tables {
            let time = format!("\"{}\"", table.time_column);
            let (rows, oldest, newest): (i64, Option<String>, Option<String>) = guard.query_row(
                &format!(
                    "SELECT COUNT(*), MIN({time}), MAX({time}) FROM \"{}\"",
                    table.name
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let coolable_rows = match boundaries.get(&table.name) {
                Some(boundary) => guard.query_row(
                    &format!(
                        "SELECT COUNT(*) FROM \"{}\" WHERE \"_flush\" IS NULL AND {time} < ?1",
                        table.name
                    ),
                    [boundary],
                    |row| row.get(0),
                )?,
                None => 0,
            };
            stats.push(TableStat {
                table: table.name.clone(),
                rows,
                coolable_rows,
                oldest_time: oldest,
                newest_time: newest,
            });
        }
        Ok(stats)
    }

    /// Counts rows of one table that crossed the cooling boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the table cannot be read.
    pub fn coolable_rows(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        boundary: &str,
    ) -> Result<i64, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM \"{}\" WHERE \"_flush\" IS NULL AND \"{}\" < ?1",
                    table.name, table.time_column
                ),
                [boundary],
                |row| row.get(0),
            )
            .map_err(HotError::Sqlite)
    }

    /// Reads the hot window of one table, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error when the table cannot be read.
    pub fn rows(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        since: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Vec<Value>>, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let names = table
            .columns
            .iter()
            .map(|column| format!("\"{}\"", column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!(
            "SELECT {names} FROM \"{}\" WHERE ?1 IS NULL OR \"{}\" >= ?1 ORDER BY \"{}\" ASC LIMIT ?2",
            table.name, table.time_column, table.time_column
        );
        let mut prepared = guard.prepare(&statement)?;
        let rows =
            prepared.query_map(rusqlite::params![since, limit], |row| read_row(row, table))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(HotError::Sqlite)
    }

    /// Marks every coolable row of one table as claimed by a flush batch.
    ///
    /// # Errors
    ///
    /// Returns an error when the claim cannot be written.
    pub fn claim(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        boundary: &str,
        batch: i64,
    ) -> Result<i64, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let changed = guard.execute(
            &format!(
                "UPDATE \"{}\" SET \"_flush\" = ?1 WHERE \"_flush\" IS NULL AND \"{}\" < ?2",
                table.name, table.time_column
            ),
            rusqlite::params![batch, boundary],
        )?;
        Ok(changed as i64)
    }

    /// Reads the rows claimed by one flush batch, grouped by cold partition.
    ///
    /// # Errors
    ///
    /// Returns an error when the claimed rows cannot be read.
    pub fn claimed(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        batch: i64,
    ) -> Result<Vec<HotRow>, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let names = table
            .columns
            .iter()
            .map(|column| format!("\"{}\"", column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let time_index = time_index(table);
        let statement = format!(
            "SELECT {names} FROM \"{}\" WHERE \"_flush\" = ?1 ORDER BY \"{}\" ASC",
            table.name, table.time_column
        );
        let mut prepared = guard.prepare(&statement)?;
        let rows = prepared.query_map([batch], |row| read_row(row, table))?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|values| {
                let Value::Timestamp(timestamp) = &values[time_index] else {
                    return Err(HotError::Invalid(format!(
                        "table {} holds a row without a timestamp",
                        table.name
                    )));
                };
                let bucket = table.granularity.bucket(timestamp).ok_or_else(|| {
                    HotError::Invalid(format!(
                        "table {} holds an unparsable timestamp {timestamp}",
                        table.name
                    ))
                })?;
                Ok(HotRow {
                    values: values.clone(),
                    bucket,
                })
            })
            .collect()
    }

    /// Releases rows claimed by a batch that never reached cold storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the release cannot be written.
    pub fn release(&self, dataset_id: &str, table: &TableSpec, batch: i64) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard.execute(
            &format!(
                "UPDATE \"{}\" SET \"_flush\" = NULL WHERE \"_flush\" = ?1",
                table.name
            ),
            [batch],
        )?;
        Ok(())
    }

    /// Drops claims left behind by batches that are already in cold storage.
    ///
    /// A claim only survives a committed batch when the process died between
    /// recording the batch and deleting the claimed rows, so the rows are
    /// already safe in cold storage and can go.
    ///
    /// # Errors
    ///
    /// Returns an error when the cleanup cannot be written.
    pub fn purge_stale_claims(
        &self,
        dataset_id: &str,
        tables: &[TableSpec],
        batch: i64,
    ) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        for table in tables {
            guard.execute(
                &format!(
                    "DELETE FROM \"{}\" WHERE \"_flush\" IS NOT NULL AND \"_flush\" < ?1",
                    table.name
                ),
                [batch],
            )?;
        }
        Ok(())
    }

    /// Deletes the rows of a batch that are now safely in cold storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the deletion cannot be written.
    pub fn purge(&self, dataset_id: &str, table: &TableSpec, batch: i64) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard.execute(
            &format!("DELETE FROM \"{}\" WHERE \"_flush\" = ?1", table.name),
            [batch],
        )?;
        Ok(())
    }

    fn connection(&self, dataset_id: &str) -> Result<Arc<Mutex<Connection>>, HotError> {
        let mut connections = self.connections.lock().map_err(|_| HotError::Poisoned)?;
        if let Some(connection) = connections.get(dataset_id) {
            return Ok(Arc::clone(connection));
        }
        std::fs::create_dir_all(self.directory.as_ref()).map_err(HotError::Files)?;
        let connection = Connection::open(self.database_path(dataset_id))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )?;
        let connection = Arc::new(Mutex::new(connection));
        connections.insert(dataset_id.to_owned(), Arc::clone(&connection));
        Ok(connection)
    }
}

/// The index of the timestamp column inside the declared columns.
pub fn time_index(table: &TableSpec) -> usize {
    table
        .columns
        .iter()
        .position(|column| column.name == table.time_column)
        .unwrap_or(0)
}

/// Formats the values of one row as a JSON object.
pub fn row_json(table: &TableSpec, values: &[Value]) -> serde_json::Map<String, Json> {
    let mut object = serde_json::Map::with_capacity(values.len());
    for (column, value) in table.columns.iter().zip(values) {
        object.insert(column.name.clone(), value.to_json());
    }
    object
}

fn lock<'a>(
    connection: &'a Arc<Mutex<Connection>>,
) -> Result<MutexGuard<'a, Connection>, HotError> {
    connection.lock().map_err(|_| HotError::Poisoned)
}

fn read_row(row: &rusqlite::Row<'_>, table: &TableSpec) -> rusqlite::Result<Vec<Value>> {
    let mut values = Vec::with_capacity(table.columns.len());
    for (index, column) in table.columns.iter().enumerate() {
        let stored: rusqlite::types::Value = row.get(index)?;
        values.push(convert(index, column, stored)?);
    }
    Ok(values)
}

fn convert(
    index: usize,
    column: &ColumnSpec,
    stored: rusqlite::types::Value,
) -> rusqlite::Result<Value> {
    Value::from_sql(column, stored).map_err(|message| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Null,
            Box::new(std::io::Error::other(message)),
        )
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::HotStore;
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec};

    fn column(name: &str, column_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            column_type,
            nullable: false,
        }
    }

    fn klines() -> TableSpec {
        TableSpec {
            name: "klines".to_owned(),
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

    fn store() -> (TempDir, HotStore) {
        let state = TempDir::new().expect("state directory");
        let store = HotStore::new(state.path().to_path_buf()).expect("store");
        store.create("demo", &[klines()]).expect("schema");
        (state, store)
    }

    fn row(
        symbol: &str,
        timestamp: &str,
        close: f64,
    ) -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({ "symbol": symbol, "updated_at": timestamp, "close": close })
            .as_object()
            .expect("object")
            .clone()
    }

    #[test]
    fn upserts_replace_by_primary_key() {
        let (_state, store) = store();
        let counts = store
            .ingest(
                "demo",
                &klines(),
                &[
                    row("BTCUSDT", "2026-10-01T00:00:00Z", 1.0),
                    row("BTCUSDT", "2026-10-01T00:00:00Z", 2.0),
                ],
                &[],
            )
            .expect("ingest");
        assert_eq!(counts.upserted, 2);
        let rows = store.rows("demo", &klines(), None, 10).expect("rows");
        assert_eq!(rows.len(), 1, "same key replaces the row");
        assert_eq!(rows[0][2], crate::schema::Value::Real(2.0));
    }

    #[test]
    fn claims_are_isolated_by_batch_and_releasable() {
        let (_state, store) = store();
        store
            .ingest(
                "demo",
                &klines(),
                &[row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0)],
                &[],
            )
            .expect("ingest");
        let table = klines();
        assert_eq!(
            store
                .claim("demo", &table, "2026-10-01T00:00:00.000000Z", 3)
                .expect("claim"),
            1
        );
        let claimed = store.claimed("demo", &table, 3).expect("claimed");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].bucket, "2026-09-01");
        store.release("demo", &table, 3).expect("release");
        assert!(
            store
                .claimed("demo", &table, 3)
                .expect("claimed")
                .is_empty()
        );
        assert_eq!(store.rows("demo", &table, None, 10).expect("rows").len(), 1);
    }

    #[test]
    fn writes_take_back_a_claimed_row() {
        let (_state, store) = store();
        let table = klines();
        store
            .ingest(
                "demo",
                &table,
                &[row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0)],
                &[],
            )
            .expect("ingest");
        store
            .claim("demo", &table, "2026-10-01T00:00:00.000000Z", 1)
            .expect("claim");
        store
            .ingest(
                "demo",
                &table,
                &[row("BTCUSDT", "2026-09-01T00:00:00Z", 9.0)],
                &[],
            )
            .expect("rewrite");
        assert!(
            store
                .claimed("demo", &table, 1)
                .expect("claimed")
                .is_empty(),
            "a rewrite clears the claim so the new version is flushed again"
        );
    }
}

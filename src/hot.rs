//! The hot side of a dataset: one writable `SQLite` database per dataset.
//!
//! The hot database holds the rows that are still mutable. A dataset is one
//! table, so the database holds exactly one `data` table (plus its indexes).
//! Rows leave it the moment they cool past the current period: they are
//! exported to cold Parquet first, then deleted here. Dataset ids are validated
//! identifiers, so they are also safe path segments.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, params, params_from_iter};
use serde::Serialize;
use serde_json::{Map, Value as Json};

use crate::cooling;
use crate::schema::{self, ColumnSpec, HOT_TABLE, TableSpec, Value};

/// State of the hot store of one dataset.
#[derive(Clone, Debug, Serialize)]
pub struct HotStat {
    /// Rows currently in the hot store.
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
    /// Returns an error when the file or its table cannot be created.
    pub fn create(&self, dataset_id: &str, table: &TableSpec) -> Result<(), HotError> {
        self.apply_schema(dataset_id, table)
    }

    /// Creates the missing hot table of a dataset, leaving existing rows alone.
    ///
    /// # Errors
    ///
    /// Returns an error when the hot database cannot be opened or written.
    pub fn apply_schema(&self, dataset_id: &str, table: &TableSpec) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        for statement in schema::create_statements(table) {
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

        self.write(dataset_id, table, rows, keys)
    }

    /// Inserts rows that are already coerced to their declared types; the
    /// Parquet import path lands here.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction cannot be committed.
    pub fn insert_typed(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        rows: Vec<Vec<Value>>,
    ) -> Result<IngestCounts, HotError> {
        self.write(dataset_id, table, rows, Vec::new())
    }

    fn write(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        rows: Vec<Vec<Value>>,
        keys: Vec<Vec<Value>>,
    ) -> Result<IngestCounts, HotError> {
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
                "INSERT INTO \"{HOT_TABLE}\" (\"_flush\", {names}) VALUES ({placeholders}) ON CONFLICT ({key}) DO UPDATE SET {}, \"_flush\" = NULL",
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
            let statement = format!("DELETE FROM \"{HOT_TABLE}\" WHERE {predicate}");
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

    /// Sums up the hot store of a dataset.
    ///
    /// # Errors
    ///
    /// Returns an error when the hot table cannot be read.
    pub fn stats(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        boundary: &str,
    ) -> Result<HotStat, HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let time = format!("\"{}\"", table.time_column);
        let (rows, oldest, newest): (i64, Option<String>, Option<String>) = guard.query_row(
            &format!("SELECT COUNT(*), MIN({time}), MAX({time}) FROM \"{HOT_TABLE}\""),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let coolable_rows = guard.query_row(
            &format!(
                "SELECT COUNT(*) FROM \"{HOT_TABLE}\" WHERE \"_flush\" IS NULL AND {time} < ?1"
            ),
            [boundary],
            |row| row.get(0),
        )?;
        Ok(HotStat {
            rows,
            coolable_rows,
            oldest_time: oldest,
            newest_time: newest,
        })
    }

    /// Lists the buckets that hold rows past the cooling boundary, oldest
    /// first. Each entry is the bucket key used in the cold tree.
    ///
    /// # Errors
    ///
    /// Returns an error when the table cannot be read.
    pub fn coolable_buckets(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        boundary: &str,
        limit: i64,
    ) -> Result<Vec<String>, HotError> {
        let width = cooling::key_length(table.granularity) as i64;
        let time = format!("\"{}\"", table.time_column);
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let mut statement = guard.prepare(&format!(
            "SELECT substr({time}, 1, ?1) FROM \"{HOT_TABLE}\" WHERE \"_flush\" IS NULL AND {time} < ?2 GROUP BY 1 ORDER BY 1 ASC LIMIT ?3"
        ))?;
        let buckets = statement
            .query_map(params![width, boundary, limit], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(buckets)
    }

    /// Claims every coolable row of one bucket for a pass and reports how many
    /// rows that is.
    ///
    /// # Errors
    ///
    /// Returns an error when the claim cannot be written.
    pub fn claim_bucket(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        bucket: &str,
        boundary: &str,
        pass: i64,
    ) -> Result<i64, HotError> {
        let width = cooling::key_length(table.granularity) as i64;
        let time = format!("\"{}\"", table.time_column);
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        let changed = guard.execute(
            &format!(
                "UPDATE \"{HOT_TABLE}\" SET \"_flush\" = ?1 WHERE \"_flush\" IS NULL AND {time} < ?2 AND substr({time}, 1, ?3) = ?4"
            ),
            params![pass, boundary, width, bucket],
        )?;
        Ok(changed as i64)
    }

    /// Inserts the rows of an existing cold partition into the hot table under
    /// a pass, so a rewrite can merge them with the claimed rows. Rows that
    /// already exist (the newly written ones) win; the partition copy only
    /// fills the gaps.
    ///
    /// # Errors
    ///
    /// Returns an error when the rows cannot be written.
    pub fn merge_partition(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        rows: &[Vec<Value>],
        pass: i64,
    ) -> Result<i64, HotError> {
        let connection = self.connection(dataset_id)?;
        let mut guard = lock(&connection)?;
        let transaction = guard.transaction()?;
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
        let key = table
            .primary_key
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!(
            "INSERT INTO \"{HOT_TABLE}\" (\"_flush\", {names}) VALUES ({placeholders}) ON CONFLICT ({key}) DO NOTHING"
        );
        let mut inserted = 0_i64;
        {
            let mut prepared = transaction.prepare(&statement)?;
            for values in rows {
                if values.len() != table.columns.len() {
                    return Err(HotError::Invalid(
                        "a merged row does not match the declared columns".to_owned(),
                    ));
                }
                let mut parameters = Vec::with_capacity(values.len() + 1);
                parameters.push(Value::Integer(pass).to_sql());
                parameters.extend(values.iter().map(Value::to_sql));
                inserted += prepared.execute(params_from_iter(parameters.iter()))? as i64;
            }
        }
        transaction.commit()?;
        Ok(inserted)
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
                    "SELECT COUNT(*) FROM \"{HOT_TABLE}\" WHERE \"_flush\" IS NULL AND \"{}\" < ?1",
                    table.time_column
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
            "SELECT {names} FROM \"{HOT_TABLE}\" WHERE ?1 IS NULL OR \"{}\" >= ?1 ORDER BY \"{}\" ASC LIMIT ?2",
            table.time_column, table.time_column
        );
        let mut prepared = guard.prepare(&statement)?;
        let rows =
            prepared.query_map(rusqlite::params![since, limit], |row| read_row(row, table))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(HotError::Sqlite)
    }

    /// Reads the rows claimed by one pass, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error when the claimed rows cannot be read.
    pub fn claimed(
        &self,
        dataset_id: &str,
        table: &TableSpec,
        pass: i64,
    ) -> Result<Vec<Vec<Value>>, HotError> {
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
            "SELECT {names} FROM \"{HOT_TABLE}\" WHERE \"_flush\" = ?1 ORDER BY \"{}\" ASC",
            table.time_column
        );
        let mut prepared = guard.prepare(&statement)?;
        let rows = prepared.query_map([pass], |row| read_row(row, table))?;
        let rows = rows.collect::<Result<Vec<_>, _>>()?;
        for values in &rows {
            if !matches!(values.get(time_index), Some(Value::Timestamp(_))) {
                return Err(HotError::Invalid(
                    "the hot table holds a row without a timestamp".to_owned(),
                ));
            }
        }
        Ok(rows)
    }

    /// Releases the rows one pass claimed, after it failed.
    ///
    /// # Errors
    ///
    /// Returns an error when the release cannot be written.
    pub fn release(&self, dataset_id: &str, pass: i64) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard.execute(
            &format!("UPDATE \"{HOT_TABLE}\" SET \"_flush\" = NULL WHERE \"_flush\" = ?1"),
            [pass],
        )?;
        Ok(())
    }

    /// Releases claims left behind by a pass that died mid-flight.
    ///
    /// A rewrite is not atomic across partitions, so a stale claim does not mean
    /// the rows reached cold storage. They go back to the unclaimed pool and the
    /// next pass rewrites that bucket from scratch; rewriting is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error when the release cannot be written.
    pub fn release_stale_claims(&self, dataset_id: &str, pass: i64) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard.execute(
            &format!(
                "UPDATE \"{HOT_TABLE}\" SET \"_flush\" = NULL WHERE \"_flush\" IS NOT NULL AND \"_flush\" != ?1"
            ),
            [pass],
        )?;
        Ok(())
    }

    /// Deletes the rows of a batch that are now safely in cold storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the deletion cannot be written.
    pub fn purge(&self, dataset_id: &str, pass: i64) -> Result<(), HotError> {
        let connection = self.connection(dataset_id)?;
        let guard = lock(&connection)?;
        guard.execute(
            &format!("DELETE FROM \"{HOT_TABLE}\" WHERE \"_flush\" = ?1"),
            [pass],
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
        store.create("demo", &klines()).expect("schema");
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
    fn claims_are_isolated_by_pass_and_releasable() {
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
                .claim_bucket(
                    "demo",
                    &table,
                    "2026-09-01",
                    "2026-10-01T00:00:00.000000Z",
                    3
                )
                .expect("claim"),
            1
        );
        let claimed = store.claimed("demo", &table, 3).expect("claimed");
        assert_eq!(claimed.len(), 1);
        store.release("demo", 3).expect("release");
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
            .claim_bucket(
                "demo",
                &table,
                "2026-09-01",
                "2026-10-01T00:00:00.000000Z",
                1,
            )
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

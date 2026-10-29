//! Cooling: the one operation that turns the hot window into cold history.
//!
//! A pass claims every row that has aged past the dataset's hot window, writes
//! it as immutable Parquet into the matching time partition, verifies the
//! object, records the batch (the commit point), and only then deletes the rows
//! from the hot database. A crash before the commit leaves the hot rows alone;
//! a crash after it leaves rows that are already safe in cold storage, and the
//! next pass drops them.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::cold::{self, ColdObjectMetadata, ColdStore, object_key, rows_to_parquet};
use crate::db::{ColdFile, Database, Dataset};
use crate::files::sha256_hex;
use crate::hot::{self, HotRow, HotStore};
use crate::schema::{Granularity, TableSpec, Value};

/// Rows per Parquet file; a bucket larger than this is split into several
/// files rather than buffered as one giant object.
const FILE_ROWS: usize = 500_000;

/// What one cooling pass wrote.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlushReport {
    /// Batch sequence number.
    pub batch: i64,
    /// Files written.
    pub files: i64,
    /// Rows written.
    pub rows: i64,
    /// Bytes written.
    pub bytes: u64,
}

/// The instant before which rows are cold, aligned down to the partition edge.
///
/// Aligning to the partition keeps a pass emitting whole buckets, so a table
/// normally gains one file per partition instead of one file per minute.
pub fn cooling_boundary(
    now: DateTime<Utc>,
    hot_window_seconds: i64,
    granularity: Granularity,
) -> String {
    let cutoff = now - chrono::Duration::seconds(hot_window_seconds.max(0));
    granularity
        .truncate(cutoff)
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Runs one cooling pass over a dataset.
///
/// # Errors
///
/// Returns the failure message of the pass; the hot rows stay put and the pass
/// is recorded as failed.
pub async fn run_flush(
    database: &Database,
    cold_store: &dyn ColdStore,
    dataset_id: &str,
    trigger: &str,
) -> Result<FlushReport, String> {
    let dataset = database
        .dataset(dataset_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("unknown dataset {dataset_id}"))?;
    let pass = database
        .begin_flush(dataset_id, trigger)
        .map_err(|error| error.to_string())?;
    let hot = database.hot();
    match cool(database, hot, cold_store, &dataset, pass.batch).await {
        Ok(report) => {
            database
                .finish_flush(
                    &pass.id,
                    "completed",
                    report.files,
                    report.rows,
                    report.bytes as i64,
                    "",
                )
                .map_err(|error| error.to_string())?;
            Ok(report)
        }
        Err(message) => {
            for table in &dataset.tables {
                let _ = hot.release(dataset_id, table, pass.batch);
            }
            let _ = database.finish_flush(&pass.id, "failed", 0, 0, 0, &message);
            Err(message)
        }
    }
}

async fn cool(
    database: &Database,
    hot: &HotStore,
    cold_store: &dyn ColdStore,
    dataset: &Dataset,
    batch: i64,
) -> Result<FlushReport, String> {
    hot.purge_stale_claims(&dataset.id, &dataset.tables, batch)
        .map_err(|error| error.to_string())?;
    let now = Utc::now();
    let mut report = FlushReport {
        batch,
        ..FlushReport::default()
    };
    let mut written: Vec<ColdFile> = Vec::new();
    let mut sequence = 0_usize;
    for table in &dataset.tables {
        let boundary = cooling_boundary(now, dataset.hot_window_seconds, table.granularity);
        let claimed = hot
            .claim(&dataset.id, table, &boundary, batch)
            .map_err(|error| error.to_string())?;
        if claimed == 0 {
            continue;
        }
        let rows = hot
            .claimed(&dataset.id, table, batch)
            .map_err(|error| error.to_string())?;
        for (bucket, group) in group_by_bucket(&rows) {
            for chunk in group.chunks(FILE_ROWS) {
                let file = write_partition_file(
                    cold_store, dataset, table, batch, sequence, &bucket, chunk,
                )
                .await?;
                sequence += 1;
                report.files += 1;
                report.rows += file.rows;
                report.bytes += file.bytes;
                written.push(file);
            }
        }
    }
    if !written.is_empty() {
        let catalog = json!({
            "dataset_id": dataset.id,
            "batch": batch,
            "created_at": Utc::now().timestamp(),
            "files": written,
        });
        let bytes = serde_json::to_vec_pretty(&catalog)
            .map_err(|error| format!("catalogue encoding failed: {error}"))?;
        let sha256 = sha256_hex(&bytes);
        cold_store
            .put_object(
                &cold::catalog_key(&dataset.id, batch),
                bytes,
                &ColdObjectMetadata { sha256 },
            )
            .await
            .map_err(|error| format!("catalogue upload failed: {error}"))?;
    }
    database
        .record_batch(&dataset.id, batch, &written)
        .map_err(|error| error.to_string())?;
    for table in &dataset.tables {
        hot.purge(&dataset.id, table, batch)
            .map_err(|error| error.to_string())?;
    }
    Ok(report)
}

async fn write_partition_file(
    cold_store: &dyn ColdStore,
    dataset: &Dataset,
    table: &TableSpec,
    batch: i64,
    sequence: usize,
    bucket: &str,
    rows: &[&HotRow],
) -> Result<ColdFile, String> {
    let values = rows
        .iter()
        .map(|row| row.values.clone())
        .collect::<Vec<Vec<Value>>>();
    let body = rows_to_parquet(table, &values).map_err(|error| error.to_string())?;
    let path = format!(
        "{}/{key}={bucket}/{batch:06}-{sequence:04}.parquet",
        table.name,
        key = table.granularity.key()
    );
    let partition = format!("{}={bucket}", table.granularity.key());
    let sha256 = sha256_hex(&body);
    let size = body.len() as u64;
    let key = object_key(&dataset.id, &path);
    cold_store
        .put_object(
            &key,
            body,
            &ColdObjectMetadata {
                sha256: sha256.clone(),
            },
        )
        .await
        .map_err(|error| format!("upload {path} failed: {error}"))?;
    let head = cold_store
        .head_object(&key)
        .await
        .map_err(|error| format!("verify {path} failed: {error}"))?
        .ok_or_else(|| format!("verify {path} failed: object is missing"))?;
    if head.size != size || head.sha256.as_deref() != Some(sha256.as_str()) {
        return Err(format!("verify {path} failed: checksum mismatch"));
    }
    let time_index = hot::time_index(table);
    let mut stamps = rows
        .iter()
        .filter_map(|row| match row.values.get(time_index) {
            Some(Value::Timestamp(value)) => Some(value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    stamps.sort_unstable();
    Ok(ColdFile {
        dataset_id: dataset.id.clone(),
        batch,
        table_name: table.name.clone(),
        path,
        partition,
        rows: rows.len() as i64,
        bytes: size,
        sha256,
        min_time: stamps.first().copied().unwrap_or_default().to_owned(),
        max_time: stamps.last().copied().unwrap_or_default().to_owned(),
        created_at: Utc::now().timestamp(),
    })
}

fn group_by_bucket(rows: &[HotRow]) -> BTreeMap<String, Vec<&HotRow>> {
    let mut groups: BTreeMap<String, Vec<&HotRow>> = BTreeMap::new();
    for row in rows {
        groups.entry(row.bucket.clone()).or_default().push(row);
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use aws_sdk_s3::primitives::ByteStream;
    use chrono::DateTime;
    use tempfile::TempDir;

    use super::{cooling_boundary, run_flush};
    use crate::cold::{ColdError, ColdObjectBody, ColdObjectHead, ColdObjectMetadata, ColdStore};
    use crate::db::Database;
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec};

    #[derive(Default)]
    struct FakeColdStore {
        objects: Mutex<HashMap<String, (Vec<u8>, String)>>,
    }

    impl FakeColdStore {
        fn keys(&self) -> Vec<String> {
            let mut keys = self
                .objects
                .lock()
                .expect("objects lock")
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            keys
        }

        fn object(&self, key: &str) -> Vec<u8> {
            self.objects
                .lock()
                .expect("objects lock")
                .get(key)
                .map(|(bytes, _)| bytes.clone())
                .expect("object exists")
        }
    }

    #[async_trait]
    impl ColdStore for FakeColdStore {
        async fn put_object(
            &self,
            key: &str,
            bytes: Vec<u8>,
            metadata: &ColdObjectMetadata,
        ) -> Result<(), ColdError> {
            self.objects
                .lock()
                .expect("objects lock")
                .insert(key.to_owned(), (bytes, metadata.sha256.clone()));
            Ok(())
        }

        async fn head_object(&self, key: &str) -> Result<Option<ColdObjectHead>, ColdError> {
            Ok(self
                .objects
                .lock()
                .expect("objects lock")
                .get(key)
                .map(|(bytes, sha256)| ColdObjectHead {
                    size: bytes.len() as u64,
                    sha256: Some(sha256.clone()),
                }))
        }

        async fn get_object(&self, key: &str) -> Result<Option<ColdObjectBody>, ColdError> {
            Ok(self
                .objects
                .lock()
                .expect("objects lock")
                .get(key)
                .map(|(bytes, _)| ColdObjectBody {
                    size: bytes.len() as u64,
                    body: ByteStream::from(bytes.clone()),
                }))
        }
    }

    fn table() -> TableSpec {
        TableSpec {
            name: "bars".to_owned(),
            primary_key: vec!["symbol".to_owned(), "updated_at".to_owned()],
            time_column: "updated_at".to_owned(),
            granularity: Granularity::Day,
            columns: vec![
                ColumnSpec {
                    name: "symbol".to_owned(),
                    column_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnSpec {
                    name: "updated_at".to_owned(),
                    column_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnSpec {
                    name: "close".to_owned(),
                    column_type: ColumnType::Real,
                    nullable: false,
                },
            ],
        }
    }

    fn seeded() -> (TempDir, Database) {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database");
        database
            .create_dataset("bars", "Bars", "", 0, &[table()])
            .expect("dataset");
        (state, database)
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
    fn the_boundary_follows_the_window_and_the_partition_edge() {
        let now = DateTime::parse_from_rfc3339("2026-10-05T13:45:00Z")
            .expect("parses")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            cooling_boundary(now, 86_400, Granularity::Day),
            "2026-10-04T00:00:00.000000Z"
        );
        assert_eq!(
            cooling_boundary(now, 3 * 3600, Granularity::Hour),
            "2026-10-05T10:00:00.000000Z"
        );
        assert_eq!(
            cooling_boundary(now, 0, Granularity::Month),
            "2026-10-01T00:00:00.000000Z"
        );
    }

    #[tokio::test]
    async fn a_pass_writes_one_file_per_partition_and_empties_the_window() {
        let (_state, database) = seeded();
        let table = table();
        let hot = database.hot();
        hot.ingest(
            "bars",
            &table,
            &[
                row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0),
                row("BTCUSDT", "2026-09-01T00:01:00Z", 2.0),
                row("ETHUSDT", "2026-09-02T00:00:00Z", 3.0),
                row("BTCUSDT", "2999-01-01T00:00:00Z", 4.0),
            ],
            &[],
        )
        .expect("ingest");

        let cold = FakeColdStore::default();
        let report = run_flush(&database, &cold, "bars", "manual")
            .await
            .expect("flush");
        assert_eq!(report.files, 2, "one file per partition");
        assert_eq!(report.rows, 3);
        assert_eq!(
            cold.keys(),
            vec![
                "datasets/bars/_catalog/000001.json".to_owned(),
                "datasets/bars/bars/day=2026-09-01/000001-0000.parquet".to_owned(),
                "datasets/bars/bars/day=2026-09-02/000001-0001.parquet".to_owned(),
            ]
        );
        assert_eq!(
            hot.rows("bars", &table, None, 10).expect("hot rows").len(),
            1,
            "only the row still inside the window stays hot"
        );
        let catalog = database.cold_files("bars", 0, None).expect("catalogue");
        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog[0].min_time, "2026-09-01T00:00:00.000000Z");
        assert_eq!(catalog[0].max_time, "2026-09-01T00:01:00.000000Z");
    }

    #[tokio::test]
    async fn parquet_carries_only_the_declared_columns() {
        let (_state, database) = seeded();
        let cold = FakeColdStore::default();
        database
            .hot()
            .ingest(
                "bars",
                &table(),
                &[row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0)],
                &[],
            )
            .expect("ingest");
        run_flush(&database, &cold, "bars", "manual")
            .await
            .expect("flush");

        let bytes = cold.object("datasets/bars/bars/day=2026-09-01/000001-0000.parquet");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .expect("opens")
        .build()
        .expect("reader");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("reads");
        let names = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["symbol", "updated_at", "close"]);
    }

    #[tokio::test]
    async fn a_failed_upload_keeps_every_row_hot() {
        struct BrokenStore;

        #[async_trait]
        impl ColdStore for BrokenStore {
            async fn put_object(
                &self,
                _key: &str,
                _bytes: Vec<u8>,
                _metadata: &ColdObjectMetadata,
            ) -> Result<(), ColdError> {
                Err(ColdError::S3("bucket is on fire".to_owned()))
            }

            async fn head_object(&self, _key: &str) -> Result<Option<ColdObjectHead>, ColdError> {
                Ok(None)
            }

            async fn get_object(&self, _key: &str) -> Result<Option<ColdObjectBody>, ColdError> {
                Ok(None)
            }
        }

        let (_state, database) = seeded();
        let table = table();
        database
            .hot()
            .ingest(
                "bars",
                &table,
                &[row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0)],
                &[],
            )
            .expect("ingest");

        let error = run_flush(&database, &BrokenStore, "bars", "manual")
            .await
            .expect_err("upload fails");
        assert!(error.contains("upload"), "{error}");
        assert_eq!(
            database
                .hot()
                .rows("bars", &table, None, 10)
                .expect("rows")
                .len(),
            1,
            "the row is still hot and can be retried"
        );
        assert!(
            database
                .cold_files("bars", 0, None)
                .expect("catalogue")
                .is_empty()
        );
        let latest = database
            .latest_flush("bars")
            .expect("latest")
            .expect("exists");
        assert_eq!(latest.status, "failed");
        assert!(!latest.message.is_empty());
    }

    #[tokio::test]
    async fn a_rewritten_row_is_flushed_again_with_its_new_version() {
        let (_state, database) = seeded();
        let table = table();
        let hot = database.hot();
        hot.ingest(
            "bars",
            &table,
            &[row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0)],
            &[],
        )
        .expect("ingest");
        let cold = FakeColdStore::default();
        run_flush(&database, &cold, "bars", "manual")
            .await
            .expect("first flush");

        hot.ingest(
            "bars",
            &table,
            &[row("BTCUSDT", "2026-09-01T00:00:00Z", 9.0)],
            &[],
        )
        .expect("rewrite");
        let report = run_flush(&database, &cold, "bars", "manual")
            .await
            .expect("second flush");
        assert_eq!(
            report.rows, 1,
            "the new version is a new row in cold storage"
        );
        assert_eq!(
            database
                .cold_files("bars", 0, None)
                .expect("catalogue")
                .len(),
            2,
            "cold storage appends versions instead of rewriting history"
        );
    }
}

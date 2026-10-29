//! Cooling: moving rows out of the hot window into the cold tree.
//!
//! One dataset owns one object per bucket (`2026-10-01.parquet`). A pass claims
//! the rows of one bucket, merges them with the partition that is already there,
//! uploads the result and only then deletes the hot rows. The upload is the
//! commit point; a pass that dies halfway leaves claims behind, and the next
//! pass releases them and rewrites that bucket from scratch.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;

use crate::cold::{self, ColdObjectMetadata, ColdStore, object_key};
use crate::cooling::{self, Cadence};
use crate::db::{ColdFile, Database, Dataset, Flush};
use crate::files::sha256_hex;
use crate::hot;
use crate::schema::Value;

/// Partitions one pass rewrites before it yields to the next tick.
const PARTITIONS_PER_PASS: i64 = 128;

/// Rows one pass rewrites before it yields, so a backfill still makes progress
/// in bounded slices.
const ROWS_PER_PASS: i64 = 1_000_000;

/// A bucket larger than this means the declared granularity is too coarse for
/// the data; the pass fails instead of exhausting memory on a small host.
const MAX_PARTITION_ROWS: usize = 5_000_000;

/// What one cooling pass did.
#[derive(Clone, Debug, Default, Serialize)]
pub struct FlushReport {
    /// Pass number.
    pub pass: i64,
    /// Partitions rewritten.
    pub partitions: i64,
    /// Rows written.
    pub rows: i64,
    /// Bytes uploaded.
    pub bytes: u64,
}

/// The instant the current calendar period started, in the fixed-width UTC form
/// the hot store compares against.
pub fn cooling_boundary(now: DateTime<Utc>, cadence: Cadence) -> String {
    cooling::boundary(now, cadence).to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Runs one cooling pass over a dataset.
///
/// # Errors
///
/// Returns the failure message of the pass; the hot rows stay put, their claims
/// are released and the pass is recorded as failed.
pub async fn run_pass(
    database: &Database,
    cold_store: &dyn ColdStore,
    dataset_id: &str,
    trigger: &str,
) -> Result<FlushReport, String> {
    let pass = database
        .begin_pass(dataset_id, trigger)
        .map_err(|error| error.to_string())?;
    match cool(database, cold_store, dataset_id, &pass).await {
        Ok(report) => {
            database
                .finish_pass(
                    &pass.id,
                    "completed",
                    report.partitions,
                    report.rows,
                    report.bytes as i64,
                    "",
                )
                .map_err(|error| error.to_string())?;
            Ok(report)
        }
        Err(message) => {
            if let Ok(Some(dataset)) = database.dataset(dataset_id) {
                let _ = database.hot().release(&dataset.id, pass.pass);
            }
            let _ = database.finish_pass(&pass.id, "failed", 0, 0, 0, &message);
            Err(message)
        }
    }
}

async fn cool(
    database: &Database,
    cold_store: &dyn ColdStore,
    dataset_id: &str,
    pass: &Flush,
) -> Result<FlushReport, String> {
    let dataset = database
        .dataset(dataset_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("unknown dataset {dataset_id}"))?;
    let hot = database.hot();
    hot.release_stale_claims(&dataset.id, pass.pass)
        .map_err(|error| error.to_string())?;
    let boundary = cooling_boundary(Utc::now(), dataset.cadence);
    let mut report = FlushReport {
        pass: pass.pass,
        ..FlushReport::default()
    };
    let mut rewritten = 0_i64;
    while rewritten < PARTITIONS_PER_PASS && report.rows < ROWS_PER_PASS {
        let buckets = hot
            .coolable_buckets(
                &dataset.id,
                &dataset.table,
                &boundary,
                PARTITIONS_PER_PASS - rewritten,
            )
            .map_err(|error| error.to_string())?;
        let Some(bucket) = buckets.first() else {
            break;
        };
        let Some(written) =
            rewrite(database, &dataset, cold_store, bucket, &boundary, pass.pass).await?
        else {
            break;
        };
        report.partitions += 1;
        report.rows += written.rows;
        report.bytes += written.bytes;
        rewritten += 1;
    }
    Ok(report)
}

/// What rewriting one partition cost.
struct Rewritten {
    rows: i64,
    bytes: u64,
}

/// Rewrites the cold object of one bucket from the hot rows that cooled out of
/// the window, merged with whatever the object already holds.
async fn rewrite(
    database: &Database,
    dataset: &Dataset,
    cold_store: &dyn ColdStore,
    bucket: &str,
    boundary: &str,
    pass: i64,
) -> Result<Option<Rewritten>, String> {
    let hot = database.hot();
    let claimed = hot
        .claim_bucket(&dataset.id, &dataset.table, bucket, boundary, pass)
        .map_err(|error| error.to_string())?;
    if claimed == 0 {
        return Ok(None);
    }
    let path = format!("{bucket}.parquet");
    let key = object_key(&dataset.id, &path);
    if database
        .cold_file(&dataset.id, &path)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        let object = cold_store
            .get_object(&key)
            .await
            .map_err(|error| format!("read {path} failed: {error}"))?
            .ok_or_else(|| format!("{path} is catalogued but missing from cold storage"))?;
        let body = object
            .body
            .collect()
            .await
            .map_err(|error| format!("read {path} failed: {error}"))?
            .into_bytes();
        let rows =
            cold::parquet_to_rows(&dataset.table, &body).map_err(|error| error.to_string())?;
        hot.merge_partition(&dataset.id, &dataset.table, &rows, pass)
            .map_err(|error| error.to_string())?;
    }
    let rows = hot
        .claimed(&dataset.id, &dataset.table, pass)
        .map_err(|error| error.to_string())?;
    if rows.len() > MAX_PARTITION_ROWS {
        return Err(format!(
            "partition {bucket} holds {} rows, too many for one object; declare a finer granularity",
            rows.len()
        ));
    }
    let values = rows.clone();
    let body = cold::rows_to_parquet(&dataset.table, &values).map_err(|error| error.to_string())?;
    let sha256 = sha256_hex(&body);
    let size = body.len() as u64;
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
    let revision = database
        .next_revision(&dataset.id)
        .map_err(|error| error.to_string())?;
    let time_index = hot::time_index(&dataset.table);
    let mut stamps = rows
        .iter()
        .filter_map(|values| match values.get(time_index) {
            Some(Value::Timestamp(value)) => Some(value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    stamps.sort_unstable();
    database
        .record_partition(
            &dataset.id,
            &ColdFile {
                dataset_id: dataset.id.clone(),
                revision,
                path: path.clone(),
                bucket: bucket.to_owned(),
                rows: rows.len() as i64,
                bytes: size,
                sha256,
                min_time: stamps.first().copied().unwrap_or_default().to_owned(),
                max_time: stamps.last().copied().unwrap_or_default().to_owned(),
                created_at: Utc::now().timestamp(),
            },
        )
        .map_err(|error| error.to_string())?;
    hot.purge(&dataset.id, pass)
        .map_err(|error| error.to_string())?;
    Ok(Some(Rewritten {
        rows: rows.len() as i64,
        bytes: size,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use aws_sdk_s3::primitives::ByteStream;
    use chrono::DateTime;
    use tempfile::TempDir;

    use super::{cooling_boundary, run_pass};
    use crate::cold::{
        ColdError, ColdObjectBody, ColdObjectHead, ColdObjectMetadata, ColdStore, parquet_to_rows,
    };
    use crate::cooling::Cadence;
    use crate::db::Database;
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec, Value};

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
            .create_dataset("bars", "Bars", "", Cadence::Daily, &table())
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
    fn the_boundary_is_the_start_of_the_current_period() {
        let now = DateTime::parse_from_rfc3339("2026-10-05T13:45:00Z")
            .expect("parses")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            cooling_boundary(now, Cadence::Daily),
            "2026-10-05T00:00:00.000000Z"
        );
        assert_eq!(
            cooling_boundary(now, Cadence::Monthly),
            "2026-10-01T00:00:00.000000Z"
        );
    }

    #[tokio::test]
    async fn a_pass_writes_one_object_per_bucket_and_clears_cooled_rows() {
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
        let report = run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("flush");
        assert_eq!(report.partitions, 2, "one object per bucket");
        assert_eq!(report.rows, 3);
        assert_eq!(
            cold.keys(),
            vec![
                "datasets/bars/2026-09-01.parquet".to_owned(),
                "datasets/bars/2026-09-02.parquet".to_owned(),
            ]
        );
        assert_eq!(
            hot.rows("bars", &table, None, 10).expect("hot rows").len(),
            1,
            "only the row still inside the window stays hot"
        );
        let mut catalog = database.cold_files("bars", 0).expect("catalogue");
        catalog.sort_by(|left, right| left.path.cmp(&right.path));
        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog[0].revision, 1);
        assert_eq!(catalog[1].revision, 2);
        assert_eq!(catalog[0].bucket, "2026-09-01");
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
        run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("flush");

        let bytes = cold.object("datasets/bars/2026-09-01.parquet");
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

        let error = run_pass(&database, &BrokenStore, "bars", "manual")
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
                .cold_files("bars", 0)
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
    async fn a_rewritten_row_replaces_the_same_partition() {
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
        run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("first flush");

        hot.ingest(
            "bars",
            &table,
            &[row("BTCUSDT", "2026-09-01T00:00:00Z", 9.0)],
            &[],
        )
        .expect("rewrite");
        let report = run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("second flush");
        assert_eq!(report.partitions, 1, "the same partition is rewritten");
        assert_eq!(report.rows, 1);

        let catalog = database.cold_files("bars", 0).expect("catalogue");
        assert_eq!(catalog.len(), 1, "one object per bucket");
        assert_eq!(catalog[0].revision, 2, "the rewrite bumps the revision");
        let rows = parquet_to_rows(&table, &cold.object("datasets/bars/2026-09-01.parquet"))
            .expect("decodes");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Value::Real(9.0), "the newer version wins");
    }

    #[tokio::test]
    async fn a_late_row_merges_into_the_existing_partition() {
        let (_state, database) = seeded();
        let table = table();
        let hot = database.hot();
        hot.ingest(
            "bars",
            &table,
            &[
                row("BTCUSDT", "2026-09-01T00:00:00Z", 1.0),
                row("ETHUSDT", "2026-09-01T00:01:00Z", 2.0),
            ],
            &[],
        )
        .expect("ingest");
        let cold = FakeColdStore::default();
        run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("first flush");

        hot.ingest(
            "bars",
            &table,
            &[row("SOLUSDT", "2026-09-01T12:00:00Z", 3.0)],
            &[],
        )
        .expect("backfill");
        let report = run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("second flush");
        assert_eq!(report.rows, 3, "old and new rows travel together");

        let rows = parquet_to_rows(&table, &cold.object("datasets/bars/2026-09-01.parquet"))
            .expect("decodes");
        assert_eq!(rows.len(), 3, "the partition keeps its earlier rows");
        assert_eq!(database.cold_files("bars", 0).expect("catalogue").len(), 1);
    }

    #[tokio::test]
    async fn stale_claims_are_released_and_recooled() {
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
        let boundary = cooling_boundary(chrono::Utc::now(), Cadence::Daily);
        hot.claim_bucket("bars", &table, "2026-09-01", &boundary, 99)
            .expect("stale claim");
        assert_eq!(
            hot.rows("bars", &table, None, 10).expect("rows").len(),
            1,
            "the claimed row is still there"
        );

        let cold = FakeColdStore::default();
        let report = run_pass(&database, &cold, "bars", "manual")
            .await
            .expect("flush");
        assert_eq!(report.rows, 1, "the released row is cooled again");
        assert_eq!(hot.rows("bars", &table, None, 10).expect("rows").len(), 0);
    }
}

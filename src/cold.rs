//! The cold side: an append-only Parquet tree in object storage.
//!
//! Objects are written once and never rewritten. That is what makes an
//! incremental mirror trivially correct: a mirror only ever adds files, and a
//! file it already has can never become stale.
//!
//! Besides the data itself, every batch writes a small immutable catalogue
//! object next to it, so the cold tree stays readable even if this application
//! is gone.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::db::{
    DEFAULT_COLD_BUCKET, DEFAULT_COLD_REGION, Database, DatabaseError, META_COLD_BUCKET,
    META_COLD_REGION,
};
use crate::schema::{ColumnType, TableSpec, Value};

#[derive(Debug, Error)]
pub enum ColdError {
    #[error("cold storage state failed")]
    Database(#[from] DatabaseError),
    #[error("cold storage request failed: {0}")]
    S3(String),
    #[error("Parquet encoding failed: {0}")]
    Parquet(String),
}

#[derive(Clone, Debug)]
pub struct ColdObjectMetadata {
    pub sha256: String,
}

#[derive(Debug)]
pub struct ColdObjectHead {
    pub size: u64,
    pub sha256: Option<String>,
}

#[derive(Debug)]
pub struct ColdObjectBody {
    pub body: ByteStream,
    pub size: u64,
}

/// The blob layer behind the cold catalogue: S3 in production, fakes in tests.
#[async_trait]
pub trait ColdStore: Send + Sync {
    async fn put_object(
        &self,
        key: &str,
        bytes: Vec<u8>,
        metadata: &ColdObjectMetadata,
    ) -> Result<(), ColdError>;
    async fn head_object(&self, key: &str) -> Result<Option<ColdObjectHead>, ColdError>;
    async fn get_object(&self, key: &str) -> Result<Option<ColdObjectBody>, ColdError>;
}

pub struct S3ColdStore {
    database: Database,
    cache: RwLock<Option<CachedClient>>,
}

struct CachedClient {
    bucket: String,
    region: String,
    client: Client,
}

impl S3ColdStore {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            cache: RwLock::new(None),
        }
    }

    async fn client(&self) -> Result<(String, Client), ColdError> {
        let bucket = self
            .database
            .meta_value(META_COLD_BUCKET)?
            .unwrap_or_else(|| DEFAULT_COLD_BUCKET.to_owned());
        let region = self
            .database
            .meta_value(META_COLD_REGION)?
            .unwrap_or_else(|| DEFAULT_COLD_REGION.to_owned());
        if let Some(cached) = self.cache.read().await.as_ref()
            && cached.bucket == bucket
            && cached.region == region
        {
            return Ok((bucket, cached.client.clone()));
        }
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;
        let client = Client::new(&config);
        *self.cache.write().await = Some(CachedClient {
            bucket: bucket.clone(),
            region,
            client: client.clone(),
        });
        Ok((bucket, client))
    }
}

fn sdk_message(error: impl std::fmt::Display) -> ColdError {
    ColdError::S3(error.to_string())
}

#[async_trait]
impl ColdStore for S3ColdStore {
    async fn put_object(
        &self,
        key: &str,
        bytes: Vec<u8>,
        metadata: &ColdObjectMetadata,
    ) -> Result<(), ColdError> {
        let (bucket, client) = self.client().await?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .metadata("sha256", &metadata.sha256)
            .send()
            .await
            .map_err(sdk_message)?;
        Ok(())
    }

    async fn head_object(&self, key: &str) -> Result<Option<ColdObjectHead>, ColdError> {
        let (bucket, client) = self.client().await?;
        match client.head_object().bucket(bucket).key(key).send().await {
            Ok(output) => Ok(Some(ColdObjectHead {
                size: output.content_length().unwrap_or_default().max(0) as u64,
                sha256: output
                    .metadata()
                    .and_then(|metadata| metadata.get("sha256"))
                    .cloned(),
            })),
            Err(error) => {
                if let SdkError::ServiceError(context) = &error
                    && context.err().is_not_found()
                {
                    return Ok(None);
                }
                Err(sdk_message(error))
            }
        }
    }

    async fn get_object(&self, key: &str) -> Result<Option<ColdObjectBody>, ColdError> {
        let (bucket, client) = self.client().await?;
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let size = output.content_length().unwrap_or_default().max(0) as u64;
                Ok(Some(ColdObjectBody {
                    body: output.body,
                    size,
                }))
            }
            Err(error) => {
                if let SdkError::ServiceError(context) = &error
                    && context.err().is_no_such_key()
                {
                    return Ok(None);
                }
                Err(sdk_message(error))
            }
        }
    }
}

/// The object key of one cold data file.
pub fn object_key(dataset_id: &str, path: &str) -> String {
    format!("datasets/{dataset_id}/{path}")
}

/// The object key of the catalogue record of one batch.
pub fn catalog_key(dataset_id: &str, batch: i64) -> String {
    format!("datasets/{dataset_id}/_catalog/{batch:06}.json")
}

/// Encodes rows of one table into a Snappy Parquet file.
///
/// # Errors
///
/// Returns an error when a value does not match its declared type or the
/// Parquet writer fails.
pub fn rows_to_parquet(table: &TableSpec, rows: &[Vec<Value>]) -> Result<Vec<u8>, ColdError> {
    let mut fields = Vec::with_capacity(table.columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(table.columns.len());
    for (index, column) in table.columns.iter().enumerate() {
        let data_type = match column.column_type {
            ColumnType::Integer => DataType::Int64,
            ColumnType::Real => DataType::Float64,
            ColumnType::Text => DataType::Utf8,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        };
        fields.push(Field::new(&column.name, data_type.clone(), column.nullable));
        let array: ArrayRef = match column.column_type {
            ColumnType::Integer => {
                Arc::new(Int64Array::from_iter(rows.iter().map(
                    |row| match row.get(index) {
                        Some(Value::Integer(value)) => Some(*value),
                        _ => None,
                    },
                )))
            }
            ColumnType::Real => {
                Arc::new(Float64Array::from_iter(rows.iter().map(
                    |row| match row.get(index) {
                        Some(Value::Real(value)) => Some(*value),
                        _ => None,
                    },
                )))
            }
            ColumnType::Text => {
                Arc::new(StringArray::from_iter(rows.iter().map(
                    |row| match row.get(index) {
                        Some(Value::Text(value)) => Some(value.as_str()),
                        _ => None,
                    },
                )))
            }
            ColumnType::Boolean => {
                Arc::new(BooleanArray::from_iter(rows.iter().map(
                    |row| match row.get(index) {
                        Some(Value::Boolean(value)) => Some(*value),
                        _ => None,
                    },
                )))
            }
            ColumnType::Timestamp => {
                let micros = rows
                    .iter()
                    .map(|row| match row.get(index) {
                        Some(Value::Timestamp(value)) => {
                            chrono::DateTime::parse_from_rfc3339(value)
                                .ok()
                                .map(|moment| moment.timestamp_micros())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                Arc::new(TimestampMicrosecondArray::from_iter(micros).with_timezone("UTC"))
            }
        };
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .map_err(|error| ColdError::Parquet(error.to_string()))?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut buffer = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, schema, Some(properties))
        .map_err(|error| ColdError::Parquet(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| ColdError::Parquet(error.to_string()))?;
    writer
        .close()
        .map_err(|error| ColdError::Parquet(error.to_string()))?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::{catalog_key, object_key, rows_to_parquet};
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec, Value};

    fn table() -> TableSpec {
        TableSpec {
            primary_key: vec!["symbol".to_owned(), "updated_at".to_owned()],
            time_column: "updated_at".to_owned(),
            granularity: Granularity::Hour,
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
                    nullable: true,
                },
            ],
        }
    }

    #[test]
    fn parquet_round_trips_declared_columns_only() {
        let rows = vec![
            vec![
                Value::Text("BTCUSDT".to_owned()),
                Value::Timestamp("2026-10-01T00:00:00.000000Z".to_owned()),
                Value::Real(1.5),
            ],
            vec![
                Value::Text("BTCUSDT".to_owned()),
                Value::Timestamp("2026-10-01T00:01:00.000000Z".to_owned()),
                Value::Null,
            ],
        ];
        let bytes = rows_to_parquet(&table(), &rows).expect("encodes");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .expect("opens")
        .build()
        .expect("reader");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("reads");
        let total: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(total, 2);
        let schema = batches[0].schema();
        let names: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, vec!["symbol", "updated_at", "close"]);
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            )
        );
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[test]
    fn keys_keep_the_dataset_and_batch_visible() {
        assert_eq!(
            object_key("bars", "bars/day=2026-10-01/000007-0000.parquet"),
            "datasets/bars/bars/day=2026-10-01/000007-0000.parquet"
        );
        assert_eq!(catalog_key("bars", 7), "datasets/bars/_catalog/000007.json");
    }
}

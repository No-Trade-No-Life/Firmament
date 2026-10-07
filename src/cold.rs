use std::io::Cursor;
use std::sync::Arc;

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

#[derive(Debug, Error)]
pub enum ColdError {
    #[error("cold storage state failed")]
    Database(#[from] DatabaseError),
    #[error("cold storage request failed: {0}")]
    S3(String),
    #[error("file conversion failed: {0}")]
    Conversion(String),
}

#[derive(Clone, Debug)]
pub struct ColdObjectMetadata {
    pub sha256: String,
    pub source: String,
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

/// The blob layer behind the cold catalog: S3 in production, fakes in tests.
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
            .metadata("source", &metadata.source)
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

/// Maps a dataset-relative path to the Parquet object path used in the cold store.
pub fn parquet_path_for(relative: &str) -> Option<String> {
    let lower = relative.to_ascii_lowercase();
    if lower.ends_with(".csv") {
        Some(format!("{}.parquet", &relative[..relative.len() - 4]))
    } else if lower.ends_with(".parquet") {
        Some(relative.to_owned())
    } else {
        None
    }
}

pub fn object_key(dataset_id: &str, path: &str) -> String {
    format!("datasets/{dataset_id}/{path}")
}

/// Converts CSV bytes into a columnar Parquet file (Snappy compressed).
///
/// # Errors
///
/// Returns an error when the CSV cannot be parsed or the Parquet writer fails.
pub fn convert_csv_to_parquet(bytes: &[u8]) -> Result<Vec<u8>, ColdError> {
    let format = arrow::csv::reader::Format::default().with_header(true);
    let (schema, _) = format
        .infer_schema(Cursor::new(bytes), Some(100))
        .map_err(|error| ColdError::Conversion(error.to_string()))?;
    let schema = Arc::new(schema);
    let reader = arrow::csv::ReaderBuilder::new(schema.clone())
        .with_format(format)
        .build(Cursor::new(bytes))
        .map_err(|error| ColdError::Conversion(error.to_string()))?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut buffer = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, schema, Some(properties))
        .map_err(|error| ColdError::Conversion(error.to_string()))?;
    for batch in reader {
        let batch = batch.map_err(|error| ColdError::Conversion(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| ColdError::Conversion(error.to_string()))?;
    }
    writer
        .close()
        .map_err(|error| ColdError::Conversion(error.to_string()))?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::{
        ColdObjectMetadata, ColdStore, S3ColdStore, convert_csv_to_parquet, parquet_path_for,
    };
    use crate::db::Database;
    use tempfile::TempDir;

    #[test]
    fn parquet_path_for_converts_tabular_files() {
        assert_eq!(
            parquet_path_for("daily/2026-10-08.csv").as_deref(),
            Some("daily/2026-10-08.parquet")
        );
        assert_eq!(
            parquet_path_for("ONCHAIN.CSV").as_deref(),
            Some("ONCHAIN.parquet")
        );
        assert_eq!(
            parquet_path_for("ready.parquet").as_deref(),
            Some("ready.parquet")
        );
        assert_eq!(parquet_path_for("notes.md"), None);
    }

    #[test]
    fn converts_csv_to_parquet_round_trip() {
        let csv = b"price,label\n1.5,alpha\n2.25,beta\n";
        let parquet_bytes = convert_csv_to_parquet(csv).expect("conversion succeeds");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(parquet_bytes),
        )
        .expect("parquet opens")
        .build()
        .expect("reader builds");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("batches read");
        let rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    #[ignore = "requires AWS credentials and network access"]
    async fn s3_round_trip_smoke() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        let store = S3ColdStore::new(database);
        let key = format!("tests/round-trip-{}.txt", uuid::Uuid::new_v4());
        let payload = b"firmament cold storage smoke test".to_vec();
        let sha256 = crate::files::sha256_hex(&payload);
        store
            .put_object(
                &key,
                payload.clone(),
                &ColdObjectMetadata {
                    sha256: sha256.clone(),
                    source: "smoke.txt".to_owned(),
                },
            )
            .await
            .expect("put succeeds");
        let head = store
            .head_object(&key)
            .await
            .expect("head succeeds")
            .expect("object exists");
        assert_eq!(head.size, payload.len() as u64);
        assert_eq!(head.sha256.as_deref(), Some(sha256.as_str()));
        let object = store
            .get_object(&key)
            .await
            .expect("get succeeds")
            .expect("object exists");
        let bytes = object
            .body
            .collect()
            .await
            .expect("body collects")
            .into_bytes();
        assert_eq!(bytes.as_ref(), payload.as_slice());

        // Clean up with a direct client; the trait deliberately omits deletion.
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(
                crate::db::DEFAULT_COLD_REGION,
            ))
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&config);
        client
            .delete_object()
            .bucket(crate::db::DEFAULT_COLD_BUCKET)
            .key(&key)
            .send()
            .await
            .expect("cleanup delete succeeds");
    }
}

use chrono::Utc;
use std::path::Path;

use crate::cold::{
    ColdObjectMetadata, ColdStore, convert_csv_to_parquet, object_key, parquet_path_for,
};
use crate::db::{ArchiveCounts, Database};
use crate::files::{ScannedFile, scan_directory, sha256_hex};

/// Runs one archive job to completion; per-file failures are recorded on the job row.
pub async fn run_archive_job(
    database: &Database,
    cold: &dyn ColdStore,
    dataset_id: &str,
    job_id: &str,
) {
    let directory = database.dataset_directory(dataset_id);
    let files = match scan_directory(&directory) {
        Ok(files) => files,
        Err(error) => {
            let _ = database.finish_archive_job(
                job_id,
                "failed",
                ArchiveCounts::default(),
                &format!("dataset scan failed: {error}"),
            );
            return;
        }
    };
    let mut counts = ArchiveCounts {
        total_files: files.len() as i64,
        ..ArchiveCounts::default()
    };
    let mut first_error: Option<String> = None;
    let _ = database.update_archive_job_progress(job_id, counts);
    for file in files {
        match archive_file(database, cold, dataset_id, &directory, &file).await {
            Ok(true) => counts.archived_files += 1,
            Ok(false) => counts.skipped_files += 1,
            Err(error) => {
                counts.failed_files += 1;
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        let _ = database.update_archive_job_progress(job_id, counts);
    }
    let status = if counts.archived_files == 0 && counts.failed_files > 0 {
        "failed"
    } else {
        "completed"
    };
    let message = first_error.unwrap_or_default();
    let _ = database.finish_archive_job(job_id, status, counts, &message);
}

async fn archive_file(
    database: &Database,
    cold: &dyn ColdStore,
    dataset_id: &str,
    dataset_directory: &Path,
    file: &ScannedFile,
) -> Result<bool, String> {
    let Some(target) = parquet_path_for(&file.relative) else {
        return Ok(false);
    };
    let bytes = std::fs::read(&file.absolute)
        .map_err(|error| format!("read {}: {error}", file.relative))?;
    let payload = if file.relative.to_ascii_lowercase().ends_with(".csv") {
        convert_csv_to_parquet(&bytes)
            .map_err(|error| format!("convert {}: {error}", file.relative))?
    } else {
        bytes
    };
    let sha256 = sha256_hex(&payload);
    let key = object_key(dataset_id, &target);
    if let Some(existing) = database
        .cold_file(dataset_id, &target)
        .map_err(|error| error.to_string())?
        && existing.sha256 == sha256
        && cold
            .head_object(&key)
            .await
            .map_err(|error| format!("verify {}: {error}", file.relative))?
            .is_some()
    {
        remove_hot_copy(dataset_directory, &file.absolute)?;
        return Ok(false);
    }
    let size = payload.len() as u64;
    cold.put_object(
        &key,
        payload,
        &ColdObjectMetadata {
            sha256: sha256.clone(),
            source: file.relative.clone(),
        },
    )
    .await
    .map_err(|error| format!("upload {}: {error}", file.relative))?;
    let head = cold
        .head_object(&key)
        .await
        .map_err(|error| format!("verify {}: {error}", file.relative))?
        .ok_or_else(|| format!("verify {}: uploaded object is missing", file.relative))?;
    if head.size != size || head.sha256.as_deref() != Some(sha256.as_str()) {
        return Err(format!("verify {}: checksum mismatch", file.relative));
    }
    database
        .upsert_cold_file(
            dataset_id,
            &target,
            size,
            &sha256,
            &file.relative,
            Utc::now().timestamp(),
        )
        .map_err(|error| error.to_string())?;
    remove_hot_copy(dataset_directory, &file.absolute)?;
    Ok(true)
}

fn remove_hot_copy(dataset_directory: &Path, absolute: &Path) -> Result<(), String> {
    std::fs::remove_file(absolute)
        .map_err(|error| format!("remove {}: {error}", absolute.display()))?;
    prune_empty_directories(dataset_directory, absolute.parent());
    Ok(())
}

fn prune_empty_directories(root: &Path, start: Option<&Path>) {
    let mut current = start;
    while let Some(directory) = current {
        if directory == root || !directory.starts_with(root) {
            break;
        }
        let empty = std::fs::read_dir(directory)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false);
        if !empty || std::fs::remove_dir(directory).is_err() {
            break;
        }
        current = directory.parent();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use aws_sdk_s3::primitives::ByteStream;
    use tempfile::TempDir;

    use super::run_archive_job;
    use crate::cold::{ColdError, ColdObjectBody, ColdObjectHead, ColdObjectMetadata, ColdStore};
    use crate::db::Database;

    #[derive(Default)]
    struct FakeColdStore {
        objects: Mutex<HashMap<String, (Vec<u8>, String)>>,
        uploads: Mutex<usize>,
    }

    impl FakeColdStore {
        fn upload_count(&self) -> usize {
            *self.uploads.lock().expect("uploads lock")
        }

        fn object(&self, key: &str) -> Option<(Vec<u8>, String)> {
            self.objects.lock().expect("objects lock").get(key).cloned()
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
            *self.uploads.lock().expect("uploads lock") += 1;
            self.objects
                .lock()
                .expect("objects lock")
                .insert(key.to_owned(), (bytes, metadata.sha256.clone()));
            Ok(())
        }

        async fn head_object(&self, key: &str) -> Result<Option<ColdObjectHead>, ColdError> {
            Ok(self.object(key).map(|(bytes, sha256)| ColdObjectHead {
                size: bytes.len() as u64,
                sha256: Some(sha256),
            }))
        }

        async fn get_object(&self, key: &str) -> Result<Option<ColdObjectBody>, ColdError> {
            Ok(self.object(key).map(|(bytes, _)| ColdObjectBody {
                size: bytes.len() as u64,
                body: ByteStream::from(bytes),
            }))
        }
    }

    const KLINES_CSV: &str = "timestamp,open,high,low,close,volume\n\
        2026-09-03T00:00:00Z,78400.1,78420.5,78390.0,78410.2,112.5\n\
        2026-09-03T00:01:00Z,78410.2,78450.0,78405.7,78448.3,98.2\n\
        2026-09-03T00:02:00Z,78448.3,78455.9,78420.0,78425.1,76.4\n";
    const FUNDING_CSV: &str = "timestamp,funding_rate\n\
        2026-09-01T00:00:00Z,0.000010\n\
        2026-09-01T08:00:00Z,0.000025\n";

    fn seeded_database(state: &TempDir) -> Database {
        let database = Database::open(state.path()).expect("database opens");
        database
            .create_dataset("okx-btc-swap", "OKX BTC 合约", "测试")
            .expect("dataset creates");
        let directory = database.dataset_directory("okx-btc-swap");
        std::fs::create_dir_all(directory.join("klines-1m")).expect("klines directory");
        std::fs::create_dir_all(directory.join("funding-rate")).expect("funding directory");
        std::fs::write(directory.join("klines-1m/2026-09-03.csv"), KLINES_CSV).expect("klines");
        std::fs::write(directory.join("funding-rate/2026-09.csv"), FUNDING_CSV).expect("funding");
        std::fs::write(directory.join("README.md"), "非表格文件").expect("readme");
        database
    }

    #[tokio::test]
    async fn archive_converts_tabular_files_and_records_the_cold_catalog() {
        let state = TempDir::new().expect("state directory");
        let database = seeded_database(&state);
        let cold = FakeColdStore::default();
        let job = database
            .create_archive_job_if_idle("okx-btc-swap")
            .expect("job creates")
            .expect("idle");
        run_archive_job(&database, &cold, "okx-btc-swap", &job.id).await;

        let latest = database
            .latest_archive_job("okx-btc-swap")
            .expect("latest")
            .expect("exists");
        assert_eq!(latest.status, "completed");
        assert_eq!(latest.total_files, 3);
        assert_eq!(latest.archived_files, 2);
        assert_eq!(latest.skipped_files, 1);
        assert_eq!(latest.failed_files, 0);

        let directory = database.dataset_directory("okx-btc-swap");
        assert!(!directory.join("klines-1m/2026-09-03.csv").exists());
        assert!(!directory.join("funding-rate/2026-09.csv").exists());
        assert!(
            directory.join("README.md").exists(),
            "non-tabular files stay hot"
        );
        assert!(
            !directory.join("klines-1m").exists(),
            "empty directories are pruned"
        );

        let cold_files = database
            .list_cold_files("okx-btc-swap")
            .expect("cold files");
        let paths: Vec<_> = cold_files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "funding-rate/2026-09.parquet",
                "klines-1m/2026-09-03.parquet"
            ]
        );

        let object = cold
            .object("datasets/okx-btc-swap/klines-1m/2026-09-03.parquet")
            .expect("parquet object");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(object.0),
        )
        .expect("parquet opens")
        .build()
        .expect("reader builds");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("batches");
        let rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3, "every CSV row survives the conversion");
        assert_eq!(cold.upload_count(), 2);
    }

    #[tokio::test]
    async fn republishing_a_matching_file_skips_the_upload() {
        let state = TempDir::new().expect("state directory");
        let database = seeded_database(&state);
        let cold = FakeColdStore::default();
        let job = database
            .create_archive_job_if_idle("okx-btc-swap")
            .expect("job creates")
            .expect("idle");
        run_archive_job(&database, &cold, "okx-btc-swap", &job.id).await;

        // The publisher republishes identical bytes; the archive recognizes the
        // stored checksum and only clears the hot copy.
        let directory = database.dataset_directory("okx-btc-swap");
        std::fs::create_dir_all(directory.join("klines-1m")).expect("klines directory");
        std::fs::write(directory.join("klines-1m/2026-09-03.csv"), KLINES_CSV).expect("klines");
        let job = database
            .create_archive_job_if_idle("okx-btc-swap")
            .expect("job creates")
            .expect("idle");
        run_archive_job(&database, &cold, "okx-btc-swap", &job.id).await;

        let latest = database
            .latest_archive_job("okx-btc-swap")
            .expect("latest")
            .expect("exists");
        assert_eq!(latest.status, "completed");
        assert_eq!(latest.archived_files, 0);
        assert_eq!(
            latest.skipped_files, 2,
            "readme and the matching file are skipped"
        );
        assert_eq!(cold.upload_count(), 2, "no redundant upload happens");
        assert!(!directory.join("klines-1m/2026-09-03.csv").exists());
    }
}

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

use std::{
    io,
    path::{Path, PathBuf},
};

use chrono::Utc;
use serde::Serialize;
use sysinfo::{CpuRefreshKind, Disks, System};

#[derive(Debug, Serialize)]
pub struct SystemResourcesSnapshot {
    pub sampled_at: i64,
    pub cpu: CpuSnapshot,
    pub memory: MemorySnapshot,
    pub disk: Option<DiskSnapshot>,
    pub sqlite: SqliteSnapshot,
}

#[derive(Debug, Serialize)]
pub struct CpuSnapshot {
    pub usage_percent: f32,
    pub load_1m: f64,
    pub logical_cpus: usize,
}

#[derive(Debug, Serialize)]
pub struct MemorySnapshot {
    pub used_bytes: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct DiskSnapshot {
    pub mount_point: String,
    pub used_bytes: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct SqliteSnapshot {
    pub main_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug)]
pub struct ResourceMonitor {
    database_path: PathBuf,
    system: System,
    disks: Disks,
}

impl ResourceMonitor {
    pub fn new(database_path: PathBuf) -> Self {
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_list(CpuRefreshKind::nothing().with_cpu_usage());
        Self {
            database_path,
            system,
            disks: Disks::new_with_refreshed_list(),
        }
    }

    pub fn sample(&mut self) -> Result<SystemResourcesSnapshot, io::Error> {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.disks.refresh(true);
        let memory_used = self.system.used_memory();
        let memory_total = self.system.total_memory();

        Ok(SystemResourcesSnapshot {
            sampled_at: Utc::now().timestamp(),
            cpu: CpuSnapshot {
                usage_percent: self.system.global_cpu_usage(),
                load_1m: System::load_average().one,
                logical_cpus: self.system.cpus().len(),
            },
            memory: MemorySnapshot {
                used_bytes: memory_used,
                total_bytes: memory_total,
                available_bytes: self.system.available_memory(),
            },
            disk: disk_usage(&self.disks, &self.database_path),
            sqlite: sqlite_file_usage(&self.database_path)?,
        })
    }
}

fn disk_usage(disks: &Disks, database_path: &Path) -> Option<DiskSnapshot> {
    let disk = disks
        .list()
        .iter()
        .filter(|disk| database_path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())?;
    let total = disk.total_space();
    let available = disk.available_space();
    Some(DiskSnapshot {
        mount_point: disk.mount_point().to_string_lossy().into_owned(),
        used_bytes: total.saturating_sub(available),
        total_bytes: total,
        available_bytes: available,
    })
}

fn sqlite_file_usage(database_path: &Path) -> Result<SqliteSnapshot, io::Error> {
    let main_bytes = file_size(database_path)?;
    let wal_bytes = file_size(&with_suffix(database_path, "-wal"))?;
    let shm_bytes = file_size(&with_suffix(database_path, "-shm"))?;
    Ok(SqliteSnapshot {
        main_bytes,
        wal_bytes,
        shm_bytes,
        total_bytes: main_bytes
            .saturating_add(wal_bytes)
            .saturating_add(shm_bytes),
    })
}

fn file_size(path: &Path) -> Result<u64, io::Error> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{sqlite_file_usage, with_suffix};

    #[test]
    fn sqlite_file_usage_includes_wal_and_shared_memory_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let database_path = directory.path().join("default.sqlite3");
        let connection = Connection::open(&database_path)?;
        connection
            .execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE samples(value INTEGER);")?;
        drop(connection);
        fs::write(with_suffix(&database_path, "-wal"), [0_u8; 7])?;
        fs::write(with_suffix(&database_path, "-shm"), [0_u8; 3])?;

        let usage = sqlite_file_usage(&database_path)?;

        assert!(usage.main_bytes > 0);
        assert_eq!(usage.wal_bytes, 7);
        assert_eq!(usage.shm_bytes, 3);
        assert_eq!(usage.total_bytes, usage.main_bytes + 10);
        Ok(())
    }
}

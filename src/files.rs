use std::path::{Component, Path as FsPath, PathBuf};

use sha2::{Digest, Sha256};
use uuid::Uuid;

pub struct ScannedFile {
    pub relative: String,
    pub absolute: PathBuf,
    pub size: u64,
    pub updated_at: i64,
}

/// Scans a dataset directory into a sorted list of visible files.
///
/// # Errors
///
/// Returns an error when the directory tree cannot be read.
pub fn scan_directory(root: &FsPath) -> std::io::Result<Vec<ScannedFile>> {
    let mut files = Vec::new();
    collect_directory(root, root, &mut files)?;
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(files)
}

fn collect_directory(
    root: &FsPath,
    directory: &FsPath,
    files: &mut Vec<ScannedFile>,
) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_directory(root, &path, files)?;
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let updated_at = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_secs() as i64);
        files.push(ScannedFile {
            relative,
            absolute: path,
            size: metadata.len(),
            updated_at,
        });
    }
    Ok(())
}

/// Normalizes a user supplied relative path into a safe filesystem path.
pub fn normalize_relative_path(path: &str) -> Option<PathBuf> {
    let mut clean = PathBuf::new();
    for component in FsPath::new(path).components() {
        match component {
            Component::Normal(part) => clean.push(part),
            _ => return None,
        }
    }
    if clean.as_os_str().is_empty() {
        return None;
    }
    Some(clean)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Normalizes a publish path: safe components only, and no hidden segments —
/// dot-prefixed files would be skipped by scans and stay invisible to readers.
pub fn normalize_publish_path(path: &str) -> Option<PathBuf> {
    let clean = normalize_relative_path(path)?;
    let hidden = clean
        .components()
        .any(|component| component.as_os_str().to_string_lossy().starts_with('.'));
    if hidden {
        return None;
    }
    Some(clean)
}

/// Writes bytes under the dataset directory through a hidden temporary file and
/// a rename, so readers never observe a half-written publish.
///
/// # Errors
///
/// Returns an error when the parent directory, temporary file or rename fails.
pub fn write_file_atomically(path: &FsPath, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(FsPath::new("."));
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_owned());
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4().simple()));
    std::fs::write(&temporary, bytes)?;
    if let Err(error) = std::fs::rename(&temporary, path) {
        // RECOVERY: drop the temporary copy so a failed publish leaves no debris.
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::{normalize_publish_path, sha256_hex, write_file_atomically};

    #[test]
    fn publish_paths_reject_traversal_and_hidden_segments() {
        assert_eq!(
            normalize_publish_path("klines-1m/2026-09-03.csv").as_deref(),
            Some(Path::new("klines-1m/2026-09-03.csv"))
        );
        assert_eq!(normalize_publish_path("../escape.csv"), None);
        assert_eq!(normalize_publish_path("klines-1m/../../escape.csv"), None);
        assert_eq!(normalize_publish_path(".hidden/file.csv"), None);
        assert_eq!(normalize_publish_path("dir/.hidden.csv"), None);
        assert_eq!(normalize_publish_path("/absolute.csv"), None);
        assert_eq!(normalize_publish_path(""), None);
    }

    #[test]
    fn atomic_writes_create_directories_and_replace_files() {
        let state = TempDir::new().expect("state directory");
        let target = state.path().join("klines-1m/2026-09-03.csv");

        write_file_atomically(&target, b"first").expect("initial write");
        assert_eq!(std::fs::read(&target).expect("read"), b"first");

        write_file_atomically(&target, b"second").expect("replacement write");
        assert_eq!(std::fs::read(&target).expect("read"), b"second");

        let entries = std::fs::read_dir(target.parent().expect("parent"))
            .expect("directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        assert_eq!(entries.len(), 1, "no temporary files are left behind");
    }

    #[test]
    fn sha256_matches_known_vectors() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}

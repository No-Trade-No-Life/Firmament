use std::path::{Component, Path as FsPath, PathBuf};

use sha2::{Digest, Sha256};

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
    format!("{:x}", Sha256::digest(bytes))
}

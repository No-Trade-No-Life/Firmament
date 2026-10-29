//! Small byte helpers shared by the token, cold-file and audit paths.

use sha2::{Digest, Sha256};

/// Lowercase hexadecimal rendering of arbitrary bytes.
pub fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

/// The SHA-256 of a payload, rendered as lowercase hexadecimal.
pub fn sha256_hex(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex(&hasher.finalize())
}

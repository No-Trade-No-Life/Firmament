use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, OsRng, rand_core::RngCore};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct Cipher {
    key: [u8; 32],
}

#[derive(Debug, Error)]
pub enum CipherError {
    #[error("failed to read credential key")]
    Read(#[source] std::io::Error),
    #[error("failed to write credential key")]
    Write(#[source] std::io::Error),
    #[error("credential key is malformed")]
    MalformedKey,
    #[error("credential cipher text is malformed")]
    MalformedCiphertext,
    #[error("credential encryption failed")]
    Encrypt,
    #[error("credential decryption failed")]
    Decrypt,
    #[error("credential plaintext is not UTF-8")]
    Utf8(#[from] std::string::FromUtf8Error),
}

impl Cipher {
    /// Loads the host-local credential key or makes one readable only by the service user.
    ///
    /// # Errors
    ///
    /// Returns an error when the key cannot be created, read or decoded.
    pub fn load_or_create(state_directory: &Path) -> Result<Self, CipherError> {
        let path = state_directory.join("credential.key");
        if path.exists() {
            let mut encoded = String::new();
            File::open(path)
                .map_err(CipherError::Read)?
                .read_to_string(&mut encoded)
                .map_err(CipherError::Read)?;
            let key = URL_SAFE_NO_PAD
                .decode(encoded.trim())
                .map_err(|_| CipherError::MalformedKey)?;
            return key
                .try_into()
                .map(|key| Self { key })
                .map_err(|_| CipherError::MalformedKey);
        }

        let mut key = [0_u8; 32];
        OsRng.fill_bytes(&mut key);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(CipherError::Write)?;
        file.write_all(URL_SAFE_NO_PAD.encode(key).as_bytes())
            .map_err(CipherError::Write)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(CipherError::Write)?;
        }
        Ok(Self { key })
    }

    /// Encrypts a credential JSON document with a random AES-GCM nonce.
    pub fn encrypt(&self, plaintext: &str) -> Result<String, CipherError> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| CipherError::Encrypt)?;
        let mut nonce = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| CipherError::Encrypt)?;
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(nonce),
            URL_SAFE_NO_PAD.encode(ciphertext)
        ))
    }

    /// Decrypts one credential JSON document.
    pub fn decrypt(&self, encoded: &str) -> Result<String, CipherError> {
        let Some((nonce, ciphertext)) = encoded.split_once('.') else {
            return Err(CipherError::MalformedCiphertext);
        };
        let nonce = URL_SAFE_NO_PAD
            .decode(nonce)
            .map_err(|_| CipherError::MalformedCiphertext)?;
        let nonce: [u8; 12] = nonce
            .try_into()
            .map_err(|_| CipherError::MalformedCiphertext)?;
        let ciphertext = URL_SAFE_NO_PAD
            .decode(ciphertext)
            .map_err(|_| CipherError::MalformedCiphertext)?;
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| CipherError::Decrypt)?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| CipherError::Decrypt)?;
        String::from_utf8(plaintext).map_err(CipherError::Utf8)
    }
}

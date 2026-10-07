#![forbid(unsafe_code)]

mod archive;
mod cold;
mod crypto;
mod db;
mod files;
mod resources;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use auth_mini_axum::{AuthMiniLayer, JwksCachePolicy};
use tokio::sync::Mutex;

use db::Database;
use resources::ResourceMonitor;

#[derive(Clone, Debug)]
pub struct App {
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpServerError {
    #[error("failed to prepare Firmament data directory")]
    DataDirectory(#[source] std::io::Error),
    #[error("failed to initialize Firmament database")]
    Database(#[from] db::DatabaseError),
    #[error("failed to initialize Auth Mini verification")]
    Auth(#[from] auth_mini_axum::AuthMiniError),
    #[error("HTTP server I/O error")]
    Io(#[from] std::io::Error),
}

impl App {
    /// Creates Firmament from its deliberately small, user-home based runtime state.
    ///
    /// # Errors
    ///
    /// Returns an error when the home directory, encryption key or `SQLite` state cannot be used.
    pub fn from_home() -> Result<Self, HttpServerError> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let state_directory = home.join(".firmament");
        std::fs::create_dir_all(&state_directory).map_err(HttpServerError::DataDirectory)?;
        let database = Database::open(&state_directory)?;
        let resources = Arc::new(Mutex::new(ResourceMonitor::new(database.database_path())));
        Ok(Self {
            database,
            resources,
        })
    }

    /// Serves the embedded web application and its API.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket cannot bind or the server exits unexpectedly.
    pub async fn serve(self, address: SocketAddr) -> Result<(), HttpServerError> {
        let auth = AuthMiniLayer::from_issuer_background(
            "https://auth.ntnl.io",
            "firma.ntnl.io",
            JwksCachePolicy::default(),
        )?;
        let app = web::router(self.database, self.resources, auth);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

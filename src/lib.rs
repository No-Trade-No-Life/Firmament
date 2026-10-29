#![forbid(unsafe_code)]

mod cold;
mod cooling;
mod crypto;
mod db;
mod files;
mod flush;
mod hot;
mod linkit;
mod resources;
mod schema;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use auth_mini_axum::{AuthMiniLayer, JwksCachePolicy};
use chrono::Utc;
use tokio::sync::Mutex;

use cold::{ColdStore, S3ColdStore};
use db::Database;
use resources::ResourceMonitor;

/// How often the sweeper looks for rows that cooled out of the current period.
const COOLING_INTERVAL: Duration = Duration::from_secs(60);

/// A failed pass is retried only after this long, so a broken bucket does not
/// turn into a busy loop.
const FAILURE_BACKOFF_SECONDS: i64 = 600;

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
    /// Returns an error when the home directory, encryption key or state
    /// databases cannot be used.
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
        let cold: Arc<dyn ColdStore> = Arc::new(S3ColdStore::new(self.database.clone()));
        spawn_sweeper(self.database.clone(), Arc::clone(&cold));
        let app = web::router(self.database, self.resources, cold, auth);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// The sweeper is the only automatic mover of data: it advances every dataset's
// cooling boundary and lets the pass do the rest. It never pushes a dataset that
// still has a running pass or that failed moments ago.
fn spawn_sweeper(database: Database, cold: Arc<dyn ColdStore>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(COOLING_INTERVAL);
        loop {
            ticker.tick().await;
            sweep_once(&database, cold.as_ref()).await;
        }
    });
}

async fn sweep_once(database: &Database, cold: &dyn ColdStore) {
    let datasets = match database.list_datasets() {
        Ok(datasets) => datasets,
        Err(error) => {
            eprintln!("firmament: dataset listing failed: {error}");
            return;
        }
    };
    let now = Utc::now();
    for dataset in datasets {
        match database.latest_flush(&dataset.id) {
            Ok(Some(flush)) if flush.status == "running" => continue,
            Ok(Some(flush))
                if flush.status == "failed"
                    && now.timestamp() - flush.created_at < FAILURE_BACKOFF_SECONDS =>
            {
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!(
                    "firmament: {} flush history unreadable: {error}",
                    dataset.id
                );
                continue;
            }
        }
        let boundary = match cooling::boundary(now, &dataset.cooling) {
            Ok(boundary) => boundary.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            Err(message) => {
                eprintln!(
                    "firmament: {} cooling calendar invalid: {message}",
                    dataset.id
                );
                continue;
            }
        };
        let coolable = match database
            .hot()
            .coolable_rows(&dataset.id, &dataset.table, &boundary)
        {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("firmament: {} hot data unreadable: {error}", dataset.id);
                continue;
            }
        };
        if coolable == 0 {
            continue;
        }
        match flush::run_flush(database, cold, &dataset.id, "schedule").await {
            Ok(report) => eprintln!(
                "firmament: {} flushed batch {} ({} rows, {} files)",
                dataset.id, report.batch, report.rows, report.files
            ),
            Err(message) => eprintln!("firmament: {} cooling failed: {message}", dataset.id),
        }
    }
}

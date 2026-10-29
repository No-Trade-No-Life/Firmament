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
use tokio::sync::{Mutex, Notify};

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
        let wake = Arc::new(Notify::new());
        spawn_cooling(self.database.clone(), Arc::clone(&cold), Arc::clone(&wake));
        let app = web::router(self.database, self.resources, cold, auth, wake);
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// Cooling gets its own OS thread and a single-threaded runtime: a pass does
// blocking SQLite work and Parquet encoding, and the HTTP runtime has only two
// workers on the production host.
fn spawn_cooling(database: Database, cold: Arc<dyn ColdStore>, wake: Arc<Notify>) {
    let spawned = std::thread::Builder::new()
        .name("firmament-cooling".to_owned())
        .spawn(move || {
            // I/O is not optional: a pass talks to S3. A runtime without the
            // I/O driver accepts the request and then never makes progress.
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("firmament: cooling runtime failed: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                loop {
                    tokio::select! {
                        () = tokio::time::sleep(COOLING_INTERVAL) => {}
                        () = wake.notified() => {}
                    }
                    sweep_once(&database, cold.as_ref()).await;
                }
            });
        });
    if let Err(error) = spawned {
        eprintln!("firmament: cooling thread failed: {error}");
    }
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
        // A pass that died left claims behind. They are invisible to the
        // coolable count, so without releasing them first a crash would look
        // exactly like "nothing to cool" and the rows would never move again.
        if let Err(error) = database.hot().release_stale_claims(&dataset.id, 0) {
            eprintln!("firmament: {} stale claims unreadable: {error}", dataset.id);
            continue;
        }
        let boundary = cooling::boundary(now, dataset.cadence)
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
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
        match flush::run_pass(database, cold, &dataset.id, "schedule").await {
            Ok(report) => eprintln!(
                "firmament: {} pass {} rewrote {} partitions ({} rows)",
                dataset.id, report.pass, report.partitions, report.rows
            ),
            Err(message) => eprintln!("firmament: {} cooling failed: {message}", dataset.id),
        }
    }
}

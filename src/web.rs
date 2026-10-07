use std::path::{Component, Path as FsPath, PathBuf};
use std::sync::Arc;

use auth_mini_axum::{AuthMiniLayer, AuthMiniPrincipal};
use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::db::{Database, DatabaseError, LinkitSettings, Syncer};
use crate::resources::{ResourceMonitor, SystemResourcesSnapshot};

#[derive(Clone)]
struct AppState {
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
}

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct WebAssets;

pub fn router(
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    auth: AuthMiniLayer,
) -> Router {
    let state = AppState {
        database,
        resources,
    };
    let private = Router::new()
        .route("/me", get(me))
        .route("/setup", post(setup_root))
        .route("/datasets", get(list_datasets))
        .route("/datasets/{id}/manifest", get(dataset_manifest))
        .route("/datasets/{id}/files/{*path}", get(dataset_file))
        .route("/syncers", get(list_syncers).post(create_syncer))
        .route("/syncers/{id}", delete(remove_syncer))
        .route("/syncers/{id}/completed", post(mark_syncer_completed))
        .route("/linkit", get(get_linkit).put(put_linkit))
        .route("/linkit/test", post(test_linkit))
        .route("/system/resources", get(system_resources))
        .route_layer(auth);
    Router::new()
        .route("/api/health", get(health))
        .nest("/api/v1", private)
        .fallback(static_asset)
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(cors())
}

fn cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers(Any)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "firmament", "version": env!("CARGO_PKG_VERSION")}))
}

#[derive(Debug, Serialize)]
struct Me {
    user_id: String,
    is_root: bool,
    setup_required: bool,
}

async fn me(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Me>, ApiError> {
    let root_user_id = state.database.root_user_id()?;
    Ok(Json(Me {
        user_id: principal.subject.clone(),
        is_root: root_user_id.as_deref() == Some(&principal.subject),
        setup_required: root_user_id.is_none(),
    }))
}

async fn setup_root(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Me>, ApiError> {
    let root_user_id = state.database.root_user_id()?;
    if let Some(root_user_id) = root_user_id {
        if root_user_id != principal.subject {
            return Err(ApiError::forbidden(
                "root user has already been initialized",
            ));
        }
    } else {
        state.database.initialize_root_user(&principal.subject)?;
    }
    Ok(Json(Me {
        user_id: principal.subject,
        is_root: true,
        setup_required: false,
    }))
}

struct Actor {
    user_id: String,
    is_root: bool,
}

impl Actor {
    fn from_principal(
        database: &Database,
        principal: &AuthMiniPrincipal,
    ) -> Result<Self, ApiError> {
        let root_user_id = database.root_user_id()?;
        Ok(Self {
            user_id: principal.subject.clone(),
            is_root: root_user_id.as_deref() == Some(&principal.subject),
        })
    }

    fn assert_root(&self) -> Result<(), ApiError> {
        if self.is_root {
            return Ok(());
        }
        Err(ApiError::forbidden("root user is required"))
    }

    fn assert_owner(&self, owner_id: &str) -> Result<(), ApiError> {
        if self.is_root || self.user_id == owner_id {
            return Ok(());
        }
        Err(ApiError::forbidden("resource belongs to another user"))
    }
}

#[derive(Debug, Serialize)]
struct DatasetView {
    id: String,
    name: String,
    description: String,
    tier: String,
    file_count: usize,
    bytes: u64,
    updated_at: i64,
}

async fn list_datasets(State(state): State<AppState>) -> Result<Json<Vec<DatasetView>>, ApiError> {
    let datasets = state.database.list_datasets()?;
    let mut views = Vec::with_capacity(datasets.len());
    for dataset in datasets {
        let files = scan_directory(&state.database.dataset_directory(&dataset.id))?;
        let bytes = files.iter().map(|file| file.size).sum();
        let updated_at = files
            .iter()
            .map(|file| file.updated_at)
            .max()
            .unwrap_or(dataset.created_at);
        views.push(DatasetView {
            id: dataset.id,
            name: dataset.name,
            description: dataset.description,
            tier: dataset.tier,
            file_count: files.len(),
            bytes,
            updated_at,
        });
    }
    Ok(Json(views))
}

#[derive(Debug, Serialize)]
struct DatasetManifestFileView {
    path: String,
    size: u64,
    sha256: String,
    updated_at: i64,
}

#[derive(Debug, Serialize)]
struct DatasetManifestView {
    dataset_id: String,
    generated_at: i64,
    files: Vec<DatasetManifestFileView>,
}

async fn dataset_manifest(
    State(state): State<AppState>,
    Path(dataset_id): Path<String>,
) -> Result<Json<DatasetManifestView>, ApiError> {
    let dataset = state
        .database
        .dataset(&dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let files = scan_directory(&state.database.dataset_directory(&dataset.id))?
        .into_iter()
        .map(|file| -> Result<DatasetManifestFileView, ApiError> {
            let bytes = std::fs::read(&file.absolute).map_err(|error| ApiError::files(&error))?;
            Ok(DatasetManifestFileView {
                path: file.relative,
                size: file.size,
                sha256: sha256_hex(&bytes),
                updated_at: file.updated_at,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(DatasetManifestView {
        dataset_id: dataset.id,
        generated_at: Utc::now().timestamp(),
        files,
    }))
}

async fn dataset_file(
    State(state): State<AppState>,
    Path((dataset_id, path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let dataset = state
        .database
        .dataset(&dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let relative =
        normalize_relative_path(&path).ok_or_else(|| ApiError::bad_request("invalid file path"))?;
    let absolute = state
        .database
        .dataset_directory(&dataset.id)
        .join(&relative);
    let bytes = match std::fs::read(&absolute) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ApiError::not_found());
        }
        Err(error) => return Err(ApiError::files(&error)),
    };
    let mime = mime_guess::from_path(&absolute).first_or_octet_stream();
    Ok(([(header::CONTENT_TYPE, mime.as_ref())], Body::from(bytes)).into_response())
}

#[derive(Debug, Deserialize)]
struct SyncerInput {
    name: String,
    dataset_id: String,
    subdir: Option<String>,
}

async fn list_syncers(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Vec<Syncer>>, ApiError> {
    let actor = Actor::from_principal(&state.database, &principal)?;
    let owner_filter = if actor.is_root {
        None
    } else {
        Some(actor.user_id.as_str())
    };
    Ok(Json(state.database.list_syncers(owner_filter)?))
}

async fn create_syncer(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<SyncerInput>,
) -> Result<(StatusCode, Json<Syncer>), ApiError> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("syncer name is required"));
    }
    if state.database.dataset(&input.dataset_id)?.is_none() {
        return Err(ApiError::not_found());
    }
    let subdir = match input
        .subdir
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => normalize_relative_path(value)
            .ok_or_else(|| ApiError::bad_request("invalid subdir"))?
            .to_string_lossy()
            .replace('\\', "/"),
        None => String::new(),
    };
    let syncer =
        state
            .database
            .create_syncer(&principal.subject, name, &input.dataset_id, &subdir)?;
    Ok((StatusCode::CREATED, Json(syncer)))
}

async fn remove_syncer(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let actor = Actor::from_principal(&state.database, &principal)?;
    let syncer = state
        .database
        .get_syncer(&id)?
        .ok_or_else(ApiError::not_found)?;
    actor.assert_owner(&syncer.owner_id)?;
    state.database.delete_syncer(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn mark_syncer_completed(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
) -> Result<Json<Syncer>, ApiError> {
    let actor = Actor::from_principal(&state.database, &principal)?;
    let syncer = state
        .database
        .get_syncer(&id)?
        .ok_or_else(ApiError::not_found)?;
    actor.assert_owner(&syncer.owner_id)?;
    let updated = state
        .database
        .mark_syncer_synced(&id, Utc::now().timestamp())?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(updated))
}

async fn get_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Option<LinkitSettings>>, ApiError> {
    Ok(Json(state.database.linkit_settings(&principal.subject)?))
}

#[derive(Debug, Deserialize)]
struct LinkitInput {
    recipient_username: String,
    bot_token: String,
}

async fn put_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<LinkitInput>,
) -> Result<Json<LinkitSettings>, ApiError> {
    let recipient_username = input.recipient_username.trim();
    let bot_token = input.bot_token.trim();
    if recipient_username.is_empty() || bot_token.is_empty() {
        return Err(ApiError::bad_request(
            "recipient_username and a Linkit sk- token are required",
        ));
    }
    Ok(Json(state.database.put_linkit_settings(
        &principal.subject,
        recipient_username,
        bot_token,
    )?))
}

async fn test_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Value>, ApiError> {
    let settings = state
        .database
        .secret_linkit_settings(&principal.subject)?
        .ok_or_else(|| ApiError::bad_request("Linkit notifications are not configured"))?;
    let response = reqwest::Client::new()
        .post("https://linkit.ntnl.io/bot/v1/messages")
        .bearer_auth(&settings.bot_token)
        .json(&json!({
            "recipient_username": settings.recipient_username,
            "body": "Firmament 测试消息：通知通道正常。",
        }))
        .send()
        .await
        .map_err(|error| ApiError::internal(format!("Linkit request failed: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::internal(format!(
            "Linkit responded {status}: {body}"
        )));
    }
    Ok(Json(json!({"sent": true})))
}

async fn system_resources(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<SystemResourcesSnapshot>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let snapshot = state
        .resources
        .lock()
        .await
        .sample()
        .map_err(|error| ApiError::files(&error))?;
    Ok(Json(snapshot))
}

async fn static_asset(uri: axum::extract::OriginalUri) -> Response {
    let path = uri.0.path().trim_start_matches('/');
    if path.starts_with("api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = if path.is_empty() { "index.html" } else { path };
    let asset = WebAssets::get(path).or_else(|| WebAssets::get("index.html"));
    let Some(asset) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    (
        [(header::CONTENT_TYPE, mime.as_ref())],
        Body::from(asset.data.into_owned()),
    )
        .into_response()
}

struct ScannedFile {
    relative: String,
    absolute: PathBuf,
    size: u64,
    updated_at: i64,
}

fn scan_directory(root: &FsPath) -> Result<Vec<ScannedFile>, ApiError> {
    let mut files = Vec::new();
    collect_directory(root, root, &mut files)?;
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(files)
}

fn collect_directory(
    root: &FsPath,
    directory: &FsPath,
    files: &mut Vec<ScannedFile>,
) -> Result<(), ApiError> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(ApiError::files(&error)),
    };
    for entry in entries {
        let entry = entry.map_err(|error| ApiError::files(&error))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        let metadata = entry.metadata().map_err(|error| ApiError::files(&error))?;
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

fn normalize_relative_path(path: &str) -> Option<PathBuf> {
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

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: "resource not found".to_owned(),
        }
    }

    fn files(error: &std::io::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("dataset file operation failed: {error}"),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        Self::internal(format!("state storage failed: {error}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}

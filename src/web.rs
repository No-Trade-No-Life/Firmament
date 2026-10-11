use std::collections::HashSet;
use std::sync::Arc;

use auth_mini_axum::{AuthMiniLayer, AuthMiniPrincipal};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::cold::{ColdStore, S3ColdStore, object_key};
use crate::crypto::write_token_secret;
use crate::db::{
    ArchiveJob, DEFAULT_COLD_BUCKET, DEFAULT_COLD_REGION, Database, DatabaseError, Dataset,
    LinkitStatus, META_COLD_BUCKET, META_COLD_REGION, PublishEvent, Syncer, WriteToken,
    WriteTokenCreated,
};
use crate::files::{
    normalize_publish_path, normalize_relative_path, scan_directory, sha256_hex,
    write_file_atomically,
};
use crate::linkit::{self, LinkitError};
use crate::resources::{ResourceMonitor, SystemResourcesSnapshot};

#[derive(Clone)]
struct AppState {
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    cold: Arc<dyn ColdStore>,
}

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct WebAssets;

// One publish carries a single table file; 32 MiB covers a full month of
// one-minute candles with room to spare.
const PUBLISH_BODY_LIMIT: usize = 32 * 1024 * 1024;

pub fn router(
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    auth: AuthMiniLayer,
) -> Router {
    let cold: Arc<dyn ColdStore> = Arc::new(S3ColdStore::new(database.clone()));
    let state = AppState {
        database,
        resources,
        cold,
    };
    let private = Router::new()
        .route("/me", get(me))
        .route("/setup", post(setup_root))
        .route("/datasets", get(list_datasets).post(create_dataset))
        .route("/datasets/{id}/manifest", get(dataset_manifest))
        .route("/datasets/{id}/files/{*path}", get(dataset_file))
        .route(
            "/datasets/{id}/archive",
            get(archive_status).post(start_archive),
        )
        .route("/syncers", get(list_syncers).post(create_syncer))
        .route("/syncers/{id}", delete(remove_syncer))
        .route("/syncers/{id}/completed", post(mark_syncer_completed))
        .route(
            "/write-tokens",
            get(list_write_tokens).post(create_write_token),
        )
        .route("/write-tokens/{id}", delete(remove_write_token))
        .route("/publish-events", get(list_publish_events))
        .route("/linkit", get(get_linkit).post(ensure_linkit))
        .route("/linkit/test", post(test_linkit))
        .route("/cold", get(get_cold_settings).put(put_cold_settings))
        .route("/system/resources", get(system_resources))
        .route_layer(auth);
    // Publishers authenticate with write tokens instead of an Auth Mini
    // session, so the publish route lives outside the session layer.
    let publish = Router::new()
        .route("/{dataset_id}/{*path}", put(publish_file))
        .route_layer(DefaultBodyLimit::max(PUBLISH_BODY_LIMIT));
    Router::new()
        .route("/api/health", get(health))
        .nest("/api/v1/publish", publish)
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
    cold_file_count: usize,
    cold_bytes: u64,
    updated_at: i64,
}

async fn list_datasets(State(state): State<AppState>) -> Result<Json<Vec<DatasetView>>, ApiError> {
    let datasets = state.database.list_datasets()?;
    let mut views = Vec::with_capacity(datasets.len());
    for dataset in datasets {
        let files = scan_directory(&state.database.dataset_directory(&dataset.id))
            .map_err(|error| ApiError::files(&error))?;
        let cold_files = state.database.list_cold_files(&dataset.id)?;
        let mut paths = files
            .iter()
            .map(|file| file.relative.as_str())
            .collect::<HashSet<_>>();
        let mut file_count = files.len();
        let mut bytes: u64 = files.iter().map(|file| file.size).sum();
        let mut updated_at = files
            .iter()
            .map(|file| file.updated_at)
            .max()
            .unwrap_or(dataset.created_at);
        let mut cold_count = 0usize;
        let mut cold_bytes: u64 = 0;
        for cold in &cold_files {
            cold_count += 1;
            cold_bytes += cold.size;
            if paths.insert(cold.path.as_str()) {
                file_count += 1;
                bytes += cold.size;
            }
            updated_at = updated_at.max(cold.archived_at);
        }
        views.push(DatasetView {
            id: dataset.id,
            name: dataset.name,
            description: dataset.description,
            tier: dataset.tier,
            file_count,
            bytes,
            cold_file_count: cold_count,
            cold_bytes,
            updated_at,
        });
    }
    Ok(Json(views))
}

#[derive(Debug, Deserialize)]
struct DatasetInput {
    id: String,
    name: String,
    description: Option<String>,
}

fn valid_dataset_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && id.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

async fn create_dataset(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<DatasetInput>,
) -> Result<(StatusCode, Json<Dataset>), ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let id = input.id.trim().to_ascii_lowercase();
    let name = input.name.trim();
    let description = input.description.unwrap_or_default().trim().to_owned();
    if !valid_dataset_id(&id) {
        return Err(ApiError::bad_request("invalid dataset id"));
    }
    if name.is_empty() {
        return Err(ApiError::bad_request("dataset name is required"));
    }
    if !state.database.create_dataset(&id, name, &description)? {
        return Err(ApiError::conflict("dataset already exists"));
    }
    let dataset = state
        .database
        .dataset(&id)?
        .ok_or_else(ApiError::not_found)?;
    Ok((StatusCode::CREATED, Json(dataset)))
}

#[derive(Debug, Serialize)]
struct DatasetManifestFileView {
    path: String,
    size: u64,
    sha256: String,
    updated_at: i64,
    tier: &'static str,
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
    let mut files = scan_directory(&state.database.dataset_directory(&dataset.id))
        .map_err(|error| ApiError::files(&error))?
        .into_iter()
        .map(|file| -> Result<DatasetManifestFileView, ApiError> {
            let bytes = std::fs::read(&file.absolute).map_err(|error| ApiError::files(&error))?;
            Ok(DatasetManifestFileView {
                path: file.relative,
                size: file.size,
                sha256: sha256_hex(&bytes),
                updated_at: file.updated_at,
                tier: "hot",
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let mut seen = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<HashSet<_>>();
    for cold in state.database.list_cold_files(&dataset.id)? {
        if !seen.insert(cold.path.clone()) {
            continue;
        }
        files.push(DatasetManifestFileView {
            path: cold.path,
            size: cold.size,
            sha256: cold.sha256,
            updated_at: cold.archived_at,
            tier: "cold",
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
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
            return cold_file_response(&state, &dataset.id, &relative).await;
        }
        Err(error) => return Err(ApiError::files(&error)),
    };
    let mime = mime_guess::from_path(&absolute).first_or_octet_stream();
    Ok(([(header::CONTENT_TYPE, mime.as_ref())], Body::from(bytes)).into_response())
}

async fn cold_file_response(
    state: &AppState,
    dataset_id: &str,
    relative: &std::path::Path,
) -> Result<Response, ApiError> {
    let path = relative.to_string_lossy().replace('\\', "/");
    let key = object_key(dataset_id, &path);
    let object = state
        .cold
        .get_object(&key)
        .await
        .map_err(|error| ApiError::internal(format!("cold storage fetch failed: {error}")))?
        .ok_or_else(ApiError::not_found)?;
    let mime = mime_guess::from_path(relative).first_or_octet_stream();
    let stream = tokio_util::io::ReaderStream::new(object.body.into_async_read());
    let mut response = Response::new(Body::from_stream(stream));
    if let Ok(value) = header::HeaderValue::from_str(mime.as_ref()) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&object.size.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    Ok(response)
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

#[derive(Debug, Deserialize)]
struct WriteTokenInput {
    name: String,
}

async fn list_write_tokens(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Vec<WriteToken>>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    Ok(Json(state.database.list_write_tokens()?))
}

async fn create_write_token(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<WriteTokenInput>,
) -> Result<(StatusCode, Json<WriteTokenCreated>), ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let name = input.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("write token name is required"));
    }
    let secret = write_token_secret();
    let token = state
        .database
        .create_write_token(name, &sha256_hex(secret.as_bytes()))?;
    Ok((
        StatusCode::CREATED,
        Json(WriteTokenCreated {
            id: token.id,
            name: token.name,
            secret,
            created_at: token.created_at,
        }),
    ))
}

async fn remove_write_token(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    if !state.database.delete_write_token(&id)? {
        return Err(ApiError::not_found());
    }
    Ok(StatusCode::NO_CONTENT)
}

// The audit trail shows the latest publishes to root; the cap keeps the
// response bounded while the table itself keeps every event.
const PUBLISH_EVENTS_LIMIT: i64 = 50;

async fn list_publish_events(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Vec<PublishEvent>>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    Ok(Json(
        state.database.list_publish_events(PUBLISH_EVENTS_LIMIT)?,
    ))
}

async fn publish_file(
    State(state): State<AppState>,
    Path((dataset_id, path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secret =
        bearer_token(&headers).ok_or_else(|| ApiError::unauthorized("write token is required"))?;
    let token = state
        .database
        .write_token_by_secret_hash(&sha256_hex(secret.as_bytes()))?
        .ok_or_else(|| ApiError::unauthorized("unknown write token"))?;
    if state.database.dataset(&dataset_id)?.is_none() {
        return Err(ApiError::not_found());
    }
    let relative = normalize_publish_path(&path)
        .ok_or_else(|| ApiError::bad_request("invalid publish path"))?;
    let target = state
        .database
        .dataset_directory(&dataset_id)
        .join(&relative);
    write_file_atomically(&target, &body).map_err(|error| ApiError::files(&error))?;
    let normalized = relative.to_string_lossy().replace('\\', "/");
    let size = body.len() as u64;
    let sha256 = sha256_hex(&body);
    state.database.record_publish_event(
        &dataset_id,
        &normalized,
        size,
        &sha256,
        &token.id,
        &token.name,
    )?;
    state
        .database
        .touch_write_token(&token.id, Utc::now().timestamp())?;
    Ok(Json(json!({
        "path": normalized,
        "size": size,
        "sha256": sha256,
    })))
}

async fn get_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<LinkitStatus>, ApiError> {
    Ok(Json(linkit::status(&state.database, &principal.subject)?))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

// INVARIANT: the auth middleware verified this same `Authorization` header, so
// this bearer belongs to the authenticated user; Linkit authorizes it as that
// user when the Bot is provisioned on their account.
async fn ensure_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    headers: HeaderMap,
) -> Result<Json<LinkitStatus>, ApiError> {
    let bearer =
        bearer_token(&headers).ok_or_else(|| ApiError::bad_request("Bearer token is required"))?;
    Ok(Json(
        linkit::ensure(&state.database, linkit::API_URL, &principal.subject, bearer).await?,
    ))
}

async fn test_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Value>, ApiError> {
    linkit::send_test(&state.database, linkit::API_URL, &principal.subject).await?;
    Ok(Json(json!({"sent": true})))
}

#[derive(Debug, Serialize)]
struct ColdSettingsView {
    bucket: String,
    region: String,
}

async fn get_cold_settings(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<ColdSettingsView>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    Ok(Json(ColdSettingsView {
        bucket: state
            .database
            .meta_value(META_COLD_BUCKET)?
            .unwrap_or_else(|| DEFAULT_COLD_BUCKET.to_owned()),
        region: state
            .database
            .meta_value(META_COLD_REGION)?
            .unwrap_or_else(|| DEFAULT_COLD_REGION.to_owned()),
    }))
}

#[derive(Debug, Deserialize)]
struct ColdSettingsInput {
    bucket: String,
    region: String,
}

async fn put_cold_settings(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<ColdSettingsInput>,
) -> Result<Json<ColdSettingsView>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let bucket = input.bucket.trim().to_ascii_lowercase();
    let region = input.region.trim().to_ascii_lowercase();
    let bucket_ok = !bucket.is_empty()
        && bucket.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '-'
                || character == '.'
        });
    let region_ok = !region.is_empty()
        && region.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if !bucket_ok || !region_ok {
        return Err(ApiError::bad_request("invalid cold storage settings"));
    }
    state.database.put_meta_value(META_COLD_BUCKET, &bucket)?;
    state.database.put_meta_value(META_COLD_REGION, &region)?;
    Ok(Json(ColdSettingsView { bucket, region }))
}

async fn archive_status(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(dataset_id): Path<String>,
) -> Result<Json<Option<ArchiveJob>>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    if state.database.dataset(&dataset_id)?.is_none() {
        return Err(ApiError::not_found());
    }
    Ok(Json(state.database.latest_archive_job(&dataset_id)?))
}

async fn start_archive(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(dataset_id): Path<String>,
) -> Result<(StatusCode, Json<ArchiveJob>), ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let dataset = state
        .database
        .dataset(&dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let Some(job) = state.database.create_archive_job_if_idle(&dataset.id)? else {
        return Err(ApiError::conflict("an archive job is already running"));
    };
    let database = state.database.clone();
    let cold = state.cold.clone();
    let job_id = job.id.clone();
    let dataset_id = dataset.id.clone();
    tokio::spawn(async move {
        crate::archive::run_archive_job(&database, cold.as_ref(), &dataset_id, &job_id).await;
    });
    Ok((StatusCode::ACCEPTED, Json(job)))
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

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
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

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
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

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }
}

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        Self::internal(format!("state storage failed: {error}"))
    }
}

impl From<LinkitError> for ApiError {
    fn from(error: LinkitError) -> Self {
        match error {
            LinkitError::Database(error) => Self::from(error),
            LinkitError::Conflict(message) => Self::conflict(message),
            LinkitError::Forbidden(message) => Self::forbidden(message),
            LinkitError::Unavailable(message) => Self::unavailable(message),
            LinkitError::Request(error) => {
                Self::unavailable(format!("Linkit request failed: {error}"))
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}

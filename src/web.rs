//! The HTTP surface: datasets and their schemas, the ingest path publishers
//! use, the cold manifest mirrors follow, and the small admin corners.

use std::collections::HashMap;
use std::sync::Arc;

use auth_mini_axum::{AuthMiniLayer, AuthMiniPrincipal};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Extension, Path, Query, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::cold::{ColdStore, object_key};
use crate::crypto::write_token_secret;
use crate::db::{
    DEFAULT_COLD_BUCKET, DEFAULT_COLD_REGION, Database, DatabaseError, Flush, IngestEvent,
    LinkitStatus, META_COLD_BUCKET, META_COLD_REGION, Syncer, WriteToken, WriteTokenCreated,
};
use crate::files::sha256_hex;
use crate::flush::{FlushReport, cooling_boundary, run_flush};
use crate::hot::{HotError, TableStat, row_json};
use crate::linkit::{self, LinkitError};
use crate::resources::{ResourceMonitor, SystemResourcesSnapshot};
use crate::schema::{self, ColumnSpec, Granularity, TableSpec};

#[derive(Clone)]
struct AppState {
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    cold: Arc<dyn ColdStore>,
}

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct WebAssets;

// One ingest request carries one batch of rows, not a file: 32 MiB is a few
// hundred thousand candles, which is far more than a publisher needs per call.
const INGEST_BODY_LIMIT: usize = 32 * 1024 * 1024;

/// Builds the application router.
pub fn router(
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    cold: Arc<dyn ColdStore>,
    auth: AuthMiniLayer,
) -> Router {
    let state = AppState {
        database,
        resources,
        cold,
    };
    let private = Router::new()
        .route("/me", get(me))
        .route("/setup", post(setup_root))
        .route("/datasets", get(list_datasets).post(create_dataset))
        .route("/datasets/{id}", get(dataset_detail))
        .route("/datasets/{id}/manifest", get(dataset_manifest))
        .route("/datasets/{id}/rows", get(dataset_rows))
        .route("/datasets/{id}/files/{*path}", get(dataset_file))
        .route(
            "/datasets/{id}/flushes",
            get(list_flushes).post(start_flush),
        )
        .route("/syncers", get(list_syncers).post(create_syncer))
        .route("/syncers/{id}", delete(remove_syncer))
        .route("/syncers/{id}/completed", post(mark_syncer_completed))
        .route(
            "/write-tokens",
            get(list_write_tokens).post(create_write_token),
        )
        .route("/write-tokens/{id}", delete(remove_write_token))
        .route("/ingest-events", get(list_ingest_events))
        .route("/linkit", get(get_linkit).post(ensure_linkit))
        .route("/linkit/test", post(test_linkit))
        .route("/cold", get(get_cold_settings).put(put_cold_settings))
        .route("/system/resources", get(system_resources))
        .route_layer(auth);
    // Publishers authenticate with write tokens instead of an Auth Mini
    // session, so the ingest route lives outside the session layer.
    let ingest = Router::new()
        .route("/{dataset_id}/{table}", post(ingest_rows))
        .route_layer(DefaultBodyLimit::max(INGEST_BODY_LIMIT));
    Router::new()
        .route("/api/health", get(health))
        .nest("/api/v1/ingest", ingest)
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
struct TableView {
    name: String,
    primary_key: Vec<String>,
    time_column: String,
    granularity: Granularity,
    columns: Vec<ColumnSpec>,
    rows: i64,
    coolable_rows: i64,
    oldest_time: Option<String>,
    newest_time: Option<String>,
    cold_files: i64,
    cold_bytes: u64,
    cold_from: Option<String>,
    cold_until: Option<String>,
}

#[derive(Debug, Serialize)]
struct DatasetView {
    id: String,
    name: String,
    description: String,
    hot_window_seconds: i64,
    last_batch: i64,
    created_at: i64,
    tables: Vec<TableView>,
    hot_rows: i64,
    coolable_rows: i64,
    cold_files: i64,
    cold_bytes: u64,
    cold_from: Option<String>,
    cold_until: Option<String>,
}

fn dataset_view(database: &Database, id: &str) -> Result<DatasetView, ApiError> {
    let dataset = database.dataset(id)?.ok_or_else(ApiError::not_found)?;
    let now = Utc::now();
    let boundaries = dataset
        .tables
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                cooling_boundary(now, dataset.hot_window_seconds, table.granularity),
            )
        })
        .collect::<HashMap<_, _>>();
    let stats = database
        .hot()
        .stats(&dataset.id, &dataset.tables, &boundaries)?;
    let cold = database.cold_summary(&dataset.id)?;
    let mut views = Vec::with_capacity(dataset.tables.len());
    let mut hot_rows = 0;
    let mut coolable_rows = 0;
    let mut cold_files = 0;
    let mut cold_bytes = 0_u64;
    let mut cold_from: Option<String> = None;
    let mut cold_until: Option<String> = None;
    for table in &dataset.tables {
        let stat = stats
            .iter()
            .find(|stat: &&TableStat| stat.table == table.name);
        let summary = cold.iter().find(|entry| entry.table_name == table.name);
        let rows = stat.map_or(0, |stat| stat.rows);
        let coolable = stat.map_or(0, |stat| stat.coolable_rows);
        let files = summary.map_or(0, |entry| entry.files);
        let bytes = summary.map_or(0, |entry| entry.bytes);
        hot_rows += rows;
        coolable_rows += coolable;
        cold_files += files;
        cold_bytes += bytes;
        cold_from = earliest(
            cold_from,
            summary.and_then(|entry| entry.earliest_time.clone()),
        );
        cold_until = latest(
            cold_until,
            summary.and_then(|entry| entry.latest_time.clone()),
        );
        views.push(TableView {
            name: table.name.clone(),
            primary_key: table.primary_key.clone(),
            time_column: table.time_column.clone(),
            granularity: table.granularity,
            columns: table.columns.clone(),
            rows,
            coolable_rows: coolable,
            oldest_time: stat.and_then(|stat| stat.oldest_time.clone()),
            newest_time: stat.and_then(|stat| stat.newest_time.clone()),
            cold_files: files,
            cold_bytes: bytes,
            cold_from: summary.and_then(|entry| entry.earliest_time.clone()),
            cold_until: summary.and_then(|entry| entry.latest_time.clone()),
        });
    }
    Ok(DatasetView {
        id: dataset.id,
        name: dataset.name,
        description: dataset.description,
        hot_window_seconds: dataset.hot_window_seconds,
        last_batch: dataset.last_batch,
        created_at: dataset.created_at,
        tables: views,
        hot_rows,
        coolable_rows,
        cold_files,
        cold_bytes,
        cold_from,
        cold_until,
    })
}

fn earliest(current: Option<String>, candidate: Option<String>) -> Option<String> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (current, candidate) => current.or(candidate),
    }
}

fn latest(current: Option<String>, candidate: Option<String>) -> Option<String> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (current, candidate) => current.or(candidate),
    }
}

async fn list_datasets(State(state): State<AppState>) -> Result<Json<Vec<DatasetView>>, ApiError> {
    let mut views = Vec::new();
    for dataset in state.database.list_datasets()? {
        views.push(dataset_view(&state.database, &dataset.id)?);
    }
    Ok(Json(views))
}

#[derive(Debug, Deserialize)]
struct DatasetInput {
    id: String,
    name: String,
    description: Option<String>,
    hot_window_seconds: i64,
    tables: Vec<TableSpec>,
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

// Ten years: long enough for any research dataset, short enough to catch a
// value that was meant as milliseconds.
const MAX_HOT_WINDOW_SECONDS: i64 = 10 * 365 * 24 * 3600;

async fn create_dataset(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Json(input): Json<DatasetInput>,
) -> Result<(StatusCode, Json<DatasetView>), ApiError> {
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
    if !(0..=MAX_HOT_WINDOW_SECONDS).contains(&input.hot_window_seconds) {
        return Err(ApiError::bad_request("invalid hot window"));
    }
    schema::validate(&input.tables).map_err(ApiError::bad_request)?;
    state.database.create_dataset(
        &id,
        name,
        &description,
        input.hot_window_seconds,
        &input.tables,
    )?;
    Ok((
        StatusCode::CREATED,
        Json(dataset_view(&state.database, &id)?),
    ))
}

async fn dataset_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<DatasetView>, ApiError> {
    Ok(Json(dataset_view(&state.database, &id)?))
}

#[derive(Debug, Serialize)]
struct ManifestView {
    dataset_id: String,
    latest_batch: i64,
    files: Vec<crate::db::ColdFile>,
}

#[derive(Debug, Deserialize)]
struct ManifestQuery {
    #[serde(default)]
    since: i64,
    table: Option<String>,
}

async fn dataset_manifest(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ManifestQuery>,
) -> Result<Json<ManifestView>, ApiError> {
    let dataset = state
        .database
        .dataset(&id)?
        .ok_or_else(ApiError::not_found)?;
    if let Some(table) = query.table.as_deref()
        && !dataset.tables.iter().any(|declared| declared.name == table)
    {
        return Err(ApiError::not_found());
    }
    let files =
        state
            .database
            .cold_files(&dataset.id, query.since.max(0), query.table.as_deref())?;
    Ok(Json(ManifestView {
        dataset_id: dataset.id,
        latest_batch: dataset.last_batch,
        files,
    }))
}

#[derive(Debug, Deserialize)]
struct RowsQuery {
    table: String,
    since: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RowsView {
    dataset_id: String,
    table: String,
    hot_window_seconds: i64,
    rows: Vec<Map<String, Value>>,
}

async fn dataset_rows(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<RowsQuery>,
) -> Result<Json<RowsView>, ApiError> {
    let dataset = state
        .database
        .dataset(&id)?
        .ok_or_else(ApiError::not_found)?;
    let table = dataset
        .tables
        .iter()
        .find(|table| table.name == query.table)
        .ok_or_else(ApiError::not_found)?;
    let limit = query.limit.unwrap_or(1_000).clamp(1, 10_000);
    let rows = state
        .database
        .hot()
        .rows(&dataset.id, table, query.since.as_deref(), limit)?;
    Ok(Json(RowsView {
        dataset_id: dataset.id,
        table: table.name.clone(),
        hot_window_seconds: dataset.hot_window_seconds,
        rows: rows.iter().map(|values| row_json(table, values)).collect(),
    }))
}

// Only catalogued objects are served: the path is a lookup key, never a free
// pointer into the bucket.
async fn dataset_file(
    State(state): State<AppState>,
    Path((dataset_id, path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let file = state
        .database
        .cold_file(&dataset_id, &path)?
        .ok_or_else(ApiError::not_found)?;
    let object = state
        .cold
        .get_object(&object_key(&dataset_id, &file.path))
        .await
        .map_err(|error| ApiError::internal(format!("cold storage fetch failed: {error}")))?
        .ok_or_else(ApiError::not_found)?;
    let stream = tokio_util::io::ReaderStream::new(object.body.into_async_read());
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/vnd.apache.parquet"),
    );
    if let Ok(value) = header::HeaderValue::from_str(&object.size.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    Ok(response)
}

#[derive(Debug, Serialize)]
struct FlushResponse {
    batch: i64,
    files: i64,
    rows: i64,
    bytes: u64,
}

async fn list_flushes(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Flush>>, ApiError> {
    if state.database.dataset(&id)?.is_none() {
        return Err(ApiError::not_found());
    }
    Ok(Json(state.database.list_flushes(&id, 20)?))
}

async fn start_flush(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
) -> Result<Json<FlushResponse>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    if state.database.dataset(&id)?.is_none() {
        return Err(ApiError::not_found());
    }
    if let Some(flush) = state.database.latest_flush(&id)?
        && flush.status == "running"
    {
        return Err(ApiError::conflict("a cooling pass is already running"));
    }
    let report: FlushReport = run_flush(&state.database, state.cold.as_ref(), &id, "manual")
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(FlushResponse {
        batch: report.batch,
        files: report.files,
        rows: report.rows,
        bytes: report.bytes,
    }))
}

#[derive(Debug, Deserialize)]
struct IngestRequest {
    #[serde(default)]
    upsert: Vec<Map<String, Value>>,
    #[serde(default)]
    delete: Vec<Map<String, Value>>,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    dataset_id: String,
    table: String,
    upserted: i64,
    deleted: i64,
}

async fn ingest_rows(
    State(state): State<AppState>,
    Path((dataset_id, table_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<IngestResponse>, ApiError> {
    let secret =
        bearer_token(&headers).ok_or_else(|| ApiError::unauthorized("write token is required"))?;
    let token = state
        .database
        .write_token_by_secret_hash(&sha256_hex(secret.as_bytes()))?
        .ok_or_else(|| ApiError::unauthorized("unknown write token"))?;
    let dataset = state
        .database
        .dataset(&dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let table = dataset
        .tables
        .iter()
        .find(|declared| declared.name == table_name)
        .ok_or_else(ApiError::not_found)?;
    let request: IngestRequest = serde_json::from_slice(&body)
        .map_err(|error| ApiError::bad_request(format!("invalid ingest body: {error}")))?;
    if request.upsert.is_empty() && request.delete.is_empty() {
        return Err(ApiError::bad_request("ingest carries no rows"));
    }
    let counts =
        state
            .database
            .hot()
            .ingest(&dataset.id, table, &request.upsert, &request.delete)?;
    state.database.record_ingest_event(
        &dataset.id,
        &table.name,
        counts.upserted,
        counts.deleted,
        &token.id,
        &token.name,
    )?;
    state
        .database
        .touch_write_token(&token.id, Utc::now().timestamp())?;
    Ok(Json(IngestResponse {
        dataset_id: dataset.id,
        table: table.name.clone(),
        upserted: counts.upserted,
        deleted: counts.deleted,
    }))
}

#[derive(Debug, Deserialize)]
struct SyncerInput {
    name: String,
    dataset_id: String,
    table_name: Option<String>,
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
    let dataset = state
        .database
        .dataset(&input.dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let table_name = input
        .table_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(table) = table_name
        && !dataset.tables.iter().any(|declared| declared.name == table)
    {
        return Err(ApiError::bad_request("unknown table"));
    }
    let syncer = state
        .database
        .create_syncer(&principal.subject, name, &dataset.id, table_name)?;
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

#[derive(Debug, Deserialize)]
struct SyncerCompleted {
    batch: i64,
}

async fn mark_syncer_completed(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
    Json(input): Json<SyncerCompleted>,
) -> Result<Json<Syncer>, ApiError> {
    let actor = Actor::from_principal(&state.database, &principal)?;
    let syncer = state
        .database
        .get_syncer(&id)?
        .ok_or_else(ApiError::not_found)?;
    actor.assert_owner(&syncer.owner_id)?;
    let updated = state
        .database
        .mark_syncer_synced(&id, Utc::now().timestamp(), input.batch.max(0))?
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

// The audit trail shows the latest ingests to root; the cap keeps the response
// bounded while the table itself keeps every event.
const INGEST_EVENTS_LIMIT: i64 = 50;

async fn list_ingest_events(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<Vec<IngestEvent>>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    Ok(Json(
        state.database.list_ingest_events(INGEST_EVENTS_LIMIT)?,
    ))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

async fn get_linkit(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<LinkitStatus>, ApiError> {
    Ok(Json(linkit::status(&state.database, &principal.subject)?))
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
        .map_err(|error| ApiError::internal(format!("resource sampling failed: {error}")))?;
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
        match error {
            DatabaseError::Conflict(message) => Self::conflict(message),
            error @ DatabaseError::Hot(HotError::Invalid(_)) => {
                Self::bad_request(error.to_string())
            }
            error => Self::internal(format!("state storage failed: {error}")),
        }
    }
}

impl From<HotError> for ApiError {
    fn from(error: HotError) -> Self {
        match error {
            HotError::Invalid(message) => Self::bad_request(message),
            error => Self::internal(format!("hot storage failed: {error}")),
        }
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
                Self::internal(format!("Linkit request failed: {error}"))
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::dataset_view;
    use crate::db::Database;
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec};

    fn column(name: &str, column_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            column_type,
            nullable: false,
        }
    }

    fn klines() -> TableSpec {
        TableSpec {
            name: "klines".to_owned(),
            primary_key: vec!["symbol".to_owned(), "updated_at".to_owned()],
            time_column: "updated_at".to_owned(),
            granularity: Granularity::Day,
            columns: vec![
                column("symbol", ColumnType::Text),
                column("updated_at", ColumnType::Timestamp),
                column("close", ColumnType::Real),
            ],
        }
    }

    #[test]
    fn the_dataset_view_separates_the_hot_window_from_the_cold_tree() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        database
            .create_dataset("bars", "Bars", "", 0, &[klines()])
            .expect("dataset creates");
        let table = klines();
        database
            .hot()
            .ingest(
                "bars",
                &table,
                &[
                    serde_json::json!({ "symbol": "BTCUSDT", "updated_at": "2026-09-01T00:00:00Z", "close": 1.0 })
                        .as_object()
                        .expect("object")
                        .clone(),
                    serde_json::json!({ "symbol": "BTCUSDT", "updated_at": "2999-01-01T00:00:00Z", "close": 2.0 })
                        .as_object()
                        .expect("object")
                        .clone(),
                ],
                &[],
            )
            .expect("ingest");

        let view = dataset_view(&database, "bars").expect("view");
        assert_eq!(view.hot_rows, 2);
        assert_eq!(
            view.coolable_rows, 1,
            "a zero window cools everything older than today"
        );
        assert_eq!(view.cold_files, 0);
        let table_view = &view.tables[0];
        assert_eq!(table_view.rows, 2);
        assert_eq!(table_view.granularity, Granularity::Day);
        assert_eq!(
            table_view.oldest_time.as_deref(),
            Some("2026-09-01T00:00:00.000000Z")
        );
        assert_eq!(
            table_view.newest_time.as_deref(),
            Some("2999-01-01T00:00:00.000000Z")
        );
        assert!(table_view.cold_from.is_none());
    }
}

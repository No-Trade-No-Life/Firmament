//! The HTTP surface: datasets and their schemas, the ingest path publishers
//! use, the cold manifest mirrors follow, and the small admin corners.

use std::sync::Arc;

use auth_mini_axum::{AuthMiniLayer, AuthMiniPrincipal};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Extension, FromRequest, Path, Query, Request, State,
        rejection::JsonRejection,
    },
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, Notify};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::cold::{self, ColdStore, object_key};
use crate::cooling::Cadence;
use crate::crypto::write_token_secret;
use crate::db::{
    ColdSettings, Database, DatabaseError, Flush, IngestEvent, LinkitStatus, Syncer, WriteToken,
    WriteTokenCreated,
};
use crate::files::sha256_hex;
use crate::flush::cooling_boundary;
use crate::hot::{HotError, row_json};
use crate::linkit::{self, LinkitError};
use crate::resources::{ResourceMonitor, SystemResourcesSnapshot};
use crate::schema::{self, ColumnSpec, Granularity, TableSpec};

#[derive(Clone)]
struct AppState {
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    cold: Arc<dyn ColdStore>,
    /// Wakes the cooling thread so a manual request does not wait for the next
    /// tick.
    wake: Arc<Notify>,
}

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct WebAssets;

// One ingest request carries one batch of rows, not a file: 32 MiB is a few
// hundred thousand candles, which is far more than a publisher needs per call.
const INGEST_BODY_LIMIT: usize = 32 * 1024 * 1024;

// One cold partition, not a database: a day of 1-minute bars for a hundred
// symbols is a few hundred kilobytes.
const IMPORT_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Builds the application router.
pub fn router(
    database: Database,
    resources: Arc<Mutex<ResourceMonitor>>,
    cold: Arc<dyn ColdStore>,
    auth: AuthMiniLayer,
    wake: Arc<Notify>,
) -> Router {
    let state = AppState {
        database,
        resources,
        cold,
        wake,
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
        .route("/{dataset_id}", post(ingest_rows))
        .route_layer(DefaultBodyLimit::max(INGEST_BODY_LIMIT));
    // Parquet import is a write-side operation with the same write tokens, so it
    // lives outside the session layer next to ingest.
    let import = Router::new()
        .route("/{dataset_id}", post(import_parquet))
        .route_layer(DefaultBodyLimit::max(IMPORT_BODY_LIMIT));
    Router::new()
        .route("/api/health", get(health))
        .nest("/api/v1/ingest", ingest)
        .nest("/api/v1/import", import)
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
    cadence: Cadence,
    last_revision: i64,
    created_at: i64,
    table: TableView,
}

fn dataset_view(database: &Database, id: &str) -> Result<DatasetView, ApiError> {
    let dataset = database.dataset(id)?.ok_or_else(ApiError::not_found)?;
    let boundary = cooling_boundary(Utc::now(), dataset.cadence);
    let stats = database
        .hot()
        .stats(&dataset.id, &dataset.table, &boundary)?;
    let cold = database.cold_summary(&dataset.id)?;
    Ok(DatasetView {
        id: dataset.id,
        name: dataset.name,
        description: dataset.description,
        cadence: dataset.cadence,
        last_revision: dataset.last_revision,
        created_at: dataset.created_at,
        table: TableView {
            primary_key: dataset.table.primary_key,
            time_column: dataset.table.time_column,
            granularity: dataset.table.granularity,
            columns: dataset.table.columns,
            rows: stats.rows,
            coolable_rows: stats.coolable_rows,
            oldest_time: stats.oldest_time,
            newest_time: stats.newest_time,
            cold_files: cold.partitions,
            cold_bytes: cold.bytes,
            cold_from: cold.earliest_time,
            cold_until: cold.latest_time,
        },
    })
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
    cadence: Cadence,
    table: TableSpec,
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
    ApiJson(input): ApiJson<DatasetInput>,
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
    schema::validate(&input.table).map_err(ApiError::bad_request)?;
    state
        .database
        .create_dataset(&id, name, &description, input.cadence, &input.table)?;
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
    latest_revision: i64,
    files: Vec<crate::db::ColdFile>,
}

#[derive(Debug, Deserialize)]
struct ManifestQuery {
    #[serde(default)]
    since: i64,
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
    let files = state.database.cold_files(&dataset.id, query.since.max(0))?;
    Ok(Json(ManifestView {
        dataset_id: dataset.id,
        latest_revision: dataset.last_revision,
        files,
    }))
}

#[derive(Debug, Deserialize)]
struct RowsQuery {
    since: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RowsView {
    dataset_id: String,
    cadence: Cadence,
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
    let limit = query.limit.unwrap_or(1_000).clamp(1, 10_000);
    let rows =
        state
            .database
            .hot()
            .rows(&dataset.id, &dataset.table, query.since.as_deref(), limit)?;
    Ok(Json(RowsView {
        dataset_id: dataset.id,
        cadence: dataset.cadence,
        rows: rows
            .iter()
            .map(|values| row_json(&dataset.table, values))
            .collect(),
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
struct CoolingRequest {
    requested: bool,
    coolable_rows: i64,
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

// Cooling never runs inside a write or a read: this only wakes the cooling
// thread, which then takes the pass at its own pace.
async fn start_flush(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<CoolingRequest>), ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    let dataset = state
        .database
        .dataset(&id)?
        .ok_or_else(ApiError::not_found)?;
    let boundary = cooling_boundary(Utc::now(), dataset.cadence);
    let coolable_rows = state
        .database
        .hot()
        .stats(&dataset.id, &dataset.table, &boundary)?
        .coolable_rows;
    state.wake.notify_one();
    Ok((
        StatusCode::ACCEPTED,
        Json(CoolingRequest {
            requested: true,
            coolable_rows,
        }),
    ))
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
    upserted: i64,
    deleted: i64,
}

async fn ingest_rows(
    State(state): State<AppState>,
    Path(dataset_id): Path<String>,
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
    let request: IngestRequest = serde_json::from_slice(&body)
        .map_err(|error| ApiError::bad_request(format!("invalid ingest body: {error}")))?;
    if request.upsert.is_empty() && request.delete.is_empty() {
        return Err(ApiError::bad_request("ingest carries no rows"));
    }
    let counts = state.database.hot().ingest(
        &dataset.id,
        &dataset.table,
        &request.upsert,
        &request.delete,
    )?;
    state.database.record_ingest_event(
        &dataset.id,
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
        upserted: counts.upserted,
        deleted: counts.deleted,
    }))
}

#[derive(Debug, Serialize)]
struct ImportResponse {
    dataset_id: String,
    /// Rows the request carried.
    rows: i64,
    /// Rows actually written; zero when the file was already imported.
    upserted: i64,
    /// Whether this file had already been applied.
    skipped: bool,
}

// Parquet import is the bulk door next to the row-by-row ingest API: the object
// is decoded, cleaned against the declared schema and written to the hot store,
// where it waits for the next cooling pass. Nothing is written to cold storage
// here, so the request costs one SQLite transaction.
async fn import_parquet(
    State(state): State<AppState>,
    Path(dataset_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ImportResponse>, ApiError> {
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
    if body.is_empty() {
        return Err(ApiError::bad_request("import carries no Parquet object"));
    }
    let digest = sha256_hex(&body);
    if let Some(rows) = state.database.imported_rows(&dataset.id, &digest)? {
        return Ok(Json(ImportResponse {
            dataset_id: dataset.id,
            rows,
            upserted: 0,
            skipped: true,
        }));
    }
    let rows = cold::parquet_to_rows(&dataset.table, &body)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let counts = state
        .database
        .hot()
        .insert_typed(&dataset.id, &dataset.table, rows.clone())?;
    state
        .database
        .record_import(&dataset.id, &digest, rows.len() as i64)?;
    state
        .database
        .record_ingest_event(&dataset.id, counts.upserted, 0, &token.id, &token.name)?;
    state
        .database
        .touch_write_token(&token.id, Utc::now().timestamp())?;
    state.wake.notify_one();
    Ok(Json(ImportResponse {
        dataset_id: dataset.id,
        rows: rows.len() as i64,
        upserted: counts.upserted,
        skipped: false,
    }))
}

#[derive(Debug, Deserialize)]
struct SyncerInput {
    name: String,
    dataset_id: String,
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
    ApiJson(input): ApiJson<SyncerInput>,
) -> Result<(StatusCode, Json<Syncer>), ApiError> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("syncer name is required"));
    }
    let dataset = state
        .database
        .dataset(&input.dataset_id)?
        .ok_or_else(ApiError::not_found)?;
    let syncer = state
        .database
        .create_syncer(&principal.subject, name, &dataset.id)?;
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
    /// The cold revision this mirror now holds. `batch` is the name the field had
    /// before the frontend was renamed to `revision`, and a page cached from an
    /// older bundle still posts it, so both spellings are accepted.
    #[serde(alias = "batch")]
    revision: i64,
}

async fn mark_syncer_completed(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    Path(id): Path<String>,
    ApiJson(input): ApiJson<SyncerCompleted>,
) -> Result<Json<Syncer>, ApiError> {
    let actor = Actor::from_principal(&state.database, &principal)?;
    let syncer = state
        .database
        .get_syncer(&id)?
        .ok_or_else(ApiError::not_found)?;
    actor.assert_owner(&syncer.owner_id)?;
    let updated = state
        .database
        .mark_syncer_synced(&id, Utc::now().timestamp(), input.revision.max(0))?
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
    ApiJson(input): ApiJson<WriteTokenInput>,
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
    endpoint: Option<String>,
    access_key_id: Option<String>,
    has_secret: bool,
}

impl From<ColdSettings> for ColdSettingsView {
    fn from(settings: ColdSettings) -> Self {
        Self {
            bucket: settings.bucket,
            region: settings.region,
            endpoint: settings.endpoint,
            access_key_id: settings.access_key_id,
            has_secret: settings.secret_access_key.is_some(),
        }
    }
}

async fn get_cold_settings(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
) -> Result<Json<ColdSettingsView>, ApiError> {
    Actor::from_principal(&state.database, &principal)?.assert_root()?;
    Ok(Json(state.database.cold_settings()?.into()))
}

#[derive(Debug, Deserialize)]
struct ColdSettingsInput {
    bucket: String,
    region: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    access_key_id: String,
    #[serde(default)]
    secret_access_key: String,
}

async fn put_cold_settings(
    State(state): State<AppState>,
    Extension(principal): Extension<AuthMiniPrincipal>,
    ApiJson(input): ApiJson<ColdSettingsInput>,
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
    let endpoint = input.endpoint.trim().trim_end_matches('/').to_owned();
    let endpoint_ok =
        endpoint.is_empty() || endpoint.starts_with("https://") || endpoint.starts_with("http://");
    if !endpoint_ok {
        return Err(ApiError::bad_request(
            "the cold endpoint must be an http(s) URL",
        ));
    }
    let access_key_id = input.access_key_id.trim().to_owned();
    let secret_access_key = input.secret_access_key.trim().to_owned();
    let existing = state.database.cold_settings()?;
    let mut settings = ColdSettings {
        bucket,
        region,
        endpoint: (!endpoint.is_empty()).then_some(endpoint),
        access_key_id: None,
        secret_access_key: None,
    };
    if !access_key_id.is_empty() {
        let rotated = existing.access_key_id.as_deref() != Some(access_key_id.as_str());
        let secret = match (secret_access_key.is_empty(), rotated) {
            (false, _) => secret_access_key,
            // Keeping the stored secret is only meaningful for the same key.
            (true, false) => existing
                .secret_access_key
                .ok_or_else(|| ApiError::bad_request("the secret key is required"))?,
            (true, true) => {
                return Err(ApiError::bad_request(
                    "a secret key is required with a new access key id",
                ));
            }
        };
        settings.access_key_id = Some(access_key_id);
        settings.secret_access_key = Some(secret);
    }
    state.database.save_cold_settings(&settings)?;
    Ok(Json(state.database.cold_settings()?.into()))
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

/// A `Json` body that reports a bad body through [`ApiError`].
///
/// `Json`'s own rejection answers with `text/plain`, so a browser that parses
/// every response as JSON reports `Unexpected token 'F', "Failed to "...` instead
/// of the actual reason. Routing the rejection through `ApiError` keeps every
/// API failure in the same `{"error": "..."}` envelope.
#[derive(Debug)]
struct ApiJson<T>(T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(request, state).await?;
        Ok(Self(value))
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        // The outermost source carries the useful part ("missing field `revision`
        // at line 1 column 27"); `body_text` repeats the rejection category.
        let detail = std::error::Error::source(&rejection)
            .map_or_else(|| rejection.body_text(), ToString::to_string);
        Self {
            status: rejection.status(),
            message: format!("invalid request body: {detail}"),
        }
    }
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
    use axum::{
        Router,
        body::Body,
        http::{Method, StatusCode, header},
        routing::post,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::{ApiJson, SyncerCompleted, dataset_view};
    use crate::cooling::Cadence;
    use crate::db::Database;
    use crate::schema::{ColumnSpec, ColumnType, Granularity, TableSpec};

    #[derive(Debug, Deserialize, Serialize)]
    struct Probe {
        revision: i64,
    }

    async fn echo_probe(ApiJson(probe): ApiJson<Probe>) -> axum::Json<Probe> {
        axum::Json(probe)
    }

    const BODY_LIMIT: usize = 16 * 1024;

    async fn post_json(uri: &str, body: &'static str) -> axum::response::Response {
        Router::new()
            .route(uri, post(echo_probe))
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response")
    }

    #[test]
    fn the_completed_body_reads_revision_and_still_accepts_the_legacy_batch_name() {
        let current: SyncerCompleted =
            serde_json::from_str(r#"{"revision": 5}"#).expect("revision");
        assert_eq!(current.revision, 5);
        let legacy: SyncerCompleted =
            serde_json::from_str(r#"{"batch": 5}"#).expect("legacy batch");
        assert_eq!(legacy.revision, 5);
    }

    #[tokio::test]
    async fn a_body_that_does_not_match_the_shape_answers_with_the_json_error_envelope() {
        let response = post_json("/probe", r#"{"batch": 5}"#).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let bytes = axum::body::to_bytes(response.into_body(), BODY_LIMIT)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json envelope");
        let message = body["error"].as_str().expect("error message");
        assert!(message.contains("missing field `revision`"), "{message}");
    }

    #[tokio::test]
    async fn a_body_that_is_not_json_answers_with_the_same_envelope() {
        let response = post_json("/probe", "revision=5").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), BODY_LIMIT)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json envelope");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty())
        );
    }

    fn column(name: &str, column_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            column_type,
            nullable: false,
        }
    }

    fn klines() -> TableSpec {
        TableSpec {
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
    fn the_dataset_view_separates_the_hot_store_from_the_cold_tree() {
        let state = TempDir::new().expect("state directory");
        let database = Database::open(state.path()).expect("database opens");
        database
            .create_dataset("bars", "Bars", "", Cadence::Daily, &klines())
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
        assert_eq!(view.table.rows, 2);
        assert_eq!(
            view.table.coolable_rows, 1,
            "only rows older than the current period may cool"
        );
        assert_eq!(view.table.cold_files, 0);
        assert_eq!(view.table.granularity, Granularity::Day);
        assert_eq!(
            view.table.oldest_time.as_deref(),
            Some("2026-09-01T00:00:00.000000Z")
        );
        assert_eq!(
            view.table.newest_time.as_deref(),
            Some("2999-01-01T00:00:00.000000Z")
        );
        assert!(view.table.cold_from.is_none());
    }
}

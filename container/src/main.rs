use std::{
    collections::BTreeMap,
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::future::try_join_all;
use object_store::{ObjectStore, aws::AmazonS3Builder, prefix::PrefixStore};
use serde::{Deserialize, Serialize};
use slatedb::{
    Db,
    config::{
        FlushOptions, FlushType, MetricLevel, ObjectStoreCacheOptions, PutOptions, Settings,
        WriteOptions,
    },
    db_cache::foyer::{FoyerCache, FoyerCacheOptions},
};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::sync::Mutex;

const CACHE_ROOT: &str = "/var/cache/slatedb";
const CACHE_BYTES: usize = 512 * 1024 * 1024;
const MEMORY_CACHE_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    active: Arc<Mutex<Option<ActiveDb>>>,
}

struct Config {
    endpoint: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
}

struct ActiveDb {
    name: String,
    db: Db,
    metrics: Arc<DefaultMetricsRecorder>,
}

#[derive(Debug, Deserialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct KeyRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct ScanQuery {
    prefix: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct GetQuery {
    key: String,
}

#[derive(Debug, Deserialize)]
struct BenchmarkBatchRequest {
    value: String,
    clients: Vec<Vec<BenchmarkOperation>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BenchmarkOperationKind {
    Get,
    Put,
}

#[derive(Debug, Deserialize)]
struct BenchmarkOperation {
    operation: BenchmarkOperationKind,
    key: String,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkOperationMeasurements {
    operations: u64,
    unmeasurable_operations: u64,
    measurable_latency_ns: Vec<u64>,
}

#[derive(Debug, Default, Serialize)]
struct BenchmarkBatchResponse {
    get: BenchmarkOperationMeasurements,
    put: BenchmarkOperationMeasurements,
}

#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct ValueResponse {
    key: String,
    value: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScanItem {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ScanResponse {
    items: Vec<ScanItem>,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    runtime: &'static str,
    open: bool,
    cache_populated: bool,
    cache_storage_bytes: u64,
    adapter: BTreeMap<String, u64>,
    slatedb: Vec<SlateMetric>,
}

#[derive(Debug, Serialize)]
struct SlateMetric {
    name: String,
    labels: Vec<(String, String)>,
    value: SlateMetricValue,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum SlateMetricValue {
    Counter {
        value: u64,
    },
    Gauge {
        value: i64,
    },
    UpDownCounter {
        value: i64,
    },
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        boundaries: Vec<f64>,
        bucket_counts: Vec<u64>,
    },
}

#[derive(Debug)]
struct AppError(StatusCode, String);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl AppError {
    fn internal(error: impl std::fmt::Display) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let state = AppState {
        config: Arc::new(Config::from_env().unwrap_or_else(|error| panic!("{error}"))),
        active: Arc::new(Mutex::new(None)),
    };
    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/v1/db/{database}/put", post(put))
        .route("/v1/db/{database}/get", get(get_value))
        .route("/v1/db/{database}/delete", post(delete))
        .route("/v1/db/{database}/scan", get(scan))
        .route("/v1/db/{database}/stats", get(stats))
        .route("/v1/db/{database}/admin/reopen", post(reopen))
        .route("/v1/db/{database}/admin/flush", post(flush))
        .route("/v1/db/{database}/admin/cache/clear", post(clear_cache))
        .route(
            "/v1/db/{database}/admin/benchmark/batch",
            post(benchmark_batch),
        )
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!(address = %listener.local_addr().unwrap(), "container listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
    close_active(&state).await;
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let account_id = required_env("R2_ACCOUNT_ID")?;
        Ok(Self {
            endpoint: format!("https://{account_id}.r2.cloudflarestorage.com"),
            bucket: required_env("R2_BUCKET")?,
            access_key_id: required_env("R2_ACCESS_KEY_ID")?,
            secret_access_key: required_env("R2_SECRET_ACCESS_KEY")?,
        })
    }
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing environment variable {name}"))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true, "service": "slatedb-cloudflare-container" }))
}

async fn database(state: &AppState, name: &str) -> Result<Db, AppError> {
    validate_database_name(name)?;
    let mut active = state.active.lock().await;
    if let Some(current) = active.as_ref() {
        if current.name != name {
            return Err(AppError::internal(
                "container was routed with two database names",
            ));
        }
        return Ok(current.db.clone());
    }
    let opened = open_database(&state.config, name).await?;
    let db = opened.db.clone();
    *active = Some(opened);
    Ok(db)
}

async fn open_database(config: &Config, name: &str) -> Result<ActiveDb, AppError> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_region("auto")
        .with_bucket_name(&config.bucket)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_virtual_hosted_style_request(false)
        .build()
        .map_err(AppError::internal)?;
    let store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(store, name));
    let metrics = Arc::new(DefaultMetricsRecorder::new());
    let cache_dir = cache_dir(name);
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .map_err(AppError::internal)?;
    let db = Db::builder("slatedb", store)
        .with_db_cache(Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: MEMORY_CACHE_BYTES,
            shards: 8,
        })))
        .with_settings(Settings {
            l0_sst_size_bytes: 64 * 1024 * 1024,
            max_unflushed_bytes: 256 * 1024 * 1024,
            object_store_cache_options: ObjectStoreCacheOptions {
                root_folder: Some(cache_dir),
                max_cache_size_bytes: Some(CACHE_BYTES),
                ..ObjectStoreCacheOptions::default()
            },
            metric_level: MetricLevel::Debug,
            ..Settings::default()
        })
        .with_metrics_recorder(metrics.clone())
        .build()
        .await
        .map_err(AppError::internal)?;
    Ok(ActiveDb {
        name: name.to_owned(),
        db,
        metrics,
    })
}

async fn put(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<PutRequest>,
) -> Result<Json<OkResponse>, AppError> {
    database(&state, &name)
        .await?
        .put(body.key, body.value)
        .await
        .map_err(AppError::internal)?;
    Ok(Json(OkResponse { ok: true }))
}

async fn get_value(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<GetQuery>,
) -> Result<Json<ValueResponse>, AppError> {
    let value = database(&state, &name)
        .await?
        .get(&query.key)
        .await
        .map_err(AppError::internal)?
        .map(|value| String::from_utf8_lossy(&value).into_owned());
    Ok(Json(ValueResponse {
        key: query.key,
        value,
    }))
}

async fn delete(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<KeyRequest>,
) -> Result<Json<OkResponse>, AppError> {
    database(&state, &name)
        .await?
        .delete(body.key)
        .await
        .map_err(AppError::internal)?;
    Ok(Json(OkResponse { ok: true }))
}

async fn scan(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<ScanQuery>,
) -> Result<Json<ScanResponse>, AppError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let db = database(&state, &name).await?;
    let mut iterator = match query.prefix.as_deref() {
        Some(prefix) if !prefix.is_empty() => db.scan_prefix(prefix, ..).await,
        _ => db.scan(..).await,
    }
    .map_err(AppError::internal)?;
    let mut items = Vec::new();
    while items.len() <= limit {
        let Some(item) = iterator.next().await.map_err(AppError::internal)? else {
            break;
        };
        items.push(ScanItem {
            key: String::from_utf8_lossy(&item.key).into_owned(),
            value: String::from_utf8_lossy(&item.value).into_owned(),
        });
    }
    let truncated = items.len() > limit;
    items.truncate(limit);
    Ok(Json(ScanResponse { items, truncated }))
}

async fn flush(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<OkResponse>, AppError> {
    database(&state, &name)
        .await?
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .map_err(AppError::internal)?;
    Ok(Json(OkResponse { ok: true }))
}

async fn reopen(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<OkResponse>, AppError> {
    validate_database_name(&name)?;
    let mut active = state.active.lock().await;
    if let Some(current) = active.take() {
        current.db.close().await.map_err(AppError::internal)?;
    }
    *active = Some(open_database(&state.config, &name).await?);
    Ok(Json(OkResponse { ok: true }))
}

async fn clear_cache(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<OkResponse>, AppError> {
    validate_database_name(&name)?;
    let mut active = state.active.lock().await;
    if let Some(current) = active.take() {
        current.db.close().await.map_err(AppError::internal)?;
    }
    let path = cache_dir(&name);
    match tokio::fs::remove_dir_all(&path).await {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(AppError::internal(error)),
    }
    *active = Some(open_database(&state.config, &name).await?);
    Ok(Json(OkResponse { ok: true }))
}

async fn stats(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<StatusResponse>, AppError> {
    validate_database_name(&name)?;
    let (open, slatedb) = state
        .active
        .lock()
        .await
        .as_ref()
        .map(|active| (true, slate_metrics(&active.metrics)))
        .unwrap_or((false, Vec::new()));
    let cache_storage_bytes = directory_size(&cache_dir(&name))
        .await
        .map_err(AppError::internal)?;
    Ok(Json(StatusResponse {
        runtime: "container",
        open,
        cache_populated: cache_storage_bytes > 0,
        cache_storage_bytes,
        adapter: BTreeMap::new(),
        slatedb,
    }))
}

async fn benchmark_batch(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<BenchmarkBatchRequest>,
) -> Result<Json<BenchmarkBatchResponse>, AppError> {
    if body.clients.is_empty()
        || body.clients.len() > 256
        || body
            .clients
            .iter()
            .any(|operations| operations.is_empty() || operations.len() > 512)
    {
        return Err(AppError::bad_request(
            "benchmark batch must contain 1 to 256 clients with 1 to 512 operations each",
        ));
    }
    let db = database(&state, &name).await?;
    let value = Arc::<[u8]>::from(body.value.into_bytes());
    let clients = try_join_all(
        body.clients
            .into_iter()
            .map(|operations| run_benchmark_client(db.clone(), operations, value.clone())),
    )
    .await?;
    let mut result = BenchmarkBatchResponse::default();
    for client in clients {
        result.get.merge(client.get);
        result.put.merge(client.put);
    }
    Ok(Json(result))
}

async fn run_benchmark_client(
    db: Db,
    operations: Vec<BenchmarkOperation>,
    value: Arc<[u8]>,
) -> Result<BenchmarkBatchResponse, AppError> {
    let mut result = BenchmarkBatchResponse::default();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    for operation in operations {
        let started = Instant::now();
        match operation.operation {
            BenchmarkOperationKind::Get => {
                if db
                    .get(operation.key)
                    .await
                    .map_err(AppError::internal)?
                    .is_none()
                {
                    return Err(AppError::internal("benchmark key not found"));
                }
                result.get.record(started.elapsed().as_nanos());
            }
            BenchmarkOperationKind::Put => {
                db.put_with_options(operation.key, value.as_ref(), &put_options, &write_options)
                    .await
                    .map_err(AppError::internal)?;
                result.put.record(started.elapsed().as_nanos());
            }
        }
    }
    Ok(result)
}

impl BenchmarkOperationMeasurements {
    fn record(&mut self, latency_ns: u128) {
        self.operations += 1;
        self.measurable_latency_ns
            .push(latency_ns.min(u128::from(u64::MAX)) as u64);
    }

    fn merge(&mut self, mut other: Self) {
        self.operations += other.operations;
        self.unmeasurable_operations += other.unmeasurable_operations;
        self.measurable_latency_ns
            .append(&mut other.measurable_latency_ns);
    }
}

fn validate_database_name(name: &str) -> Result<(), AppError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    valid
        .then_some(())
        .ok_or_else(|| AppError::bad_request("invalid database name"))
}

fn cache_dir(name: &str) -> PathBuf {
    Path::new(CACHE_ROOT).join(name)
}

async fn directory_size(path: &Path) -> io::Result<u64> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || directory_size_sync(&path))
        .await
        .map_err(io::Error::other)?
}

fn directory_size_sync(path: &Path) -> io::Result<u64> {
    let mut total = 0;
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            directory_size_sync(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}

fn slate_metrics(recorder: &DefaultMetricsRecorder) -> Vec<SlateMetric> {
    recorder
        .snapshot()
        .all()
        .iter()
        .map(|metric| SlateMetric {
            name: metric.name.clone(),
            labels: metric.labels.clone(),
            value: match &metric.value {
                MetricValue::Counter(value) => SlateMetricValue::Counter { value: *value },
                MetricValue::Gauge(value) => SlateMetricValue::Gauge { value: *value },
                MetricValue::UpDownCounter(value) => {
                    SlateMetricValue::UpDownCounter { value: *value }
                }
                MetricValue::Histogram {
                    count,
                    sum,
                    min,
                    max,
                    boundaries,
                    bucket_counts,
                } => SlateMetricValue::Histogram {
                    count: *count,
                    sum: *sum,
                    min: *min,
                    max: *max,
                    boundaries: boundaries.clone(),
                    bucket_counts: bucket_counts.clone(),
                },
            },
        })
        .collect()
}

async fn close_active(state: &AppState) {
    if let Some(active) = state.active.lock().await.take()
        && let Err(error) = active.db.close().await
    {
        tracing::error!(%error, "failed to close SlateDB during shutdown");
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_names_are_safe_for_routing_and_cache_paths() {
        assert!(validate_database_name("tenant_42-primary").is_ok());
        for invalid in ["", "../escape", "has/slash", "has space", "💾"] {
            assert!(
                validate_database_name(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert_eq!(
            cache_dir("tenant_42-primary"),
            Path::new(CACHE_ROOT).join("tenant_42-primary")
        );
    }

    #[test]
    fn benchmark_measurements_merge_without_losing_samples() {
        let mut left = BenchmarkOperationMeasurements::default();
        left.record(10);
        let mut right = BenchmarkOperationMeasurements::default();
        right.record(20);
        left.merge(right);
        assert_eq!(left.operations, 2);
        assert_eq!(left.measurable_latency_ns, [10, 20]);
    }
}

use std::sync::Arc;

use futures::{future::try_join_all, lock::Mutex};
use serde::{Deserialize, Serialize};
use slatedb::Db;
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, FlushOptions, FlushType, MetricLevel, PutOptions,
    Settings, WriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::prefix::PrefixStore;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use worker::*;

mod db_cache;
mod do_cache;
mod perf;
mod r2_store;

use db_cache::QuickDbCache;
use do_cache::DoCacheStorage;
use perf::{PerfCounters, PerfSnapshot};
use r2_store::R2Store;

const DB_BINDING: &str = "SLATEDB_OBJECTS";
const R2_BINDING: &str = "SLATEDB_BUCKET";
const TOKEN_SECRET: &str = "PROBE_TOKEN";
const DB_ROOT: &str = "slatedb";
const CACHE_PART_SIZE: usize = 64 * 1024;

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
struct OkResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    open: bool,
    cache_populated: bool,
    adapter: PerfSnapshot,
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

struct ActiveDb {
    name: String,
    db: Db,
    metrics: Arc<DefaultMetricsRecorder>,
}

#[event(fetch, respond_with_errors)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let url = req.url()?;
    if url.path() == "/" || url.path() == "/health" {
        return Response::from_json(&serde_json::json!({
            "ok": true,
            "service": "slatedb-cloudflare-feasibility"
        }));
    }

    if !authorized(&req, &env)? {
        return Response::error("unauthorized", 401);
    }
    let segments = route_segments(url.path())?;
    let database = segments[2];
    validate_database_name(database)?;

    let namespace = env.durable_object(DB_BINDING)?;
    let stub = namespace.id_from_name(database)?.get_stub()?;
    stub.fetch_with_request(req).await
}

fn authorized(req: &Request, env: &Env) -> Result<bool> {
    let expected = env.secret(TOKEN_SECRET)?.to_string();
    let authorization = req.headers().get("authorization")?;
    let supplied = authorization
        .as_deref()
        .and_then(|value| value.strip_prefix("Bearer "));
    Ok(supplied == Some(expected.as_str()))
}

fn route_segments(path: &str) -> Result<Vec<&str>> {
    let segments: Vec<_> = path.trim_matches('/').split('/').collect();
    if segments.len() < 4 || segments[0] != "v1" || segments[1] != "db" {
        return Err(Error::RustError("route not found".to_owned()));
    }
    Ok(segments)
}

fn validate_database_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(Error::RustError("invalid database name".to_owned()))
    }
}

#[durable_object]
pub struct SlateDbObject {
    env: Env,
    cache: Arc<DoCacheStorage>,
    perf: Arc<PerfCounters>,
    active: Mutex<Option<ActiveDb>>,
}

impl SlateDbObject {
    async fn open(&self, database: &str) -> Result<ActiveDb> {
        let metrics = Arc::new(DefaultMetricsRecorder::new());
        let r2: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            R2Store::new(self.env.clone(), R2_BINDING, Arc::clone(&self.perf)),
            database,
        ));
        let cached: Arc<dyn ObjectStore> = CachedObjectStore::from_storage(
            r2,
            self.cache.clone(),
            CACHE_PART_SIZE,
            metrics.clone(),
            MetricLevel::Debug,
        )
        .await
        .map_err(slatedb_error)?;
        let worker = CompactionWorkerOptions {
            max_concurrent_compactions: 1,
            max_sst_size: 4 * 1024 * 1024,
            max_fetch_tasks: 1,
            max_subcompactions: 1,
            ..CompactionWorkerOptions::default()
        };
        let compactor = CompactorOptions {
            max_concurrent_compactions: 1,
            worker: Some(worker),
            ..CompactorOptions::default()
        };
        let db = Db::builder(DB_ROOT, cached)
            .with_db_cache(Arc::new(QuickDbCache::new()))
            .with_settings(Settings {
                l0_sst_size_bytes: 4 * 1024 * 1024,
                l0_flush_parallelism: 1,
                max_unflushed_bytes: 16 * 1024 * 1024,
                compactor_options: Some(compactor),
                metric_level: MetricLevel::Debug,
                ..Settings::default()
            })
            .with_metrics_recorder(metrics.clone())
            .build()
            .await
            .map_err(slatedb_error)?;
        Ok(ActiveDb {
            name: database.to_owned(),
            db,
            metrics,
        })
    }

    async fn database(&self, name: &str) -> Result<Db> {
        let mut active = self.active.lock().await;
        if let Some(current) = active.as_ref() {
            if current.name != name {
                return Err(Error::RustError(
                    "Durable Object was routed with two database names".to_owned(),
                ));
            }
            return Ok(current.db.clone());
        }
        let opened = self.open(name).await?;
        let db = opened.db.clone();
        *active = Some(opened);
        Ok(db)
    }

    async fn reopen(&self, name: &str) -> Result<()> {
        let mut active = self.active.lock().await;
        if let Some(current) = active.take() {
            current.db.close().await.map_err(slatedb_error)?;
        }
        *active = Some(self.open(name).await?);
        Ok(())
    }

    async fn handle(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        let segments = route_segments(url.path())?;
        let database = segments[2];
        validate_database_name(database)?;
        let tail = &segments[3..];

        match (req.method(), tail) {
            (Method::Post, ["put"]) => {
                let body: PutRequest = req.json().await?;
                self.database(database)
                    .await?
                    .put(body.key.as_bytes(), body.value.as_bytes())
                    .await
                    .map_err(slatedb_error)?;
                Response::from_json(&OkResponse { ok: true })
            }
            (Method::Post, ["admin", "benchmark", "batch"]) => {
                let body: BenchmarkBatchRequest = req.json().await?;
                if body.clients.is_empty()
                    || body.clients.len() > 256
                    || body
                        .clients
                        .iter()
                        .any(|operations| operations.is_empty() || operations.len() > 512)
                {
                    return Response::error(
                        "benchmark batch must contain 1 to 256 clients with 1 to 512 operations each",
                        400,
                    );
                }
                let db = self.database(database).await?;
                let value = body.value.into_bytes();
                let client_measurements =
                    try_join_all(body.clients.into_iter().map(|operations| {
                        run_benchmark_client(db.clone(), operations, value.as_slice())
                    }))
                    .await?;
                let mut measurements = BenchmarkBatchResponse::default();
                for client in client_measurements {
                    measurements.get.merge(client.get);
                    measurements.put.merge(client.put);
                }
                Response::from_json(&measurements)
            }
            (Method::Get, ["get"]) => {
                let key = query(&url, "key")?;
                let value = self
                    .database(database)
                    .await?
                    .get(key.as_bytes())
                    .await
                    .map_err(slatedb_error)?
                    .map(|value| String::from_utf8_lossy(&value).into_owned());
                Response::from_json(&ValueResponse { key, value })
            }
            (Method::Post, ["delete"]) => {
                let body: KeyRequest = req.json().await?;
                self.database(database)
                    .await?
                    .delete(body.key.as_bytes())
                    .await
                    .map_err(slatedb_error)?;
                Response::from_json(&OkResponse { ok: true })
            }
            (Method::Get, ["scan"]) => {
                let prefix = url
                    .query_pairs()
                    .find(|(name, _)| name == "prefix")
                    .map(|(_, value)| value.into_owned())
                    .unwrap_or_default();
                let limit = url
                    .query_pairs()
                    .find(|(name, _)| name == "limit")
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(100)
                    .clamp(1, 1000);
                let db = self.database(database).await?;
                let mut iterator = if prefix.is_empty() {
                    db.scan(..).await.map_err(slatedb_error)?
                } else {
                    db.scan_prefix(prefix.as_bytes(), ..)
                        .await
                        .map_err(slatedb_error)?
                };
                let mut items = Vec::new();
                while items.len() <= limit {
                    let Some(item) = iterator.next().await.map_err(slatedb_error)? else {
                        break;
                    };
                    items.push(ScanItem {
                        key: String::from_utf8_lossy(&item.key).into_owned(),
                        value: String::from_utf8_lossy(&item.value).into_owned(),
                    });
                }
                let truncated = items.len() > limit;
                items.truncate(limit);
                Response::from_json(&ScanResponse { items, truncated })
            }
            (Method::Post, ["admin", "reopen"]) => {
                self.reopen(database).await?;
                Response::from_json(&OkResponse { ok: true })
            }
            (Method::Post, ["admin", "flush"]) => {
                self.database(database)
                    .await?
                    .flush_with_options(FlushOptions {
                        flush_type: FlushType::MemTable,
                    })
                    .await
                    .map_err(slatedb_error)?;
                Response::from_json(&OkResponse { ok: true })
            }
            (Method::Post, ["admin", "cache", "clear"]) => {
                self.cache.clear().await?;
                Response::from_json(&OkResponse { ok: true })
            }
            (Method::Get, ["stats"]) => {
                let (open, slatedb) = self
                    .active
                    .lock()
                    .await
                    .as_ref()
                    .map(|active| (true, slate_metrics(&active.metrics)))
                    .unwrap_or_else(|| (false, Vec::new()));
                let cache_populated = self.cache.is_populated().await?;
                Response::from_json(&StatusResponse {
                    open,
                    cache_populated,
                    adapter: self.perf.snapshot(),
                    slatedb,
                })
            }
            _ => Response::error("route not found", 404),
        }
    }
}

impl DurableObject for SlateDbObject {
    fn new(state: State, env: Env) -> Self {
        let perf = Arc::new(PerfCounters::default());
        Self {
            env,
            cache: Arc::new(DoCacheStorage::new(state.storage(), Arc::clone(&perf))),
            perf,
            active: Mutex::new(None),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        match self.handle(req).await {
            Ok(response) => Ok(response),
            Err(error) => Response::from_json(&serde_json::json!({
                "error": error.to_string()
            }))
            .map(|response| response.with_status(500)),
        }
    }
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

fn query(url: &Url, name: &str) -> Result<String> {
    url.query_pairs()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| Error::RustError(format!("missing query parameter: {name}")))
}

fn slatedb_error(error: slatedb::Error) -> Error {
    Error::RustError(format!("SlateDB: {error}"))
}

fn elapsed_ns(started: web_time::Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

async fn run_benchmark_client(
    db: Db,
    operations: Vec<BenchmarkOperation>,
    value: &[u8],
) -> Result<BenchmarkBatchResponse> {
    let mut measurements = BenchmarkBatchResponse::default();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    for operation in operations {
        let started = web_time::Instant::now();
        match operation.operation {
            BenchmarkOperationKind::Get => {
                let found = db
                    .get(operation.key.as_bytes())
                    .await
                    .map_err(slatedb_error)?;
                let latency_ns = elapsed_ns(started);
                if found.is_none() {
                    return Err(Error::RustError("benchmark key not found".to_owned()));
                }
                measurements.get.record(latency_ns);
            }
            BenchmarkOperationKind::Put => {
                db.put_with_options(
                    operation.key.as_bytes(),
                    value,
                    &put_options,
                    &write_options,
                )
                .await
                .map_err(slatedb_error)?;
                measurements.put.record(elapsed_ns(started));
            }
        }
    }
    Ok(measurements)
}

impl BenchmarkOperationMeasurements {
    fn record(&mut self, latency_ns: u64) {
        self.operations += 1;
        if latency_ns == 0 {
            self.unmeasurable_operations += 1;
        } else {
            self.measurable_latency_ns.push(latency_ns);
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.operations += other.operations;
        self.unmeasurable_operations += other.unmeasurable_operations;
        self.measurable_latency_ns
            .append(&mut other.measurable_latency_ns);
    }
}

use std::sync::Arc;

use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use slatedb::Db;
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::prefix::PrefixStore;
use worker::*;

mod do_cache;
mod r2_store;

use do_cache::DoCacheStorage;
use r2_store::R2Store;

const DB_BINDING: &str = "SLATEDB_OBJECTS";
const R2_BINDING: &str = "SLATEDB_BUCKET";
const TOKEN_SECRET: &str = "PROBE_TOKEN";
const DB_ROOT: &str = "slatedb";
const CACHE_PART_SIZE: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct KeyRequest {
    key: String,
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
}

struct ActiveDb {
    name: String,
    db: Db,
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
    active: Mutex<Option<ActiveDb>>,
}

impl SlateDbObject {
    async fn open(&self, database: &str) -> Result<ActiveDb> {
        let r2: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            R2Store::new(self.env.clone(), R2_BINDING),
            database,
        ));
        let cached: Arc<dyn ObjectStore> =
            CachedObjectStore::from_storage(r2, self.cache.clone(), CACHE_PART_SIZE)
                .await
                .map_err(slatedb_error)?;
        let db = Db::builder(DB_ROOT, cached)
            .with_db_cache_disabled()
            .build()
            .await
            .map_err(slatedb_error)?;
        Ok(ActiveDb {
            name: database.to_owned(),
            db,
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
                let open = self.active.lock().await.is_some();
                let cache_populated = self.cache.is_populated().await?;
                Response::from_json(&StatusResponse {
                    open,
                    cache_populated,
                })
            }
            _ => Response::error("route not found", 404),
        }
    }
}

impl DurableObject for SlateDbObject {
    fn new(state: State, env: Env) -> Self {
        Self {
            env,
            cache: Arc::new(DoCacheStorage::new(state.storage())),
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

fn query(url: &Url, name: &str) -> Result<String> {
    url.query_pairs()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| Error::RustError(format!("missing query parameter: {name}")))
}

fn slatedb_error(error: slatedb::Error) -> Error {
    Error::RustError(format!("SlateDB: {error}"))
}

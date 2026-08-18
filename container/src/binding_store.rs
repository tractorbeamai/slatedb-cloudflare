use std::{
    collections::BTreeMap,
    fmt::{Debug, Display, Formatter},
    io,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use futures::{
    FutureExt, StreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    Attributes, CopyMode, CopyOptions, Error, Extensions, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use reqwest::{Client, Response, StatusCode, header};
use serde::{Deserialize, Serialize};

const STORE: &str = "cloudflare-r2-binding";
const ENDPOINT: &str = "http://slatedb.r2";
const MAX_MULTIPART_PARTS: u16 = 10_000;

#[derive(Debug, Default)]
pub struct BindingStoreCounters {
    gets: AtomicU64,
    heads: AtomicU64,
    puts: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
    multipart_uploads: AtomicU64,
    multipart_parts: AtomicU64,
    multipart_completes: AtomicU64,
    read_bytes: AtomicU64,
    written_bytes: AtomicU64,
    errors: AtomicU64,
}

impl BindingStoreCounters {
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        [
            ("r2Gets", &self.gets),
            ("r2Heads", &self.heads),
            ("r2Puts", &self.puts),
            ("r2Lists", &self.lists),
            ("r2Deletes", &self.deletes),
            ("r2MultipartUploads", &self.multipart_uploads),
            ("r2MultipartParts", &self.multipart_parts),
            ("r2MultipartCompletes", &self.multipart_completes),
            ("r2ReadBytes", &self.read_bytes),
            ("r2WrittenBytes", &self.written_bytes),
            ("r2Errors", &self.errors),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.load(Ordering::Relaxed)))
        .collect()
    }
}

#[derive(Clone)]
pub struct BindingStore {
    client: Client,
    counters: Arc<BindingStoreCounters>,
}

impl BindingStore {
    pub fn new(counters: Arc<BindingStoreCounters>) -> Self {
        Self {
            client: Client::new(),
            counters,
        }
    }

    fn request(&self, method: reqwest::Method, location: &Path) -> reqwest::RequestBuilder {
        self.client
            .request(method, ENDPOINT)
            .header("x-slate-path", location.as_ref())
    }

    async fn list_page(
        &self,
        prefix: Option<&str>,
        cursor: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<ListPage> {
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        let mut query = Vec::new();
        if let Some(value) = prefix {
            query.push(("prefix", value));
        }
        if let Some(value) = cursor {
            query.push(("cursor", value));
        }
        if let Some(value) = delimiter {
            query.push(("delimiter", value));
        }
        let response = self
            .client
            .get(ENDPOINT)
            .header("x-slate-operation", "list")
            .query(&query)
            .send()
            .await
            .map_err(|error| self.failed(error))?;
        check_response(response)
            .await?
            .json()
            .await
            .map_err(generic)
    }
}

impl Debug for BindingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BindingStore").finish_non_exhaustive()
    }
}

impl Display for BindingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("CloudflareR2Binding")
    }
}

impl BindingStore {
    fn failed(&self, error: impl std::fmt::Display) -> Error {
        self.counters.errors.fetch_add(1, Ordering::Relaxed);
        generic(error)
    }
}

#[async_trait]
impl ObjectStore for BindingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        let size = payload.content_length() as u64;
        let mut request = self.request(reqwest::Method::PUT, location);
        request = match &opts.mode {
            PutMode::Overwrite => request,
            PutMode::Create => request.header(header::IF_NONE_MATCH, "*"),
            PutMode::Update(version) => {
                let etag = version.e_tag.as_ref().ok_or_else(|| Error::NotSupported {
                    source: message("R2 conditional update requires an ETag"),
                })?;
                request.header(header::IF_MATCH, etag)
            }
        };
        let body =
            reqwest::Body::wrap_stream(stream::iter(payload.into_iter().map(Ok::<_, io::Error>)));
        let response = request
            .body(body)
            .send()
            .await
            .map_err(|error| self.failed(error))?;
        if response.status() == StatusCode::PRECONDITION_FAILED {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
            return Err(match opts.mode {
                PutMode::Create => Error::AlreadyExists {
                    path: location.to_string(),
                    source: message("R2 create precondition failed"),
                },
                _ => Error::Precondition {
                    path: location.to_string(),
                    source: message("R2 update precondition failed"),
                },
            });
        }
        let response = check_response(response).await?;
        self.counters
            .written_bytes
            .fetch_add(size, Ordering::Relaxed);
        put_result(&response)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.counters
            .multipart_uploads
            .fetch_add(1, Ordering::Relaxed);
        let response = self
            .request(reqwest::Method::POST, location)
            .header("x-slate-operation", "multipart-start")
            .send()
            .await
            .map_err(|error| self.failed(error))?;
        let started: MultipartStarted = check_response(response)
            .await?
            .json()
            .await
            .map_err(generic)?;
        Ok(Box::new(BindingMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            upload_id: started.upload_id,
            parts: Arc::new(Mutex::new(Vec::new())),
            next_part: 1,
            finished: false,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let method = if options.head {
            self.counters.heads.fetch_add(1, Ordering::Relaxed);
            reqwest::Method::HEAD
        } else {
            self.counters.gets.fetch_add(1, Ordering::Relaxed);
            reqwest::Method::GET
        };
        if options.version.is_some() {
            return Err(Error::NotSupported {
                source: message("R2 Workers bindings do not expose versioned GET"),
            });
        }
        let mut request = self.request(method, location);
        if let Some(value) = &options.if_match {
            request = request.header(header::IF_MATCH, value);
        }
        if let Some(value) = &options.if_none_match {
            request = request.header(header::IF_NONE_MATCH, value);
        }
        if let Some(value) = options.if_modified_since {
            request = request.header(header::IF_MODIFIED_SINCE, value.to_rfc2822());
        }
        if let Some(value) = options.if_unmodified_since {
            request = request.header(header::IF_UNMODIFIED_SINCE, value.to_rfc2822());
        }
        if let Some(range) = &options.range {
            request = request.header(header::RANGE, range_header(range)?);
        }
        let response = request.send().await.map_err(|error| self.failed(error))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: message("R2 object not found"),
            });
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: message("R2 read precondition failed"),
            });
        }
        if response.status() == StatusCode::NOT_MODIFIED {
            return Err(Error::NotModified {
                path: location.to_string(),
                source: message("R2 object was not modified"),
            });
        }
        let response = check_response(response).await?;
        let meta = object_meta(location.clone(), &response)?;
        options.check_preconditions(&meta)?;
        let returned = returned_range(options.range.as_ref(), meta.size)?;
        let counters = Arc::clone(&self.counters);
        let payload = if options.head {
            GetResultPayload::Stream(stream::empty().boxed())
        } else {
            GetResultPayload::Stream(
                response
                    .bytes_stream()
                    .map(move |chunk| {
                        chunk
                            .inspect(|bytes| {
                                counters
                                    .read_bytes
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            })
                            .map_err(generic)
                    })
                    .boxed(),
            )
        };
        Ok(GetResult {
            payload,
            meta,
            range: returned,
            attributes: Attributes::new(),
            extensions: Extensions::new(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.counters.deletes.fetch_add(1, Ordering::Relaxed);
                    let response = store
                        .request(reqwest::Method::DELETE, &location)
                        .send()
                        .await
                        .map_err(|error| store.failed(error))?;
                    check_response(response).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.map(ToString::to_string);
        stream::try_unfold(
            (store, prefix, None::<String>, Vec::new(), false),
            |(store, prefix, mut cursor, mut buffered, finished)| async move {
                if let Some(item) = buffered.pop() {
                    return Ok(Some((item, (store, prefix, cursor, buffered, finished))));
                }
                if finished {
                    return Ok(None);
                }
                loop {
                    let page = store
                        .list_page(prefix.as_deref(), cursor.as_deref(), None)
                        .await?;
                    let mut items = page
                        .objects
                        .into_iter()
                        .map(ListObject::into_meta)
                        .collect::<Result<Vec<_>>>()?;
                    items.reverse();
                    let next = page.cursor;
                    if let Some(item) = items.pop() {
                        return Ok(Some((
                            item,
                            (store, prefix, next.clone(), items, next.is_none()),
                        )));
                    }
                    match next {
                        Some(next) => cursor = Some(next),
                        None => return Ok(None),
                    }
                }
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.map(ToString::to_string);
        let mut cursor = None;
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        loop {
            let page = self
                .list_page(prefix.as_deref(), cursor.as_deref(), Some("/"))
                .await?;
            objects.extend(
                page.objects
                    .into_iter()
                    .map(ListObject::into_meta)
                    .collect::<Result<Vec<_>>>()?,
            );
            common_prefixes.extend(
                page.prefixes
                    .into_iter()
                    .map(|value| Path::from(value.trim_end_matches('/'))),
            );
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Extensions::new(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let bytes = self
            .get_opts(from, GetOptions::default())
            .await?
            .bytes()
            .await?;
        let mode = match opts.mode {
            CopyMode::Overwrite => PutMode::Overwrite,
            CopyMode::Create => PutMode::Create,
        };
        self.put_opts(to, bytes.into(), PutOptions::from(mode))
            .await?;
        Ok(())
    }
}

struct BindingMultipartUpload {
    store: BindingStore,
    location: Path,
    upload_id: String,
    parts: Arc<Mutex<Vec<MultipartPart>>>,
    next_part: u16,
    finished: bool,
}

impl Debug for BindingMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BindingMultipartUpload")
            .field("location", &self.location)
            .field("next_part", &self.next_part)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for BindingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.finished {
            return futures::future::ready(Err(multipart_finished(&self.location))).boxed();
        }
        if self.next_part > MAX_MULTIPART_PARTS {
            return futures::future::ready(Err(Error::NotSupported {
                source: message("R2 supports at most 10,000 multipart parts"),
            }))
            .boxed();
        }
        let part_number = self.next_part;
        self.next_part += 1;
        let store = self.store.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();
        let parts = Arc::clone(&self.parts);
        let size = data.content_length() as u64;
        async move {
            let body =
                reqwest::Body::wrap_stream(stream::iter(data.into_iter().map(Ok::<_, io::Error>)));
            let response = store
                .request(reqwest::Method::PUT, &location)
                .header("x-slate-operation", "multipart-part")
                .header("x-slate-upload-id", upload_id)
                .header("x-slate-part-number", part_number)
                .body(body)
                .send()
                .await
                .map_err(|error| store.failed(error))?;
            let part: MultipartPart = check_response(response)
                .await?
                .json()
                .await
                .map_err(generic)?;
            store
                .counters
                .multipart_parts
                .fetch_add(1, Ordering::Relaxed);
            store
                .counters
                .written_bytes
                .fetch_add(size, Ordering::Relaxed);
            parts
                .lock()
                .map_err(|_| generic("multipart state lock poisoned"))?
                .push(part);
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished {
            return Err(multipart_finished(&self.location));
        }
        self.finished = true;
        let mut parts = self
            .parts
            .lock()
            .map_err(|_| generic("multipart state lock poisoned"))?
            .clone();
        parts.sort_unstable_by_key(|part| part.part_number);
        let response = self
            .store
            .request(reqwest::Method::POST, &self.location)
            .header("x-slate-operation", "multipart-complete")
            .header("x-slate-upload-id", &self.upload_id)
            .json(&parts)
            .send()
            .await
            .map_err(|error| self.store.failed(error))?;
        let response = check_response(response).await?;
        self.store
            .counters
            .multipart_completes
            .fetch_add(1, Ordering::Relaxed);
        put_result(&response)
    }

    async fn abort(&mut self) -> Result<()> {
        if self.finished {
            return Err(multipart_finished(&self.location));
        }
        self.finished = true;
        let response = self
            .store
            .request(reqwest::Method::DELETE, &self.location)
            .header("x-slate-operation", "multipart-abort")
            .header("x-slate-upload-id", &self.upload_id)
            .send()
            .await
            .map_err(|error| self.store.failed(error))?;
        check_response(response).await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultipartStarted {
    upload_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MultipartPart {
    part_number: u16,
    etag: String,
}

#[derive(Debug, Deserialize)]
struct ListPage {
    objects: Vec<ListObject>,
    #[serde(default)]
    prefixes: Vec<String>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListObject {
    key: String,
    size: u64,
    etag: String,
    version: String,
    uploaded: chrono::DateTime<chrono::Utc>,
}

impl ListObject {
    fn into_meta(self) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: Path::parse(self.key).map_err(generic)?,
            last_modified: self.uploaded,
            size: self.size,
            e_tag: Some(self.etag),
            version: Some(self.version),
        })
    }
}

async fn check_response(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let detail = response.text().await.unwrap_or_default();
    Err(generic(format!(
        "R2 binding bridge returned {status}: {detail}"
    )))
}

fn put_result(response: &Response) -> Result<PutResult> {
    Ok(PutResult {
        e_tag: header_value(response, header::ETAG)?,
        version: header_value(response, "x-slate-version")?,
        extensions: Extensions::new(),
    })
}

fn object_meta(location: Path, response: &Response) -> Result<ObjectMeta> {
    let size = required_header(response, "x-slate-size")?
        .parse()
        .map_err(generic)?;
    let uploaded = required_header(response, "x-slate-uploaded")?
        .parse()
        .map_err(generic)?;
    Ok(ObjectMeta {
        location,
        last_modified: uploaded,
        size,
        e_tag: header_value(response, header::ETAG)?,
        version: header_value(response, "x-slate-version")?,
    })
}

fn header_value(
    response: &Response,
    name: impl reqwest::header::AsHeaderName,
) -> Result<Option<String>> {
    response
        .headers()
        .get(name)
        .map(|value| value.to_str().map(str::to_owned).map_err(generic))
        .transpose()
}

fn required_header<'a>(response: &'a Response, name: &str) -> Result<&'a str> {
    response
        .headers()
        .get(name)
        .ok_or_else(|| generic(format!("missing {name} header")))?
        .to_str()
        .map_err(generic)
}

fn range_header(range: &GetRange) -> Result<String> {
    range.is_valid().map_err(generic)?;
    Ok(match range {
        GetRange::Bounded(value) => format!("bytes={}-{}", value.start, value.end - 1),
        GetRange::Offset(value) => format!("bytes={value}-"),
        GetRange::Suffix(value) => format!("bytes=-{value}"),
    })
}

fn returned_range(range: Option<&GetRange>, size: u64) -> Result<Range<u64>> {
    range
        .map(|value| value.as_range(size).map_err(generic))
        .unwrap_or(Ok(0..size))
}

fn multipart_finished(location: &Path) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: message("multipart upload already finished"),
    }
}

fn generic(error: impl std::fmt::Display) -> Error {
    Error::Generic {
        store: STORE,
        source: message(error.to_string()),
    }
}

fn message(value: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::other(value.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_all_range_shapes() {
        assert_eq!(range_header(&GetRange::Bounded(2..7)).unwrap(), "bytes=2-6");
        assert_eq!(range_header(&GetRange::Offset(8)).unwrap(), "bytes=8-");
        assert_eq!(range_header(&GetRange::Suffix(3)).unwrap(), "bytes=-3");
    }

    #[test]
    fn counter_snapshot_uses_benchmark_names() {
        let counters = BindingStoreCounters::default();
        counters.gets.store(2, Ordering::Relaxed);
        assert_eq!(counters.snapshot()["r2Gets"], 2);
    }
}

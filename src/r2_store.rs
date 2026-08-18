use std::fmt::{Debug, Display, Formatter};
use std::ops::Range as ByteRange;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Attributes, CopyMode, CopyOptions, Error, Extensions, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use worker::send::IntoSendFuture;
use worker::{Conditional, Date, DateInit, Env, Range as R2Range};

const STORE: &str = "cloudflare-r2";
const MAX_MULTIPART_PARTS: u16 = 10_000;

#[derive(Clone)]
pub struct R2Store {
    env: Env,
    binding: &'static str,
}

impl R2Store {
    pub fn new(env: Env, binding: &'static str) -> Self {
        Self { env, binding }
    }

    fn key(&self, path: &Path) -> String {
        path.to_string()
    }

    fn bucket(&self) -> Result<worker::Bucket> {
        self.env.bucket(self.binding).map_err(generic)
    }

    async fn put_bytes(&self, location: &Path, bytes: Vec<u8>, mode: PutMode) -> Result<PutResult> {
        let bucket = self.bucket()?;
        let mut request = bucket.put(self.key(location), bytes);
        let conditional = put_conditional(&mode)?;
        if let Some(conditional) = conditional {
            request = request.only_if(conditional);
        }
        let object = request.execute().await.map_err(generic)?;
        let object = object.ok_or_else(|| failed_put_precondition(location, &mode))?;
        Ok(PutResult {
            e_tag: Some(object.etag()),
            version: Some(object.version()),
            extensions: Extensions::new(),
        })
    }

    async fn get_inner(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::NotSupported {
                source: message("R2 Workers bindings do not expose versioned GET"),
            });
        }
        let bucket = self.bucket()?;
        let key = self.key(location);
        let plain_head = options.head
            && options.range.is_none()
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none();
        let object = if plain_head {
            bucket.head(key).await.map_err(generic)?
        } else {
            let mut request = bucket.get(key);
            let conditional = Conditional {
                etag_matches: options.if_match.clone(),
                etag_does_not_match: options.if_none_match.clone(),
                uploaded_before: options
                    .if_unmodified_since
                    .map(|date| Date::from(DateInit::Millis(date.timestamp_millis() as u64))),
                uploaded_after: options
                    .if_modified_since
                    .map(|date| Date::from(DateInit::Millis(date.timestamp_millis() as u64))),
            };
            if conditional != Conditional::default() {
                request = request.only_if(conditional);
            }
            if let Some(range) = options.range.as_ref() {
                request = request.range(to_r2_range(range)?);
            }
            request.execute().await.map_err(generic)?
        }
        .ok_or_else(|| Error::NotFound {
            path: location.to_string(),
            source: message("R2 object not found"),
        })?;
        let meta = object_meta(location.clone(), &object);
        options.check_preconditions(&meta)?;
        let range = returned_range(options.range.as_ref(), meta.size)?;
        let bytes = if options.head {
            Bytes::new()
        } else {
            let body = object.body().ok_or_else(|| Error::Precondition {
                path: location.to_string(),
                source: message("R2 returned metadata without a body"),
            })?;
            Bytes::from(body.bytes().await.map_err(generic)?)
        };
        Ok(result(meta, range, bytes))
    }

    async fn list_page(
        &self,
        prefix: Option<String>,
        cursor: Option<String>,
        delimiter: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, Vec<Path>, Option<String>)> {
        let bucket = self.bucket()?;
        let mut request = bucket.list().prefix(prefix.unwrap_or_default());
        if let Some(cursor) = cursor {
            request = request.cursor(cursor);
        }
        if let Some(delimiter) = delimiter {
            request = request.delimiter(delimiter);
        }
        let objects = request.execute().await.map_err(generic)?;
        let mut metadata = Vec::new();
        for object in objects.objects() {
            metadata.push(object_meta(Path::from(object.key()), &object));
        }
        let mut common_prefixes = Vec::new();
        for prefix in objects.delimited_prefixes() {
            common_prefixes.push(Path::from(prefix.trim_end_matches('/')));
        }
        let next = next_list_cursor(objects.truncated(), objects.cursor());
        Ok((metadata, common_prefixes, next))
    }
}

impl Debug for R2Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2Store")
            .field("binding", &self.binding)
            .finish()
    }
}

impl Display for R2Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "R2({})", self.binding)
    }
}

#[async_trait]
impl ObjectStore for R2Store {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let bytes = collect_payload(payload);
        self.put_bytes(location, bytes, opts.mode).into_send().await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let key = self.key(location);
        let upload = self
            .bucket()?
            .create_multipart_upload(key.clone())
            .execute()
            .into_send()
            .await
            .map_err(generic)?;
        let upload_id = upload.upload_id().into_send().await;
        Ok(Box::new(R2MultipartUpload {
            env: self.env.clone(),
            binding: self.binding,
            location: location.clone(),
            key,
            upload_id,
            uploaded_parts: Arc::new(Mutex::new(Vec::new())),
            next_part: 1,
            finished: false,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.get_inner(location, options).into_send().await
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
                    let bucket = store.bucket()?;
                    bucket
                        .delete(store.key(&location))
                        .into_send()
                        .await
                        .map_err(generic)?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.map(ToString::to_string);
        stream::try_unfold(
            (
                store,
                prefix,
                None::<String>,
                Vec::<ObjectMeta>::new(),
                false,
            ),
            |(store, prefix, mut cursor, mut buffered, finished)| async move {
                if let Some(item) = buffered.pop() {
                    return Ok(Some((item, (store, prefix, cursor, buffered, finished))));
                }
                if finished {
                    return Ok(None);
                }
                loop {
                    let (mut items, _, next) = store
                        .list_page(prefix.clone(), cursor, None)
                        .into_send()
                        .await?;
                    items.reverse();
                    if let Some(item) = items.pop() {
                        let finished = next.is_none();
                        return Ok(Some((item, (store, prefix, next, items, finished))));
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
        let mut all_objects = Vec::new();
        let mut all_prefixes = Vec::new();
        loop {
            let (objects, prefixes, next) = self
                .list_page(prefix.clone(), cursor, Some("/"))
                .into_send()
                .await?;
            all_objects.extend(objects);
            all_prefixes.extend(prefixes);
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(ListResult {
            common_prefixes: all_prefixes,
            objects: all_objects,
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

struct R2MultipartUpload {
    env: Env,
    binding: &'static str,
    location: Path,
    key: String,
    upload_id: String,
    uploaded_parts: Arc<Mutex<Vec<(u16, String)>>>,
    next_part: u16,
    finished: bool,
}

impl Debug for R2MultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2MultipartUpload")
            .field("location", &self.location)
            .field("next_part", &self.next_part)
            .field("finished", &self.finished)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for R2MultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.finished {
            return futures::future::ready(Err(multipart_finished(&self.location))).boxed();
        }
        if self.next_part > MAX_MULTIPART_PARTS {
            return async {
                Err(Error::NotSupported {
                    source: message("R2 multipart uploads support at most 10,000 parts"),
                })
            }
            .boxed();
        }

        let part_number = self.next_part;
        self.next_part += 1;
        let env = self.env.clone();
        let binding = self.binding;
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let uploaded_parts = Arc::clone(&self.uploaded_parts);
        let bytes = collect_payload(data);
        async move {
            let upload = env
                .bucket(binding)
                .map_err(generic)?
                .resume_multipart_upload(key, upload_id)
                .map_err(generic)?;
            let part = upload
                .upload_part(part_number, bytes)
                .into_send()
                .await
                .map_err(generic)?;
            uploaded_parts
                .lock()
                .map_err(|_| multipart_lock_error())?
                .push((part.part_number(), part.etag()));
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished {
            return Err(multipart_finished(&self.location));
        }
        self.finished = true;
        let upload = self
            .env
            .bucket(self.binding)
            .map_err(generic)?
            .resume_multipart_upload(self.key.clone(), self.upload_id.clone())
            .map_err(generic)?;
        let mut parts = self
            .uploaded_parts
            .lock()
            .map_err(|_| multipart_lock_error())?
            .clone();
        parts.sort_unstable_by_key(|(part_number, _)| *part_number);
        let parts = parts
            .into_iter()
            .map(|(part_number, etag)| worker::UploadedPart::new(part_number, etag));
        let object = upload.complete(parts).into_send().await.map_err(generic)?;
        Ok(PutResult {
            e_tag: Some(object.etag()),
            version: Some(object.version()),
            extensions: Extensions::new(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        if self.finished {
            return Err(multipart_finished(&self.location));
        }
        self.finished = true;
        self.env
            .bucket(self.binding)
            .map_err(generic)?
            .resume_multipart_upload(self.key.clone(), self.upload_id.clone())
            .map_err(generic)?
            .abort()
            .into_send()
            .await
            .map_err(generic)
    }
}

fn multipart_finished(location: &Path) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: message("multipart upload already finished"),
    }
}

fn multipart_lock_error() -> Error {
    Error::Generic {
        store: STORE,
        source: message("multipart upload state lock poisoned"),
    }
}

fn collect_payload(payload: PutPayload) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.content_length());
    for chunk in payload {
        bytes.extend_from_slice(&chunk);
    }
    bytes
}

fn put_conditional(mode: &PutMode) -> Result<Option<Conditional>> {
    match mode {
        PutMode::Overwrite => Ok(None),
        PutMode::Create => Ok(Some(Conditional {
            etag_does_not_match: Some("*".to_owned()),
            ..Default::default()
        })),
        PutMode::Update(version) => {
            if version.version.is_some() && version.e_tag.is_none() {
                return Err(Error::NotSupported {
                    source: message("R2 conditional update requires an ETag"),
                });
            }
            Ok(Some(Conditional {
                etag_matches: version.e_tag.clone(),
                ..Default::default()
            }))
        }
    }
}

fn failed_put_precondition(location: &Path, mode: &PutMode) -> Error {
    match mode {
        PutMode::Create => Error::AlreadyExists {
            path: location.to_string(),
            source: message("R2 create precondition failed"),
        },
        _ => Error::Precondition {
            path: location.to_string(),
            source: message("R2 update precondition failed"),
        },
    }
}

fn next_list_cursor(truncated: bool, cursor: Option<String>) -> Option<String> {
    truncated.then_some(cursor).flatten()
}

fn object_meta(location: Path, object: &worker::Object) -> ObjectMeta {
    let uploaded = object.uploaded().as_millis() as i64;
    ObjectMeta {
        location,
        last_modified: chrono::DateTime::from_timestamp_millis(uploaded)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH),
        size: object.size(),
        e_tag: Some(object.etag()),
        version: Some(object.version()),
    }
}

fn result(meta: ObjectMeta, range: ByteRange<u64>, bytes: Bytes) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
        meta,
        range,
        attributes: Attributes::new(),
        extensions: Extensions::new(),
    }
}

fn to_r2_range(range: &GetRange) -> Result<R2Range> {
    range.is_valid().map_err(|error| Error::Generic {
        store: STORE,
        source: Box::new(error),
    })?;
    Ok(match range {
        GetRange::Bounded(range) => R2Range::OffsetWithLength {
            offset: range.start,
            length: range.end - range.start,
        },
        GetRange::Offset(offset) => R2Range::OffsetToEnd { offset: *offset },
        GetRange::Suffix(suffix) => R2Range::Suffix { suffix: *suffix },
    })
}

fn returned_range(range: Option<&GetRange>, size: u64) -> Result<ByteRange<u64>> {
    range
        .map(|range| {
            range.as_range(size).map_err(|error| Error::Generic {
                store: STORE,
                source: Box::new(error),
            })
        })
        .unwrap_or(Ok(0..size))
}

fn generic(error: worker::Error) -> Error {
    Error::Generic {
        store: STORE,
        source: message(error.to_string()),
    }
}

fn message(value: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(std::io::Error::other(value.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_all_range_shapes() {
        assert_eq!(
            returned_range(Some(&GetRange::Bounded(2..7)), 10).unwrap(),
            2..7
        );
        assert_eq!(
            returned_range(Some(&GetRange::Offset(8)), 10).unwrap(),
            8..10
        );
        assert_eq!(
            returned_range(Some(&GetRange::Suffix(3)), 10).unwrap(),
            7..10
        );
    }

    #[test]
    fn collects_chunked_payload_in_order() {
        let payload: PutPayload = vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]
            .into_iter()
            .collect();
        assert_eq!(collect_payload(payload), b"abcd");
    }

    #[test]
    fn translates_conditional_writes_and_failures() {
        assert!(put_conditional(&PutMode::Overwrite).unwrap().is_none());

        let create = put_conditional(&PutMode::Create).unwrap().unwrap();
        assert_eq!(create.etag_does_not_match.as_deref(), Some("*"));
        assert!(matches!(
            failed_put_precondition(&Path::from("manifest"), &PutMode::Create),
            Error::AlreadyExists { .. }
        ));

        let update = PutMode::Update(slatedb::object_store::UpdateVersion {
            e_tag: Some("etag-1".to_owned()),
            version: None,
        });
        let conditional = put_conditional(&update).unwrap().unwrap();
        assert_eq!(conditional.etag_matches.as_deref(), Some("etag-1"));
        assert!(matches!(
            failed_put_precondition(&Path::from("manifest"), &update),
            Error::Precondition { .. }
        ));

        let version_only = PutMode::Update(slatedb::object_store::UpdateVersion {
            e_tag: None,
            version: Some("version-1".to_owned()),
        });
        assert!(matches!(
            put_conditional(&version_only),
            Err(Error::NotSupported { .. })
        ));
    }

    #[test]
    fn pagination_uses_cursor_only_for_truncated_pages() {
        assert_eq!(
            next_list_cursor(true, Some("next".to_owned())).as_deref(),
            Some("next")
        );
        assert_eq!(next_list_cursor(false, Some("stale".to_owned())), None);
        assert_eq!(next_list_cursor(true, None), None);
    }
}

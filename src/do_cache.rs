use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde_bytes::ByteBuf;
use slatedb::cached_object_store::{LocalCacheEntry, LocalCacheHead, LocalCacheStorage, PartID};
use slatedb::object_store::path::Path;
use slatedb::object_store::{Attributes, Error, ObjectMeta, Result};
use worker::send::{IntoSendFuture, SendWrapper};
use worker::{ListOptions, Storage};

use crate::perf::{PerfCounters, increment};

const STORE: &str = "cloudflare-do-cache";
const CACHE_PREFIX: &str = "slatedb-cache:";

pub struct DoCacheStorage {
    // SlateDB requires Send + Sync; Workers keep bindings on one isolate thread.
    storage: Arc<SendWrapper<Storage>>,
    perf: Arc<PerfCounters>,
}

impl DoCacheStorage {
    pub fn new(storage: Storage, perf: Arc<PerfCounters>) -> Self {
        Self {
            storage: Arc::new(SendWrapper::new(storage)),
            perf,
        }
    }

    pub async fn clear(&self) -> worker::Result<()> {
        self.storage.delete_all().into_send().await
    }

    pub async fn is_populated(&self) -> worker::Result<bool> {
        self.storage
            .list_with_options(ListOptions::new().prefix(CACHE_PREFIX).limit(1))
            .into_send()
            .await
            .map(|entries| entries.size() != 0)
    }
}

impl Debug for DoCacheStorage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DoCacheStorage")
    }
}

impl Display for DoCacheStorage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Cloudflare Durable Object storage")
    }
}

#[async_trait]
impl LocalCacheStorage for DoCacheStorage {
    fn entry(&self, location: &Path, part_size: usize) -> Box<dyn LocalCacheEntry> {
        Box::new(DoCacheEntry {
            storage: Arc::clone(&self.storage),
            perf: Arc::clone(&self.perf),
            prefix: entry_prefix(location, part_size),
            part_size,
        })
    }

    async fn start_evictor(&self) {}
}

struct DoCacheEntry {
    storage: Arc<SendWrapper<Storage>>,
    perf: Arc<PerfCounters>,
    prefix: String,
    part_size: usize,
}

impl DoCacheEntry {
    fn head_key(&self) -> String {
        format!("{}head", self.prefix)
    }

    fn part_key(&self, part_number: PartID) -> String {
        format!("{}part:{part_number}", self.prefix)
    }
}

impl Debug for DoCacheEntry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DoCacheEntry")
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[async_trait]
impl LocalCacheEntry for DoCacheEntry {
    async fn save_part(&self, part_number: PartID, bytes: Bytes) -> Result<()> {
        let byte_count = bytes.len() as u64;
        let result = self
            .storage
            .put(
                &self.part_key(part_number),
                serde_bytes::Bytes::new(bytes.as_ref()),
            )
            .into_send()
            .await
            .map_err(cache_error);
        if result.is_ok() {
            increment(&self.perf.cache_part_writes, 1);
            increment(&self.perf.cache_written_bytes, byte_count);
        } else {
            increment(&self.perf.cache_errors, 1);
        }
        result
    }

    async fn read_part(
        &self,
        part_number: PartID,
        range_in_part: Range<usize>,
    ) -> Result<Option<Bytes>> {
        increment(&self.perf.cache_part_reads, 1);
        increment(&self.perf.cache_requested_bytes, range_in_part.len() as u64);
        let loaded = self
            .storage
            .get::<ByteBuf>(&self.part_key(part_number))
            .into_send()
            .await
            .map_err(cache_error);
        let Some(bytes) = loaded.inspect_err(|_error| {
            increment(&self.perf.cache_errors, 1);
        })?
        else {
            increment(&self.perf.cache_part_misses, 1);
            return Ok(None);
        };
        let bytes = Bytes::from(bytes.into_vec());
        increment(&self.perf.cache_part_hits, 1);
        increment(&self.perf.cache_loaded_bytes, bytes.len() as u64);
        cached_range(bytes, range_in_part, &self.perf)
    }

    async fn save_head(&self, meta: (&ObjectMeta, &Attributes)) -> Result<()> {
        let result = self
            .storage
            .put(&self.head_key(), LocalCacheHead::from(meta))
            .into_send()
            .await
            .map_err(cache_error);
        if result.is_ok() {
            increment(&self.perf.cache_head_writes, 1);
        } else {
            increment(&self.perf.cache_errors, 1);
        }
        result
    }

    async fn read_head(&self) -> Result<Option<(ObjectMeta, Attributes)>> {
        increment(&self.perf.cache_head_reads, 1);
        let result = self
            .storage
            .get::<LocalCacheHead>(&self.head_key())
            .into_send()
            .await
            .map_err(cache_error);
        match &result {
            Ok(Some(_)) => increment(&self.perf.cache_head_hits, 1),
            Ok(None) => increment(&self.perf.cache_head_misses, 1),
            Err(_) => increment(&self.perf.cache_errors, 1),
        }
        result.map(|head| head.map(|head| (head.meta(), head.attributes())))
    }

    async fn delete(&self) {
        let head = self.read_head().await.ok().flatten();
        self.storage.delete(&self.head_key()).into_send().await.ok();

        if let Some((meta, _)) = head {
            let part_count = meta.size.div_ceil(self.part_size as u64) as usize;
            let keys = (0..part_count)
                .map(|part_number| self.part_key(part_number))
                .collect::<Vec<_>>();
            for chunk in keys.chunks(128) {
                self.storage
                    .delete_multiple(chunk.to_vec())
                    .into_send()
                    .await
                    .ok();
            }
        }
    }
}

fn cached_range(bytes: Bytes, range: Range<usize>, perf: &PerfCounters) -> Result<Option<Bytes>> {
    if bytes.get(range.clone()).is_none() {
        increment(&perf.cache_errors, 1);
        return Err(cache_message("cached part is shorter than requested range"));
    }
    increment(&perf.cache_returned_bytes, range.len() as u64);
    Ok(Some(bytes.slice(range)))
}

fn entry_prefix(location: &Path, part_size: usize) -> String {
    let location = location.as_ref();
    format!("{CACHE_PREFIX}{part_size}:{}:{location}:", location.len())
}

fn cache_error(error: worker::Error) -> Error {
    cache_message(error.to_string())
}

fn cache_message(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_keys_include_part_size_and_unambiguous_path_length() {
        assert_ne!(
            entry_prefix(&Path::from("a:1"), 1024),
            entry_prefix(&Path::from("a"), 1024)
        );
        assert_ne!(
            entry_prefix(&Path::from("a"), 1024),
            entry_prefix(&Path::from("a"), 2048)
        );
    }
}

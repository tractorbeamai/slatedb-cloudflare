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

const STORE: &str = "cloudflare-do-cache";
const CACHE_PREFIX: &str = "slatedb-cache:";

pub struct DoCacheStorage {
    // SlateDB requires Send + Sync; Workers keep bindings on one isolate thread.
    storage: Arc<SendWrapper<Storage>>,
}

impl DoCacheStorage {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Arc::new(SendWrapper::new(storage)),
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
            prefix: entry_prefix(location, part_size),
            part_size,
        })
    }

    async fn start_evictor(&self) {}
}

struct DoCacheEntry {
    storage: Arc<SendWrapper<Storage>>,
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
        self.storage
            .put(
                &self.part_key(part_number),
                serde_bytes::Bytes::new(bytes.as_ref()),
            )
            .into_send()
            .await
            .map_err(cache_error)
    }

    async fn read_part(
        &self,
        part_number: PartID,
        range_in_part: Range<usize>,
    ) -> Result<Option<Bytes>> {
        let Some(bytes) = self
            .storage
            .get::<ByteBuf>(&self.part_key(part_number))
            .into_send()
            .await
            .map_err(cache_error)?
        else {
            return Ok(None);
        };
        let bytes = bytes.into_vec();
        let Some(requested) = bytes.get(range_in_part) else {
            return Err(cache_message("cached part is shorter than requested range"));
        };
        Ok(Some(Bytes::copy_from_slice(requested)))
    }

    async fn save_head(&self, meta: (&ObjectMeta, &Attributes)) -> Result<()> {
        self.storage
            .put(&self.head_key(), LocalCacheHead::from(meta))
            .into_send()
            .await
            .map_err(cache_error)
    }

    async fn read_head(&self) -> Result<Option<(ObjectMeta, Attributes)>> {
        self.storage
            .get::<LocalCacheHead>(&self.head_key())
            .into_send()
            .await
            .map(|head| head.map(|head| (head.meta(), head.attributes())))
            .map_err(cache_error)
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

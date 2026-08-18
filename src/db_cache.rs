use async_trait::async_trait;
use quick_cache::{Weighter, sync::Cache};
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};

const CACHE_BYTES: u64 = 4 * 1024 * 1024;
const ESTIMATED_ENTRIES: usize = 4096;

pub struct QuickDbCache {
    entries: Cache<CachedKey, CachedEntry, EntryWeighter>,
}

impl QuickDbCache {
    pub fn new() -> Self {
        Self {
            entries: Cache::with_weighter(ESTIMATED_ENTRIES, CACHE_BYTES, EntryWeighter),
        }
    }

    fn get(&self, key: &CachedKey) -> Option<CachedEntry> {
        self.entries.get(key)
    }
}

#[derive(Clone)]
struct EntryWeighter;

impl Weighter<CachedKey, CachedEntry> for EntryWeighter {
    fn weight(&self, _key: &CachedKey, value: &CachedEntry) -> u64 {
        value.size() as u64
    }
}

#[async_trait]
impl DbCache for QuickDbCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.entries.insert(key, value);
    }

    async fn remove(&self, key: &CachedKey) {
        self.entries.remove(key);
    }

    fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }
}

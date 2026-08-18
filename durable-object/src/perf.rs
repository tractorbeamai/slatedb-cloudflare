use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Debug, Default)]
pub struct PerfCounters {
    pub cache_part_reads: AtomicU64,
    pub cache_part_hits: AtomicU64,
    pub cache_part_misses: AtomicU64,
    pub cache_requested_bytes: AtomicU64,
    pub cache_loaded_bytes: AtomicU64,
    pub cache_returned_bytes: AtomicU64,
    pub cache_part_writes: AtomicU64,
    pub cache_written_bytes: AtomicU64,
    pub cache_head_reads: AtomicU64,
    pub cache_head_hits: AtomicU64,
    pub cache_head_misses: AtomicU64,
    pub cache_head_writes: AtomicU64,
    pub cache_errors: AtomicU64,
    pub do_kv_gets: AtomicU64,
    pub do_kv_puts: AtomicU64,
    pub do_kv_deletes: AtomicU64,
    pub do_kv_delete_alls: AtomicU64,
    pub do_kv_lists: AtomicU64,
    pub do_kv_rows_read: AtomicU64,
    pub do_kv_rows_written: AtomicU64,
    pub r2_gets: AtomicU64,
    pub r2_heads: AtomicU64,
    pub r2_puts: AtomicU64,
    pub r2_lists: AtomicU64,
    pub r2_deletes: AtomicU64,
    pub r2_read_bytes: AtomicU64,
    pub r2_written_bytes: AtomicU64,
    pub r2_multipart_uploads: AtomicU64,
    pub r2_multipart_parts: AtomicU64,
    pub r2_multipart_completes: AtomicU64,
    pub r2_errors: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PerfSnapshot {
    pub cache_part_reads: u64,
    pub cache_part_hits: u64,
    pub cache_part_misses: u64,
    pub cache_requested_bytes: u64,
    pub cache_loaded_bytes: u64,
    pub cache_returned_bytes: u64,
    pub cache_part_writes: u64,
    pub cache_written_bytes: u64,
    pub cache_head_reads: u64,
    pub cache_head_hits: u64,
    pub cache_head_misses: u64,
    pub cache_head_writes: u64,
    pub cache_errors: u64,
    pub do_kv_gets: u64,
    pub do_kv_puts: u64,
    pub do_kv_deletes: u64,
    pub do_kv_delete_alls: u64,
    pub do_kv_lists: u64,
    pub do_kv_rows_read: u64,
    pub do_kv_rows_written: u64,
    pub r2_gets: u64,
    pub r2_heads: u64,
    pub r2_puts: u64,
    pub r2_lists: u64,
    pub r2_deletes: u64,
    pub r2_read_bytes: u64,
    pub r2_written_bytes: u64,
    pub r2_multipart_uploads: u64,
    pub r2_multipart_parts: u64,
    pub r2_multipart_completes: u64,
    pub r2_errors: u64,
}

impl PerfCounters {
    pub fn snapshot(&self) -> PerfSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        PerfSnapshot {
            cache_part_reads: load(&self.cache_part_reads),
            cache_part_hits: load(&self.cache_part_hits),
            cache_part_misses: load(&self.cache_part_misses),
            cache_requested_bytes: load(&self.cache_requested_bytes),
            cache_loaded_bytes: load(&self.cache_loaded_bytes),
            cache_returned_bytes: load(&self.cache_returned_bytes),
            cache_part_writes: load(&self.cache_part_writes),
            cache_written_bytes: load(&self.cache_written_bytes),
            cache_head_reads: load(&self.cache_head_reads),
            cache_head_hits: load(&self.cache_head_hits),
            cache_head_misses: load(&self.cache_head_misses),
            cache_head_writes: load(&self.cache_head_writes),
            cache_errors: load(&self.cache_errors),
            do_kv_gets: load(&self.do_kv_gets),
            do_kv_puts: load(&self.do_kv_puts),
            do_kv_deletes: load(&self.do_kv_deletes),
            do_kv_delete_alls: load(&self.do_kv_delete_alls),
            do_kv_lists: load(&self.do_kv_lists),
            do_kv_rows_read: load(&self.do_kv_rows_read),
            do_kv_rows_written: load(&self.do_kv_rows_written),
            r2_gets: load(&self.r2_gets),
            r2_heads: load(&self.r2_heads),
            r2_puts: load(&self.r2_puts),
            r2_lists: load(&self.r2_lists),
            r2_deletes: load(&self.r2_deletes),
            r2_read_bytes: load(&self.r2_read_bytes),
            r2_written_bytes: load(&self.r2_written_bytes),
            r2_multipart_uploads: load(&self.r2_multipart_uploads),
            r2_multipart_parts: load(&self.r2_multipart_parts),
            r2_multipart_completes: load(&self.r2_multipart_completes),
            r2_errors: load(&self.r2_errors),
        }
    }
}

pub fn increment(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reads_accumulated_counters() {
        let counters = PerfCounters::default();
        increment(&counters.cache_part_hits, 2);
        increment(&counters.cache_loaded_bytes, 1_048_576);
        increment(&counters.cache_loaded_bytes, 1_048_576);
        increment(&counters.do_kv_rows_written, 3);
        increment(&counters.r2_multipart_completes, 1);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.cache_part_hits, 2);
        assert_eq!(snapshot.cache_loaded_bytes, 2_097_152);
        assert_eq!(snapshot.do_kv_rows_written, 3);
        assert_eq!(snapshot.r2_multipart_completes, 1);
        assert_eq!(snapshot.r2_gets, 0);
    }
}

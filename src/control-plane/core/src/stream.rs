//! The stream-offset concern: a gapless, per-`(table, bucket)` monotonic offset
//! allocator. Offsets are what make the inline tier an ordered log; a later
//! slice stamps appended rows with `(bucket, offset)`.

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait BucketOffsets {
    /// Allocate a contiguous run of `count` offsets for `(table_id, bucket)` and
    /// return the FIRST offset in the run. Offsets start at 0; allocations are
    /// gapless and monotonic per bucket. When issued inside a transaction the
    /// offsets are assigned iff that transaction commits.
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64>;
    /// The high-water offset for `(table_id, bucket)` — the next offset that
    /// would be handed out (0 if none has been allocated yet).
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64>;
}

#[async_trait]
pub trait StreamTables {
    /// Declare table_id as a log table with bucket_count buckets. Idempotent: a
    /// redeclare is a no-op and the first declaration's bucket_count stands.
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()>;
    /// The bucket count if table_id is a declared log table, else None.
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>>;
}

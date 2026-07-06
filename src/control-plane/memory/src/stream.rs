use async_trait::async_trait;
use control_plane_core::{BucketOffsets, Result};

use crate::MemoryControlPlane;

#[async_trait]
impl BucketOffsets for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64> {
        let mut map = self.offsets.lock();
        let next = map.entry((table_id, bucket)).or_insert(0);
        let first = *next;
        *next += count;
        Ok(first)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64> {
        Ok(self
            .offsets
            .lock()
            .get(&(table_id, bucket))
            .copied()
            .unwrap_or(0))
    }
}

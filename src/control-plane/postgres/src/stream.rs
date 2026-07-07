use async_trait::async_trait;
use control_plane_core::{BucketOffsets, ControlPlaneError, Result, StreamTables};

use crate::{PgControlPlane, backend};

/// Allocate a contiguous run of `count` offsets for `(table_id, bucket)` on the
/// given executor, returning the first offset. Usable inside a transaction (pass
/// `&mut *tx`) so the allocation commits with the caller's write.
pub(crate) async fn pg_allocate_offset<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket: i32,
    count: i64,
) -> Result<i64> {
    if count <= 0 {
        return Err(ControlPlaneError::Validation(format!(
            "allocate_offset requires count > 0, got {count}"
        )));
    }
    let first = sqlx::query_scalar!(
        "insert into stream.bucket_offset (table_id, bucket, next) values ($1, $2, $3) \
         on conflict (table_id, bucket) do update set next = stream.bucket_offset.next + $3 \
         returning next - $3 as \"first!\"",
        table_id,
        bucket,
        count,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?;
    Ok(first)
}

/// The high-water offset for `(table_id, bucket)` — 0 if none allocated yet.
pub(crate) async fn pg_peek_offset<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket: i32,
) -> Result<i64> {
    let next = sqlx::query_scalar!(
        "select coalesce( \
             (select next from stream.bucket_offset where table_id = $1 and bucket = $2), \
             0) as \"next!\"",
        table_id,
        bucket,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?;
    Ok(next)
}

#[async_trait]
impl BucketOffsets for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64> {
        pg_allocate_offset(self.pool(), table_id, bucket, count).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64> {
        pg_peek_offset(self.pool(), table_id, bucket).await
    }
}

/// Declare a log table (idempotent, first-wins on bucket_count).
pub(crate) async fn pg_declare_stream<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count) values ($1, $2) \
         on conflict (table_id) do nothing",
        table_id,
        bucket_count,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The bucket count if table_id is a declared log table, else None.
pub(crate) async fn pg_stream_bucket_count<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<Option<i32>> {
    let n = sqlx::query_scalar!(
        "select bucket_count from stream.stream_table where table_id = $1",
        table_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(n)
}

#[async_trait]
impl StreamTables for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()> {
        pg_declare_stream(self.pool(), table_id, bucket_count).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        pg_stream_bucket_count(self.pool(), table_id).await
    }
}

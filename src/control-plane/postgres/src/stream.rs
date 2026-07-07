use async_trait::async_trait;
use control_plane_core::{BucketOffsets, ControlPlaneError, Result, StreamTables, TableRef};

use crate::{PgControlPlane, backend};

/// Reconcile a write's REQUESTED stream mode (`stream_buckets`) against the mode
/// the mirror table `tid` already records, mirroring `inline_append`'s arms exactly
/// so the inline and direct-write Parquet paths cannot diverge. Returns
/// `Some(bucket_count)` iff this table is a (now-)declared log table (offset
/// stamping applies); `None` for a batch table (no stamping).
///
/// Rejections (all raised BEFORE any `pg_declare_stream`, per the Plan 1a fix):
/// a `< 1` requested count → `Validation`; a batch→stream conversion of a
/// PRE-EXISTING table → `Validation`; a bucket-count mismatch against an existing
/// stream table → `Conflict`. For a fresh `(Some(n), None)` request on a
/// brand-new table (`!pre_existing`) it declares the stream and honours the
/// recorded count (a concurrent first-writer may have won the declare with a
/// different count — re-read and reject on mismatch). Runs entirely on the
/// caller's transaction so the declare commits iff the write does.
///
/// `pre_existing`: whether the table's mirror row existed BEFORE this write began
/// (the batch→stream conversion guard). `tid`: the mirror table id (already
/// ensured by the caller).
pub(crate) async fn reconcile_stream_mode(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    stream_buckets: Option<i32>,
    pre_existing: bool,
    table: &TableRef,
) -> Result<Option<i32>> {
    // Validate the REQUESTED bucket count BEFORE any declare: a `< 1` request must
    // surface as `Validation`, not as the raw `bucket_count_positive` CHECK violation
    // that `pg_declare_stream` below would otherwise raise (an opaque `Backend` error).
    if let Some(n) = stream_buckets
        && n < 1
    {
        return Err(ControlPlaneError::Validation(format!(
            "stream bucket_count must be >= 1, got {n}"
        )));
    }

    let existing = pg_stream_bucket_count(&mut *conn, tid).await?;

    // `effective` = Some(bucket_count) iff this table is a (now-)declared log table.
    let effective: Option<i32> = match (stream_buckets, existing) {
        (Some(n), Some(m)) if n != m => {
            return Err(ControlPlaneError::Conflict(format!(
                "stream bucket count mismatch for {}.{}: requested {n}, table has {m}",
                table.schema, table.name
            )));
        }
        (Some(_), Some(m)) => Some(m),
        (Some(n), None) => {
            if pre_existing {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot convert existing batch table {}.{} to a stream table",
                    table.schema, table.name
                )));
            }
            pg_declare_stream(&mut *conn, tid, n).await?;
            // A concurrent first-writer may have won the declare with a different
            // count (our ON CONFLICT DO NOTHING then no-ops). Re-read the recorded
            // count and honour it, so the rows we stamp always agree with
            // stream_table.bucket_count.
            let stored = pg_stream_bucket_count(&mut *conn, tid)
                .await?
                .ok_or_else(|| {
                    ControlPlaneError::Backend(
                        "stream_table row missing immediately after declare".into(),
                    )
                })?;
            if stored != n {
                return Err(ControlPlaneError::Conflict(format!(
                    "stream bucket count mismatch for {}.{}: requested {n}, table has {stored}",
                    table.schema, table.name
                )));
            }
            Some(stored)
        }
        (None, existing) => existing,
    };

    // Defense in depth: a non-positive effective bucket count would otherwise
    // reach the `row % bc` arithmetic in the callers and panic (division/remainder
    // by zero, or a meaningless negative modulus). Reject it cleanly here — this is
    // currently unreachable (callers only ever pass positive counts, and the DB
    // CHECK backstops it), but a future caller threading an external `?buckets=N`
    // value through must fail with a `Validation` error, not a panic.
    if let Some(bc) = effective
        && bc < 1
    {
        return Err(ControlPlaneError::Validation(format!(
            "stream bucket_count must be >= 1, got {bc}"
        )));
    }

    Ok(effective)
}

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

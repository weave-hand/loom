use async_trait::async_trait;
use control_plane_core::{
    BucketOffsets, ControlPlaneError, Result, SnapshotId, StreamKind, StreamMeta, StreamTables,
    TableRef,
};

use crate::{PgControlPlane, backend};

/// A write's REQUESTED stream-mode declaration, threaded from `LandRequest` down
/// to [`reconcile_stream_mode`]. `Log`/`Cdc` carry the requested bucket count;
/// `Cdc` additionally carries the identity column to bucket on (the "bucket
/// key"). Confined to this crate — the ingest-facing boundary (`land`'s public
/// signature / `LandRequest`) instead carries the plain `Option<i32>`/
/// `Option<CdcDecl>` pair the brief specifies, combined into this enum once
/// inside `land`.
#[derive(Clone, Debug)]
pub(crate) enum StreamDecl {
    /// No stream intent requested this write (the common batch-table case).
    None,
    /// Declare (or confirm) a log table with this many buckets.
    Log(i32),
    /// Declare (or confirm) a PK/CDC table with this many buckets, keyed on
    /// `bucket_key` (an identity column name).
    Cdc { buckets: i32, bucket_key: String },
}

/// Reconcile a write's REQUESTED stream mode (`decl`) against the mode the
/// mirror table `tid` already records, mirroring `inline_append`'s arms exactly
/// so the inline and direct-write Parquet paths cannot diverge. Returns
/// `Some(bucket_count)` iff this table is a (now-)declared stream table (log or
/// cdc; offset stamping applies); `None` for a batch table (no stamping).
///
/// Rejections (all raised BEFORE any `pg_declare_stream`/`pg_declare_cdc`, per
/// the Plan 1a fix): a `< 1` requested count → `Validation`; a batch→stream
/// conversion of a PRE-EXISTING table → `Validation`; a bucket-count mismatch
/// against an existing stream table → `Conflict`; a `Cdc` request against an
/// already-declared table of a DIFFERENT kind (e.g. a log table) → `Validation`.
/// For a fresh `(Some(n), None)` request on a brand-new table (`!pre_existing`)
/// it declares the stream (as a log or cdc table, per `decl`) and honours the
/// recorded count (a concurrent first-writer may have won the declare with a
/// different count — re-read and reject on mismatch). Runs entirely on the
/// caller's transaction so the declare commits iff the write does.
///
/// `pre_existing`: whether the table's mirror row existed BEFORE this write began
/// (the batch→stream conversion guard). `tid`: the mirror table id (already
/// ensured by the caller). `at`: this write's already-allocated mirror snapshot
/// (the same value the caller passed to its own `ensure_table`) — reused, on a
/// CDC first-declare, as the changelog table's `iceberg_mirror.table` genesis
/// snapshot, exactly as `write_steps` shares one snapshot across every table it
/// touches in a transaction.
pub(crate) async fn reconcile_stream_mode(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    decl: &StreamDecl,
    pre_existing: bool,
    table: &TableRef,
    at: SnapshotId,
) -> Result<Option<i32>> {
    let requested: Option<i32> = match decl {
        StreamDecl::None => None,
        StreamDecl::Log(n) | StreamDecl::Cdc { buckets: n, .. } => Some(*n),
    };

    // Validate the REQUESTED bucket count BEFORE any declare: a `< 1` request must
    // surface as `Validation`, not as the raw `bucket_count_positive` CHECK violation
    // that `pg_declare_stream`/`pg_declare_cdc` below would otherwise raise (an
    // opaque `Backend` error).
    if let Some(n) = requested
        && n < 1
    {
        return Err(ControlPlaneError::Validation(format!(
            "stream bucket_count must be >= 1, got {n}"
        )));
    }

    let existing_meta = pg_stream_meta(&mut *conn, tid).await?;
    let existing = existing_meta.as_ref().map(|m| m.bucket_count);

    // `effective` = Some(bucket_count) iff this table is a (now-)declared stream table.
    let effective: Option<i32> = match (requested, existing) {
        (Some(n), Some(m)) if n != m => {
            return Err(ControlPlaneError::Conflict(format!(
                "stream bucket count mismatch for {}.{}: requested {n}, table has {m}",
                table.schema, table.name
            )));
        }
        (Some(_), Some(m)) => {
            // A `Cdc` request against an already-declared table must also match its
            // KIND, not just its bucket count — a log table with the same bucket
            // count is not a valid cdc target. (A `Log` request is unaffected: it
            // keeps its original count-only comparison, so log/batch behavior is
            // unchanged.)
            if matches!(decl, StreamDecl::Cdc { .. })
                && existing_meta
                    .as_ref()
                    .is_some_and(|meta| meta.kind != StreamKind::Cdc)
            {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot declare {}.{} as a cdc table: already declared with a \
                     different stream kind",
                    table.schema, table.name
                )));
            }
            Some(m)
        }
        (Some(n), None) => {
            if pre_existing {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot convert existing batch table {}.{} to a stream table",
                    table.schema, table.name
                )));
            }
            match decl {
                StreamDecl::Cdc { bucket_key, .. } => {
                    pg_declare_cdc(&mut *conn, tid, n, bucket_key).await?;
                    // Register the changelog table's mirror row (its Iceberg metadata
                    // was created by `land_cdc` before this tx, outside any commit) and
                    // point the registry at it, reusing this write's `at` snapshot —
                    // the changelog table's genesis shares the same snapshot as the
                    // declaring write. Its columns are projected on first flush
                    // append; an empty mirror row is a valid never-written table (no
                    // user-facing changelog read in this slice).
                    let clog = crate::iceberg_landing::changelog_table_ref(table);
                    let clog_tid = crate::iceberg_mirror::ensure_table(
                        &mut *conn,
                        &clog.schema,
                        &clog.name,
                        at,
                    )
                    .await?;
                    pg_set_changelog_table_id(&mut *conn, tid, clog_tid).await?;
                }
                _ => {
                    pg_declare_stream(&mut *conn, tid, n).await?;
                }
            }
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

/// Declare a PK/CDC table (idempotent, first-wins on all fields).
pub(crate) async fn pg_declare_cdc<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
    bucket_key: &str,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key) \
         values ($1, $2, 'cdc', $3) on conflict (table_id) do nothing",
        table_id,
        bucket_count,
        bucket_key,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Full stream metadata for table_id if declared, else None.
pub(crate) async fn pg_stream_meta<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<Option<StreamMeta>> {
    let row = sqlx::query!(
        "select bucket_count, kind, bucket_key, changelog_table_id from stream.stream_table \
         where table_id = $1",
        table_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| {
        let kind = if r.kind == "cdc" {
            StreamKind::Cdc
        } else {
            StreamKind::Log
        };
        StreamMeta {
            bucket_count: r.bucket_count,
            kind,
            bucket_key: r.bucket_key,
            changelog_table_id: r.changelog_table_id,
        }
    }))
}

/// Point a CDC table's registry row at its durable changelog table's mirror
/// `table_id`. Idempotent overwrite; only meaningful for a `kind='cdc'` row.
pub(crate) async fn pg_set_changelog_table_id<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    changelog_table_id: i64,
) -> Result<()> {
    sqlx::query!(
        "update stream.stream_table set changelog_table_id = $2 where table_id = $1",
        table_id,
        changelog_table_id,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
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

    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_cdc(&self, table_id: i64, bucket_count: i32, bucket_key: &str) -> Result<()> {
        pg_declare_cdc(self.pool(), table_id, bucket_count, bucket_key).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>> {
        pg_stream_meta(self.pool(), table_id).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()> {
        pg_set_changelog_table_id(self.pool(), table_id, changelog_table_id).await
    }
}

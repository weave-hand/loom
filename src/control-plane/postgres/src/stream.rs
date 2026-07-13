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
    /// `bucket_key` (an identity column name), folded by `merge_engine`.
    Cdc {
        buckets: i32,
        bucket_key: String,
        merge_engine: control_plane_core::MergeEngine,
    },
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

    // merge_engine=versioned requires the bound type to declare a version
    // property of an orderable type (integer/long/timestamp). Validated HERE —
    // on every declarer's path (HTTP `/models/{type}?mode=cdc` and direct
    // land_cdc), and reachable for Versioned (which is only ever created via a
    // control-plane-defined versioned type + land_cdc, since the HTTP path
    // infers types with version: None). Runs before existing/new branching so it
    // gates both first-declare and redeclare.
    if let StreamDecl::Cdc {
        merge_engine: control_plane_core::MergeEngine::Versioned,
        ..
    } = decl
    {
        let version_col = crate::ontology::version_for_table(&mut *conn, table).await?;
        let Some(vcol) = version_col else {
            return Err(ControlPlaneError::Validation(format!(
                "merge_engine=versioned requires {}.{} to declare a version property",
                table.schema, table.name
            )));
        };
        // The version property's logical type (join object_type -> property).
        let ty: Option<String> = sqlx::query_scalar!(
            "select p.ty from ontology.property p \
             join ontology.object_type o on o.name = p.type_name \
             where o.table_schema = $1 and o.table_name = $2 and p.name = $3",
            table.schema,
            table.name,
            vcol,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
        let orderable = ty
            .as_deref()
            .and_then(control_plane_core::resolve_logical)
            .is_some_and(control_plane_core::BaseType::is_version_orderable);
        if !orderable {
            return Err(ControlPlaneError::Validation(format!(
                "merge_engine=versioned requires an integer/long/timestamp version column; \
                 {}.{} version column '{vcol}' is {:?}",
                table.schema, table.name, ty
            )));
        }
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
            // Engine immutability: a `Cdc` redeclare where the existing engine
            // differs from the requested ⇒ `Conflict` (mirrors the bucket-count
            // mismatch shape). A `Log` redeclare is unaffected (engine is unused
            // for log tables).
            if matches!(
                decl,
                StreamDecl::Cdc { merge_engine, .. }
                    if existing_meta.as_ref().map(|m| m.merge_engine) != Some(*merge_engine)
            ) {
                let existing_engine = existing_meta
                    .as_ref()
                    .map(|m| m.merge_engine.as_str())
                    .unwrap_or("?");
                let requested_engine = match decl {
                    StreamDecl::Cdc { merge_engine, .. } => merge_engine.as_str(),
                    _ => existing_engine,
                };
                return Err(ControlPlaneError::Conflict(format!(
                    "stream merge_engine mismatch for {}.{}: requested {requested_engine}, \
                     table has {existing_engine}",
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
                StreamDecl::Cdc {
                    bucket_key,
                    merge_engine,
                    ..
                } => {
                    pg_declare_cdc(&mut *conn, tid, n, bucket_key, *merge_engine).await?;
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

/// Refuse `table` if it is a declared stream/CDC target that the caller's write
/// path cannot frame. Source of truth is the `stream.stream_table` registry: the
/// resolved live `table_id` is refused when it appears as EITHER `table_id` (a
/// declared log/CDC base) OR `changelog_table_id` (a CDC table's durable changelog,
/// which has no `stream_table` row of its own). A table with no live mirror row
/// passes — a brand-new output cannot be stream-declared. Returns
/// `ControlPlaneError::Validation` with the stable prefix `stream-table target
/// refused:`. `AssertSqlSafe`: static query, sqlx regen unavailable in-env (initdb
/// as root); convert to `query!` when regenerating locally.
pub async fn pg_refuse_stream_target(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
) -> Result<()> {
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut *conn, &table.schema, &table.name).await?
    else {
        return Ok(());
    };
    let hit: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from stream.stream_table \
         where table_id = $1 or changelog_table_id = $1 limit 1",
    ))
    .bind(tid)
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?;
    if hit.is_some() {
        return Err(ControlPlaneError::Validation(format!(
            "stream-table target refused: {}.{} is a declared stream/CDC table; \
             transform and multi-target write paths cannot stamp stream framing",
            table.schema, table.name
        )));
    }
    Ok(())
}

/// Declare a PK/CDC table (idempotent, first-wins on all fields).
pub(crate) async fn pg_declare_cdc<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
    bucket_key: &str,
    merge_engine: control_plane_core::MergeEngine,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key, merge_engine) \
         values ($1, $2, 'cdc', $3, $4) on conflict (table_id) do nothing",
        table_id,
        bucket_count,
        bucket_key,
        merge_engine.as_str(),
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
        "select bucket_count, kind, bucket_key, changelog_table_id, merge_engine \
         from stream.stream_table where table_id = $1",
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
        // The CHECK constraint on `merge_engine` guarantees a valid token; this
        // `.unwrap_or` is a corrupt-row backstop only (never reached in
        // practice — a bad token would have rejected the insert).
        let merge_engine = r
            .merge_engine
            .parse()
            .unwrap_or(control_plane_core::MergeEngine::LastRow);
        StreamMeta {
            bucket_count: r.bucket_count,
            kind,
            bucket_key: r.bucket_key,
            changelog_table_id: r.changelog_table_id,
            merge_engine,
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
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
        merge_engine: control_plane_core::MergeEngine,
    ) -> Result<()> {
        pg_declare_cdc(
            self.pool(),
            table_id,
            bucket_count,
            bucket_key,
            merge_engine,
        )
        .await
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

/// Fire the changelog wakeup for a CDC base table's inline write. MUST be called
/// INSIDE the write's transaction: `pg_notify` in a tx is buffered until commit,
/// so a rolled-back write is silent — the same fire-and-forget-in-commit shape as
/// the queue's enqueue (`queue::pg_insert`). Channel: `loom_changelog:{table_id}`.
pub(crate) async fn pg_notify_changelog<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(
        "select pg_notify('loom_changelog:' || $1::text, '')",
    ))
    .bind(table_id)
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Block until a CDC inline write commits against `table`, or `timeout` elapses —
/// whichever first (the poll-fallback bound for a missed notify). Never errors on
/// timeout. Mirrors the queue's `await_jobs` waiter (`queue.rs:152`). Note the
/// listen-after-scan race: an event committed between the caller's empty scan and
/// this LISTEN is missed and picked up on the next poll — bounded by `timeout`.
pub async fn await_changelog(
    pool: &sqlx::PgPool,
    table: &TableRef,
    timeout: std::time::Duration,
) -> Result<()> {
    let tid = {
        let mut conn = pool.acquire().await.map_err(backend)?;
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name)
            .await?
            .ok_or_else(|| {
                ControlPlaneError::NotFound(format!(
                    "no mirror table for {}.{}",
                    table.schema, table.name
                ))
            })?
    };
    let mut listener = sqlx::postgres::PgListener::connect_with(pool)
        .await
        .map_err(backend)?;
    listener
        .listen(&format!("loom_changelog:{tid}"))
        .await
        .map_err(backend)?;
    // A notification, or the polling-fallback timeout — whichever first.
    drop(tokio::time::timeout(timeout, listener.recv()).await);
    Ok(())
}

/// The recorded watermarks for `(mv, source_table_id)`. Executor-generic so the
/// delta scan (pool) and any tx caller share it. Buckets with no row are absent.
/// `AssertSqlSafe`: static query, sqlx regen unavailable in-env (initdb as
/// root); convert to `query_as!` when regenerating locally.
pub async fn pg_mv_watermarks<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    mv: &str,
    source_table_id: i64,
) -> Result<std::collections::BTreeMap<i32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(
        "select bucket, next_offset from stream.mv_watermark \
         where mv = $1 and source_table_id = $2",
    ))
    .bind(mv)
    .bind(source_table_id)
    .fetch_all(ex)
    .await
    .map_err(backend)?;
    Ok(rows.into_iter().collect())
}

/// CAS-advance one bucket's watermark. `from == 0` may insert (bootstrap);
/// `from > 0` only updates an existing row at exactly `from`. Zero rows
/// affected => `Conflict` — inside a transaction the caller's rollback then
/// discards the whole output commit (the exactly-once mechanism). `AssertSqlSafe`:
/// static queries (branched on `adv.from == 0`), sqlx regen unavailable in-env;
/// convert to `query!` when regenerating locally.
pub async fn pg_advance_mv_watermark<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    mv: &str,
    source_table_id: i64,
    adv: &control_plane_core::WatermarkAdvance,
) -> Result<()> {
    let sql = if adv.from == 0 {
        "insert into stream.mv_watermark (mv, source_table_id, bucket, next_offset) \
         values ($1, $2, $3, $4) \
         on conflict (mv, source_table_id, bucket) do update set next_offset = $4 \
         where stream.mv_watermark.next_offset = 0"
    } else {
        "update stream.mv_watermark set next_offset = $4 \
         where mv = $1 and source_table_id = $2 and bucket = $3 and next_offset = $5"
    };
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(mv)
        .bind(source_table_id)
        .bind(adv.bucket)
        .bind(adv.to);
    if adv.from != 0 {
        q = q.bind(adv.from);
    }
    let done = q.execute(ex).await.map_err(backend)?;
    if done.rows_affected() == 0 {
        return Err(ControlPlaneError::Conflict(format!(
            "mv watermark advanced concurrently: {mv} source {source_table_id} \
             bucket {} expected {}",
            adv.bucket, adv.from
        )));
    }
    Ok(())
}

#[async_trait]
impl control_plane_core::MvWatermarks for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn mv_watermarks(
        &self,
        mv: &str,
        source_table_id: i64,
    ) -> Result<std::collections::BTreeMap<i32, i64>> {
        pg_mv_watermarks(self.pool(), mv, source_table_id).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn advance_mv_watermark(
        &self,
        mv: &str,
        source_table_id: i64,
        advances: &[control_plane_core::WatermarkAdvance],
    ) -> Result<()> {
        for adv in advances {
            pg_advance_mv_watermark(self.pool(), mv, source_table_id, adv).await?;
        }
        Ok(())
    }
}

/// The stream declaration (kind + bucket count) for a table addressed by
/// `TableRef` — the `TableRef`-keyed sibling of the `table_id`-keyed
/// [`pg_stream_meta`]. `None` when the table has no live mirror row or no
/// stream declaration. Used by the engine-serving feed dispatch (a different
/// crate) to key the CDC-vs-log feed arm off `StreamMeta.kind`.
pub async fn stream_meta_for(
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Result<Option<control_plane_core::StreamMeta>> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    pg_stream_meta(&mut *conn, tid).await
}

/// The per-bucket high-water offsets (`BucketOffsets::peek_offset` per bucket)
/// for a declared **stream** table (CDC or log) — the `?cursor=latest`
/// join-the-tail positions, and the feed handler's "is this subscribable + how
/// many buckets" probe. `None` when `table` has no live mirror row or no
/// stream declaration.
pub async fn changelog_positions_latest(
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Result<Option<std::collections::BTreeMap<i32, i64>>> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    let Some(meta) = pg_stream_meta(&mut *conn, tid).await? else {
        return Ok(None);
    };
    if !matches!(
        meta.kind,
        control_plane_core::StreamKind::Cdc | control_plane_core::StreamKind::Log
    ) {
        return Ok(None);
    }
    let mut out = std::collections::BTreeMap::new();
    for bucket in 0..meta.bucket_count {
        out.insert(bucket, pg_peek_offset(&mut *conn, tid, bucket).await?);
    }
    Ok(Some(out))
}

//! loom-native inline writes for Iceberg tables. Small writes land as typed rows in
//! a per-table `iceberg_mirror.inline_<table_id>` table (created on the fly), NOT a
//! Parquet object — a mirror-only commit (snapshot + rows + lineage in one tx). The
//! serving engine unions them with the table's Parquet files at read time via
//! `IcebergCatalog::inline_live_batch`. External Iceberg clients don't see inline rows
//! until a future flush. See the slice-A design doc.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{Field, Schema};
use control_plane_core::{
    BaseType, ColumnSpec, ControlPlaneError, LineageEvent, NewJob, Result, SnapshotId, TableRef,
    resolve_logical,
};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres, Row};

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_mirror::{
    ProjectedColumn, arm_consolidate_trigger, arm_inline_trigger, bump_consolidate_trigger,
    bump_inline_trigger, ensure_table_witnessed, live_columns, live_table_id, next_snapshot,
    project_columns,
};
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};
use crate::iceberg_type::{
    iceberg_physical_type, logical_from_iceberg, mirror_column_type, pg_type_for,
};
use crate::lineage::pg_emit;

/// The Postgres name of a table's inline storage. `table_id` is an internal i64.
pub fn inline_table_name(table_id: i64) -> String {
    format!("iceberg_mirror.inline_{table_id}")
}

/// Quote `name` as a PG identifier: wrap in double quotes, escaping embedded
/// quotes. THE identifier-splice guard for the runtime inline-table SQL in
/// this module — quoting makes the spliced name inert, which is the safety
/// argument every `AssertSqlSafe` here leans on.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The MVCC "live at snapshot `at`" predicate over an inline table's
/// `begin_snapshot`/`end_snapshot` columns. `at` is a trusted i64 snapshot id
/// (never caller text), so splicing it is safe.
pub(crate) fn mvcc_live_pred(at: i64) -> String {
    format!("begin_snapshot <= {at} and (end_snapshot is null or end_snapshot > {at})")
}

/// True if the physical `inline_<table_id>` relation exists (`to_regclass`
/// returns NULL for a missing relation). The one inline-existence preamble —
/// parameterized, so no splice at all.
pub(crate) async fn inline_table_exists(conn: &mut PgConnection, table_id: i64) -> Result<bool> {
    let exists: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(inline_table_name(table_id))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(exists.is_some())
}

/// End-cap EVERY live inline row of `table_id` at snapshot `at` (`end_snapshot = at`
/// where `end_snapshot is null`). Used by the overwrite/replace commit so a replace
/// supersedes the inline tier as well as the file tier. No-op if the inline table was
/// never created. Runs in the caller's transaction.
///
/// `intent` declares WHY the caller is end-capping, and is checked against the MV
/// read-position floor ([`crate::mv_floor::guard_end_cap`]) before anything is written:
/// a `Removing` end-cap of offsets a micro-batch MV has not read is REFUSED. The CDC
/// flush passes `Reframing` — see `iceberg_flush::flush_locked_cdc` for why the `-U`
/// rows it drops from the BASE cannot cost an MV a delta row.
///
/// `pub` (not `pub(crate)`) so the fixture tests can drive the guarded primitive itself.
pub async fn end_cap_live_inline_rows(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    at: control_plane_core::SnapshotId,
    intent: &crate::mv_floor::EndCapIntent<'_>,
) -> control_plane_core::Result<()> {
    // to_regclass returns NULL for a non-existent relation -> skip.
    if !inline_table_exists(conn, table_id).await? {
        return Ok(());
    }
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    let sql = format!(
        "update {} set end_snapshot = {} where end_snapshot is null",
        inline_table_name(table_id),
        at.0
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}

/// End-cap the SPECIFIC inline rows `row_ids` of `table_id` at snapshot `at`
/// (`end_snapshot = at` where `loom_row_id = any($1) and end_snapshot is null`).
/// Used by commits that retire just-flushed rows at the same snapshot the new
/// Parquet becomes live, so reads never double-serve or drop them. Runs in the
/// caller's transaction.
///
/// Runtime sqlx (not a compile-time `query!`): the `inline_<table_id>` table
/// name is a dynamic identifier and `any($1)` binds a row-id array — neither is
/// expressible in a literal, schema-checked macro. Spliced via `AssertSqlSafe`.
///
/// `intent` declares WHY the caller is end-capping, and is checked against the MV
/// read-position floor ([`crate::mv_floor::guard_end_cap`]) before anything is written.
/// The flush (both branches) passes `Reframing`: the rows it retires here are exactly the
/// rows it re-projects into the new Parquet at the SAME `(loom_bucket, loom_offset)`.
///
/// `pub` (not `pub(crate)`) so the fixture tests can drive the guarded primitive itself.
pub async fn end_cap_inline_rows_by_id(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    row_ids: &[i64],
    at: SnapshotId,
    intent: &crate::mv_floor::EndCapIntent<'_>,
) -> Result<()> {
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    let sql = format!(
        "update {} set end_snapshot = {} \
         where loom_row_id = any($1) and end_snapshot is null",
        inline_table_name(table_id),
        at.0,
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(row_ids.to_vec())
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}

/// True if `table` has any live inline row at `at`. Used to refuse an additive
/// Parquet land while un-flushed inline rows exist: such a land would project a
/// new column into the mirror that the physical `inline_<tid>` table lacks, so
/// inline reconstruction (read AND flush) would fail. The caller must flush first.
pub async fn has_live_inline_rows(
    conn: &mut PgConnection,
    table: &TableRef,
    at: SnapshotId,
) -> Result<bool> {
    let Some(tid) = live_table_id(conn, &table.schema, &table.name).await? else {
        return Ok(false);
    };
    if !inline_table_exists(conn, tid).await? {
        return Ok(false);
    }
    let any: bool = sqlx::query_scalar(AssertSqlSafe(format!(
        "select exists(select 1 from {} where {})",
        inline_table_name(tid),
        mvcc_live_pred(at.0),
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(any)
}

/// One typed inline cell — the bridge between an arrow array and a Postgres bind.
#[derive(Clone, Debug)]
pub(crate) enum Cell {
    I32(Option<i32>),
    I64(Option<i64>),
    F64(Option<f64>),
    Bool(Option<bool>),
    Str(Option<String>),
    Date(Option<time::Date>),
    Ts(Option<time::PrimitiveDateTime>),
    /// A dense f32 vector cell, bound as Postgres `real[]` / decoded from it.
    /// `None` is SQL NULL.
    Vec(Option<Vec<f32>>),
}

impl Cell {
    /// True if this cell is SQL NULL (its wrapped `Option` is `None`).
    fn is_null(&self) -> bool {
        match self {
            Cell::I32(v) => v.is_none(),
            Cell::I64(v) => v.is_none(),
            Cell::F64(v) => v.is_none(),
            Cell::Bool(v) => v.is_none(),
            Cell::Str(v) => v.is_none(),
            Cell::Date(v) => v.is_none(),
            Cell::Ts(v) => v.is_none(),
            Cell::Vec(v) => v.is_none(),
        }
    }
}

/// Pull cell `(col, row)` out of an arrow batch, typed per the logical column.
/// A batch whose arrow type mismatches the declared logical column is rejected
/// with `Validation` (caller-shaped wire data), never a panic.
fn cell_from_arrow(batch: &RecordBatch, col: usize, row: usize, logical: &str) -> Result<Cell> {
    let a = batch.column(col);
    let null = a.is_null(row);
    macro_rules! dc {
        ($ty:ty) => {
            a.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                ControlPlaneError::Validation(format!(
                    "inline: column {col} is not the declared {logical} (expected {})",
                    stringify!($ty)
                ))
            })?
        };
    }
    Ok(match logical {
        "integer" => {
            let arr = dc!(Int32Array);
            Cell::I32((!null).then(|| arr.value(row)))
        }
        "long" => {
            let arr = dc!(Int64Array);
            Cell::I64((!null).then(|| arr.value(row)))
        }
        "double" => {
            let arr = dc!(Float64Array);
            Cell::F64((!null).then(|| arr.value(row)))
        }
        "boolean" => {
            let arr = dc!(BooleanArray);
            Cell::Bool((!null).then(|| arr.value(row)))
        }
        "string" => {
            let arr = dc!(StringArray);
            Cell::Str((!null).then(|| arr.value(row).to_string()))
        }
        "date" => {
            let arr = dc!(Date32Array);
            Cell::Date((!null).then(|| {
                time::macros::date!(1970 - 01 - 01) + time::Duration::days(arr.value(row) as i64)
            }))
        }
        "timestamp" => {
            let arr = dc!(TimestampMicrosecondArray);
            Cell::Ts((!null).then(|| {
                let micros = arr.value(row);
                let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                time::PrimitiveDateTime::new(odt.date(), odt.time())
            }))
        }
        v if v.starts_with("vector(") => {
            if null {
                Cell::Vec(None)
            } else {
                let list = dc!(ListArray);
                let elems = list.value(row);
                let f32s = elems
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        ControlPlaneError::Backend("inline vector child is not Float32".into())
                    })?;
                Cell::Vec(Some(f32s.values().to_vec()))
            }
        }
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline: unsupported column type {other:?}").into(),
            ));
        }
    })
}

/// Bind a cell as the next positional parameter. Binds OWNED values (copied/cloned)
/// so the cell's borrow doesn't have to outlive the query — `Option<T>` binds NULL.
fn bind_cell<'q>(
    q: Query<'q, Postgres, PgArguments>,
    cell: &Cell,
) -> Query<'q, Postgres, PgArguments> {
    match cell {
        Cell::I32(v) => q.bind(*v),
        Cell::I64(v) => q.bind(*v),
        Cell::F64(v) => q.bind(*v),
        Cell::Bool(v) => q.bind(*v),
        Cell::Str(v) => q.bind(v.clone()),
        Cell::Date(v) => q.bind(*v),
        Cell::Ts(v) => q.bind(*v),
        Cell::Vec(v) => q.bind(v.clone()),
    }
}

/// `CREATE TABLE IF NOT EXISTS` DDL for a table's inline storage from its schema.
fn inline_ddl(table_id: i64, columns: &[ColumnSpec]) -> Result<String> {
    let mut cols = String::new();
    for c in columns {
        let pg = pg_type_for(&c.ty).ok_or_else(|| {
            ControlPlaneError::Backend(format!("inline: no pg type for {:?}", c.ty).into())
        })?;
        // Column names come from the trusted schema; quote to preserve case.
        write!(cols, ", {} {}", quote_ident(&c.name), pg).map_err(backend)?;
    }
    Ok(format!(
        "create table if not exists {} (\
           loom_row_id bigserial primary key, \
           begin_snapshot bigint not null, \
           end_snapshot bigint, \
           loom_tombstone boolean not null default false, \
           loom_change_kind text not null default '+I', \
           loom_bucket int, \
           loom_offset bigint{cols})",
        inline_table_name(table_id),
    ))
}

/// True if `e` is a Postgres "object already exists" race on concurrent DDL:
/// `duplicate_table` (42P07), `duplicate_column` (42701), `duplicate_object`
/// (42710 — the implicit row-type a `CREATE TABLE` creates alongside the
/// relation, reported as `type "<name>" already exists`), or a `unique_violation`
/// (23505) on the system catalog when two sessions create the same relation at
/// once. `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` are NOT
/// concurrency-safe in Postgres — the existence check and the catalog insert are
/// not atomic against a concurrent creator — so this is a benign lost race: the
/// object now exists. All four codes are the same race surfacing at whichever
/// catalog layer the loser happens to collide on first (relation, column, row
/// type, or the backing unique index).
pub(crate) fn is_duplicate_object_race(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| matches!(code.as_ref(), "42P07" | "42701" | "42710" | "23505"))
}

/// Execute one idempotent `IF NOT EXISTS` DDL statement, tolerating Postgres's
/// concurrent-create race (see [`is_duplicate_object_race`]). The statement runs
/// inside a SAVEPOINT, so a lost race rolls back only the savepoint — the object
/// now exists and the caller's OUTER transaction survives (a raw duplicate error
/// would otherwise poison the whole transaction). On the winning path the
/// savepoint simply releases; a genuinely-different failure is restored and
/// surfaced. AssertSqlSafe: static savepoint control + a caller-trusted DDL.
async fn run_idempotent_ddl(conn: &mut sqlx::PgConnection, ddl: String) -> Result<()> {
    sqlx::query(AssertSqlSafe("savepoint loom_ddl"))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    match sqlx::query(AssertSqlSafe(ddl)).execute(&mut *conn).await {
        Ok(_) => {
            sqlx::query(AssertSqlSafe("release savepoint loom_ddl"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            Ok(())
        }
        // A concurrent creator won the race: undo the failed statement (the object
        // it tried to create already exists) and continue the outer transaction.
        Err(e) if is_duplicate_object_race(&e) => {
            sqlx::query(AssertSqlSafe("rollback to savepoint loom_ddl"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            sqlx::query(AssertSqlSafe("release savepoint loom_ddl"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            Ok(())
        }
        // A real failure: restore the pre-statement state so the outer transaction
        // is usable, then surface the error.
        Err(e) => {
            sqlx::query(AssertSqlSafe("rollback to savepoint loom_ddl"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            Err(backend(e))
        }
    }
}

/// Create the inline table if absent and guarantee the `loom_tombstone` column
/// exists (pre-existing tables created before slice 1 lack it).
///
/// Concurrency: the first mutation of a file-only object provisions `inline_<tid>`
/// lazily, so two writers (any identities) can race the first-time creation. Each
/// idempotent DDL runs via [`run_idempotent_ddl`], which absorbs the Postgres
/// concurrent-create race in a savepoint — the loser proceeds against the table the
/// winner created, WITHOUT any table-wide lock (per-identity concurrency is intact).
async fn ensure_inline_schema(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    columns: &[ColumnSpec],
) -> Result<()> {
    run_idempotent_ddl(&mut *conn, inline_ddl(tid, columns)?).await?;
    let alter_tomb = format!(
        "alter table {} add column if not exists loom_tombstone boolean not null default false",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_tomb).await?;
    let alter_kind = format!(
        "alter table {} add column if not exists loom_change_kind text not null default '+I'",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_kind).await?;
    let alter_bucket = format!(
        "alter table {} add column if not exists loom_bucket int",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_bucket).await?;
    let alter_offset = format!(
        "alter table {} add column if not exists loom_offset bigint",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_offset).await?;
    Ok(())
}

/// Land `batch` for `table` as inline rows: a mirror-only commit (snapshot + typed
/// rows + lineage) in ONE Postgres transaction. No object storage, no Iceberg
/// metadata. `columns` is the table's logical schema (authoritative). Returns the
/// new loom snapshot id.
///
/// CONTRACT: `batch`'s columns must align **positionally** with `columns` (same
/// order, same logical types) — `batch.column(i)` is read as `columns[i]`. Both
/// come from the landing's schema, so they agree by construction; a batch that
/// violates the contract (arrow type != declared logical type) is rejected with
/// `Validation`, never a panic.
///
/// Thin wrapper over [`inline_append_decl`] for the (many) callers that only ever
/// request a log declaration or none — preserves this function's signature
/// exactly so none of them need to change for the `StreamDecl` widening. The
/// `/models/{type}?mode=cdc` path (the only CDC-declaring caller) goes through
/// `inline_append_decl` directly, via `iceberg_landing::land`.
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    stream_buckets: Option<i32>,
) -> Result<SnapshotId> {
    let decl = match stream_buckets {
        Some(n) => crate::stream::StreamDecl::Log(n),
        None => crate::stream::StreamDecl::None,
    };
    inline_append_decl(
        pool,
        table,
        columns,
        batch,
        lineage,
        flush_threshold,
        &decl,
        &[],
        None,
    )
    .await
}

/// The actual inline-append implementation, parameterized by the full stream-mode
/// declaration (log bucket count, or a cdc declaration with its bucket key).
/// See [`inline_append`] (the stable public entrypoint) for the contract.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the landing params it forwards (pool, table, columns, batch, lineage, \
              flush threshold, stream decl) plus the slice-4 downstream jobs the action path threads in, \
              plus the road-stream-continuous MV-commit extras (watermark CAS + run mark)"
)]
pub(crate) async fn inline_append_decl(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    decl: &crate::stream::StreamDecl,
    jobs: &[NewJob],
    mv: Option<&MvCommit>,
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Transaction derefs to PgConnection; the helpers take `&mut PgConnection`.
    let conn: &mut PgConnection = &mut tx;

    // 1. Snapshot (no Iceberg backing) + ensure mirror table/columns exist. The
    //    ensure's WITNESS — did THIS call create the row? — is the batch->stream
    //    conversion guard's `pre_existing`. A read taken before the ensure cannot
    //    serve: `ensure_table` resolves a lost create race to the winner's table_id,
    //    so a pre-read would call a concurrent writer's brand-new table "not
    //    existing" and let a stream declare convert it.
    let at = next_snapshot(conn, None).await?;
    let (tid, created) = ensure_table_witnessed(conn, &table.schema, &table.name, at).await?;
    let pre_existing = !created;

    // Read whether this table is ALREADY a declared log table before building the
    // mirror column list below, so a stream table's reserved framing columns can be
    // registered in the mirror from the moment it is declared (and on every append
    // thereafter, so the projected list always matches what's already live). Reused
    // below by the stream-mode reconciliation, so this reads `stream.stream_table`
    // only once per call.
    let existing = crate::stream::pg_stream_bucket_count(&mut *conn, tid).await?;
    // Whether framing should be registered in the mirror for THIS append: either the
    // table is already a declared stream table (`existing.is_some()`), or this call
    // is declaring it for the first time (`decl` is not `None` on a BRAND NEW table,
    // `!pre_existing`). Deliberately excludes the "existing BATCH table asked to
    // become a stream table" case (`!matches!(decl, StreamDecl::None) && pre_existing
    // && existing.is_none()`) — that conversion is rejected below with its own
    // `Validation` message, and must reach that check via the ORIGINAL identical-
    // schema comparison, not get relabeled as a same-transaction non-nullable
    // "additive" column error by the mismatched pcols/live lengths this would
    // otherwise cause.
    let is_stream =
        existing.is_some() || (!matches!(decl, crate::stream::StreamDecl::None) && !pre_existing);

    let mut pcols = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            Ok(ProjectedColumn {
                order: i as i64,
                name: c.name.clone(),
                iceberg_type: mirror_column_type(&c.ty).ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("inline: no iceberg type for {:?}", c.ty).into(),
                    )
                })?,
                nullable: c.nullable,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Stream tables persist their log framing as reserved physical columns; register
    // them in the mirror alongside the user columns so flush/read see a consistent
    // schema. Hidden from logical reads by `is_reserved`. Ordered AFTER the user
    // columns, matching the order `ensure_iceberg_table`'s `include_framing` appends
    // them in the Iceberg physical schema at flush time.
    if is_stream {
        let base = pcols.len() as i64;
        for (i, spec) in crate::iceberg_landing::framing_column_specs()
            .iter()
            .enumerate()
        {
            pcols.push(ProjectedColumn {
                order: base + 1 + i as i64,
                name: spec.name.clone(),
                iceberg_type: iceberg_physical_type(&spec.ty)
                    .ok_or_else(|| {
                        ControlPlaneError::Backend(
                            format!("unknown framing logical type `{}`", spec.ty).into(),
                        )
                    })?
                    .to_string(),
                nullable: spec.nullable,
            });
        }
    }
    let live = live_columns(conn, tid, at).await?;
    if live.is_empty() {
        project_columns(conn, tid, at, &pcols).await?;
    } else {
        match classify_schema_change(&live, &pcols) {
            Ok(SchemaPlan::Identical) => {}
            Ok(SchemaPlan::Additive { .. }) => {
                return Err(ControlPlaneError::Validation(
                    "schema evolution unsupported: additive evolution on the inline path is deferred".into(),
                ));
            }
            Err(e) => return Err(ControlPlaneError::Validation(e.to_string())),
        }
    }

    // 2. Ensure inline storage exists (transactional DDL).
    ensure_inline_schema(&mut *conn, tid, columns).await?;

    // Reconcile stream mode (shared with the direct-write Parquet path). `effective`
    // = Some(bucket_count) iff this table is a (now-)declared log table; None =>
    // batch table (no offset stamping). Rejects a `< 1` request (Validation), a
    // batch->stream conversion (Validation), and a bucket-count mismatch (Conflict),
    // all BEFORE any declare — on this same transaction.
    let effective: Option<i32> =
        crate::stream::reconcile_stream_mode(&mut *conn, tid, decl, pre_existing, table, at)
            .await?;

    // 3. Insert each row with the new begin_snapshot. The statement text is
    //    loop-invariant — only the binds change per row.
    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(bc) = effective {
        // Per-row bucket: CDC tables hash on identity (`hash(id) % bc`, so a
        // key's whole history stays in one bucket); log tables use row_index %
        // bc (v1 simplification; bc >= 1).
        let n = batch.num_rows();
        let bc_usize = usize::try_from(bc).map_err(|e| {
            ControlPlaneError::Backend(format!("invalid stream bucket count {bc}: {e}").into())
        })?;
        let meta = crate::stream::pg_stream_meta(&mut *conn, tid).await?;
        let cdc_key: Option<String> = match &meta {
            Some(m) if m.kind == control_plane_core::StreamKind::Cdc => m.bucket_key.clone(),
            _ => None,
        };
        // Compute each row's bucket up front (so offset runs can be reserved per
        // bucket before any row is inserted).
        let mut row_bucket = vec![0usize; n];
        for row in 0..n {
            let b = match &cdc_key {
                Some(key) => {
                    let idx = columns.iter().position(|c| &c.name == key).ok_or_else(|| {
                        ControlPlaneError::Backend(
                            format!("cdc bucket_key `{key}` not in appended columns").into(),
                        )
                    })?;
                    let spec = columns.get(idx).ok_or_else(|| {
                        ControlPlaneError::Backend("bucket_key column index out of range".into())
                    })?;
                    let cell = cell_from_arrow(batch, idx, row, &spec.ty)?;
                    usize::try_from(cdc_bucket(&cell, bc)?).map_err(|e| {
                        ControlPlaneError::Backend(format!("bucket overflow: {e}").into())
                    })?
                }
                None => row % bc_usize,
            };
            let slot = row_bucket
                .get_mut(row)
                .ok_or_else(|| ControlPlaneError::Backend("row index out of range".into()))?;
            *slot = b;
        }
        // Count rows per bucket and reserve a contiguous offset run per touched
        // bucket, up front, so the per-row loop only needs to hand out offsets
        // from an already-reserved cursor (panic-free bucket indexing: `Vec::get`/
        // `get_mut` instead of `[]`, even though `b` is always `< bc` by construction).
        let mut counts = vec![0i64; bc_usize];
        for row in 0..n {
            let b = *row_bucket
                .get(row)
                .ok_or_else(|| ControlPlaneError::Backend("row index out of range".into()))?;
            let c = counts
                .get_mut(b)
                .ok_or_else(|| ControlPlaneError::Backend("bucket index out of range".into()))?;
            *c += 1;
        }
        // cursor[b] = next offset to assign for bucket b (first of the reserved run).
        let mut cursor = vec![0i64; bc_usize];
        for (b, count) in counts.iter().enumerate() {
            if *count > 0 {
                let b_i32 = i32::try_from(b).map_err(|e| {
                    ControlPlaneError::Backend(format!("bucket index overflowed i32: {e}").into())
                })?;
                let first =
                    crate::stream::pg_allocate_offset(&mut *conn, tid, b_i32, *count).await?;
                let slot = cursor.get_mut(b).ok_or_else(|| {
                    ControlPlaneError::Backend("bucket index out of range".into())
                })?;
                *slot = first;
            }
        }
        // $1 begin_snapshot, $2 loom_bucket, $3 loom_offset, then data columns from $4.
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 4))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "insert into {} (begin_snapshot, loom_bucket, loom_offset, {col_list}) \
             values ($1, $2, $3, {placeholders})",
            inline_table_name(tid),
        );
        for row in 0..n {
            let b = *row_bucket
                .get(row)
                .ok_or_else(|| ControlPlaneError::Backend("row index out of range".into()))?;
            let offset = *cursor
                .get(b)
                .ok_or_else(|| ControlPlaneError::Backend("bucket index out of range".into()))?;
            {
                let slot = cursor.get_mut(b).ok_or_else(|| {
                    ControlPlaneError::Backend("bucket index out of range".into())
                })?;
                *slot += 1;
            }
            let b_i32 = i32::try_from(b).map_err(|e| {
                ControlPlaneError::Backend(format!("bucket index overflowed i32: {e}").into())
            })?;
            let cells = columns
                .iter()
                .enumerate()
                .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
                .collect::<Result<Vec<_>>>()?;
            let mut q = sqlx::query(AssertSqlSafe(insert_sql.clone()))
                .bind(at.0)
                .bind(b_i32)
                .bind(offset);
            for cell in &cells {
                q = bind_cell(q, cell);
            }
            q.execute(&mut *conn).await.map_err(backend)?;
        }

        // Subscribe wakeup (road-stream-subscribe / road-stream-log-table-subscribe):
        // one fire-and-forget notify per committed STREAM write batch (CDC or log),
        // buffered until this tx commits. Batch (non-stream) tables never notify.
        if matches!(
            &meta,
            Some(m) if matches!(
                m.kind,
                control_plane_core::StreamKind::Cdc | control_plane_core::StreamKind::Log
            )
        ) {
            crate::stream::pg_notify_changelog(&mut *conn, tid).await?;
        }
    } else {
        // Batch table: unchanged behaviour (loom_bucket/loom_offset stay NULL,
        // loom_change_kind defaults to '+I').
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 2)) // $1 = begin_snapshot
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "insert into {} (begin_snapshot, {col_list}) values ($1, {placeholders})",
            inline_table_name(tid),
        );
        for row in 0..batch.num_rows() {
            let cells = columns
                .iter()
                .enumerate()
                .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
                .collect::<Result<Vec<_>>>()?;
            let mut q = sqlx::query(AssertSqlSafe(insert_sql.clone())).bind(at.0);
            for cell in &cells {
                q = bind_cell(q, cell);
            }
            q.execute(&mut *conn).await.map_err(backend)?;
        }
    }

    // road-stream-continuous MV-commit extras: CAS-advance the source's per-bucket
    // watermark(s) and mark the driving run succeeded, on THIS transaction — placed
    // immediately after the row-insert loop so a `Conflict` here (a stale CAS) or a
    // `NotFound` (an unknown run) propagates via `?` and rolls back the WHOLE append
    // (rows, declare, and — since this runs before them — the flush trigger, lineage,
    // jobs, and data triggers below). That's the exactly-once mechanism: the
    // watermark moves iff the output rows land, and neither lands iff the other fails.
    if let Some(m) = mv {
        for adv in &m.advances {
            crate::stream::pg_advance_mv_watermark(&mut *conn, &m.mv, m.source_table_id, adv)
                .await?;
        }
        if let Some(rid) = m.run_id {
            crate::transforms::pg_mark_run_succeeded(&mut *conn, rid, at.0).await?;
        }
    }

    // 3b. Flush trigger: accrue this batch's live bytes; on crossing the
    // (per-table or global) threshold, enqueue one flush_table job, atomically
    // with the rows. `None` => triggering disabled (preserves prior behaviour).
    if let Some(threshold) = flush_threshold {
        let add = batch.get_array_memory_size() as i64;
        let st = bump_inline_trigger(&mut *conn, tid, add, threshold).await?;
        // A table already carrying inline shadow deltas must never be enqueued for
        // the byte-trigger flush: draining it would flush a row-version/tombstone
        // into Parquet, duplicating or resurrecting a file row. Skip the enqueue AND
        // the arm so the trigger stays disarmed (and can re-fire once slice-2
        // consolidation clears the flag) rather than getting stuck "enqueued" for a
        // job that was deliberately never queued.
        // A CDC table's flush is delta-aware (dual-write of the base +I/+U/-D
        // subset and the full changelog, `flush_locked`'s CDC branch) and is
        // safe to run even while shadow deltas are pending — it never
        // duplicates/resurrects a file row the way the non-CDC cow-inline-
        // shadow flush would, so the `has_shadow` suppression below applies
        // only to non-CDC tables.
        let is_cdc = crate::stream::pg_stream_meta(&mut *conn, tid)
            .await?
            .is_some_and(|m| m.kind == control_plane_core::StreamKind::Cdc);
        if st.live_bytes >= st.effective
            && !st.enqueued
            && (is_cdc || !has_shadow(&mut *conn, tid).await?)
        {
            let job = NewJob {
                kind: control_plane_core::FLUSH_JOB_KIND.to_string(),
                payload: serde_json::json!({ "schema": table.schema, "name": table.name }),
                run_at: None,
                priority: 0,
            };
            crate::queue::pg_insert(&mut *conn, &job).await?;
            arm_inline_trigger(&mut *conn, tid).await?;
        }
    }

    // 4. Lineage, atomic with the rows.
    pg_emit(&mut *conn, &lineage).await?;

    // Slice-4: enqueue the action's resolved downstream jobs in this same tx
    // (commit-or-neither), mirroring `CommitExtras.jobs` on the land_parquet path.
    for job in jobs {
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }

    crate::transforms::pg_fire_data_triggers(
        &mut *conn,
        std::slice::from_ref(table),
        Some(lineage.run_id.0),
    )
    .await?;

    tx.commit().await.map_err(backend)?;
    Ok(at)
}

/// The micro-batch extras committed atomically with an MV's output append: the
/// per-bucket watermark CAS (Conflict rolls the whole tx back — the
/// exactly-once mechanism) and the run's success mark.
pub struct MvCommit {
    pub mv: String,
    pub source_table_id: i64,
    pub advances: Vec<control_plane_core::WatermarkAdvance>,
    pub run_id: Option<uuid::Uuid>,
}

/// Advance an MV's per-bucket watermark and mark its run succeeded in ONE
/// transaction, WITHOUT landing output — the "micro-batch consumed a source
/// delta but its SQL produced zero output rows" case (a filtering MV). The
/// watermark MUST still advance or the consumed delta is reprocessed forever.
/// A CAS `Conflict` aborts the tx (a concurrent run superseded this one). No
/// output table is declared or touched.
pub async fn advance_mv_watermark_only(pool: &PgPool, mv: &MvCommit) -> Result<()> {
    let mut tx = pool.begin().await.map_err(backend)?;
    for adv in &mv.advances {
        crate::stream::pg_advance_mv_watermark(&mut *tx, &mv.mv, mv.source_table_id, adv).await?;
    }
    if let Some(rid) = mv.run_id {
        crate::transforms::pg_mark_run_succeeded(&mut *tx, rid, 0).await?;
    }
    tx.commit().await.map_err(backend)?;
    Ok(())
}

/// Land one micro-batch result: inline-append `batch` to `table` declared (or
/// confirmed) a log stream table with `buckets` buckets — framing stamped,
/// flush byte-trigger armed, data triggers fired (composability) — plus the
/// [`MvCommit`] extras, all on ONE transaction.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors inline_append_decl's params, fixed to a Log declaration built from `buckets`"
)]
pub async fn inline_append_mv(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    buckets: i32,
    mv: &MvCommit,
) -> Result<SnapshotId> {
    inline_append_decl(
        pool,
        table,
        columns,
        batch,
        lineage,
        flush_threshold,
        &crate::stream::StreamDecl::Log(buckets),
        &[],
        Some(mv),
    )
    .await
}

// ---------------------------------------------------------------------------
// Scalable copy-on-write: O(change) inline delta writes guarded by a per-identity
// compare-and-swap. A mutation (row-version or tombstone) lands as ONE inline row
// keyed by the object's identity; the merge-on-read (identity-aware) lets the
// highest `begin_snapshot` win and a tombstone hide the file row. Inline delta rows
// are NEVER end-capped here — time travel is served by the merge + Postgres MVCC.
// ---------------------------------------------------------------------------

/// Mark `tid` as carrying inline shadow deltas, so the byte-trigger flush is
/// suppressed until slice-2 consolidation (flushing a version/tombstone would
/// duplicate or resurrect a file row). Idempotent.
///
/// AssertSqlSafe: static query against a standalone table; the `.sqlx` cache
/// cannot be regenerated in this env (initdb refuses to run as root).
pub async fn set_has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.shadow_flag (table_id) values ($1) on conflict do nothing",
    ))
    .bind(tid)
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

/// True if `tid` has been flagged as carrying inline shadow deltas (Task-5 flush
/// guard). AssertSqlSafe: see [`set_has_shadow`].
pub async fn has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<bool> {
    let v: bool = sqlx::query_scalar(AssertSqlSafe(
        "select exists(select 1 from iceberg_mirror.shadow_flag where table_id = $1)",
    ))
    .bind(tid)
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(v)
}

/// Clear `tid`'s inline-shadow flag after consolidation, re-arming the
/// byte-trigger flush. Idempotent. AssertSqlSafe: see [`set_has_shadow`].
pub async fn clear_has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "delete from iceberg_mirror.shadow_flag where table_id = $1",
    ))
    .bind(tid)
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Clear `tid`'s inline-shadow flag ONLY when no live shadow delta remains — i.e.
/// no live `inline_<tid>` row is a tombstone or a version/delete change
/// (`loom_tombstone` or `loom_change_kind in ('+U', '-D')`). Returns `true` when
/// the flag was actually cleared.
///
/// This is deliberately conditional, NOT [`clear_has_shadow`]'s unconditional
/// delete: consolidation's post-commit clear can race a brand-new mutation that
/// lands mid-run (spec §3 of the slice-2 design). A survivor delta must keep the
/// byte-trigger flush suppressed even though the consolidation that raced it
/// already completed — an unconditional clear here would re-open the corruption
/// slice-1's suppression closed (a subsequent flush could drain a still-shadowed
/// table and duplicate/resurrect a file row). If live deltas remain, the flag
/// stays set and the *next* consolidation run (re-armed by the trigger) drains
/// them and retries the clear.
///
/// The single-statement NOT-EXISTS check still has a benign residual write-skew:
/// a delta transaction whose `set_has_shadow` no-ops against the still-present
/// flag row can commit *after* this statement's NOT-EXISTS snapshot, leaving a
/// live delta with no flag. That skew is NOT closed here — it is closed at the
/// flush, by the Task 3 read-set guard (`shadow_rows_among`, consulted from
/// `flush_locked`), which inspects the rows a flush is about to write rather than
/// trusting the flag; a hit there self-heals by re-setting `has_shadow`.
///
/// Idempotent: a second call with nothing to clear returns `Ok(false)`.
///
/// AssertSqlSafe: `tid` is bound as `$1`; the `inline_<tid>` relation name is a
/// dynamic identifier spliced via [`inline_table_name`] (the slice-1 precedent —
/// see [`quote_ident`]'s doc for why splicing an internally-generated identifier
/// is safe here). Guarded by [`inline_relation_exists`] so a table with no inline
/// storage yet (a shadowed table always has one, but stay panic-free) reads as
/// "trivially quiescent" instead of erroring on `relation ... does not exist`.
pub async fn clear_has_shadow_if_quiescent(conn: &mut PgConnection, tid: i64) -> Result<bool> {
    if !inline_relation_exists(&mut *conn, tid).await? {
        // No inline relation at all: trivially no live shadow delta can exist.
        // Fall back to the unconditional clear, reporting whether the flag was
        // actually set beforehand (clear_has_shadow itself is `Result<()>`).
        let was_set = has_shadow(&mut *conn, tid).await?;
        clear_has_shadow(conn, tid).await?;
        return Ok(was_set);
    }
    let result = sqlx::query(AssertSqlSafe(format!(
        "delete from iceberg_mirror.shadow_flag where table_id = $1 \
         and not exists (select 1 from {} where end_snapshot is null \
           and (loom_tombstone or loom_change_kind in ('+U', '-D')))",
        inline_table_name(tid),
    )))
    .bind(tid)
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(result.rows_affected() > 0)
}

/// True if ANY of `row_ids` (the exact set a flush just read via
/// `inline_live_batch`) is a LIVE inline shadow row — a tombstone or a
/// row-version/delete change (`loom_tombstone` or `loom_change_kind in ('+U',
/// '-D')`), i.e. `end_snapshot is null`.
///
/// This is the Task 3 read-set guard: safety derives from the DATA a flush is
/// about to write, not from the `has_shadow` flag. A flush can only corrupt by
/// appending rows it read, so checking exactly the read set is race-free — a
/// shadow delta that commits after the read is not in `row_ids` and this
/// returns `false` for it (correctly; that delta cannot have been drained).
/// Called from `flush_locked` right after it destructures `inline_live_batch`'s
/// `row_ids`, before any Parquet work.
///
/// AssertSqlSafe: `row_ids` is bound as `$1`; the `inline_<tid>` relation name
/// is a dynamic identifier spliced via [`inline_table_name`] (see
/// [`end_cap_inline_rows_by_id`] for the identical idiom).
pub async fn shadow_rows_among(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    row_ids: &[i64],
) -> Result<bool> {
    let sql = format!(
        "select exists(select 1 from {} where loom_row_id = any($1) \
         and end_snapshot is null \
         and (loom_tombstone or loom_change_kind in ('+U', '-D')))",
        inline_table_name(tid),
    );
    let v: bool = sqlx::query_scalar(AssertSqlSafe(sql))
        .bind(row_ids.to_vec())
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(v)
}

/// Extract the id column's cell (row 0) from `batch`, typed by its `ColumnSpec`.
/// The id value never crosses a crate boundary as a `Cell`/`SqlValue`: callers pass
/// it inside an Arrow batch and name the id column, and this locates it by name.
fn extract_id_cell(columns: &[ColumnSpec], id_column: &str, batch: &RecordBatch) -> Result<Cell> {
    // Logical type from the passed specs (by name) ...
    let spec = columns
        .iter()
        .find(|c| c.name == id_column)
        .ok_or_else(|| {
            ControlPlaneError::Backend(format!("id column {id_column} not in columns").into())
        })?;
    // ... but the physical position from the BATCH's own schema, so a one-cell
    // tombstone batch (whose layout need not match `columns`) still resolves the id
    // cell instead of indexing `columns`' position into a shorter batch.
    let idx = batch.schema().index_of(id_column).map_err(|e| {
        ControlPlaneError::Backend(format!("id column {id_column} not in batch: {e}").into())
    })?;
    cell_from_arrow(batch, idx, 0, &spec.ty)
}

/// True if the physical `inline_<tid>` relation exists. A file-only object (landed
/// as Parquet, no inline write yet) has none — `to_regclass` returns NULL for an
/// absent relation. Shared by the read guards so an absent inline tier reads as
/// "no rows" instead of erroring with `relation ... does not exist`.
async fn inline_relation_exists(conn: &mut PgConnection, tid: i64) -> Result<bool> {
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "select to_regclass('{}')::text",
        inline_table_name(tid)
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(exists.is_some())
}

/// `coalesce(max(begin_snapshot), 0)` over the LIVE inline rows of one identity —
/// the identity's current inline version (0 when it has no live inline row).
///
/// Uses `sqlx::query` (not `query_scalar`) because `bind_cell` binds onto `Query`,
/// then reads column 0 by alias. AssertSqlSafe: the `inline_<tid>` name is dynamic
/// and `id_column` is a quoted mirror identifier; the id value is a bound param.
async fn read_max_version(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    id_column: &str,
    id: &Cell,
) -> Result<i64> {
    // A file-only object has a live `iceberg_mirror.table` row but no `inline_<tid>`
    // table yet; its inline version is 0. Guard the read the same way
    // `has_live_inline_rows`/`inline_live_batch` do so this returns 0 rather than
    // erroring — this is what makes the file-only first-mutation path (reached via
    // `current_inline_version`) safe.
    if !inline_relation_exists(&mut *conn, tid).await? {
        return Ok(0);
    }
    // A SQL NULL id makes `"id" = $1` silently false, so `max` reads 0 and the CAS
    // would never detect a concurrent mutation. The identity column is non-nullable
    // in practice; reject a NULL id defensively rather than mis-reading version 0.
    if id.is_null() {
        return Err(ControlPlaneError::Backend("identity value is null".into()));
    }
    let sql = format!(
        "select coalesce(max(begin_snapshot), 0) as v from {} \
         where \"{}\" = $1 and end_snapshot is null \
           and (loom_change_kind is null or loom_change_kind <> '-U')",
        inline_table_name(tid),
        id_column.replace('"', "\"\""),
    );
    let row = bind_cell(sqlx::query(AssertSqlSafe(sql)), id)
        .fetch_one(conn)
        .await
        .map_err(backend)?;
    let v: i64 = row.try_get("v").map_err(backend)?;
    Ok(v)
}

/// A deterministic 64-bit advisory-lock key for one identity of one inline table.
/// Different identities (or tables) hash to different keys, so concurrent mutations
/// of distinct identities never contend; the SAME identity always maps to the SAME
/// key, so its mutations serialize. No `rand`; mirrors `iceberg_flush::lock_key`'s
/// `DefaultHasher` scheme (stable within a process, which is all the CAS needs).
fn advisory_key_for_id(tid: i64, id: &Cell) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    tid.hash(&mut h);
    "\u{1f}".hash(&mut h);
    // Tag each variant so distinct types with equal bit patterns don't collide, then
    // hash the scalar. Floats/vectors hash their bit representation (f32/f64 are not
    // `Hash`); SQL NULL hashes as the tagged `None`.
    match id {
        Cell::I32(v) => {
            0u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::I64(v) => {
            1u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::F64(v) => {
            2u8.hash(&mut h);
            v.map(f64::to_bits).hash(&mut h);
        }
        Cell::Bool(v) => {
            3u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::Str(v) => {
            4u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::Date(v) => {
            5u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::Ts(v) => {
            6u8.hash(&mut h);
            v.hash(&mut h);
        }
        Cell::Vec(v) => {
            7u8.hash(&mut h);
            v.as_ref()
                .map(|xs| xs.iter().map(|f| f.to_bits()).collect::<Vec<u32>>())
                .hash(&mut h);
        }
    }
    h.finish() as i64
}

/// The bucket for one identity of a CDC table: `stable_hash(id) % bucket_count`,
/// so a key's whole change history (+I/-U/+U/-D) stays in one bucket (LastRow
/// merge and per-key ordering depend on it). Reuses `advisory_key_for_id`'s
/// deterministic, `rand`-free hash family; the modulus is taken on the unsigned
/// value so the result is always in `0..bucket_count`.
fn cdc_bucket(id: &Cell, bucket_count: i32) -> Result<i32> {
    if bucket_count < 1 {
        return Err(ControlPlaneError::Validation(format!(
            "cdc bucket_count must be >= 1, got {bucket_count}"
        )));
    }
    // advisory_key_for_id returns an i64 already tagged-per-variant; take it as u64
    // and mod by bucket_count. `as u64` reinterprets the bits (no sign bias), and
    // `% bucket_count` (bucket_count >= 1) yields 0..bucket_count.
    let h = advisory_key_for_id(0, id) as u64;
    let bc = u64::try_from(bucket_count).map_err(|e| {
        ControlPlaneError::Backend(format!("invalid bucket_count {bucket_count}: {e}").into())
    })?;
    let b = (h % bc) as i32;
    Ok(b)
}

/// The current inline version of one identity: `coalesce(max(begin_snapshot), 0)`
/// over its live inline rows. Returns `0` when the table has no inline storage or
/// the identity has no live inline row. `id_batch` is a one-row batch containing the
/// id column; `columns` describes it and names where the id lives.
pub async fn current_inline_version(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    id_column: &str,
    id_batch: &RecordBatch,
) -> Result<i64> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
        return Ok(0);
    };
    let id_cell = extract_id_cell(columns, id_column, id_batch)?;
    read_max_version(&mut conn, tid, id_column, &id_cell).await
}

/// The table's FULL live column set from the mirror, as `ColumnSpec`s — the
/// authoritative inline-table shape, independent of the (possibly one-column)
/// `columns` a given mutation passes. Selects the currently-live
/// `iceberg_mirror.column` rows (`end_snapshot is null`), so no snapshot bound is
/// needed. The stored `column_type` is the Iceberg physical name (e.g. `int`);
/// convert it back to the canonical loom logical name the inline DDL / cell codec
/// understand, exactly as `IcebergCatalog::schema` does.
///
/// AssertSqlSafe: static query against a fixed mirror table; the `.sqlx` cache
/// cannot be regenerated in this env (initdb refuses to run as root).
async fn full_live_column_specs(conn: &mut PgConnection, tid: i64) -> Result<Vec<ColumnSpec>> {
    let rows = sqlx::query(AssertSqlSafe(
        "select column_name, column_type, nulls_allowed from iceberg_mirror.column \
         where table_id = $1 and end_snapshot is null order by column_order",
    ))
    .bind(tid)
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    rows.into_iter()
        .map(|r| {
            let name: String = r.try_get("column_name").map_err(backend)?;
            let column_type: String = r.try_get("column_type").map_err(backend)?;
            let nullable: bool = r.try_get("nulls_allowed").map_err(backend)?;
            let ty = logical_from_iceberg(&column_type)
                .map(BaseType::canonical_name)
                .ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!(
                            "inline: mirror column type {column_type:?} has no loom logical type"
                        )
                        .into(),
                    )
                })?;
            Ok(ColumnSpec { name, ty, nullable })
        })
        .collect()
}

/// Write ONE inline delta row (a row-version or a tombstone) for a single identity,
/// guarded by a per-identity compare-and-swap. Returns the new snapshot id, or
/// `ControlPlaneError::Conflict` if a newer version for the identity already exists.
///
/// For a **version**, `batch` is the full one-row post-PATCH row and `columns` all
/// data specs. For a **tombstone**, `batch` is a one-cell batch holding just the id
/// and `columns = [id spec]`; the tombstone row carries the id + `loom_tombstone =
/// true` with the other data columns NULL, so the identity-aware merge-on-read hides
/// the object.
///
/// Concurrency: a transaction-scoped advisory lock keyed by `(tid, id)` serializes
/// concurrent mutations of the SAME identity. Under that lock, `read_max_version ==
/// expected_version` is a safe CAS — two writers cannot both pass: the first commits
/// a newer `begin_snapshot`, so the second (which only reads the max AFTER taking the
/// lock) sees a higher value and returns `Conflict`. Distinct identities hash to
/// distinct keys and never contend. Inline delta rows are never end-capped here.
#[allow(
    clippy::too_many_arguments,
    reason = "the delta write's public contract carries table + id-batch + version-vs-tombstone + optional before-image + lineage + CAS witness + optional consolidate threshold + downstream jobs; a params struct would only obscure the call sites"
)]
pub async fn write_inline_delta(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    id_column: &str,
    tombstone: bool,
    batch: &RecordBatch,
    before: Option<(&[ColumnSpec], &RecordBatch)>,
    lineage: LineageEvent,
    expected_version: i64,
    consolidate_threshold: Option<i64>,
    jobs: &[control_plane_core::NewJob],
) -> Result<SnapshotId> {
    // `before` (the prior row with its OWN positionally-aligned ColumnSpecs) is USED
    // by the CDC emit branch below; a non-CDC table ignores it and stays byte-identical.
    let mut tx = pool.begin().await.map_err(backend)?;

    // The mutation targets an existing object, so a live mirror row must exist.
    let tid = live_table_id(&mut tx, &table.schema, &table.name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!("no mirror table for {}.{}", table.schema, table.name).into(),
            )
        })?;

    // Provision inline storage (+ loom_tombstone) with the table's FULL live column
    // set, NOT the possibly-one-column `columns` arg. A tombstone passes only
    // `[id spec]`; since `create table if not exists` never adds columns later, a
    // tombstone-first write on a file-only object would otherwise create
    // `inline_<tid>` with just the id column, breaking every later full-column read
    // (`inline_live_batch` / merge-on-read select the full logical column list).
    let full_cols = full_live_column_specs(&mut tx, tid).await?;
    ensure_inline_schema(&mut tx, tid, &full_cols).await?;
    let id_cell = extract_id_cell(columns, id_column, batch)?;

    // Per-identity serialization: a transaction-scoped advisory lock keyed by
    // (tid, id). It auto-releases when this tx ends (commit/rollback/panic), so it
    // can never leak onto a pooled connection. AssertSqlSafe: static query.
    let key = advisory_key_for_id(tid, &id_cell);
    sqlx::query(AssertSqlSafe("select pg_advisory_xact_lock($1)"))
        .bind(key)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

    // CAS: under the lock, the identity's current live max version must still equal
    // the version the caller read. A mismatch means a concurrent mutation won.
    let cur = read_max_version(&mut tx, tid, id_column, &id_cell).await?;
    if cur != expected_version {
        return Err(ControlPlaneError::Conflict(format!(
            "cow: identity version advanced {expected_version} -> {cur} (concurrent mutation)"
        )));
    }

    // Allocate the new snapshot only after the CAS passes (it inserts the
    // iceberg_mirror.snapshot row that makes the delta read-visible).
    let at = next_snapshot(&mut tx, None).await?;

    // On a CDC table, a mutation emits the FULL change sequence — an update writes an
    // adjacent (-U before-image, +U after-image) pair, a delete writes a -D carrying
    // the full prior image — each stamped with a hash-on-identity bucket and gapless
    // per-bucket offsets. Non-CDC tables fall through to the existing single-row inserts.
    let meta = crate::stream::pg_stream_meta(&mut *tx, tid).await?;
    let cdc = matches!(&meta, Some(m) if m.kind == control_plane_core::StreamKind::Cdc);

    // Emit the delta row(s). Every row carries the identity value so merge-on-read
    // (partition by <id>) shadows/hides the file row for that id. A CDC table emits
    // the multi-row change sequence (below); a non-CDC table writes a single tombstone
    // or version row (the `else if`/`else` arms).
    // Count of delta rows this call emits — accrued below for the consolidate
    // trigger. A CDC call emits 1 for a delete's -D or 2 for an update's -U/+U
    // pair; a non-CDC call always emits exactly 1 (its single tombstone or
    // version row). Every branch below assigns it exactly once.
    let emitted_rows: i64;
    if cdc {
        let m = meta
            .as_ref()
            .ok_or_else(|| ControlPlaneError::Backend("cdc meta vanished after check".into()))?;
        let bucket = cdc_bucket(&id_cell, m.bucket_count)?;
        // The before-image carries its OWN columns (positionally aligned with its
        // batch) — never `full_cols`, whose order need not match.
        let (before_cols, before_batch) = before.ok_or_else(|| {
            ControlPlaneError::Backend("cdc mutation requires a before-image".into())
        })?;
        if tombstone {
            // Delete → one -D carrying the FULL prior image, so the changelog event is
            // complete. loom_tombstone=true still hides the base row in merge-on-read.
            let off = crate::stream::pg_allocate_offset(&mut *tx, tid, bucket, 1).await?;
            write_cdc_row(
                &mut tx,
                tid,
                at,
                "-D",
                true,
                bucket,
                off,
                before_cols,
                before_batch,
            )
            .await?;
            emitted_rows = 1;
        } else {
            // Update → adjacent (-U before-image, +U after-image), -U first, at
            // consecutive offsets in the identity's single bucket. -U uses the
            // before-image (its own cols+batch); +U uses the caller's after-image.
            let first = crate::stream::pg_allocate_offset(&mut *tx, tid, bucket, 2).await?;
            write_cdc_row(
                &mut tx,
                tid,
                at,
                "-U",
                false,
                bucket,
                first,
                before_cols,
                before_batch,
            )
            .await?;
            write_cdc_row(
                &mut tx,
                tid,
                at,
                "+U",
                false,
                bucket,
                first + 1,
                columns,
                batch,
            )
            .await?;
            emitted_rows = 2;
        }
    } else if tombstone {
        // Tombstone: begin_snapshot, loom_tombstone=true, loom_change_kind='-D',
        // "<id_col>"=id; data NULL.
        let sql = format!(
            "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, \"{}\") \
             values ($1, true, '-D', $2)",
            inline_table_name(tid),
            id_column.replace('"', "\"\""),
        );
        bind_cell(sqlx::query(AssertSqlSafe(sql)).bind(at.0), &id_cell)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        emitted_rows = 1;
    } else {
        // Version: mirror inline_append's INSERT but prefix
        // loom_tombstone=false, loom_change_kind='+U'. The id column is one of
        // <cols>, so the version row carries the id naturally.
        let col_list = columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 2)) // $1 = begin_snapshot
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, {col_list}) \
             values ($1, false, '+U', {placeholders})",
            inline_table_name(tid),
        );
        let cells = columns
            .iter()
            .enumerate()
            .map(|(c, spec)| cell_from_arrow(batch, c, 0, &spec.ty))
            .collect::<Result<Vec<_>>>()?;
        let mut q = sqlx::query(AssertSqlSafe(sql)).bind(at.0);
        for cell in &cells {
            q = bind_cell(q, cell);
        }
        q.execute(&mut *tx).await.map_err(backend)?;
        emitted_rows = 1;
    }

    // Consolidate trigger: accrue this call's delta rows, and on crossing the
    // threshold enqueue one `stream_consolidate` job — mirroring the byte-trigger
    // block in `inline_append_decl` above, but counting delta rows in a sibling
    // trigger table (`consolidate_trigger`, not `inline_trigger`) so the two never
    // clash. The counter is per-table and kind-agnostic: a CDC call accrues its
    // emitted changelog rows (1 for a delete's -D, 2 for an update's -U/+U pair)
    // and a non-CDC call accrues its single tombstone/version row. Either way the
    // job lands in the same `stream_consolidate` kind — `consolidate_table`
    // dispatches a non-CDC shadow-bearing table to the COW fold instead of the CDC
    // changelog fold. `None` => triggering disabled. Debounced by `enqueued`; the
    // engine's `consolidate_stream` handler clears the trigger (alongside
    // `has_shadow`) when consolidation runs, re-arming it for the next run.
    if let Some(threshold) = consolidate_threshold {
        let st = bump_consolidate_trigger(&mut tx, tid, emitted_rows, threshold).await?;
        if st.delta_count >= st.effective && !st.enqueued {
            let job = NewJob {
                kind: control_plane_core::STREAM_CONSOLIDATE_JOB_KIND.to_string(),
                payload: serde_json::json!({ "schema": table.schema, "name": table.name }),
                run_at: None,
                priority: 0,
            };
            crate::queue::pg_insert(&mut *tx, &job).await?;
            arm_consolidate_trigger(&mut tx, tid).await?;
        }
    }

    set_has_shadow(&mut tx, tid).await?;
    pg_emit(&mut *tx, &lineage).await?;
    crate::transforms::pg_fire_data_triggers(
        &mut tx,
        std::slice::from_ref(table),
        Some(lineage.run_id.0),
    )
    .await?;
    // The action's resolved downstream jobs (Update/Delete phase 2) ride this same
    // commit tx — enqueued iff the delta write commits (commit-or-neither), mirroring
    // the insert path's `write_object` and the `stream_consolidate` enqueue above.
    for job in jobs {
        crate::queue::pg_insert_if_absent(&mut *tx, job).await?;
    }

    // Subscribe wakeup: the -U/+U/-D events just written become visible on
    // commit; the notify is buffered with them (mirrors queue.rs's
    // pg_notify-in-commit).
    if cdc {
        crate::stream::pg_notify_changelog(&mut *tx, tid).await?;
    }

    tx.commit().await.map_err(backend)?;
    Ok(at)
}

/// Insert one framed CDC inline row for the given change kind: `begin_snapshot`,
/// `loom_tombstone`, `loom_change_kind`, `loom_bucket`, `loom_offset`, then the data
/// columns read positionally from `cols`/`batch` row 0. The caller picks the
/// (cols, batch) pair per row — the before-image's OWN pair for `-U`/`-D`, the
/// after-image for `+U` — so cells always bind to the matching columns.
#[allow(
    clippy::too_many_arguments,
    reason = "one framed inline-row insert; a struct would obscure the two call sites"
)]
async fn write_cdc_row(
    tx: &mut sqlx::PgConnection,
    tid: i64,
    at: SnapshotId,
    change_kind: &str, // '-U' | '+U' | '-D'
    tombstone: bool,
    bucket: i32,
    offset: i64,
    cols: &[ColumnSpec],
    batch: &RecordBatch,
) -> Result<()> {
    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    // $1 begin_snapshot, $2 tombstone, $3 change_kind, $4 bucket, $5 offset, then data from $6.
    let placeholders = (0..cols.len())
        .map(|i| format!("${}", i + 6))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, loom_bucket, loom_offset, {col_list}) \
         values ($1, $2, $3, $4, $5, {placeholders})",
        inline_table_name(tid),
    );
    let cells = cols
        .iter()
        .enumerate()
        .map(|(c, spec)| cell_from_arrow(batch, c, 0, &spec.ty))
        .collect::<Result<Vec<_>>>()?;
    let mut q = sqlx::query(AssertSqlSafe(sql))
        .bind(at.0)
        .bind(tombstone)
        .bind(change_kind)
        .bind(bucket)
        .bind(offset);
    for cell in &cells {
        q = bind_cell(q, cell);
    }
    q.execute(&mut *tx).await.map_err(backend)?;
    Ok(())
}

/// Arrow field for a logical column. Delegates to core's authoritative
/// `BaseType::arrow_data_type` map (canonical, non-`*View`, vector list child
/// `"item"`), so the inline read schema can never drift from the serving path.
fn arrow_field(name: &str, ty: BaseType, nullable: bool) -> Field {
    Field::new(name, ty.arrow_data_type(), nullable)
}

/// Build an arrow array for positional column `i` (typed `ty`) from PG rows.
///
/// THE single PG-row → Arrow decode: the inline read path (`inline_live_batch`)
/// and engine-serving's `PgTableProvider` both call this, so a new `BaseType`
/// member is a compile error here — never a silent "unsupported logical type"
/// at scan time (iss-pg-provider-vector-drift).
pub fn column_array(rows: &[sqlx::postgres::PgRow], i: usize, ty: BaseType) -> Result<ArrayRef> {
    macro_rules! get {
        ($ty:ty) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>(i))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(backend)?
        };
    }
    Ok(match ty {
        BaseType::Integer => {
            let mut b = Int32Builder::new();
            for v in get!(i32) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Long => {
            let mut b = Int64Builder::new();
            for v in get!(i64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Double => {
            let mut b = Float64Builder::new();
            for v in get!(f64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in get!(bool) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::String => {
            let mut b = StringBuilder::new();
            for v in get!(String) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Date => {
            let mut b = Date32Builder::new();
            let epoch = time::macros::date!(1970 - 01 - 01);
            for v in get!(time::Date) {
                b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
            }
            Arc::new(b.finish())
        }
        BaseType::Timestamp => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in get!(time::PrimitiveDateTime) {
                b.append_option(v.map(|t| {
                    (t.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX)
                }));
            }
            Arc::new(b.finish())
        }
        BaseType::Vector(_) => {
            use arrow_array::builder::{Float32Builder, ListBuilder};
            let item = Arc::new(control_plane_core::vector_list_field());
            let mut b = ListBuilder::new(Float32Builder::new()).with_field(item);
            for xs in get!(Vec<f32>) {
                match xs {
                    Some(xs) => {
                        b.values().append_slice(&xs);
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
    })
}

impl IcebergCatalog {
    /// The live inline rows of `table` at `at`, as (`table_id`, `loom_row_id`s,
    /// arrow batch), or `None` if there is no inline storage or no live rows.
    /// The `table_id` and row ids are returned so a flush can end-cap exactly the
    /// rows it reconstructs in the same `inline_<tid>` table. Shares the
    /// reconstruction the read path uses. Excludes `-U` before-images — they are
    /// audit-only rows that must never compete as an identity's current-state row
    /// (see `inline_live_batch_full` for the unfiltered variant).
    pub async fn inline_live_batch(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        self.inline_live_batch_impl(table, at, true).await
    }

    /// Identical to `inline_live_batch` except it does NOT exclude `-U`
    /// before-images, so the returned batch carries every live inline row
    /// (including `-U`). Used by the changelog flush, which needs the full
    /// change sequence rather than just the current-state view.
    pub async fn inline_live_batch_full(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        self.inline_live_batch_impl(table, at, false).await
    }

    /// EVERY live inline row of `table` at `at` — plain appends, row-versions
    /// (`+U`), and tombstones (`-D`); a non-CDC table never carries `-U`
    /// before-images, so no such exclusion is needed here (contrast
    /// `inline_live_batch`, which filters them for the current-state view). This
    /// is the INPUT to the slice-2 consolidation fold (`Precedence::Snapshot`,
    /// mirroring `build_inline_provider`'s merge-on-read pair): the batch projects
    /// the user columns in mirror order, then `begin_snapshot` (`Int64`,
    /// non-null), then `loom_tombstone` (`Boolean`, non-null) — the same physical
    /// names/order the engine-serving MVCC precedence reads
    /// (`serving.rs`'s `build_inline_provider`), so the fold and the live-read
    /// path can never drift apart on framing-column shape.
    ///
    /// Returns `None` when there is no inline storage or no live rows at `at`.
    /// Deliberately a SEPARATE method rather than a third mode on
    /// `inline_live_batch_impl` — a `-U`-exclusion bool AND a framing-column bool
    /// would multiply the shared body's branches for two reads with genuinely
    /// different projections (row count and column count both differ), which is
    /// worse for both clippy and readers than one clearly-named sibling.
    pub async fn inline_live_batch_shadow(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        let mut conn = self.pool.acquire().await.map_err(backend)?;
        let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
            return Ok(None);
        };
        if !inline_table_exists(&mut conn, tid).await? {
            return Ok(None);
        }

        // User columns from the mirror's logical schema. For a non-CDC table
        // `physical_columns` returns exactly the user columns (no stream framing
        // was ever registered), but resolve it via the same call `inline_live_batch`
        // uses, for symmetry — a future CDC-shadow read would need the same call.
        let columns = self.physical_columns(tid, at).await?;
        let col_list = columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        // loom_row_id(0), begin_snapshot(1), loom_tombstone(2), then user columns
        // (3..). No `-U` exclusion: every live row is in scope for the fold.
        let rows = sqlx::query(AssertSqlSafe(format!(
            "select loom_row_id, begin_snapshot, loom_tombstone, {col_list} from {} \
             where {} order by loom_row_id",
            inline_table_name(tid),
            mvcc_live_pred(at.0),
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Ok(None);
        }

        let row_ids: Vec<i64> = rows
            .iter()
            .map(|r| r.try_get::<i64, _>("loom_row_id").map_err(backend))
            .collect::<Result<Vec<_>>>()?;

        // Resolve every user column's logical type ONCE, same failure text as the
        // sibling reads.
        let types: Vec<BaseType> = columns
            .iter()
            .map(|c| {
                resolve_logical(&c.ty).ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("inline shadow read: unsupported type {:?}", c.ty).into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Fields: user columns in mirror order, then the two framing columns.
        let mut fields: Vec<Field> = columns
            .iter()
            .zip(&types)
            .map(|(c, ty)| arrow_field(&c.name, *ty, c.nullable))
            .collect();
        fields.push(Field::new(
            "begin_snapshot",
            BaseType::Long.arrow_data_type(),
            false,
        ));
        fields.push(Field::new(
            "loom_tombstone",
            BaseType::Boolean.arrow_data_type(),
            false,
        ));

        // User arrays via the shared decode (data columns start at select index 3);
        // the two framing arrays are built directly since they are non-nullable
        // scalars pulled straight from `begin_snapshot`/`loom_tombstone`.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, ty) in types.iter().enumerate() {
            arrays.push(column_array(&rows, i + 3, *ty)?);
        }
        let mut begin_snapshot = Int64Builder::with_capacity(rows.len());
        let mut loom_tombstone = BooleanBuilder::with_capacity(rows.len());
        for r in &rows {
            begin_snapshot.append_value(r.try_get::<i64, _>("begin_snapshot").map_err(backend)?);
            loom_tombstone.append_value(r.try_get::<bool, _>("loom_tombstone").map_err(backend)?);
        }
        arrays.push(Arc::new(begin_snapshot.finish()));
        arrays.push(Arc::new(loom_tombstone.finish()));

        let arrow_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema, arrays).map_err(backend)?;
        Ok(Some((tid, row_ids, batch)))
    }

    /// Shared body of `inline_live_batch`/`inline_live_batch_full`; the only
    /// difference between the two is whether `-U` before-images are excluded.
    async fn inline_live_batch_impl(
        &self,
        table: &TableRef,
        at: SnapshotId,
        exclude_minus_u: bool,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        let mut conn = self.pool.acquire().await.map_err(backend)?;
        let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
            return Ok(None);
        };
        if !inline_table_exists(&mut conn, tid).await? {
            return Ok(None);
        }

        // The PHYSICAL (unfiltered) column read, not the logical `schema()` — for a
        // stream table this also carries the reserved `loom_change_kind`/`loom_bucket`/
        // `loom_offset` framing columns (registered in the mirror at stream
        // declaration; real physical columns of `inline_<tid>` since Plan 1a), so the
        // flush path's SELECT and Arrow schema carry them into the written Parquet.
        // For a batch table `physical_columns` returns exactly the user columns (no
        // framing was ever registered), so this SELECT is byte-identical to before.
        let columns = self.physical_columns(tid, at).await?;
        let col_list = columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let minus_u_pred = if exclude_minus_u {
            " and (loom_change_kind is null or loom_change_kind <> '-U')"
        } else {
            ""
        };
        let rows = sqlx::query(AssertSqlSafe(format!(
            "select loom_row_id, {col_list} from {} where {}{minus_u_pred} order by loom_row_id",
            inline_table_name(tid),
            mvcc_live_pred(at.0),
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Ok(None);
        }

        let row_ids: Vec<i64> = rows
            .iter()
            .map(|r| r.try_get::<i64, _>("loom_row_id").map_err(backend))
            .collect::<Result<Vec<_>>>()?;

        // Resolve every column's logical type ONCE (the mirror should never hold
        // an unrecognized one) — same failure text the per-column arms used to emit.
        let types: Vec<BaseType> = columns
            .iter()
            .map(|c| {
                resolve_logical(&c.ty).ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("inline read: unsupported type {:?}", c.ty).into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build arrow arrays per column. `column_array` indexes positional columns;
        // the data columns start at index 1 (loom_row_id is column 0), so pass `i + 1`.
        let fields: Vec<Field> = columns
            .iter()
            .zip(&types)
            .map(|(c, ty)| arrow_field(&c.name, *ty, c.nullable))
            .collect();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, ty) in types.iter().enumerate() {
            arrays.push(column_array(&rows, i + 1, *ty)?);
        }
        let arrow_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema, arrays).map_err(backend)?;
        Ok(Some((tid, row_ids, batch)))
    }
}

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
    ProjectedColumn, arm_inline_trigger, bump_inline_trigger, ensure_table, live_columns,
    live_table_id, next_snapshot, project_columns,
};
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};
use crate::iceberg_type::{logical_from_iceberg, mirror_column_type, pg_type_for};
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
pub(crate) async fn end_cap_live_inline_rows(
    conn: &mut PgConnection,
    table_id: i64,
    at: control_plane_core::SnapshotId,
) -> control_plane_core::Result<()> {
    // to_regclass returns NULL for a non-existent relation -> skip.
    if !inline_table_exists(conn, table_id).await? {
        return Ok(());
    }
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
pub(crate) async fn end_cap_inline_rows_by_id(
    conn: &mut PgConnection,
    table_id: i64,
    row_ids: &[i64],
    at: SnapshotId,
) -> Result<()> {
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
/// `duplicate_table` (42P07), `duplicate_column` (42701), or a `unique_violation`
/// (23505) on the system catalog when two sessions create the same relation at
/// once. `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` are NOT
/// concurrency-safe in Postgres — the existence check and the catalog insert are
/// not atomic against a concurrent creator — so this is a benign lost race: the
/// object now exists.
fn is_duplicate_object_race(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| matches!(code.as_ref(), "42P07" | "42701" | "23505"))
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
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    stream_buckets: Option<i32>,
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Transaction derefs to PgConnection; the helpers take `&mut PgConnection`.
    let conn: &mut PgConnection = &mut tx;

    // Detect whether the table already existed (for the batch->stream conversion
    // guard below) BEFORE `ensure_table` creates the mirror row.
    let pre_existing = live_table_id(&mut *conn, &table.schema, &table.name)
        .await?
        .is_some();

    // 1. Snapshot (no Iceberg backing) + ensure mirror table/columns exist.
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    let pcols = columns
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

    // Reconcile stream mode. `effective` = Some(bucket_count) iff this table is a
    // (now-)declared log table; None => batch table (no offset stamping).
    let existing = crate::stream::pg_stream_bucket_count(&mut *conn, tid).await?;
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
            crate::stream::pg_declare_stream(&mut *conn, tid, n).await?;
            Some(n)
        }
        (None, existing) => existing,
    };

    // 3. Insert each row with the new begin_snapshot. The statement text is
    //    loop-invariant — only the binds change per row.
    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(bc) = effective {
        // Per-row bucket = row_index % bc (v1 simplification; bc >= 1).
        let n = batch.num_rows();
        // Count rows per bucket and reserve a contiguous offset run per touched
        // bucket, up front, so the per-row loop only needs to hand out offsets
        // from an already-reserved cursor (panic-free bucket indexing: `Vec::get`/
        // `get_mut` instead of `[]`, even though `b` is always `< bc` by construction).
        let bc_usize = usize::try_from(bc).map_err(|e| {
            ControlPlaneError::Backend(format!("invalid stream bucket count {bc}: {e}").into())
        })?;
        let mut counts = vec![0i64; bc_usize];
        for row in 0..n {
            let b = row % bc_usize;
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
            let b = row % bc_usize;
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
        if st.live_bytes >= st.effective && !st.enqueued && !has_shadow(&mut *conn, tid).await? {
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

    crate::transforms::pg_fire_data_triggers(
        &mut *conn,
        std::slice::from_ref(table),
        Some(lineage.run_id.0),
    )
    .await?;

    tx.commit().await.map_err(backend)?;
    Ok(at)
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
         where \"{}\" = $1 and end_snapshot is null",
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
    reason = "the delta write's public contract carries table + id-batch + version-vs-tombstone + lineage + CAS witness; a params struct would only obscure the call sites"
)]
pub async fn write_inline_delta(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    id_column: &str,
    tombstone: bool,
    batch: &RecordBatch,
    lineage: LineageEvent,
    expected_version: i64,
) -> Result<SnapshotId> {
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

    // Insert one delta row. BOTH kinds carry the identity value so the merge-on-read
    // (partition by <id>) shadows/hides the file row for that id.
    if tombstone {
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
    } else {
        // Version: mirror inline_append's INSERT but prefix loom_tombstone=false.
        // The id column is one of <cols>, so the version row carries the id naturally.
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
    }

    set_has_shadow(&mut tx, tid).await?;
    pg_emit(&mut *tx, &lineage).await?;
    crate::transforms::pg_fire_data_triggers(
        &mut tx,
        std::slice::from_ref(table),
        Some(lineage.run_id.0),
    )
    .await?;
    tx.commit().await.map_err(backend)?;
    Ok(at)
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
    /// reconstruction the read path uses.
    pub async fn inline_live_batch(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        use control_plane_core::Catalog;
        let mut conn = self.pool.acquire().await.map_err(backend)?;
        let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
            return Ok(None);
        };
        if !inline_table_exists(&mut conn, tid).await? {
            return Ok(None);
        }

        let schema = self.schema(table, at).await?;
        let col_list = schema
            .columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = sqlx::query(AssertSqlSafe(format!(
            "select loom_row_id, {col_list} from {} where {} order by loom_row_id",
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
        let types: Vec<BaseType> = schema
            .columns
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
        let fields: Vec<Field> = schema
            .columns
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

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
use crate::iceberg_type::{mirror_column_type, pg_type_for};
use crate::lineage::pg_emit;

/// The Postgres name of a table's inline storage. `table_id` is an internal i64.
pub fn inline_table_name(table_id: i64) -> String {
    format!("iceberg_mirror.inline_{table_id}")
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
    let name = inline_table_name(table_id);
    // to_regclass returns NULL for a non-existent relation -> skip.
    let exists: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(&name)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))?;
    if exists.is_none() {
        return Ok(());
    }
    let sql = format!(
        "update {name} set end_snapshot = {} where end_snapshot is null",
        at.0
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))?;
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
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "select to_regclass('{}')::text",
        inline_table_name(tid)
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if exists.is_none() {
        return Ok(false);
    }
    let any: bool = sqlx::query_scalar(AssertSqlSafe(format!(
        "select exists(select 1 from {} \
         where begin_snapshot <= {} and (end_snapshot is null or end_snapshot > {}))",
        inline_table_name(tid),
        at.0,
        at.0,
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
        write!(cols, ", \"{}\" {}", c.name.replace('"', "\"\""), pg)
            .map_err(|e| ControlPlaneError::Backend(e.into()))?;
    }
    Ok(format!(
        "create table if not exists {} (\
           loom_row_id bigserial primary key, \
           begin_snapshot bigint not null, \
           end_snapshot bigint{cols})",
        inline_table_name(table_id),
    ))
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
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Transaction derefs to PgConnection; the helpers take `&mut PgConnection`.
    let conn: &mut PgConnection = &mut tx;

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
    sqlx::query(AssertSqlSafe(inline_ddl(tid, columns)?))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;

    // 3. Insert each row with the new begin_snapshot.
    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    for row in 0..batch.num_rows() {
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 2)) // $1 = begin_snapshot
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "insert into {} (begin_snapshot, {col_list}) values ($1, {placeholders})",
            inline_table_name(tid),
        );
        let cells = columns
            .iter()
            .enumerate()
            .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
            .collect::<Result<Vec<_>>>()?;
        let mut q = sqlx::query(AssertSqlSafe(sql)).bind(at.0);
        for cell in &cells {
            q = bind_cell(q, cell);
        }
        q.execute(&mut *conn).await.map_err(backend)?;
    }

    // 3b. Flush trigger: accrue this batch's live bytes; on crossing the
    // (per-table or global) threshold, enqueue one flush_table job, atomically
    // with the rows. `None` => triggering disabled (preserves prior behaviour).
    if let Some(threshold) = flush_threshold {
        let add = batch.get_array_memory_size() as i64;
        let st = bump_inline_trigger(&mut *conn, tid, add, threshold).await?;
        if st.live_bytes >= st.effective && !st.enqueued {
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
        let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "select to_regclass('{}')::text",
            inline_table_name(tid)
        )))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if exists.is_none() {
            return Ok(None);
        }

        let schema = self.schema(table, at).await?;
        let col_list = schema
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = sqlx::query(AssertSqlSafe(format!(
            "select loom_row_id, {col_list} from {} \
             where begin_snapshot <= {} and (end_snapshot is null or end_snapshot > {}) \
             order by loom_row_id",
            inline_table_name(tid),
            at.0,
            at.0,
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
        let batch = RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some((tid, row_ids, batch)))
    }
}

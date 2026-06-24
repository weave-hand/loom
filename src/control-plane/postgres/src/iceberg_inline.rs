//! loom-native inline writes for Iceberg tables. Small writes land as typed rows in
//! a per-table `iceberg_mirror.inline_<table_id>` table (created on the fly), NOT a
//! Parquet object — a mirror-only commit (snapshot + rows + lineage in one tx). The
//! serving engine unions them with the table's Parquet files at read time via
//! `IcebergCatalog::inline_parquet`. External Iceberg clients don't see inline rows
//! until a future flush. See the slice-A design doc.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use control_plane_core::{
    ColumnSpec, ControlPlaneError, LineageEvent, NewJob, Result, SnapshotId, TableRef,
};
use parquet57::arrow::ArrowWriter;
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
use crate::iceberg_type::{iceberg_physical_type, pg_type_for};
use crate::lineage::pg_emit;

/// The Postgres name of a table's inline storage. `table_id` is an internal i64.
pub fn inline_table_name(table_id: i64) -> String {
    format!("iceberg_mirror.inline_{table_id}")
}

/// One typed inline cell — the bridge between an arrow-57 array and a Postgres bind.
#[derive(Clone, Debug)]
pub(crate) enum Cell {
    I32(Option<i32>),
    I64(Option<i64>),
    F64(Option<f64>),
    Bool(Option<bool>),
    Str(Option<String>),
    Date(Option<time::Date>),
    Ts(Option<time::PrimitiveDateTime>),
}

/// Pull cell `(col, row)` out of an arrow-57 batch, typed per the logical column.
fn cell_from_arrow(batch: &RecordBatch, col: usize, row: usize, logical: &str) -> Result<Cell> {
    let a = batch.column(col);
    let null = a.is_null(row);
    macro_rules! dc {
        ($ty:ty) => {
            a.as_any()
                .downcast_ref::<$ty>()
                .expect("inline arrow downcast")
        };
    }
    Ok(match logical {
        "integer" => Cell::I32((!null).then(|| dc!(Int32Array).value(row))),
        "long" => Cell::I64((!null).then(|| dc!(Int64Array).value(row))),
        "double" => Cell::F64((!null).then(|| dc!(Float64Array).value(row))),
        "boolean" => Cell::Bool((!null).then(|| dc!(BooleanArray).value(row))),
        "string" => Cell::Str((!null).then(|| dc!(StringArray).value(row).to_string())),
        "date" => Cell::Date((!null).then(|| {
            time::macros::date!(1970 - 01 - 01)
                + time::Duration::days(dc!(Date32Array).value(row) as i64)
        })),
        "timestamp" => Cell::Ts((!null).then(|| {
            let micros = dc!(TimestampMicrosecondArray).value(row);
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            time::PrimitiveDateTime::new(odt.date(), odt.time())
        })),
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
        cols.push_str(&format!(", \"{}\" {}", c.name.replace('"', "\"\""), pg));
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
/// come from the landing's schema, so they agree by construction.
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
                iceberg_type: iceberg_physical_type(&c.ty)
                    .ok_or_else(|| {
                        ControlPlaneError::Backend(
                            format!("inline: no iceberg type for {:?}", c.ty).into(),
                        )
                    })?
                    .to_string(),
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

/// Arrow field for a logical column (canonical, non-`*View` so it matches the
/// file-Parquet side the slice-3 engine reads).
fn arrow_field(name: &str, logical: &str, nullable: bool) -> Result<Field> {
    let dt = match logical {
        "integer" => DataType::Int32,
        "long" => DataType::Int64,
        "double" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline read: unsupported type {other:?}").into(),
            ));
        }
    };
    Ok(Field::new(name, dt, nullable))
}

/// Build an arrow-57 array for column `i` (typed `logical`) from PG rows.
fn column_array(rows: &[sqlx::postgres::PgRow], i: usize, logical: &str) -> Result<ArrayRef> {
    macro_rules! get {
        ($ty:ty) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>(i))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(backend)?
        };
    }
    Ok(match logical {
        "integer" => {
            let mut b = Int32Builder::new();
            for v in get!(i32) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        "long" => {
            let mut b = Int64Builder::new();
            for v in get!(i64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        "double" => {
            let mut b = Float64Builder::new();
            for v in get!(f64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        "boolean" => {
            let mut b = BooleanBuilder::new();
            for v in get!(bool) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        "string" => {
            let mut b = StringBuilder::new();
            for v in get!(String) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        "date" => {
            let mut b = Date32Builder::new();
            let epoch = time::macros::date!(1970 - 01 - 01);
            for v in get!(time::Date) {
                b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
            }
            Arc::new(b.finish())
        }
        "timestamp" => {
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
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline read: unsupported type {other:?}").into(),
            ));
        }
    })
}

impl IcebergCatalog {
    /// The live inline rows of `table` at `at`, as (`table_id`, `loom_row_id`s,
    /// arrow-57 batch), or `None` if there is no inline storage or no live rows.
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

        // Build arrow arrays per column. `column_array` indexes positional columns;
        // the data columns now start at index 1 (loom_row_id is column 0), so pass
        // `i + 1`.
        let fields = schema
            .columns
            .iter()
            .map(|c| arrow_field(&c.name, &c.ty, c.nullable))
            .collect::<Result<Vec<_>>>()?;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, c) in schema.columns.iter().enumerate() {
            arrays.push(column_array(&rows, i + 1, &c.ty)?);
        }
        let arrow_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some((tid, row_ids, batch)))
    }

    /// Encode `table`'s live inline rows at `at` to Parquet bytes (one row group),
    /// or `None` if there is no inline storage or no live inline rows. The serving
    /// engine drops these into an in-memory object store and unions them with the
    /// table's `file://` Parquet.
    pub async fn inline_parquet(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<Vec<u8>>> {
        let Some((_tid, _row_ids, batch)) = self.inline_live_batch(table, at).await? else {
            return Ok(None);
        };
        let schema = batch.schema();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.write(&batch)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.close()
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some(buf))
    }
}

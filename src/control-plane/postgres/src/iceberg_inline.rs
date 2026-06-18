//! loom-native inline writes for Iceberg tables. Small writes land as typed rows in
//! a per-table `iceberg_mirror.inline_<table_id>` table (created on the fly), NOT a
//! Parquet object — a mirror-only commit (snapshot + rows + lineage in one tx). The
//! serving engine unions them with the table's Parquet files at read time via
//! `IcebergCatalog::inline_parquet`. External Iceberg clients don't see inline rows
//! until a future flush. See the slice-A design doc.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use control_plane_core::{
    ColumnSpec, ControlPlaneError, LineageEvent, Result, SnapshotId, TableRef,
};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres};

use crate::backend;
use crate::iceberg_mirror::{
    ProjectedColumn, columns_exist, ensure_table, next_snapshot, project_columns,
};
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
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Transaction derefs to PgConnection; the helpers take `&mut PgConnection`.
    let conn: &mut PgConnection = &mut tx;

    // 1. Snapshot (no Iceberg backing) + ensure mirror table/columns exist.
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    if !columns_exist(conn, tid).await? {
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
        project_columns(conn, tid, at, &pcols).await?;
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

    // 4. Lineage, atomic with the rows.
    pg_emit(&mut *conn, &lineage).await?;

    tx.commit().await.map_err(backend)?;
    Ok(at)
}

//! Projection of canonical Iceberg table metadata into the loom-owned `iceberg_mirror.*`
//! schema. loom allocates a catalog-global monotonic `SnapshotId` per appended batch and records
//! MVCC `begin/end_snapshot`. The seeder (and, later, the write path) builds the neutral
//! `Projected*` structs from the `iceberg` writer output and calls these functions; the read
//! adapter (`iceberg_catalog`) serves from the rows they write. Shared with the slice-2 write
//! path, which folds these into the catalog's `update_table` transaction.

use control_plane_core::{Result, SnapshotId};
use sqlx::PgConnection;

use crate::backend;

/// A neutral view of one committed Iceberg data file the projection writes.
pub struct ProjectedFile {
    pub path: String,
    /// On-storage format, e.g. `"parquet"`.
    pub file_format: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// A neutral column definition (name, Iceberg primitive type name, nullability), in order.
pub struct ProjectedColumn {
    pub order: i64,
    pub name: String,
    /// The Iceberg primitive type name (`"long"`, `"string"`, …) that `logical_from_iceberg`
    /// decodes on read.
    pub iceberg_type: String,
    pub nullable: bool,
}

/// Allocate the next catalog-global snapshot id and insert its `iceberg_mirror.snapshot` row.
/// `iceberg_snapshot_id` is the Iceberg-side snapshot id this loom snapshot projected from
/// (traceability only; loom's id is authoritative).
pub async fn next_snapshot(
    conn: &mut PgConnection,
    iceberg_snapshot_id: Option<i64>,
) -> Result<SnapshotId> {
    // Concurrency-safe: nextval is atomic and never reuses a value, so two concurrent
    // writers get distinct ids and neither collides on the snapshot PK (migration 0013).
    let id = sqlx::query_scalar!("select nextval('iceberg_mirror.snapshot_seq') as \"next!\"")
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    sqlx::query!(
        "insert into iceberg_mirror.snapshot (snapshot_id, iceberg_snapshot_id) values ($1, $2)",
        id,
        iceberg_snapshot_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(SnapshotId(id))
}

/// Ensure a live `iceberg_mirror.table` row exists for `(ns, name)`, returning its `table_id`.
/// Inserts a new row beginning at `at` if none is currently live.
pub async fn ensure_table(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
    at: SnapshotId,
) -> Result<i64> {
    if let Some(tid) = sqlx::query_scalar!(
        "select table_id as \"id!\" from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
        ns,
        name,
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?
    {
        return Ok(tid);
    }
    let tid = sqlx::query_scalar!(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3) returning table_id as \"id!\"",
        ns,
        name,
        at.0,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(tid)
}

/// Write the column rows for loom snapshot `at`.
pub async fn project_columns(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
    columns: &[ProjectedColumn],
) -> Result<()> {
    for c in columns {
        sqlx::query!(
            "insert into iceberg_mirror.column \
             (table_id, column_order, column_name, column_type, nulls_allowed, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6)",
            table_id,
            c.order,
            c.name,
            c.iceberg_type,
            c.nullable,
            at.0,
        )
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

/// Write the data-file rows for loom snapshot `at`.
pub async fn project_files(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
    files: &[ProjectedFile],
) -> Result<()> {
    for f in files {
        sqlx::query!(
            "insert into iceberg_mirror.data_file \
             (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6)",
            table_id,
            f.path,
            f.file_format,
            f.record_count,
            f.file_size_bytes,
            at.0,
        )
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

/// Mark a table (and its live columns/files) dropped at `at` — sets `end_snapshot = at` on every
/// currently-live row. Drives `CatalogSeed::drop_table` and the MVCC `end`-bound the delete
/// contract exercises.
pub async fn mark_dropped(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
    at: SnapshotId,
) -> Result<()> {
    let tid = sqlx::query_scalar!(
        "update iceberg_mirror.table set end_snapshot = $3 \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null \
         returning table_id as \"id!\"",
        ns,
        name,
        at.0,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    sqlx::query!(
        "update iceberg_mirror.column set end_snapshot = $2 where table_id = $1 and end_snapshot is null",
        tid,
        at.0,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    sqlx::query!(
        "update iceberg_mirror.data_file set end_snapshot = $2 where table_id = $1 and end_snapshot is null",
        tid,
        at.0,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

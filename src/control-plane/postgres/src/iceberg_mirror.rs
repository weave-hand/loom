//! Projection of canonical Iceberg table metadata into the loom-owned `iceberg_mirror.*`
//! schema. loom allocates a catalog-global monotonic `SnapshotId` per appended batch and records
//! MVCC `begin/end_snapshot`. The seeder (and, later, the write path) builds the neutral
//! `Projected*` structs from the `iceberg` writer output and calls these functions; the read
//! adapter (`iceberg_catalog`) serves from the rows they write. Shared with the slice-2 write
//! path, which folds these into the catalog's `update_table` transaction.

use control_plane_core::{ControlPlaneError, Result, SnapshotId};
use iceberg::table::Table;
use sqlx::PgConnection;

use crate::backend;

/// Map an `iceberg` error into the control-plane backend error.
fn iceberg_err(e: iceberg::Error) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

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

/// The `table_id` of the currently-live mirror row for `(ns, name)`, or `None` if the table has
/// no live mirror state (e.g. created via the catalog but never appended — `create_table` writes
/// the Iceberg pointer but does not project the mirror). Lets a drop skip the mirror leg (and its
/// snapshot allocation) when there is nothing to end.
pub async fn live_table_id(conn: &mut PgConnection, ns: &str, name: &str) -> Result<Option<i64>> {
    sqlx::query_scalar!(
        "select table_id as \"id!\" from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
        ns,
        name,
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)
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

/// Build `ProjectedColumn`s from an Iceberg table's current schema (in-memory).
/// Columns are emitted in schema order; only primitive types are supported (the
/// only types loom's ontology maps — see `iceberg_type`).
pub fn columns_of(table: &Table) -> Vec<ProjectedColumn> {
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| ProjectedColumn {
            order: (i + 1) as i64,
            name: field.name.clone(),
            iceberg_type: match field.field_type.as_ref() {
                iceberg::spec::Type::Primitive(p) => p.to_string(),
                other => panic!("project: non-primitive column type {other:?}"),
            },
            nullable: !field.required,
        })
        .collect()
}

/// Read the data files the table's current snapshot ADDED, as neutral
/// `ProjectedFile`s, by loading that snapshot's manifests via the table's FileIO.
/// Returns empty if the table has no current snapshot (e.g. a create with no
/// append). Only entries stamped with the current snapshot id are taken, so an
/// append projects exactly its new files (existing files stay live in the mirror).
pub async fn added_files_of(table: &Table) -> Result<Vec<ProjectedFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .map_err(iceberg_err)?;
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if entry.snapshot_id() == Some(snapshot.snapshot_id()) {
                let df = entry.data_file();
                files.push(ProjectedFile {
                    path: df.file_path().to_string(),
                    file_format: "parquet".to_string(),
                    record_count: df.record_count() as i64,
                    file_size_bytes: df.file_size_in_bytes() as i64,
                });
            }
        }
    }
    Ok(files)
}

/// True if the mirror already has any column row for `table_id` — used to project
/// columns exactly once per table lifetime (slice 2 has no schema evolution).
pub async fn columns_exist(conn: &mut PgConnection, table_id: i64) -> Result<bool> {
    let exists = sqlx::query_scalar!(
        "select exists(select 1 from iceberg_mirror.column where table_id = $1) as \"e!\"",
        table_id,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(exists)
}

/// The trigger row after a bump: the new running total, the effective threshold
/// (per-table override or the passed global default), and whether a flush job is
/// already pending for this table.
#[derive(Debug, Clone, Copy)]
pub struct TriggerState {
    pub live_bytes: i64,
    pub effective: i64,
    pub enqueued: bool,
}

/// Add `add_bytes` to the table's live-inline-bytes counter (creating the row on
/// first write), returning the post-bump state plus the effective threshold
/// (`COALESCE(threshold, global_threshold)`). Idempotent row creation via upsert.
pub async fn bump_inline_trigger(
    conn: &mut PgConnection,
    table_id: i64,
    add_bytes: i64,
    global_threshold: i64,
) -> Result<TriggerState> {
    let row = sqlx::query!(
        "insert into iceberg_mirror.inline_trigger (table_id, live_bytes) \
         values ($1, $2) \
         on conflict (table_id) do update \
           set live_bytes = iceberg_mirror.inline_trigger.live_bytes + excluded.live_bytes \
         returning live_bytes as \"live_bytes!\", \
                   coalesce(threshold, $3) as \"effective!\", \
                   enqueued as \"enqueued!\"",
        table_id,
        add_bytes,
        global_threshold,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(TriggerState {
        live_bytes: row.live_bytes,
        effective: row.effective,
        enqueued: row.enqueued,
    })
}

/// Mark the table's trigger as having a pending flush job (debounce). No-op if the
/// row is absent (it is always created by a preceding `bump_inline_trigger`).
pub async fn arm_inline_trigger(conn: &mut PgConnection, table_id: i64) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.inline_trigger set enqueued = true where table_id = $1",
        table_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Reset the table's trigger after a flush: clear the counter and re-arm. No-op if
/// the row is absent.
pub async fn reset_inline_trigger(conn: &mut PgConnection, table_id: i64) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.inline_trigger \
         set live_bytes = 0, enqueued = false where table_id = $1",
        table_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

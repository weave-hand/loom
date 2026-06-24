//! Projection of canonical Iceberg table metadata into the loom-owned `iceberg_mirror.*`
//! schema. loom allocates a catalog-global monotonic `SnapshotId` per appended batch and records
//! MVCC `begin/end_snapshot`. The seeder (and, later, the write path) builds the neutral
//! `Projected*` structs from the `iceberg` writer output and calls these functions; the read
//! adapter (`iceberg_catalog`) serves from the rows they write. Shared with the slice-2 write
//! path, which folds these into the catalog's `update_table` transaction.

use control_plane_core::{ControlPlaneError, Result, SnapshotId, TableRef};
use iceberg::table::Table;
use sqlx::PgConnection;

use crate::backend;
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};

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
    /// Per-column min/max/null-count merged from the file's Parquet footer, in
    /// table schema order. Persisted alongside the data-file row by `project_files`.
    pub column_stats: Vec<control_plane_core::ColumnStat>,
}

/// A neutral column definition (name, Iceberg primitive type name, nullability), in order.
#[derive(Debug, Clone, PartialEq)]
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
        let data_file_id = sqlx::query_scalar!(
            "insert into iceberg_mirror.data_file \
             (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6) returning data_file_id as \"id!\"",
            table_id,
            f.path,
            f.file_format,
            f.record_count,
            f.file_size_bytes,
            at.0,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;

        // Per-column footer stats, in the same transaction as the data-file row so
        // a written file always carries its stats (no second pass to backfill).
        for s in &f.column_stats {
            let min = s.min.as_ref().map(crate::iceberg_stats::stat_to_text);
            let max = s.max.as_ref().map(crate::iceberg_stats::stat_to_text);
            sqlx::query!(
                "insert into iceberg_mirror.data_file_column_stat \
                 (data_file_id, column_name, null_count, column_size_bytes, min_value, max_value) \
                 values ($1, $2, $3, $4, $5, $6)",
                data_file_id,
                s.column_name,
                s.null_count,
                s.column_size_bytes,
                min,
                max,
            )
            .execute(&mut *conn)
            .await
            .map_err(backend)?;
        }
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

/// End-cap (set `end_snapshot = at`) every currently-live `iceberg_mirror.data_file`
/// row for `table_id`, leaving its `table`/`column` rows untouched. This is the
/// data-file leg of a drop ([`mark_dropped`]) and the whole "expire old files" step
/// of an overwrite/replace (the Iceberg twin of DuckLake's `Tx::replace_files`).
/// Old rows keep their `begin_snapshot < at`, so prior snapshots still time-travel.
pub async fn end_cap_live_data_files(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.data_file set end_snapshot = $2 where table_id = $1 and end_snapshot is null",
        table_id,
        at.0,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
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
    end_cap_live_data_files(conn, tid, at).await?;
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
                // A `list<float>` field is a loom vector column. The dimension `N` is
                // stashed in the field doc as `vector(N)` (Iceberg lists are
                // length-free), which the read decoder parses back to `BaseType::Vector`.
                iceberg::spec::Type::List(_) => field
                    .doc
                    .clone()
                    .filter(|d| d.starts_with("vector("))
                    .unwrap_or_else(|| {
                        panic!("project: list column {} lacks a vector(N) doc", field.name)
                    }),
                other => panic!("project: unsupported column type {other:?}"),
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
    // Column names in table schema order — the same order Iceberg writes columns to
    // the Parquet file, so `column_stats_from_parquet` records each by name.
    let names: Vec<String> = columns_of(table).into_iter().map(|c| c.name).collect();
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if entry.snapshot_id() == Some(snapshot.snapshot_id()) {
                let df = entry.data_file();
                // Read the file's bytes and merge per-column Parquet-footer stats in the
                // same projection that records the file row (atomic with the snapshot commit).
                let bytes = table
                    .file_io()
                    .new_input(df.file_path())
                    .map_err(iceberg_err)?
                    .read()
                    .await
                    .map_err(iceberg_err)?;
                let column_stats = crate::iceberg_stats::column_stats_from_parquet(bytes, &names)?;
                files.push(ProjectedFile {
                    path: df.file_path().to_string(),
                    file_format: "parquet".to_string(),
                    record_count: df.record_count() as i64,
                    file_size_bytes: df.file_size_in_bytes() as i64,
                    column_stats,
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

/// Reset the table's trigger after a flush: clear the counter and disarm
/// (`enqueued = false`), so the next accumulation can re-trigger. No-op if the
/// row is absent.
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

/// Return the columns that were live at snapshot `at` for `table_id`, in column order.
///
/// Uses the same MVCC predicate as `IcebergCatalog::schema` (`begin_snapshot <= $2`)
/// so the two views are byte-for-byte consistent. Callers pass the snapshot to read
/// as-of directly; no `at + 1` arithmetic is performed anywhere.
pub async fn live_columns(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
) -> Result<Vec<ProjectedColumn>> {
    let rows = sqlx::query!(
        "select column_order as \"column_order!\", column_name as \"column_name!\", \
               column_type as \"column_type!\", nulls_allowed as \"nulls_allowed!\" \
         from iceberg_mirror.column \
         where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
         order by column_order",
        table_id,
        at.0,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(rows
        .into_iter()
        .map(|r| ProjectedColumn {
            order: r.column_order,
            name: r.column_name,
            iceberg_type: r.column_type,
            nullable: r.nulls_allowed,
        })
        .collect())
}

/// Return the live columns for the named table at `at`, resolving the `table_id` by
/// looking up the currently-live `iceberg_mirror.table` row.
///
/// Returns `Vec::new()` if no live table row exists, which causes
/// `reconcile_and_project` to treat the incoming write as a table creation.
pub async fn live_columns_for(
    conn: &mut PgConnection,
    table: &TableRef,
    at: SnapshotId,
) -> Result<Vec<ProjectedColumn>> {
    let tid: Option<i64> = sqlx::query_scalar!(
        "select table_id as \"id!\" from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
        table.schema,
        table.name,
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?;
    match tid {
        Some(tid) => live_columns(conn, tid, at).await,
        None => Ok(Vec::new()),
    }
}

/// Stamp the `schema_version` on an `iceberg_mirror.snapshot` row.
///
/// The per-table schema generation is defined as the count of distinct
/// `begin_snapshot` values visible at `at` in `iceberg_mirror.column`. Creation
/// yields generation 1; each additive evolution increments it; a no-change append
/// leaves it unchanged.
pub async fn stamp_schema_version(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.snapshot set schema_version = (\
             select count(distinct begin_snapshot) from iceberg_mirror.column \
             where table_id = $1 and begin_snapshot <= $2\
         ) where snapshot_id = $2",
        table_id,
        at.0,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Shared policy entry point: compare the incoming column set against the live mirror
/// and project only the delta (additive) or all columns (creation).
///
/// - If `live_columns` is empty (first write for this table), all `incoming` columns
///   are projected regardless of nullability — the landing validator has already
///   accepted them.
/// - If `Identical`, nothing is written.
/// - If `Additive`, only the new nullable columns are projected.
/// - Any other change (drop, rename, reorder, type change) is rejected with
///   `ControlPlaneError::Validation`.
pub async fn reconcile_and_project(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
    incoming: &[ProjectedColumn],
) -> Result<()> {
    let live = live_columns(conn, table_id, at).await?;
    if live.is_empty() {
        // First write (table creation): project all columns regardless of nullability.
        project_columns(conn, table_id, at, incoming).await?;
        return Ok(());
    }
    match classify_schema_change(&live, incoming)
        .map_err(|e| ControlPlaneError::Validation(e.to_string()))?
    {
        SchemaPlan::Identical => {}
        SchemaPlan::Additive { new_columns } => {
            project_columns(conn, table_id, at, &new_columns).await?;
        }
    }
    Ok(())
}

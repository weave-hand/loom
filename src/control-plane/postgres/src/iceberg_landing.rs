//! The Iceberg landing entrypoint: decode an Arrow IPC body (arrow-57), route by
//! in-memory size between an inline (mirror-only) write and a real Parquet write,
//! and return the loom mirror snapshot id. Both branches emit lineage atomically.
//!
//! This lives in the postgres crate (not ingest) because the iceberg writer chain
//! is arrow-57 and the ingest crate is arrow-58 — the ingest `IcebergMaterializer`
//! forwards the raw IPC body so the cross-major boundary stays inside this crate.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc57::reader::StreamReader;
use arrow_schema::Schema;
use arrow_select57::concat::concat_batches;
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, FileFormat, LineageEvent, Result, SnapshotId,
    TableRef,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog as IceCatalog, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::inline_append;
use crate::iceberg_mirror::{
    ProjectedColumn, ProjectedFile, columns_exist, end_cap_live_data_files, ensure_table,
    project_columns, project_files,
};
use crate::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};
use crate::iceberg_type::iceberg_physical_type;
use crate::iceberg_writer::append_batches_with_extras;

/// Boxing helper: wrap any boxable error as a control-plane `Backend` fault.
fn be<E: std::error::Error + Send + Sync + 'static>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

/// Decode an Arrow IPC stream body into its (arrow-57) schema + batches.
fn decode_ipc_57(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(body), None).map_err(be)?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(be)?;
    Ok((schema, batches))
}

/// Land an Iceberg request. `inline_byte_limit` is the in-memory (uncompressed)
/// Arrow size at/below which the request inlines (mirror-only typed rows) instead
/// of writing real Parquet. `flush_byte_threshold` is the live-inline-byte total
/// at/above which a `flush_table` job is enqueued after an inline write. Returns
/// the loom mirror snapshot id either way.
#[allow(clippy::too_many_arguments)]
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let (schema, batches) = decode_ipc_57(ipc_body)?;
    // Project the decoded columns to `columns` order, by name. Both downstream
    // branches align columns POSITIONALLY (inline indexes `columns[c]` against
    // batch column `c`; the Parquet branch re-wraps under the table's schema in
    // `columns` order), but on the model-gate path `columns` is the model's
    // declared order, which need not match the wire order — so without this
    // realignment, same-typed reordered columns would silently swap values.
    let (schema, batches) = align_to_columns(&schema, batches, columns)?;
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(be)?;
        inline_append(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(flush_byte_threshold),
        )
        .await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage).await
    }
}

/// Project `batches` to `columns` order, matching by name, preserving the wire
/// arrow types. Extra wire columns not named in `columns` are dropped (the model
/// defines the landed schema). Errors if a declared column is absent from the data.
/// Fast-paths the common inferred case (`columns` already in wire order) to a
/// no-op clone, so that path is unchanged.
fn align_to_columns(
    schema: &Schema,
    batches: Vec<RecordBatch>,
    columns: &[ColumnSpec],
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let already_aligned = columns.len() == schema.fields().len()
        && columns
            .iter()
            .zip(schema.fields())
            .all(|(c, f)| c.name == *f.name());
    if already_aligned {
        return Ok((Arc::new(schema.clone()), batches));
    }

    let indices = columns
        .iter()
        .map(|c| {
            schema.index_of(&c.name).map_err(|_| {
                ControlPlaneError::Backend(
                    format!(
                        "landing: column {:?} declared but absent from the data",
                        c.name
                    )
                    .into(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
    let projected = Arc::new(Schema::new(fields));
    let batches = batches
        .into_iter()
        .map(|b| {
            let cols: Vec<_> = indices.iter().map(|&i| b.column(i).clone()).collect();
            RecordBatch::try_new(projected.clone(), cols).map_err(be)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((projected, batches))
}

/// Ensure the iceberg table exists (create-if-absent from `columns`), append
/// `batches` (bare arrow-57 — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
    overwrite: bool,
) -> Result<SnapshotId> {
    ensure_iceberg_table(catalog, table, columns).await?;
    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let ice_table = catalog.load_table(&ident).await.map_err(be)?;

    // A decoded IPC body carries a bare arrow schema; the iceberg writer chain needs
    // the table's arrow schema (which carries the iceberg field-id metadata) or it
    // can't map columns to field ids. Re-wrap each batch's columns under that
    // field-id-bearing schema. Positional alignment is safe here because `batches`
    // were already projected to `columns` order (= the table schema order) by
    // `align_to_columns` upstream.
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema())
            .map_err(be)?,
    );
    let batches = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(ice_arrow.clone(), b.columns().to_vec()).map_err(be))
        .collect::<Result<Vec<_>>>()?;

    append_batches_with_extras(catalog, &ice_table, batches, lineage, end_cap, overwrite)
        .await
        .map_err(be)?;

    Ok(IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await?
        .id)
}

/// Create the Iceberg namespace + table for `table` if absent (idempotent), from
/// `columns`. Writes only the Iceberg catalog pointer — it does NOT project the
/// mirror (the mirror is projected when files are registered/appended). Shared by
/// the landing Parquet path and the transform [`register_files`] path.
pub(crate) async fn ensure_iceberg_table(
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
) -> Result<()> {
    let ns = NamespaceIdent::new(table.schema.clone());
    if !catalog.namespace_exists(&ns).await.map_err(be)? {
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .map_err(be)?;
    }
    let ident = TableIdent::new(ns.clone(), table.name.clone());
    if !catalog.table_exists(&ident).await.map_err(be)? {
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(columns)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(be)?;
    }
    Ok(())
}

/// How [`register_files`] folds new files into the table's live set.
#[derive(Clone, Copy)]
pub enum WriteMode {
    /// Add `files` to the currently-live set.
    Append,
    /// End-cap every currently-live data file at the new snapshot first, so `files`
    /// become the sole live set (prior files still time-travel). The
    /// `road-iceberg-overwrite-mode` contract, over already-written files.
    Overwrite,
}

/// Register already-written Parquet `files` into the `iceberg_mirror.*` projection
/// for `table` at snapshot `at`, in the caller's transaction `conn`. The caller
/// allocated `at` (via [`crate::iceberg_mirror::next_snapshot`]) and owns commit.
///
/// **Mirror-only** — this drives NO Iceberg `fast_append`. loom-governed reads
/// resolve entirely through `iceberg_mirror.*` ([`IcebergCatalog`]), so projecting
/// the mirror rows makes the files readable; the raw Iceberg metadata not referencing
/// them is the accepted gap class of `iss-iceberg-inline-visibility` (and is moot
/// here — the transform worker's arrow-58 Parquet lacks the iceberg field-id footer
/// metadata an external reader would need). The loom `DataFile`s already carry their
/// per-column stats, so the mirror rows are a direct field map with no footer re-read.
pub async fn register_files(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
    columns: &[ColumnSpec],
    files: &[DataFile],
    mode: WriteMode,
    at: SnapshotId,
) -> Result<()> {
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    // Overwrite: end-cap pre-existing live files BEFORE projecting the new ones, so
    // only the prior files are retired (same ordering law as `write_mirror`).
    if let WriteMode::Overwrite = mode {
        end_cap_live_data_files(conn, tid, at).await?;
    }
    if !columns_exist(conn, tid).await? {
        project_columns(conn, tid, at, &projected_columns(columns)?).await?;
    }
    project_files(conn, tid, at, &projected_files(files)?).await?;
    Ok(())
}

/// Map loom `ColumnSpec`s to mirror `ProjectedColumn`s, storing the Iceberg primitive
/// type name (`iceberg_physical_type`) — the exact `column_type` the normal write path
/// records (via `columns_of`), so reads decode identically (`logical_from_iceberg`).
fn projected_columns(columns: &[ColumnSpec]) -> Result<Vec<ProjectedColumn>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let iceberg_type = iceberg_physical_type(&c.ty).ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!("register: no iceberg type for {:?}", c.ty).into(),
                )
            })?;
            Ok(ProjectedColumn {
                order: (i + 1) as i64,
                name: c.name.clone(),
                iceberg_type: iceberg_type.to_string(),
                nullable: c.nullable,
            })
        })
        .collect()
}

/// Map already-written loom `DataFile`s to mirror `ProjectedFile`s — a direct field
/// copy (the `DataFile` already carries path/counts/size and per-column stats).
fn projected_files(files: &[DataFile]) -> Result<Vec<ProjectedFile>> {
    files
        .iter()
        .map(|f| {
            if f.file_format != FileFormat::Parquet {
                return Err(ControlPlaneError::Backend(
                    format!(
                        "register: only Parquet files supported, got {:?}",
                        f.file_format
                    )
                    .into(),
                ));
            }
            Ok(ProjectedFile {
                path: f.path.clone(),
                file_format: "parquet".to_string(),
                record_count: f.record_count,
                file_size_bytes: f.file_size_bytes,
                column_stats: f.column_stats.clone(),
            })
        })
        .collect()
}

/// The Parquet branch: ensure the namespace + table exist, then append real
/// Parquet with an atomic lineage emit, and read the resulting mirror snapshot id
/// back. Idempotent on namespace/table (create-if-absent). Delegates to
/// [`append_parquet_snapshot`].
async fn land_parquet(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        Some(&lineage),
        None,
        false,
    )
    .await
}

/// Replace `table`'s live data with `batches` in one Postgres transaction — the
/// Iceberg twin of DuckLake's `Tx::replace_files`. End-caps every currently-live
/// `iceberg_mirror.data_file` row at the new snapshot, projects the new files (with
/// per-column stats), appends them on the Iceberg side (`fast_append`), and emits
/// `lineage` atomically; prior files stay reachable by time travel. The insert side
/// mirrors [`append_parquet_snapshot`] — only the live-file end-cap differs.
///
/// Overwrite semantics live entirely in the mirror end-cap (loom-governed reads
/// resolve through the mirror); the raw Iceberg metadata still references the
/// replaced files until GC — the accepted gap class of `iss-iceberg-inline-visibility`.
///
/// A **zero-file** overwrite is a truncation: the Iceberg writer cannot produce a
/// snapshot from an empty file set, so this takes a mirror-only branch that allocates
/// a snapshot, end-caps all live files, and emits lineage — making the empty set the
/// sole live set while preserving time travel.
pub async fn overwrite_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
) -> Result<SnapshotId> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return overwrite_truncate(pool, table, lineage).await;
    }
    append_parquet_snapshot(pool, catalog, table, columns, batches, lineage, None, true).await
}

/// The zero-file branch of [`overwrite_parquet_snapshot`]: in one Postgres tx allocate
/// a mirror snapshot, ensure the table row, end-cap every live data file at that
/// snapshot, and emit `lineage`. Touches no object storage and no Iceberg metadata —
/// loom reads resolve `current_snapshot` through `iceberg_mirror.snapshot`, so the
/// truncation is immediately visible and prior snapshots still time-travel.
async fn overwrite_truncate(
    pool: &PgPool,
    table: &TableRef,
    lineage: Option<&LineageEvent>,
) -> Result<SnapshotId> {
    use crate::iceberg_mirror::{end_cap_live_data_files, ensure_table, next_snapshot};
    use crate::lineage::pg_emit;

    let mut tx = pool.begin().await.map_err(be)?;
    let conn = &mut *tx;
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    end_cap_live_data_files(conn, tid, at).await?;
    if let Some(ev) = lineage {
        pg_emit(&mut *conn, ev).await?;
    }
    tx.commit().await.map_err(be)?;
    Ok(at)
}

/// Build an iceberg `Schema` from loom `ColumnSpec`s, assigning 1-based field ids.
fn ice_schema(columns: &[ColumnSpec]) -> Result<IceSchema> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let ty = Type::Primitive(primitive_from(&c.ty)?);
            let id = (i + 1) as i32;
            Ok(Arc::new(if c.nullable {
                NestedField::optional(id, &c.name, ty)
            } else {
                NestedField::required(id, &c.name, ty)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    IceSchema::builder().with_fields(fields).build().map_err(be)
}

/// loom logical type name -> iceberg `PrimitiveType` (the physical mapping reuses
/// `iceberg_physical_type` so the closed vocabulary stays in one place).
fn primitive_from(logical: &str) -> Result<PrimitiveType> {
    let phys = iceberg_physical_type(logical).ok_or_else(|| {
        ControlPlaneError::Backend(format!("landing: no iceberg type for {logical:?}").into())
    })?;
    Ok(match phys {
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "double" => PrimitiveType::Double,
        "boolean" => PrimitiveType::Boolean,
        "string" => PrimitiveType::String,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamp,
        other => {
            return Err(ControlPlaneError::Backend(
                format!("landing: unsupported iceberg primitive {other:?}").into(),
            ));
        }
    })
}

//! The Iceberg landing entrypoint: decode an Arrow IPC body, route by
//! in-memory size between an inline (mirror-only) write and a real Parquet write,
//! and return the loom mirror snapshot id. Both branches emit lineage atomically.
//!
//! This lives in the postgres crate (not ingest) because it owns the iceberg
//! writer chain and the mirror projection; the ingest `IcebergMaterializer`
//! forwards the raw IPC body here. (Historically this crate was arrow-57 while
//! ingest was arrow-58; the arrow-58 converge removed that split — the whole tree
//! now shares one arrow major — but the landing path stays here by ownership.)

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Schema};
use arrow_select::concat::concat_batches;
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, FileFormat, LineageEvent, Result, SnapshotId,
    TableRef,
};
use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog as IceCatalog, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::inline_append;
use crate::iceberg_mirror::{
    ProjectedColumn, ProjectedFile, end_cap_files_by_path, end_cap_live_data_files, ensure_table,
    live_columns_for, next_snapshot, project_files, reconcile_and_project, stamp_schema_version,
};
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};
use crate::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};
use crate::iceberg_type::iceberg_physical_type;
use crate::iceberg_writer::append_batches_with_extras;

/// Boxing helper: wrap any boxable error as a control-plane `Backend` fault.
fn be<E: std::error::Error + Send + Sync + 'static>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

/// Decode an Arrow IPC stream body into its arrow schema + batches.
fn decode_ipc(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
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
#[allow(clippy::too_many_arguments, reason = "iceberg landing functions have many required parameters with no sensible grouping")]
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
    let (schema, batches) = decode_ipc(ipc_body)?;
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
            schema.index_of(&c.name).map_err(|e| {
                ControlPlaneError::Backend(
                    format!(
                        "landing: column {:?} declared but absent from the data: {e}",
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
/// `batches` (bare arrow — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
#[allow(clippy::too_many_arguments, reason = "iceberg landing functions have many required parameters with no sensible grouping")]
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

    // Decide identical/create vs additive vs reject against the live mirror. We need a
    // snapshot to read live columns "as of"; live columns have begin_snapshot <= the
    // current snapshot, so the current snapshot is the read point — or an empty set if
    // there is no snapshot yet (a fresh creation).
    let incoming = projected_columns(columns)?;
    let icb = IcebergCatalog::new(pool.clone());
    let (live, current) = match icb.current_snapshot(table).await {
        Ok(snap) => {
            let mut conn = pool.acquire().await.map_err(be)?;
            // `live_columns_for` resolves the tid internally and reads columns live at
            // `snap.id` (its predicate is `begin_snapshot <= $2`, no +1).
            (
                live_columns_for(&mut conn, table, snap.id).await?,
                Some(snap.id),
            )
        }
        Err(_) => (Vec::new(), None), // no snapshot yet — creation
    };
    if !live.is_empty() {
        match classify_schema_change(&live, &incoming) {
            // Identical → fall through to the existing fast_append path below.
            Ok(SchemaPlan::Identical) => {}
            // Additive → mirror-only superset Parquet + reconcile/project. NO fast_append.
            Ok(SchemaPlan::Additive { .. }) => {
                // Guard the cross-task seam: an additive land projects a new column into the
                // mirror, but the physical `inline_<tid>` table is NOT altered. If live inline
                // rows exist, inline reconstruction (read AND flush) would then select a column
                // the table lacks and fail. Refuse before writing any Parquet — the caller must
                // let the flush job run first, so mirror and inline table never diverge. Until
                // additive-on-inline is implemented (`fut-iceberg-additive-inline`).
                if let Some(at) = current {
                    let mut conn = pool.acquire().await.map_err(be)?;
                    if crate::iceberg_inline::has_live_inline_rows(&mut conn, table, at).await? {
                        return Err(ControlPlaneError::Validation(
                            "schema evolution unsupported: flush inline rows before an additive land"
                                .into(),
                        ));
                    }
                }
                return land_additive(pool, catalog, table, columns, batches, lineage, end_cap)
                    .await;
            }
            Err(e) => return Err(ControlPlaneError::Validation(e.to_string())),
        }
    }

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
        .map(|b| coerce_batch_to_ice(&b, &ice_arrow, columns))
        .collect::<Result<Vec<_>>>()?;

    append_batches_with_extras(catalog, &ice_table, batches, lineage, end_cap, overwrite)
        .await
        .map_err(be)?;

    Ok(IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await?
        .id)
}

/// Re-wrap `batch` under the iceberg-derived arrow schema `ice_arrow`. Primitive
/// columns pass through; a `vector(N)` (`list<float>`) column is **rebuilt under
/// Iceberg's exact element field** (name `element` + `PARQUET:field_id`) — the wire
/// list's element field (`item`, no field id) would otherwise be rejected by
/// `RecordBatch::try_new`, which compares the full nested field. Same data, relabeled
/// element field. Also validates each row's element count equals the declared `N`.
fn coerce_batch_to_ice(
    batch: &RecordBatch,
    ice_arrow: &Arc<Schema>,
    columns: &[ColumnSpec],
) -> Result<RecordBatch> {
    let cols = ice_arrow
        .fields()
        .iter()
        .enumerate()
        .map(|(i, ice_field)| match ice_field.data_type() {
            DataType::List(child) => {
                let col = batch.column(i);
                let list = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("landing: column {:?} expected a list", ice_field.name()).into(),
                    )
                })?;
                if let Some(control_plane_core::BaseType::Vector(n)) =
                    control_plane_core::resolve_logical(&columns.get(i).ok_or_else(|| ControlPlaneError::Backend("landing: column index out of range".into()))?.ty)
                {
                    for r in 0..list.len() {
                        let len = list.value_length(r);
                        if !list.is_null(r) && i64::from(len) != i64::from(n) {
                            let nm = ice_field.name();
                            return Err(ControlPlaneError::Backend(
                                format!("landing: vector {nm:?} row {r}: {len} elements, want {n}")
                                    .into(),
                            ));
                        }
                    }
                }
                Ok(Arc::new(ListArray::new(
                    child.clone(),
                    list.offsets().clone(),
                    list.values().clone(),
                    list.nulls().cloned(),
                )) as ArrayRef)
            }
            _ => Ok(batch.column(i).clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(ice_arrow.clone(), cols).map_err(be)
}

/// The additive landing path (mirror-only). Writes the landing `batches` as Parquet
/// stamped with the SUPERSET arrow schema (incl. the newly-added nullable columns), then
/// projects the new columns + files into the `iceberg_mirror.*` projection in one
/// transaction — exactly the transform worker's [`register_files`] flow. Drives NO
/// Iceberg `fast_append`: iceberg-rust 0.9 has no schema-evolution transaction action, so
/// the real Iceberg metadata schema is intentionally not evolved (external `ATTACH`
/// clients not seeing the new column is the accepted `iss-iceberg-inline-visibility` gap).
/// loom-governed reads resolve entirely through the mirror, so the new columns/files are
/// immediately visible. Returns the mirror snapshot id.
#[allow(clippy::too_many_arguments, reason = "iceberg landing functions have many required parameters with no sensible grouping")]
async fn land_additive(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    use crate::lineage::pg_emit;

    // Load the table and build the SUPERSET arrow schema from the landing `columns`
    // (field-ids 1..N), so the writer chain stamps every column (incl. the new ones)
    // into the Parquet footer.
    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let ice_table = catalog.load_table(&ident).await.map_err(be)?;
    let superset = ice_schema(columns)?;
    let ice_arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&superset).map_err(be)?);
    let batches = batches
        .into_iter()
        .map(|b| coerce_batch_to_ice(&b, &ice_arrow, columns))
        .collect::<Result<Vec<_>>>()?;

    // Write Parquet with the superset schema (no fast_append).
    let ice_files =
        crate::iceberg_writer::write_parquet_with_schema(&ice_table, superset.into(), batches)
            .await
            .map_err(be)?;

    // Convert iceberg DataFiles -> loom DataFiles, computing per-column stats from each
    // written file's bytes (exactly like `iceberg_mirror::added_files_of`).
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut loom_files = Vec::with_capacity(ice_files.len());
    for df in &ice_files {
        let bytes = ice_table
            .file_io()
            .new_input(df.file_path())
            .map_err(be)?
            .read()
            .await
            .map_err(be)?;
        let column_stats = crate::iceberg_stats::column_stats_from_parquet(bytes, &names)?;
        loom_files.push(DataFile {
            path: df.file_path().to_string(),
            path_is_relative: false,
            file_format: FileFormat::Parquet,
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
            column_stats,
            parquet_footer_size: None,
        });
    }

    // One snapshot: project new columns (reconcile gate) + files, end-cap any flushed
    // inline rows, emit lineage, stamp schema_version (the last inside `register_files`).
    // `WriteMode::Append` so the prior files stay live (additive does not end-cap data).
    let mut tx = pool.begin().await.map_err(be)?;
    let at = next_snapshot(&mut tx, None).await?;
    register_files(&mut tx, table, columns, &loom_files, WriteMode::Append, at).await?;
    if let Some(cap) = end_cap {
        // Retire the flushed inline rows at the same snapshot the new files become live
        // (faithful to `do_update_table`'s inline end-cap).
        let sql = format!(
            "update {} set end_snapshot = {} \
             where loom_row_id = any($1) and end_snapshot is null",
            crate::iceberg_inline::inline_table_name(cap.table_id),
            at.0,
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(cap.row_ids)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
    }
    if let Some(ev) = lineage {
        pg_emit(&mut *tx, ev).await?;
    }
    tx.commit().await.map_err(be)?;
    Ok(at)
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
#[derive(Clone)]
pub enum WriteMode {
    /// Add `files` to the currently-live set.
    Append,
    /// End-cap every currently-live data file at the new snapshot first, so `files`
    /// become the sole live set (prior files still time-travel). The
    /// `road-iceberg-overwrite-mode` contract, over already-written files.
    Overwrite,
    /// Expire the specific live files named by `expire_paths`, then add `files`, at
    /// the new snapshot. Schema-invariant (compaction never changes columns), so it
    /// skips *column* reconciliation (`reconcile_and_project`) — callers pass `&[]`
    /// for `columns` — but still stamps the snapshot's schema version
    /// (`stamp_schema_version`), since `current_snapshot` reads a `schema_version`
    /// for every snapshot row.
    Compact { expire_paths: Vec<String> },
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
    match &mode {
        WriteMode::Append => {}
        WriteMode::Overwrite => {
            end_cap_live_data_files(conn, tid, at).await?;
        }
        WriteMode::Compact { expire_paths } => {
            // Subset-expire the named files; project the new ones below. Schema is
            // unchanged, so skip reconcile_and_project (it would require `columns`).
            end_cap_files_by_path(conn, tid, expire_paths, at).await?;
            project_files(conn, tid, at, &projected_files(files)?).await?;
            stamp_schema_version(conn, tid, at).await?;
            return Ok(());
        }
    }
    reconcile_and_project(conn, tid, at, &projected_columns(columns)?).await?;
    project_files(conn, tid, at, &projected_files(files)?).await?;
    stamp_schema_version(conn, tid, at).await?;
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
            // A vector column's mirror type is `vector(N)` (matching `columns_of`'s decode
            // of the list field doc); every other type maps to its iceberg primitive name.
            let iceberg_type = match control_plane_core::resolve_logical(&c.ty) {
                Some(control_plane_core::BaseType::Vector(n)) => format!("vector({n})"),
                _ => iceberg_physical_type(&c.ty)
                    .ok_or_else(|| {
                        ControlPlaneError::Backend(
                            format!("register: no iceberg type for {:?}", c.ty).into(),
                        )
                    })?
                    .to_string(),
            };
            Ok(ProjectedColumn {
                order: (i + 1) as i64,
                name: c.name.clone(),
                iceberg_type,
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

/// Build an iceberg `Schema` from loom `ColumnSpec`s. Field ids are assigned from a
/// running counter (a `vector(N)` column's `list<float>` element consumes its own id),
/// so every nested field is schema-wide unique as Iceberg requires.
fn ice_schema(columns: &[ColumnSpec]) -> Result<IceSchema> {
    let mut next_id = 1i32;
    let fields = columns
        .iter()
        .map(|c| {
            let id = next_id;
            next_id += 1;
            let field = match control_plane_core::resolve_logical(&c.ty) {
                // A vector is stored as Iceberg `list<float>`; the dimension `N` rides in
                // the field doc (`vector(N)`) since an Iceberg list is length-free.
                Some(control_plane_core::BaseType::Vector(n)) => {
                    let elem_id = next_id;
                    next_id += 1;
                    let element = Arc::new(NestedField::list_element(
                        elem_id,
                        Type::Primitive(PrimitiveType::Float),
                        true,
                    ));
                    let list = Type::List(ListType::new(element));
                    let f = if c.nullable {
                        NestedField::optional(id, &c.name, list)
                    } else {
                        NestedField::required(id, &c.name, list)
                    };
                    f.with_doc(format!("vector({n})"))
                }
                _ => {
                    let ty = Type::Primitive(primitive_from(&c.ty)?);
                    if c.nullable {
                        NestedField::optional(id, &c.name, ty)
                    } else {
                        NestedField::required(id, &c.name, ty)
                    }
                }
            };
            Ok(Arc::new(field))
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

//! The Iceberg landing entrypoint: take pre-decoded Arrow batches, route by
//! in-memory size between an inline (mirror-only) write and a real Parquet write,
//! and return the loom mirror snapshot id. Both branches emit lineage atomically.
//!
//! This lives in the postgres crate (not ingest) because it owns the iceberg
//! writer chain and the mirror projection; callers decode the Arrow IPC body
//! themselves (via `datafusion_io::decode_ipc` on the umbrella-arrow side) and
//! pass the schema + batches in. (Historically this crate was arrow-57 while
//! ingest was arrow-58; the arrow-58 converge removed that split — the whole tree
//! now shares one arrow major — but the landing path stays here by ownership.)

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, FileFormat, LineageEvent, NewJob, Result,
    SnapshotId, TableRef,
};
use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog as IceCatalog, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::inline_append;
use crate::iceberg_mirror::{
    ProjectedColumn, ProjectedFile, end_cap_files_by_path, end_cap_live_data_files, ensure_table,
    live_columns_for, next_snapshot, project_files, reconcile_and_project, stamp_schema_version,
};
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};
use crate::iceberg_sql_catalog::{CommitExtras, SqlCatalog};
use crate::iceberg_type::{iceberg_physical_type, mirror_column_type};
use crate::iceberg_writer::append_batches_with_extras;

/// The inline-tier routing limits carried by [`land`]: at/below
/// `inline_byte_limit` a request inlines (mirror-only typed rows) instead of
/// writing real Parquet; at/above `flush_byte_threshold` live inline bytes an
/// inline write enqueues a `flush_table` job.
#[derive(Clone, Copy, Debug)]
pub struct InlineLimits {
    /// In-memory (uncompressed) Arrow size at/below which the request inlines.
    pub inline_byte_limit: usize,
    /// Live-inline-byte total at/above which a `flush_table` job is enqueued
    /// after an inline write.
    pub flush_byte_threshold: i64,
}

/// Land an Iceberg request, routing by in-memory size per `limits` (see
/// [`InlineLimits`]). Returns the loom mirror snapshot id either way.
#[expect(
    clippy::too_many_arguments,
    reason = "eight cohesive positional params: routing target (pool/catalog/table/columns), \
              pre-decoded payload (schema/batches — split from the single ipc_body: &[u8] this \
              replaces), and behavior (limits/lineage); grouping any of these into a struct would \
              obscure the mechanical 1:1 mapping callers already have to the removed decode step"
)]
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    limits: InlineLimits,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    // Project the decoded columns to `columns` order, by name. Both downstream
    // branches align columns POSITIONALLY (inline indexes `columns[c]` against
    // batch column `c`; the Parquet branch re-wraps under the table's schema in
    // `columns` order), but on the model-gate path `columns` is the model's
    // declared order, which need not match the wire order — so without this
    // realignment, same-typed reordered columns would silently swap values.
    let (schema, batches) = align_to_columns(&schema, batches, columns)?;
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= limits.inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(backend)?;
        inline_append(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(limits.flush_byte_threshold),
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
            RecordBatch::try_new(projected.clone(), cols).map_err(backend)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((projected, batches))
}

/// Ensure the iceberg table exists (create-if-absent from `columns`), append
/// `batches` (bare arrow — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
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
            let mut conn = pool.acquire().await.map_err(backend)?;
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
                    let mut conn = pool.acquire().await.map_err(backend)?;
                    if crate::iceberg_inline::has_live_inline_rows(&mut conn, table, at).await? {
                        return Err(ControlPlaneError::Validation(
                            "schema evolution unsupported: flush inline rows before an additive land"
                                .into(),
                        ));
                    }
                }
                return land_additive(pool, catalog, table, columns, batches, extras).await;
            }
            Err(e) => return Err(ControlPlaneError::Validation(e.to_string())),
        }
    }

    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let ice_table = catalog.load_table(&ident).await.map_err(backend)?;

    // A decoded IPC body carries a bare arrow schema; the iceberg writer chain needs
    // the table's arrow schema (which carries the iceberg field-id metadata) or it
    // can't map columns to field ids. Re-wrap each batch's columns under that
    // field-id-bearing schema. Positional alignment is safe here because `batches`
    // were already projected to `columns` order (= the table schema order) by
    // `align_to_columns` upstream.
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema())
            .map_err(backend)?,
    );
    let batches = batches
        .into_iter()
        .map(|b| coerce_batch_to_ice(&b, &ice_arrow, columns))
        .collect::<Result<Vec<_>>>()?;

    append_batches_with_extras(catalog, &ice_table, batches, extras)
        .await
        .map_err(backend)?;

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
                    control_plane_core::resolve_logical(
                        &columns
                            .get(i)
                            .ok_or_else(|| {
                                ControlPlaneError::Backend(
                                    "landing: column index out of range".into(),
                                )
                            })?
                            .ty,
                    )
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
    RecordBatch::try_new(ice_arrow.clone(), cols).map_err(backend)
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
///
/// `extras.overwrite` is deliberately NOT applied here: an additive land is
/// always an append (`WriteMode::Append`, prior files stay live) — faithful to
/// the pre-`CommitExtras` chain, which never forwarded the flag to this path.
async fn land_additive(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
) -> Result<SnapshotId> {
    // Write the landing `batches` as Parquet stamped with the SUPERSET schema (field-ids
    // 1..N, incl. the newly-added columns) and build the loom `DataFile`s + per-column
    // stats — pure IO, no commit. Shared with the multi-target `write_steps` path.
    let loom_files = write_object_data_files(catalog, table, columns, batches).await?;

    // One snapshot: project new columns (reconcile gate) + files, end-cap any flushed
    // inline rows, emit lineage, stamp schema_version (the last inside `register_files`).
    // `WriteMode::Append` so the prior files stay live (additive does not end-cap data).
    let mut tx = pool.begin().await.map_err(backend)?;
    let at = next_snapshot(&mut tx, None).await?;
    register_files(&mut tx, table, columns, &loom_files, WriteMode::Append, at).await?;
    crate::iceberg_sql_catalog::apply_commit_extras(&mut tx, at, &extras).await?;
    tx.commit().await.map_err(backend)?;
    Ok(at)
}

/// Write `batches` (typed by `columns`) as real Parquet under `table`'s Iceberg
/// location and return the loom `DataFile`s (path + counts + per-column stats),
/// WITHOUT committing anything or projecting the mirror. `table` must already exist in
/// `catalog` (call [`ensure_iceberg_table`] first). The Parquet footer is stamped with
/// the field-id schema built from `columns` (via [`ice_schema`]); the caller registers
/// the returned files into a snapshot in its own transaction. This is the pure-IO half
/// shared by the additive-land and multi-target `write_steps` paths.
async fn write_object_data_files(
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let ice_table = catalog.load_table(&ident).await.map_err(backend)?;
    let sch = ice_schema(columns)?;
    let ice_arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&sch).map_err(backend)?);
    let batches = batches
        .into_iter()
        .map(|b| coerce_batch_to_ice(&b, &ice_arrow, columns))
        .collect::<Result<Vec<_>>>()?;

    let ice_files =
        crate::iceberg_writer::write_parquet_with_schema(&ice_table, sch.into(), batches)
            .await
            .map_err(backend)?;

    // Convert iceberg DataFiles -> loom DataFiles, computing per-column stats from each
    // written file's bytes (exactly like `iceberg_mirror::added_files_of`).
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut loom_files = Vec::with_capacity(ice_files.len());
    for df in &ice_files {
        let bytes = ice_table
            .file_io()
            .new_input(df.file_path())
            .map_err(backend)?
            .read()
            .await
            .map_err(backend)?;
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
    Ok(loom_files)
}

/// One target of a multi-target atomic write ([`write_steps`]): the batches to land
/// into `table` (typed by `columns`), appended or overwriting the live set.
pub struct StepLand {
    pub table: TableRef,
    pub columns: Vec<ColumnSpec>,
    pub batches: Vec<RecordBatch>,
    /// `true` replaces the table's live set (both tiers end-capped); `false` appends.
    pub overwrite: bool,
}

/// Stage N per-target writes AND one lineage event in ONE Postgres transaction — a
/// single mirror snapshot covering every target, so all targets and the lineage land
/// or roll back together. Generalises [`land`]'s Parquet path + [`register_files`] to N
/// tables, exactly as [`crate::iceberg_control_plane::IcebergTx::commit`] does for the
/// transform seam: each step's Parquet is written first (pure IO, outside the tx), then
/// one transaction allocates a single snapshot, registers every step's files
/// (`Append` or `Overwrite`), emits `lineage`, and commits. Empty `steps` is rejected
/// (no snapshot to allocate) — the caller always has at least one target.
pub async fn write_steps(
    pool: &PgPool,
    catalog: &SqlCatalog,
    steps: Vec<StepLand>,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    if steps.is_empty() {
        return Err(ControlPlaneError::Validation(
            "write_steps: no targets".into(),
        ));
    }

    // Phase 1 (pure IO, pre-tx): ensure each target exists + write its Parquet, building
    // the loom `DataFile`s. A failure here commits nothing (no tx opened yet).
    struct Staged {
        table: TableRef,
        columns: Vec<ColumnSpec>,
        files: Vec<DataFile>,
        overwrite: bool,
    }
    let mut staged: Vec<Staged> = Vec::with_capacity(steps.len());
    for step in steps {
        // A zero-row `Overwrite` (a multi-step Delete/Update that emptied the table) is a
        // truncate: it writes NO Parquet, so skip the Iceberg table-create + Parquet write and
        // stage zero files. Its `columns` are KEPT so Phase 2's `Overwrite` register still
        // projects the schema at the shared snapshot — the emptied table then reads as empty
        // (schema present, zero live files/inline rows), NOT as a missing table.
        if step.overwrite && step.batches.iter().all(|b| b.num_rows() == 0) {
            staged.push(Staged {
                table: step.table,
                columns: step.columns,
                files: Vec::new(),
                overwrite: true,
            });
            continue;
        }
        ensure_iceberg_table(catalog, &step.table, &step.columns).await?;
        let files =
            write_object_data_files(catalog, &step.table, &step.columns, step.batches).await?;
        staged.push(Staged {
            table: step.table,
            columns: step.columns,
            files,
            overwrite: step.overwrite,
        });
    }

    // Phase 2 (one tx): one snapshot for the whole write; register every step's files,
    // emit the single lineage event, commit. Two steps targeting the same table both
    // stage against this one snapshot (their file rows coexist, live at `at`). An empty-file
    // `Overwrite` end-caps both tiers + re-projects the schema with zero files (a truncate).
    let mut tx = pool.begin().await.map_err(backend)?;
    let at = next_snapshot(&mut tx, None).await?;
    for s in &staged {
        let mode = if s.overwrite {
            WriteMode::Overwrite
        } else {
            WriteMode::Append
        };
        register_files(&mut tx, &s.table, &s.columns, &s.files, mode, at).await?;
    }
    crate::lineage::pg_emit(&mut *tx, &lineage).await?;
    tx.commit().await.map_err(backend)?;
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
    if !catalog.namespace_exists(&ns).await.map_err(backend)? {
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .map_err(backend)?;
    }
    let ident = TableIdent::new(ns.clone(), table.name.clone());
    if !catalog.table_exists(&ident).await.map_err(backend)? {
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(columns)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(backend)?;
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
            // End-cap BOTH tiers, exactly as the `append_parquet_snapshot` overwrite
            // path does (`commit_mirror`): the file tier's live data files AND any live
            // inline-shadow rows are retired at `at`, so `files` become the sole live
            // set. This is how a multi-step Update/Delete via `replace_files` supersedes
            // an inline-written object (inline-only tables no-op the data-file end-cap;
            // file-only tables no-op the inline end-cap — each end-cap is independently
            // safe). Time travel is preserved (older snapshots still see the retired rows).
            end_cap_live_data_files(conn, tid, at).await?;
            crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
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

/// Map loom `ColumnSpec`s to mirror `ProjectedColumn`s, storing the column type via
/// `mirror_column_type` — the exact `column_type` the normal write path records (via
/// `columns_of`), so reads decode identically (`logical_from_iceberg`). A vector column
/// keeps its `vector(N)` form; the dimension is carried in the `column_type` text.
fn projected_columns(columns: &[ColumnSpec]) -> Result<Vec<ProjectedColumn>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let iceberg_type = mirror_column_type(&c.ty).ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!("register: no iceberg type for {:?}", c.ty).into(),
                )
            })?;
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
        CommitExtras {
            lineage: Some(&lineage),
            ..CommitExtras::default()
        },
    )
    .await
}

/// Replace `table`'s live data with `batches` in one Postgres transaction — the
/// overwrite/replace commit primitive (`Tx::replace_files`). End-caps every currently-live
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
///
/// Like the flush path, an overwrite commit enqueues one deduped
/// `build_vector_index` rebuild job per vector index declared on the table's
/// ontology type (via the shared `rebuild_jobs_for`), atomically with the
/// commit, so replaced rows can't leave a stale index serving silently.
pub async fn overwrite_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
) -> Result<SnapshotId> {
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;
    if batches.iter().all(|b| b.num_rows() == 0) {
        return overwrite_truncate(pool, table, lineage, &rebuild_jobs).await;
    }
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage,
            overwrite: true,
            jobs: &rebuild_jobs,
            ..CommitExtras::default()
        },
    )
    .await
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
    jobs: &[NewJob],
) -> Result<SnapshotId> {
    use crate::iceberg_mirror::{end_cap_live_data_files, ensure_table, next_snapshot};
    use crate::lineage::pg_emit;

    let mut tx = pool.begin().await.map_err(backend)?;
    let conn = &mut *tx;
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    end_cap_live_data_files(conn, tid, at).await?;
    crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
    if let Some(ev) = lineage {
        pg_emit(&mut *conn, ev).await?;
    }
    for job in jobs {
        // Same pending-dedup as CommitExtras.jobs; pg_notify is buffered until
        // this tx commits, so a rolled-back truncate enqueues nothing.
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }
    tx.commit().await.map_err(backend)?;
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
    IceSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(backend)
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

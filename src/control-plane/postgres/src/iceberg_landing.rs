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

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
use crate::iceberg_inline::inline_append_decl;
use crate::iceberg_mirror::{
    ProjectedColumn, ProjectedFile, end_cap_files_by_path, end_cap_live_data_files, ensure_table,
    ensure_table_witnessed, live_columns_for, live_table_id, next_snapshot, project_files,
    reconcile_and_project, stamp_schema_version,
};
use crate::iceberg_schema_evolution::{SchemaPlan, classify_schema_change};
use crate::iceberg_sql_catalog::{CommitExtras, InlineEndCap, SqlCatalog};
use crate::iceberg_type::{iceberg_physical_type, mirror_column_type};
use crate::iceberg_writer::append_batches_with_extras;
use crate::mv_floor::EndCapIntent;
use crate::stream::StreamDecl;

/// A CDC (PK/identity-bearing) stream declaration: the requested bucket count and
/// the identity column to bucket rows on (`hash(bucket_key) % buckets`).
/// Constructed by callers that declare a table as a PK/CDC stream table — today
/// only the `/models/{type}?mode=cdc` path — and threaded through `LandRequest`/
/// [`land`] alongside the pre-existing `stream_buckets` (log-declare) parameter.
#[derive(Clone, Debug)]
pub struct CdcDecl {
    pub buckets: i32,
    pub bucket_key: String,
    pub merge_engine: control_plane_core::MergeEngine,
}

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

/// Combine the two mutually-exclusive stream-declaration request shapes carried
/// on [`land`]'s boundary (`stream_buckets` for a log declare, `cdc` for a cdc
/// declare — each HTTP path sets at most one) into the internal [`StreamDecl`]
/// `reconcile_stream_mode` matches on. Both `Some` is an internal-caller bug
/// (no HTTP path ever sets both), not a client-triggerable state — surfaced as an
/// opaque `Backend` fault rather than guessed at.
fn combine_stream_decl(stream_buckets: Option<i32>, cdc: Option<CdcDecl>) -> Result<StreamDecl> {
    match (stream_buckets, cdc) {
        (Some(_), Some(_)) => Err(ControlPlaneError::Backend(
            "land: cannot request both a log and a cdc stream declaration".into(),
        )),
        (Some(n), None) => Ok(StreamDecl::Log(n)),
        (
            None,
            Some(CdcDecl {
                buckets,
                bucket_key,
                merge_engine,
            }),
        ) => Ok(StreamDecl::Cdc {
            buckets,
            bucket_key,
            merge_engine,
        }),
        (None, None) => Ok(StreamDecl::None),
    }
}

/// Land an Iceberg request, routing by in-memory size per `limits` (see
/// [`InlineLimits`]). Returns the loom mirror snapshot id either way.
///
/// Thin wrapper over [`land_cdc`] (with `cdc: None`) for the very large set of
/// callers (production and test) that only ever request a log declaration or
/// none — preserves this function's signature exactly so none of them need to
/// change for the CDC-declare widening. The `/models/{type}?mode=cdc` path (the
/// only CDC-declaring caller) goes through `land_cdc` directly.
#[expect(
    clippy::too_many_arguments,
    reason = "nine cohesive positional params: routing target (pool/catalog/table/columns), \
              pre-decoded payload (schema/batches — split from the single ipc_body: &[u8] this \
              replaces), behavior (limits/lineage), and the stream-mode declaration \
              (stream_buckets); grouping any of these into a struct would obscure the mechanical \
              1:1 mapping callers already have to the removed decode step"
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
    stream_buckets: Option<i32>,
) -> Result<SnapshotId> {
    land_cdc(
        pool,
        catalog,
        table,
        columns,
        schema,
        batches,
        limits,
        lineage,
        stream_buckets,
        None,
        &[],
    )
    .await
}

/// The actual landing implementation behind [`land`], extended with an optional
/// CDC stream declaration (`cdc`) alongside the pre-existing log-declare
/// `stream_buckets` — see [`CdcDecl`] and [`combine_stream_decl`]. `land` is the
/// stable, unchanged entrypoint for the many callers with no CDC intent; this
/// entrypoint is for the one caller that does (the ingest model-bind path, via
/// `LandRequest::cdc`).
#[expect(
    clippy::too_many_arguments,
    reason = "same nine cohesive params as `land`, plus the CDC stream declaration \
              (`cdc`, mutually exclusive with `stream_buckets` — see `combine_stream_decl`) \
              and the resolved downstream `jobs` to enqueue atomically with the write \
              (slice 4; empty for the non-action callers via `land`)"
)]
pub async fn land_cdc(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    limits: InlineLimits,
    lineage: LineageEvent,
    stream_buckets: Option<i32>,
    cdc: Option<CdcDecl>,
    jobs: &[control_plane_core::NewJob],
) -> Result<SnapshotId> {
    let decl = combine_stream_decl(stream_buckets, cdc)?;

    // Project the decoded columns to `columns` order, by name. Both downstream
    // branches align columns POSITIONALLY (inline indexes `columns[c]` against
    // batch column `c`; the Parquet branch re-wraps under the table's schema in
    // `columns` order), but on the model-gate path `columns` is the model's
    // declared order, which need not match the wire order — so without this
    // realignment, same-typed reordered columns would silently swap values.
    let (schema, batches) = align_to_columns(&schema, batches, columns)?;

    // A CDC declaration also owns a durable changelog Iceberg table (spec §4). Its
    // object-store metadata is created HERE (before any commit tx), so both landing
    // routes can assume it exists; its mirror row + registry pointer are set by
    // `reconcile_stream_mode`, which on the Parquet route now runs inside
    // `claim_stream_mode`'s claim transaction rather than the write tx. Idempotent.
    //
    // NOTE: this create precedes the mode claim, so it is the one piece of physical
    // work the claim does NOT gate. A CDC declare the claim then refuses leaves an
    // orphan `<table>__changelog` behind. That cannot produce #623's divergence — it
    // carries no mirror row for the schema to disagree with, and a later legitimate
    // CDC declare finds it already correct — so it is deliberately left alone.
    if matches!(decl, StreamDecl::Cdc { .. }) {
        let clog = changelog_table_ref(table);
        ensure_iceberg_table(catalog, &clog, columns, true).await?;
    }

    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= limits.inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(backend)?;
        inline_append_decl(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(limits.flush_byte_threshold),
            &decl,
            jobs,
            None,
        )
        .await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage, &decl, jobs).await
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
///
/// `include_framing`: `true` for a stream/log table — the physical schema (Iceberg
/// creation, the mirror-reconcile "incoming" comparison, and the field-id rewrap)
/// is computed from `columns` PLUS [`framing_column_specs`] (appended after, same
/// order stream declaration registers them in the mirror — see `iceberg_inline`).
/// This is computed HERE (not by the caller) so callers keep passing their plain
/// logical `columns` — `flush_locked` in particular does not need to build a
/// framing-aware column list itself. `false` (batch tables) makes every step below
/// behave exactly as before this parameter existed (`full_columns == columns`).
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
    include_framing: bool,
) -> Result<SnapshotId> {
    ensure_iceberg_table(catalog, table, columns, include_framing).await?;

    // The physical column view this commit reconciles/writes against: for a stream
    // table this is `columns` (user) + the reserved framing specs, in the exact
    // order `ensure_iceberg_table` built the Iceberg schema in and `iceberg_inline`
    // registered them in the mirror at declaration — so `incoming` below matches
    // `live` (already carrying framing from declaration) instead of looking like a
    // dropped-columns schema change.
    let full_columns: Vec<ColumnSpec> = augment_with_framing(columns, include_framing);

    // Decide identical/create vs additive vs reject against the live mirror. We need a
    // snapshot to read live columns "as of"; live columns have begin_snapshot <= the
    // current snapshot, so the current snapshot is the read point — or an empty set if
    // there is no snapshot yet (a fresh creation).
    let incoming = projected_columns(&full_columns)?;
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
                return land_additive(pool, catalog, table, &full_columns, batches, extras).await;
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
    // were already projected to `full_columns` order (= the table schema order) by
    // `align_to_columns` upstream (batch path) or by the physical mirror read
    // (stream flush path).
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema())
            .map_err(backend)?,
    );
    let batches = batches
        .into_iter()
        .map(|b| coerce_batch_to_ice(&b, &ice_arrow, &full_columns))
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
pub(crate) fn coerce_batch_to_ice(
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
    // Event-driven compaction auto-trigger (mirrors the tail of
    // `SqlCatalog::write_mirror`): evaluated once the new files are already
    // registered above. `None` (default) is a no-op, preserving every existing
    // path byte-identically.
    if let Some(cfg) = &catalog.compact_trigger {
        crate::iceberg_compact::maybe_enqueue_compact(&mut tx, table, cfg).await?;
    }
    crate::iceberg_sql_catalog::apply_commit_extras(&mut tx, table, at, &extras).await?;
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
pub async fn write_object_data_files(
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
    jobs: &[NewJob],
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
        // Multi-target writes don't carry stream framing (out of this slice's scope).
        ensure_iceberg_table(catalog, &step.table, &step.columns, false).await?;
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

    // Refuse any stream/CDC-registered target before registering files: the
    // multi-target path is framing-unaware, so a stream target would corrupt the
    // stream (missing/garbage framing). In-tx placement makes the refusal atomic —
    // if any step targets a stream table, the whole commit rolls back (nothing
    // lands), honoring the all-or-nothing contract multi-step actions promise.
    let mut refuse: Vec<&TableRef> = staged.iter().map(|s| &s.table).collect();
    refuse.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    refuse.dedup();
    for table in refuse {
        crate::stream::pg_refuse_stream_target(&mut tx, table).await?;
    }

    for s in &staged {
        let mode = if s.overwrite {
            WriteMode::Overwrite
        } else {
            WriteMode::Append
        };
        register_files(&mut tx, &s.table, &s.columns, &s.files, mode, at).await?;
    }
    crate::lineage::pg_emit(&mut *tx, &lineage).await?;
    let written: Vec<TableRef> = staged.iter().map(|s| s.table.clone()).collect();
    crate::transforms::pg_fire_data_triggers(&mut tx, &written, Some(lineage.run_id.0)).await?;
    // The action's resolved downstream jobs (multi-step phase 2) ride this same commit
    // tx — enqueued iff the multi-target write commits (commit-or-neither), mirroring
    // the single-step insert/update/delete paths' enqueue-before-commit.
    for job in jobs {
        crate::queue::pg_insert_if_absent(&mut *tx, job).await?;
    }
    tx.commit().await.map_err(backend)?;
    Ok(at)
}

/// Create the Iceberg namespace + table for `table` if absent (idempotent), from
/// `columns`. Writes only the Iceberg catalog pointer — it does NOT project the
/// mirror (the mirror is projected when files are registered/appended). Shared by
/// the landing Parquet path and the transform [`register_files`] path.
///
/// `include_framing`: when `true` (a stream/log table), the three reserved log
/// framing columns ([`framing_column_specs`]) are appended AFTER `columns` in the
/// created Iceberg schema, so they land in the physical schema (and therefore the
/// mirror, via `columns_of` on the commit path) while staying invisible to logical
/// reads (`is_reserved`). Only matters on table CREATION — a pre-existing table's
/// schema is untouched. Batch tables pass `false`, leaving `ice_schema(columns)`
/// byte-identical to before this parameter existed.
pub(crate) async fn ensure_iceberg_table(
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    include_framing: bool,
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
        let cols = augment_with_framing(columns, include_framing);
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(&cols)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(backend)?;
    }
    Ok(())
}

/// The three reserved log-framing columns a stream/log table's Iceberg physical
/// schema carries, in fixed order, appended AFTER a table's user columns. Hidden
/// from every logical read by `is_reserved` (`iceberg_catalog::is_reserved`);
/// present in the physical read (`IcebergCatalog::physical_columns`) that the
/// flush path uses to carry them into Parquet. A batch table never gets these —
/// `ice_schema`/the mirror stay exactly as before this column existed.
pub fn framing_column_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "loom_change_kind".into(),
            ty: "string".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "loom_bucket".into(),
            ty: "integer".into(),
            nullable: true,
        },
        ColumnSpec {
            name: "loom_offset".into(),
            ty: "long".into(),
            nullable: true,
        },
    ]
}

/// `columns` for a batch table, or `columns` + [`framing_column_specs`] (appended
/// AFTER, in fixed order) for a stream/log table. THE single place the
/// "user columns + framing" physical column list is built — shared by
/// [`ensure_iceberg_table`] (Iceberg schema), [`append_parquet_snapshot`] (mirror
/// reconcile + field-id rewrap), and the direct-write stream path — so the three
/// sites can never drift on ordering. `include_framing == false` returns a plain
/// clone (batch tables byte-identical to before this helper existed).
pub(crate) fn augment_with_framing(
    columns: &[ColumnSpec],
    include_framing: bool,
) -> Vec<ColumnSpec> {
    if include_framing {
        columns
            .iter()
            .cloned()
            .chain(framing_column_specs())
            .collect()
    } else {
        columns.to_vec()
    }
}

/// The durable changelog table's `TableRef` for a CDC base table: same schema,
/// name suffixed `__changelog` (slice 2b, spec §4).
pub fn changelog_table_ref(base: &TableRef) -> TableRef {
    TableRef {
        schema: base.schema.clone(),
        name: format!("{}__changelog", base.name),
    }
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
            //
            // REMOVING: a replace consumes the old rows — whatever offsets they carried
            // leave the live set. Both of this mode's callers already refuse a stream
            // target outright (`pg_refuse_stream_target`), so the floor is `None` in
            // practice; the intent is the honest declaration, not a behavior change.
            end_cap_live_data_files(conn, table, tid, at, &EndCapIntent::Removing).await?;
            crate::iceberg_inline::end_cap_live_inline_rows(
                conn,
                table,
                tid,
                at,
                &EndCapIntent::Removing,
            )
            .await?;
        }
        WriteMode::Compact { expire_paths } => {
            // Subset-expire the named files; project the new ones below. Schema is
            // unchanged, so skip reconcile_and_project (it would require `columns`).
            //
            // REFRAMING: compaction rewrites the very rows of `expire_paths` into the
            // coalesced `files` projected two lines below, in this same transaction and
            // at this same snapshot, carrying their `loom_bucket`/`loom_offset` values
            // through unchanged. Nothing leaves the live set, so no MV can miss a row —
            // consulting the floor here would refuse every compaction of a floored
            // stream table for no benefit.
            end_cap_files_by_path(conn, table, tid, expire_paths, at, &EndCapIntent::Reframing)
                .await?;
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

/// Phase 1 of the Parquet land: establish this write's stream-vs-batch mode as
/// **committed** state, and return it. `Some(bucket_count)` = stream, `None` = batch.
///
/// This replaces the read-only pre-transaction probe that used to decide the route.
/// The probe was not merely stale — its answer was then baked into the Iceberg
/// **physical schema** by whichever path it selected (`ensure_iceberg_table` with
/// `include_framing` true on the stream path, false on the batch path), and that
/// object-store create is not transactional and is never rolled back. Two racers
/// creating the same brand-new table could therefore disagree about framing, leaving
/// a framing-schema'd Iceberg table under a mirror row that says batch. Committing
/// the decision first makes both racers read the same answer, so their `create_table`
/// calls agree and the concurrent create is idempotent rather than divergent.
///
/// Two arms:
///
/// **Fast path — the mode is already permanent.** A live `iceberg_mirror.table` row
/// means the mode can no longer change: [`crate::stream::reconcile_stream_mode`]
/// refuses to convert an existing batch table to a stream table, and a declared
/// stream table is never undeclared. So a committed registry answer needs no claim
/// transaction, and the steady-state append path keeps costing exactly what it did
/// before — no extra transaction, no burned snapshot id. The one case that still
/// falls through is a declaration against a live-but-undeclared table: that is the
/// batch->stream conversion attempt, and it must reach `reconcile_stream_mode` to be
/// refused.
///
/// **Claim path — commit the decision.** `next_snapshot` -> `ensure_table_witnessed`
/// -> `reconcile_stream_mode` -> COMMIT. `ensure_table_witnessed`'s unique-index race
/// is what serializes two first-landers: the loser resolves to the winner's
/// `table_id` with `created == false`, so its `pre_existing` witness is honest and
/// the conversion guard fires. This deliberately takes **no** explicit advisory lock:
/// `reconcile_stream_mode` already takes `lock_key(table)` inside its first-declare
/// arm, AFTER the mirror-row ensure, so this transaction reproduces
/// `iceberg_inline::inline_append`'s insert-then-lock ordering exactly and keeps the
/// first-declare-vs-`define_transform` serialization intact. Taking the lock HERE
/// (before the ensure) would make this lock-then-insert against `inline_append`'s
/// insert-then-lock, which deadlocks (40P01) under precisely the concurrency this
/// function exists to fix.
///
/// ACCEPTED RESIDUE, stated plainly because it is wider than the stream case: the
/// claim commits even if the write that follows fails, and it does so on EVERY first
/// Parquet land, batch included. A failed first land therefore now leaves a live but
/// column-less `iceberg_mirror.table` row — visible to
/// [`crate::iceberg_catalog::IcebergCatalog::live_tables`] and so to dataset
/// listings — where it previously left nothing. The row's `begin_snapshot` also now
/// precedes its own columns and files, so a read as of the claim snapshot sees the
/// table live and empty. This is accepted as strictly better than the divergence it
/// replaces — an empty table is inert, a framing-schema'd table under a batch mirror
/// row is not — and a consumed snapshot id is not a scarce resource. But it is a
/// real, observable change, not a no-op: every other `ensure_table` call site runs
/// in the same transaction as the write it accompanies, so this is the first place
/// loom commits a mirror row that a later failure will not roll back.
async fn claim_stream_mode(
    pool: &PgPool,
    table: &TableRef,
    decl: &StreamDecl,
) -> Result<Option<i32>> {
    {
        let mut conn = pool.acquire().await.map_err(backend)?;
        if let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? {
            let recorded = crate::stream::pg_stream_bucket_count(&mut *conn, tid).await?;
            // Permanent, so answerable without a claim — unless this is a declaration
            // against an undeclared table, which is the conversion attempt the claim
            // path exists to refuse.
            if recorded.is_some() || matches!(decl, StreamDecl::None) {
                return Ok(recorded);
            }
        }
    }

    let mut tx = pool.begin().await.map_err(backend)?;
    let at = next_snapshot(&mut tx, None).await?;
    let (tid, created) = ensure_table_witnessed(&mut tx, &table.schema, &table.name, at).await?;
    // A rejected conversion (`Validation`) or bucket mismatch (`Conflict`) is
    // terminal — roll back and surface it, before any Iceberg table is created.
    let effective =
        match crate::stream::reconcile_stream_mode(&mut tx, tid, decl, !created, table, at).await {
            Ok(e) => e,
            Err(e) => {
                drop(tx.rollback().await);
                return Err(e);
            }
        };
    tx.commit().await.map_err(backend)?;
    Ok(effective)
}

/// The Parquet branch: ensure the namespace + table exist, then append real
/// Parquet with an atomic lineage emit, and read the resulting mirror snapshot id
/// back. Idempotent on namespace/table (create-if-absent).
///
/// Routes by stream mode, which [`claim_stream_mode`] establishes as **committed**
/// state before this function does any physical work — so the Iceberg table's
/// physical schema (created with framing on the stream path, without it on the batch
/// path) can never encode a decision a concurrent racer disagrees with. (`land_cdc`
/// creates the CDC changelog table's metadata before calling here; see the note
/// there.) A BATCH write takes the
/// unchanged [`append_parquet_snapshot`] path (`include_framing = false`); a STREAM
/// write takes [`land_parquet_stream`], which stamps gapless per-bucket offsets into
/// the written Parquet atomically with the snapshot commit.
#[expect(
    clippy::too_many_arguments,
    reason = "the landing params (pool, catalog, table, columns, batches, lineage, stream decl) \
              plus the slice-4 downstream jobs the action path threads into the commit"
)]
async fn land_parquet(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
    decl: &StreamDecl,
    jobs: &[control_plane_core::NewJob],
) -> Result<SnapshotId> {
    // Phase 1: commit the mode claim before any physical work.
    let effective = claim_stream_mode(pool, table, decl).await?;

    // Phases 2 and 3 both derive from that committed answer: each branch's first act
    // is `ensure_iceberg_table` with the matching `include_framing`, so the Iceberg
    // schema now encodes a decision that is already durable.
    if effective.is_some() {
        return land_parquet_stream(pool, catalog, table, columns, batches, lineage, decl, jobs)
            .await;
    }

    // BATCH path — unchanged.
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage: Some(&lineage),
            data_trigger_tables: std::slice::from_ref(table),
            jobs,
            ..CommitExtras::default()
        },
        false,
    )
    .await
}

/// The atomic direct-write path for a stream/log table: stamp gapless per-bucket
/// offsets into the written Parquet so the offset allocation commits **iff** the
/// snapshot commits. The whole attempt — allocate offsets, stamp, write Parquet,
/// CAS-commit — rides ONE Postgres transaction (via
/// [`crate::iceberg_writer::append_batches_on_tx`], Task 4's caller-tx commit), so a
/// rolled-back attempt frees the offset run and orphans the Parquet, and a retry
/// re-allocates + re-writes fresh files.
///
/// NOTE: this holds a Postgres transaction across the object-store Parquet write for
/// the bulk stream path — the accepted tradeoff per the full-atomic-parity decision.
/// The common inline/flush path is untouched.
#[expect(
    clippy::too_many_arguments,
    reason = "same cohesive routing/payload/behavior params as `land` (pool, catalog, table, \
              columns, batches, lineage, stream decl) plus the downstream jobs threaded into \
              the commit"
)]
async fn land_parquet_stream(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
    decl: &StreamDecl,
    jobs: &[control_plane_core::NewJob],
) -> Result<SnapshotId> {
    // Concatenate the write's batches: offsets are assigned in row order across the
    // whole write, and the framing columns are stamped onto the one batch.
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| ControlPlaneError::Validation("stream land: no batches".into()))?;
    let concat = concat_batches(&schema, &batches).map_err(backend)?;
    let n = concat.num_rows();

    // The Iceberg table, created WITH framing so its physical schema (and thus the
    // mirror, via `columns_of` on the commit) carries loom_change_kind/bucket/offset.
    // Idempotent + outside the tx (a rolled-back retry leaves the created table for
    // the next attempt, exactly as the batch path leaves it). `include_framing = true`
    // is safe here because `claim_stream_mode` has already COMMITTED this table's
    // stream mode — a concurrent creator reads the same decision, so the create
    // converges instead of diverging (#623). A batch table asked to convert never
    // reaches this line: the claim refuses it first. The `reconcile_stream_mode` call
    // below is now the idempotent redeclare arm.
    ensure_iceberg_table(catalog, table, columns, true).await?;
    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let mut ice_table = catalog.load_table(&ident).await.map_err(backend)?;
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema())
            .map_err(backend)?,
    );
    let full_columns = augment_with_framing(columns, true);

    // Atomic attempt loop, reusing the batch path's retry budget/backoff.
    let mut attempt: u32 = 0;
    loop {
        let mut tx = pool.begin().await.map_err(backend)?;

        // ONE snapshot for the whole write: allocated up front (offset allocation keys off
        // the mirror row it seeds), then REUSEd by the commit (`CommitExtras.reuse_snapshot`).
        let at = next_snapshot(&mut tx, None).await?;
        // `pre_existing` = `!created`. After `claim_stream_mode` this is normally
        // `true` (the claim committed the row), so a declaration lands on
        // `reconcile_stream_mode`'s idempotent redeclare arm rather than a
        // first-declare. The witness is kept honest here regardless, so this tx does
        // not depend on the claim having run.
        let (tid, created) =
            ensure_table_witnessed(&mut tx, &table.schema, &table.name, at).await?;

        // Reconcile stream mode on THIS tx (shared with `inline_append`). A rejected
        // convert (`Validation`) or bucket mismatch (`Conflict`) is terminal — roll
        // back and return, never retry.
        let effective =
            match crate::stream::reconcile_stream_mode(&mut tx, tid, decl, !created, table, at)
                .await
            {
                Ok(e) => e,
                Err(e) => {
                    drop(tx.rollback().await);
                    return Err(e);
                }
            };
        let Some(bc) = effective else {
            // Unreachable: this fn is only entered for a stream write, and reconcile
            // only returns `None` for a batch table. Fail loudly rather than write a
            // framing-less commit into the framing-schema'd Iceberg table.
            drop(tx.rollback().await);
            return Err(ControlPlaneError::Backend(
                "stream land: reconcile returned batch mode for a stream write".into(),
            ));
        };

        let (buckets, offsets) = allocate_row_framing(&mut tx, tid, bc, n).await?;

        // Stamp framing + rewrap under the table's field-id schema (which carries the
        // framing fields), exactly like the batch path's `coerce_batch_to_ice`.
        let stamped = stamp_framing(&concat, &buckets, &offsets)?;
        let coerced = coerce_batch_to_ice(&stamped, &ice_arrow, &full_columns)?;

        let extras = CommitExtras {
            lineage: Some(&lineage),
            data_trigger_tables: std::slice::from_ref(table),
            reuse_snapshot: Some(at),
            jobs,
            ..CommitExtras::default()
        };
        match crate::iceberg_writer::append_batches_on_tx(
            catalog,
            &ice_table,
            vec![coerced],
            extras,
            &mut tx,
        )
        .await
        {
            Ok(_) => {
                // Offsets + snapshot + framing-mirror durable together.
                tx.commit().await.map_err(backend)?;
                return Ok(at);
            }
            Err(e)
                if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < crate::iceberg_writer::COMMIT_MAX_RETRIES =>
            {
                // Roll back: frees the reserved offset run + the seed snapshot/table
                // row; the just-written Parquet is orphaned (fresh UUID next attempt).
                drop(tx.rollback().await);
                tokio::time::sleep(crate::iceberg_writer::commit_backoff(
                    ice_table.identifier(),
                    attempt,
                    None,
                ))
                .await;
                ice_table = catalog.load_table(&ident).await.map_err(backend)?;
                attempt += 1;
            }
            Err(e) => {
                drop(tx.rollback().await);
                return Err(backend(e));
            }
        }
    }
}

/// Assign every row of a stream write its `(bucket, offset)` framing pair, reserving
/// the offset runs on the caller's transaction.
///
/// Per-row bucket is `row_index % bc` (`bc >= 1` by `reconcile_stream_mode`, so the
/// modulo is panic-free). Rows are counted per bucket, a contiguous offset run is
/// reserved for each TOUCHED bucket on `tx` — so the allocation commits iff the
/// caller's snapshot does — and each row then takes the next offset from its bucket's
/// cursor. The k-th row of bucket b gets `first_b + k`. This is the exact scheme
/// `iceberg_inline::inline_append` uses, so the inline and direct-write paths cannot
/// drift on offset semantics.
///
/// Returns `(buckets, offsets)`, both aligned to the concatenated batch in row order
/// and both of length `n` — the shape [`stamp_framing`] requires.
///
/// Extracted from [`land_parquet_stream`]'s attempt loop, which was over the
/// complexity register's MI threshold; the arithmetic and the allocation order are
/// unchanged.
async fn allocate_row_framing(
    tx: &mut sqlx::PgConnection,
    tid: i64,
    bc: i32,
    n: usize,
) -> Result<(Vec<i32>, Vec<i64>)> {
    let oor = || ControlPlaneError::Backend("bucket index out of range".into());
    let bc_usize = usize::try_from(bc).map_err(|e| {
        ControlPlaneError::Backend(format!("invalid stream bucket count {bc}: {e}").into())
    })?;

    // Rows are handed to buckets round-robin (`row % bc`), so the count per bucket is
    // closed-form: bucket b takes `n / bc` rows, plus one more iff `b < n % bc`. (The
    // previous counting loop computed exactly this.) `bc_usize >= 1`, so no div-by-zero.
    #[expect(
        clippy::integer_division,
        reason = "truncation is the point: the floor is each bucket's guaranteed share, \
                  and `remainder` below hands the dropped rows to the first buckets"
    )]
    let per_bucket = n / bc_usize;
    let remainder = n % bc_usize;

    let mut cursor = vec![0i64; bc_usize];
    for (b, slot) in cursor.iter_mut().enumerate() {
        let count = i64::try_from(per_bucket + usize::from(b < remainder)).map_err(|e| {
            ControlPlaneError::Backend(format!("bucket row count overflowed i64: {e}").into())
        })?;
        if count > 0 {
            let b_i32 = i32::try_from(b).map_err(|e| {
                ControlPlaneError::Backend(format!("bucket index overflowed i32: {e}").into())
            })?;
            *slot = crate::stream::pg_allocate_offset(&mut *tx, tid, b_i32, count).await?;
        }
    }

    let mut buckets: Vec<i32> = Vec::with_capacity(n);
    let mut offsets: Vec<i64> = Vec::with_capacity(n);
    for row in 0..n {
        let b = row % bc_usize;
        let slot = cursor.get_mut(b).ok_or_else(oor)?;
        let off = *slot;
        *slot += 1;
        buckets.push(i32::try_from(b).map_err(|e| {
            ControlPlaneError::Backend(format!("bucket index overflowed i32: {e}").into())
        })?);
        offsets.push(off);
    }
    Ok((buckets, offsets))
}

/// Append the three log-framing columns to `batch` in fixed order —
/// `loom_change_kind` (all `"+I"`), `loom_bucket`, `loom_offset` — under a schema
/// extended with the framing fields. The caller rewraps the result under the table's
/// field-id schema via [`coerce_batch_to_ice`]. `buckets`/`offsets` align to `batch`'s
/// rows in order.
fn stamp_framing(batch: &RecordBatch, buckets: &[i32], offsets: &[i64]) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    // Derive the framing fields from `framing_column_specs` (the single source of
    // truth for the reserved log columns) so a future edit there propagates here:
    // map each spec's logical type to its Arrow `DataType` via `BaseType`. The arrays
    // pushed below align to this order (change_kind, bucket, offset).
    for spec in framing_column_specs() {
        let dt = control_plane_core::resolve_logical(&spec.ty)
            .ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!("stamp_framing: unknown framing type {:?}", spec.ty).into(),
                )
            })?
            .arrow_data_type();
        fields.push(Field::new(&spec.name, dt, spec.nullable));
    }

    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    cols.push(Arc::new(StringArray::from(vec!["+I"; n])));
    cols.push(Arc::new(Int32Array::from(buckets.to_vec())));
    cols.push(Arc::new(Int64Array::from(offsets.to_vec())));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(backend)
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
///
/// **Refuses a declared stream/CDC target** (`pg_refuse_stream_target`): overwriting an
/// offset-framed log is not a legitimate operation from any caller here — see
/// [`overwrite_stream_base`], the CDC consolidate fold's private door.
pub async fn overwrite_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    jobs: &[NewJob],
) -> Result<SnapshotId> {
    overwrite_with_cap(
        pool, catalog, table, columns, batches, lineage, jobs, None, false,
    )
    .await
}

/// The **consuming** overwrite/replace commit primitive: identical to
/// [`overwrite_parquet_snapshot`] except the inline end-cap is TARGETED rather
/// than blanket. `consumed` names exactly the inline rows the caller folded into
/// `batches` (the consolidation fold, later tasks); only those rows are retired —
/// every other live inline row of the table SURVIVES the commit and keeps
/// shadowing the new base, including a mutation that landed mid-consolidation
/// (after the fold read its input snapshot but before this commit lands). Carries
/// no downstream jobs of its own — consolidation has none; the vector-index
/// rebuild jobs are still computed and enqueued internally, exactly as the plain
/// overwrite does.
///
/// **Refuses a declared stream/CDC target**, exactly like [`overwrite_parquet_snapshot`]:
/// the COW identity fold (its only caller) supplies user-columns-only batches and must
/// never land on an offset-framed log.
pub async fn overwrite_parquet_snapshot_consuming(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    consumed: InlineEndCap<'_>,
) -> Result<SnapshotId> {
    overwrite_with_cap(
        pool,
        catalog,
        table,
        columns,
        batches,
        lineage,
        &[],
        Some(consumed),
        false,
    )
    .await
}

/// The CDC consolidate fold's private door: the ONLY legitimate overwrite of a declared
/// stream table. `batches` MUST carry the framing columns (`loom_change_kind` /
/// `loom_bucket` / `loom_offset`).
///
/// This bypasses the `pg_refuse_stream_target` check the two public entrypoints carry, so
/// it is the one path that can remove a stream table's offsets — and therefore the one
/// overwrite whose end-cap can be blocked by the MV floor (`EndCapIntent::Removing` →
/// `guard_end_cap`). `consolidate_table` pre-checks that with `mv_floor::removal_blocked`
/// and skips cleanly rather than erroring.
pub async fn overwrite_stream_base(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    consumed: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    overwrite_with_cap(
        pool,
        catalog,
        table,
        columns,
        batches,
        lineage,
        &[],
        consumed,
        true,
    )
    .await
}

/// Shared body of [`overwrite_parquet_snapshot`],
/// [`overwrite_parquet_snapshot_consuming`] and [`overwrite_stream_base`] — everything is
/// identical between them (rebuild-job computation, the zero-file/real-file branch split,
/// the `CommitExtras` shape) except whether the inline end-cap is blanket (`None`,
/// plain overwrite — every live inline row retires) or targeted (`Some`,
/// consuming overwrite — only the named rows retire, everything else survives), and
/// whether the batches are already stream-FRAMED (`framed`, the CDC fold's door) or
/// user-columns-only (every other caller, which is refused a stream target outright).
#[expect(
    clippy::too_many_arguments,
    reason = "one cohesive commit call: routing target (pool/catalog/table/columns), payload \
              (batches), behavior (lineage/jobs), the blanket-vs-targeted inline cap \
              (consumed), and whether the payload carries stream framing (framed — the CDC \
              fold's door, which is also what waives the stream-target refusal); the three \
              public entrypoints differ only in the last three args"
)]
async fn overwrite_with_cap(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    jobs: &[NewJob],
    consumed: Option<InlineEndCap<'_>>,
    framed: bool,
) -> Result<SnapshotId> {
    // Refuse a declared stream/CDC target. The framing-preserving overwrite exists for
    // exactly ONE caller — the CDC consolidate fold, via `overwrite_stream_base`. Every
    // other caller supplies user-columns-only batches; letting one land on a framed table
    // either destroys the offset range (the truncate branch) or panics in
    // `coerce_batch_to_ice` (the batch is three columns short of the framed schema).
    //
    // Pre-flight, on its own connection: the non-empty branch's commit tx is opened deep
    // inside `append_parquet_snapshot`. A stream declaration is never removed, so the only
    // window is "declared WHILE an overwrite is in flight". The truncate branch re-checks
    // INSIDE its own tx, atomically.
    if !framed {
        let mut conn = pool.acquire().await.map_err(backend)?;
        crate::stream::pg_refuse_stream_target(&mut conn, table).await?;
    }
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;
    let mut all_jobs = rebuild_jobs;
    all_jobs.extend_from_slice(jobs); // action downstream jobs; pg_insert_if_absent dedups
    if batches.iter().all(|b| b.num_rows() == 0) {
        return overwrite_truncate(pool, table, lineage, &all_jobs, consumed, framed).await;
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
            end_cap: consumed,
            jobs: &all_jobs,
            data_trigger_tables: std::slice::from_ref(table),
            // A stream base's fold REMOVES offsets from the live set; a plain overwrite
            // removes rows from a table no MV can read. Either way: Removing.
            intent: EndCapIntent::Removing,
            ..CommitExtras::default()
        },
        // With the refusal above in place, `framed` IS "the target is a declared stream
        // table" — a non-framed overwrite can no longer reach one, so the old
        // `include_framing` registry lookup collapses into this flag.
        framed,
    )
    .await
}

/// The zero-file branch of [`overwrite_with_cap`]: in one Postgres tx allocate
/// a mirror snapshot, ensure the table row, end-cap every live data file at that
/// snapshot, and emit `lineage`. Touches no object storage and no Iceberg metadata —
/// loom reads resolve `current_snapshot` through `iceberg_mirror.snapshot`, so the
/// truncation is immediately visible and prior snapshots still time-travel.
///
/// `consumed`, when `Some`, retires exactly those inline rows
/// (`end_cap_inline_rows_by_id`) instead of blanket-capping every live inline
/// row — same targeted-vs-blanket rule as the non-empty branch, so a truncating
/// consolidation fold (all input rows folded into nothing, e.g. a full delete)
/// still leaves an unrelated concurrent mutation's inline row live.
///
/// `framed` is the CDC fold's waiver of the stream-target refusal (see
/// [`overwrite_with_cap`]). When it is false this re-checks `pg_refuse_stream_target`
/// **inside the truncate's own transaction**, so the refusal is atomic with the
/// delete-all it prevents — this is the branch that, unguarded, end-caps every live file
/// and every live inline row of an offset-framed log.
async fn overwrite_truncate(
    pool: &PgPool,
    table: &TableRef,
    lineage: Option<&LineageEvent>,
    jobs: &[NewJob],
    consumed: Option<InlineEndCap<'_>>,
    framed: bool,
) -> Result<SnapshotId> {
    use crate::iceberg_inline::{end_cap_inline_rows_by_id, end_cap_live_inline_rows};
    use crate::iceberg_mirror::{end_cap_live_data_files, ensure_table, next_snapshot};
    use crate::lineage::pg_emit;

    let mut tx = pool.begin().await.map_err(backend)?;
    let conn = &mut *tx;
    // In-tx, so the refusal is atomic with the truncate it prevents.
    if !framed {
        crate::stream::pg_refuse_stream_target(conn, table).await?;
    }
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    // REMOVING (all three caps): a truncating overwrite writes NO files back — every
    // offset it end-caps leaves the live set for good, so an MV that has not read them
    // would lose them. The floor refuses exactly that.
    end_cap_live_data_files(conn, table, tid, at, &EndCapIntent::Removing).await?;
    match &consumed {
        Some(cap) => {
            end_cap_inline_rows_by_id(
                conn,
                table,
                cap.table_id,
                cap.row_ids,
                at,
                &EndCapIntent::Removing,
            )
            .await?;
        }
        None => end_cap_live_inline_rows(conn, table, tid, at, &EndCapIntent::Removing).await?,
    }
    if let Some(ev) = lineage {
        pg_emit(&mut *conn, ev).await?;
    }
    for job in jobs {
        // Same pending-dedup as CommitExtras.jobs; pg_notify is buffered until
        // this tx commits, so a rolled-back truncate enqueues nothing.
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }
    crate::transforms::pg_fire_data_triggers(
        conn,
        std::slice::from_ref(table),
        lineage.map(|ev| ev.run_id.0),
    )
    .await?;
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

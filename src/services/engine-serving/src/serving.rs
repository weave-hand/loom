//! loom-native, DataFusion-backed execution for Iceberg tables.
//! Reads the `iceberg_mirror` projection (via `IcebergCatalog`), registers each
//! live table's Parquet files (absolute paths: `file://` or `s3://`) as a DataFusion
//! table, and runs the governed/compiled SQL through DataFusion.
//! See docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use control_plane_core::snapshot::StatValue;
use control_plane_core::{SnapshotId, TableRef, resolve_logical};
use control_plane_postgres::iceberg_catalog::{FileWithStats, IcebergCatalog};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{MemorySchemaProvider, Session, TableProvider};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::scalar::ScalarValue;
use datafusion_io::object_store_url_for;
use object_store::local::LocalFileSystem;
use store_config::ServingStore;

use crate::provider::PgTableProvider;

/// Any execution/mirror/DataFusion error → opaque engine-serving error.
#[derive(Debug, thiserror::Error)]
pub enum EngineServingError {
    #[error("engine serving: {0}")]
    Engine(String),
    /// The SQL failed DataFusion *planning* (`ctx.sql(...)`) — a parse/logical-plan
    /// fault in the statement itself: the client's error class, never the engine's.
    /// Wire callers map this to `invalid_argument` (query-api surfaces 400);
    /// execution/stream/catalog faults stay [`Engine`](Self::Engine) (internal/500).
    /// Classified conservatively: ONLY the `ctx.sql()` call sites construct it.
    #[error("query planning failed: {0}")]
    Plan(#[source] datafusion::error::DataFusionError),
    /// No vector index has been built for the requested (table, column) at the
    /// current snapshot. Callers should surface this as a 404/not-found, never
    /// panic. See FUTURE `fut-inline-vector-hot-delta`.
    #[error("no vector index: {0}")]
    NoIndex(String),
    /// The query vector's length does not match the index's declared dimension.
    /// Callers should surface this as a 400/bad-request.
    #[error("dimension mismatch: {0}")]
    DimMismatch(String),
    /// An inline-delta CAS lost a race: the identity's live version had already
    /// moved past `expected_version` by the time the write was attempted. Callers
    /// should surface this as a retryable conflict, never a generic 500.
    #[error("conflict: {0}")]
    Conflict(String),
}

/// Any error (mirror/Postgres, DataFusion, object_store, URL) -> opaque engine-serving error.
/// WARNING: class-erasing — never use this on a `ctx.sql()` planning fault
/// (that is `EngineServingError::Plan`, the client-fault class).
pub(crate) fn to_serving<E: std::fmt::Display>(e: E) -> EngineServingError {
    EngineServingError::Engine(e.to_string())
}

/// Build the combined serving `TableProvider` for `table` at its live snapshot:
/// the pruning-aware file provider UNION-ALL the inline PG provider (either alone,
/// or `None` when the table has no live data). Registers the needed object store(s)
/// on `ctx` (idempotent). Factored out of `register_iceberg_table` so the governed
/// path (`execute_governed_sql_stream`) can wrap the same relation.
pub async fn build_serving_provider(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&ServingStore>,
    at: Option<SnapshotId>,
) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError> {
    use control_plane_core::Catalog;

    // Local store for absolute file:// warehouse paths (back-compat default).
    // Idempotent across calls on one ctx.
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    // S3 store for s3:// warehouse paths, registered under s3://{bucket}.
    if let Some(ServingStore { bucket, store }) = serving_store {
        let url = ObjectStoreUrl::parse(format!("s3://{bucket}")).map_err(to_serving)?;
        ctx.register_object_store(url.as_ref(), store.clone());
    }

    let snap_id = match at {
        None => {
            catalog
                .current_snapshot(table)
                .await
                .map_err(to_serving)?
                .id
        }
        Some(id) => id,
    };
    // The MIRROR is authoritative for the served schema at `snap_id`. For an
    // as-of read (`at = Some`), a table not live at `snap_id` (created later) is
    // simply skipped: `schema` NotFound -> Ok(None). For the current path (`at =
    // None`), `snap_id` came from `current_snapshot`, so this never NotFounds.
    let table_schema = match catalog.schema(table, snap_id).await {
        Ok(s) => s,
        Err(control_plane_core::ControlPlaneError::NotFound(_)) if at.is_some() => {
            return Ok(None);
        }
        Err(e) => return Err(to_serving(e)),
    };
    // The MIRROR is authoritative for the served schema (not per-file Parquet
    // footers): an additively-evolved table presents its superset, and files written
    // before a newer column existed are null-filled by DataFusion's default schema
    // adapter (the column is nullable). File-backed data uses the pruning-aware
    // provider over the mirror's per-column stats; its `scan` skips files a query's
    // predicates provably cannot match.
    let schema = arrow_schema_from_mirror(&table_schema.columns)?;

    // Resolve the type's identity column (if any). An identity-bearing type serves an
    // identity-dedup MERGE (inline deltas shadow file rows by precedence, and a
    // tombstone winner hides the id); an identity-less type keeps the additive union.
    let identity = control_plane_postgres::ontology::identity_for_table(&catalog.pool, table)
        .await
        .map_err(to_serving)?;
    // For an identity-bearing table, resolve CDC-ness AND the declared merge
    // engine in one lookup. A `kind='cdc'` base's flushed file tier can carry
    // MULTIPLE physical rows (and `-D` tombstones) per identity, so its fold uses
    // `loom_offset` precedence (`Precedence::Offset`) with the table's engine.
    let cdc_meta = if identity.is_some() {
        control_plane_postgres::ontology::stream_meta_for_table(&catalog.pool, table)
            .await
            .map_err(to_serving)?
            .filter(|m| m.kind == control_plane_core::StreamKind::Cdc)
    } else {
        None
    };
    let is_cdc = cdc_meta.is_some();

    let files_with_stats = catalog
        .files_with_stats(table, snap_id)
        .await
        .map_err(to_serving)?;
    // A CDC identity table's physical Parquet files carry the reserved framing
    // columns (`loom_change_kind`, `loom_offset`) alongside the user columns
    // (`augment_with_framing`); expose them under their physical names so the
    // CDC-aware fold below can read them. Every other table's file provider keeps
    // the plain mirror data schema — byte-identical to before this branch existed.
    let file_schema = if is_cdc {
        with_cdc_framing_fields(&schema)
    } else {
        schema.clone()
    };
    let file_provider = if files_with_stats.is_empty() {
        None
    } else {
        Some(IcebergMirrorTableProvider::try_new_with_schema(
            files_with_stats,
            file_schema,
        ))
    };

    // Inline rows (mirror-only typed rows) are served DIRECTLY from Postgres via
    // a PG TableProvider that pushes filter/limit/projection into a per-query
    // SELECT — no Arrow->Parquet->Arrow round-trip. The snapshot is baked into a
    // base predicate so MVCC visibility matches `inline_live_batch`.
    let inline_provider = build_inline_provider(
        catalog,
        table,
        &schema,
        &table_schema.columns,
        snap_id,
        identity.as_deref(),
    )
    .await?;

    // Combine. Identity-less: file-only, inline-only, or an additive UNION ALL of both
    // (the file provider presents the mirror schema; the union's nullability-widening
    // is defensive here — names + datatypes match because both derive from the same
    // table schema). Identity-bearing: a plain file provider when there are no live
    // inline rows AND the table is not CDC (file rows are already identity-unique),
    // else the identity-dedup merge view (CDC uses `loom_offset` precedence; non-CDC
    // uses the MVCC `begin_snapshot`/`loom_tombstone` precedence).
    let provider: Arc<dyn datafusion::catalog::TableProvider> = match identity.as_deref() {
        None => match (file_provider, inline_provider) {
            (Some(f), Some(i)) => {
                let file_view: Arc<dyn TableProvider> = Arc::new(f);
                let df = ctx
                    .read_table(file_view)
                    .map_err(to_serving)?
                    .union(ctx.read_table(Arc::new(i)).map_err(to_serving)?)
                    .map_err(to_serving)?;
                df.into_view()
            }
            (Some(f), None) => Arc::new(f),
            (None, Some(i)) => Arc::new(i),
            (None, None) => return Ok(None), // a live table with no data; nothing to register
        },
        Some(id) => match (file_provider, inline_provider) {
            // Identity but no live inline rows: for a non-CDC table file rows are
            // already identity-unique, so return the plain file provider (cheaper,
            // schema preserved trivially). A CDC table's flushed base can legitimately
            // carry multiple physical rows (and `-D` tombstones) per identity even
            // with no live inline tail, so it still routes through the fold.
            (Some(f), None) if !is_cdc => Arc::new(f),
            (Some(f), None) => build_merge_view(
                ctx,
                &schema,
                id,
                Some(f),
                None,
                offset_precedence(&catalog.pool, cdc_meta.as_ref(), table).await?,
            )?,
            (None, None) => return Ok(None),
            // Identity + inline present (with or without a file tier): dedup by
            // identity, using CDC's `loom_offset` precedence when the table is a
            // declared CDC stream, else the MVCC precedence merge.
            (file_opt, Some(i)) => {
                let precedence = if is_cdc {
                    offset_precedence(&catalog.pool, cdc_meta.as_ref(), table).await?
                } else {
                    Precedence::Snapshot
                };
                build_merge_view(ctx, &schema, id, file_opt, Some(i), precedence)?
            }
        },
    };

    Ok(Some(provider))
}

/// Extend `schema` with a CDC base's reserved physical framing columns
/// (`loom_change_kind`, `loom_offset`) under their physical names, appended AFTER
/// the mirror data columns. Both columns are present in a CDC table's physical
/// Parquet schema (`augment_with_framing`) for every file, since a table can only
/// ever be DECLARED `kind='cdc'` before its first write (`reconcile_stream_mode`
/// rejects a batch->stream conversion) — so every live file already carries them.
/// Used by [`build_serving_provider`] to make the file tier's precedence/tombstone
/// columns visible to the CDC-aware fold ([`Precedence::Offset`]).
fn with_cdc_framing_fields(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("loom_change_kind", DataType::Utf8, false));
    fields.push(Field::new("loom_offset", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

/// Which per-identity precedence + tombstone predicate [`build_merge_view`] folds
/// on. The two physical tiers (file, inline) always carry the SAME pair of
/// framing columns for a given mode — only which columns those are, and how a
/// delete is recognized, differs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Precedence {
    /// Non-CDC identity tables (unchanged): file tier synthesizes precedence `0`
    /// + `false` tombstone; inline tier uses `begin_snapshot` / `loom_tombstone`.
    Snapshot,
    /// CDC tables: both tiers carry real `loom_offset`/`loom_change_kind` framing.
    /// `engine` selects the winner ordering; `version_col` is set only for
    /// `Versioned` (the quoted domain version column the window orders by).
    Offset {
        engine: control_plane_core::MergeEngine,
        version_col: Option<String>,
    },
}

/// Build the identity-dedup merge view for an identity-bearing type. Each tier
/// (file, inline — either may be absent) is projected to `[<data_cols>,
/// _loom_prec, _loom_tomb]` per `precedence`'s column mapping, UNION-ALL'd, then
/// deduped per identity keeping the greatest precedence; a tombstoned winner is
/// dropped, and the result is projected back to EXACTLY the mirror data `schema`.
///
/// Equivalent to (the reference SQL): for identity column `<id>`,
/// ```sql
/// SELECT <data_cols> FROM (
///   SELECT <data_cols>, _loom_tomb,
///          ROW_NUMBER() OVER (PARTITION BY <id> ORDER BY _loom_prec DESC) AS _loom_rn
///   FROM ( <file tier>  UNION ALL  <inline tier> )
/// ) WHERE _loom_rn = 1 AND _loom_tomb = false
/// ```
/// `Precedence::Snapshot`'s file tier is `<0, false>`; its inline tier is
/// `<begin_snapshot, loom_tombstone>`. `Precedence::Offset`'s file AND inline
/// tiers are both `<loom_offset, loom_change_kind = '-D'>` — the same fold
/// `consolidate.rs` uses to physically collapse a CDC base.
///
/// A `ROW_NUMBER()` window (not `DISTINCT ON`) is used deliberately: the window is a
/// pass-through over the data columns, so they keep their mirror `DataType` AND
/// nullability end to end (union coerces identical schemas to themselves; window /
/// filter / final projection are pass-through). Thus the final projection needs no
/// casts and the view's schema equals `schema` exactly — which the governed layer
/// and callers require. (`DISTINCT ON` would widen the identity column to nullable.)
/// Build the CDC `Precedence::Offset` from the table's stream meta, fetching the
/// version column live for the Versioned engine.
async fn offset_precedence(
    pool: &sqlx::PgPool,
    meta: Option<&control_plane_core::StreamMeta>,
    table: &TableRef,
) -> Result<Precedence, EngineServingError> {
    let engine = meta
        .map(|m| m.merge_engine)
        .unwrap_or(control_plane_core::MergeEngine::LastRow);
    let version_col = if matches!(engine, control_plane_core::MergeEngine::Versioned) {
        Some(
            control_plane_postgres::ontology::version_for_table(pool, table)
                .await
                .map_err(to_serving)?
                .ok_or_else(|| {
                    EngineServingError::Engine(format!(
                        "cdc table {}.{} is merge_engine=versioned but has no version column",
                        table.schema, table.name
                    ))
                })?,
        )
    } else {
        None
    };
    Ok(Precedence::Offset {
        engine,
        version_col,
    })
}

fn build_merge_view(
    ctx: &SessionContext,
    schema: &SchemaRef,
    identity: &str,
    file: Option<IcebergMirrorTableProvider>,
    inline: Option<PgTableProvider>,
    precedence: Precedence,
) -> Result<Arc<dyn TableProvider>, EngineServingError> {
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::{ExprFunctionExt, lit};

    // Case-preserving unqualified column reference. DataFusion's `col()` parses its
    // argument as a SQL identifier and LOWERCASES unquoted names (e.g. `col("unitPrice")`
    // resolves to a nonexistent `unitprice`), which would fail every mixed-case mirror
    // column. `Column::new_unqualified` takes the name verbatim, so the merge view's
    // schema equals the mirror data schema exactly — camelCase names preserved — for
    // ALL identity types. (The helper column names are lowercase, but routing them
    // through the same builder keeps every reference consistent.)
    let cref = |name: &str| Expr::Column(Column::new_unqualified(name));

    // Mirror data columns in order — the exact output projection.
    let data_cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Project a tier down to [<data_cols>, _loom_prec, _loom_tomb, (_loom_off)], so
    // both tiers union under one schema regardless of which physical columns fed
    // them. `_loom_off` (the physical `loom_offset`) is projected only for Offset
    // precedence, where the Versioned engine's window tie-break orders by it
    // (Snapshot/LastRow/FirstRow never reference it; it is dropped by the final
    // projection).
    let tier_select = |prec: Expr, tomb: Expr, off: Option<Expr>| -> Vec<Expr> {
        let mut v: Vec<Expr> = data_cols.iter().map(|n| cref(n.as_str())).collect();
        v.push(prec.alias("_loom_prec"));
        v.push(tomb.alias("_loom_tomb"));
        if let Some(o) = off {
            v.push(o.alias("_loom_off"));
        }
        v
    };

    // The precedence expr projected as `_loom_prec` — the column the per-identity
    // window orders by. Snapshot synthesizes a literal 0 (the file tier); LastRow
    // and FirstRow order by loom_offset; Versioned orders by the quoted domain
    // version column (a user column, case-preserving via Column::new_unqualified).
    let prec_expr: Expr = match &precedence {
        Precedence::Snapshot => lit(0_i64),
        Precedence::Offset {
            engine,
            version_col,
        } => match engine {
            control_plane_core::MergeEngine::Versioned => {
                let vcol = version_col.as_deref().ok_or_else(|| {
                    EngineServingError::Engine(
                        "Versioned precedence requires a version column".into(),
                    )
                })?;
                Expr::Column(Column::new_unqualified(vcol))
            }
            control_plane_core::MergeEngine::LastRow
            | control_plane_core::MergeEngine::FirstRow => cref("loom_offset"),
        },
    };

    // Project each tier to [<data_cols>, _loom_prec, _loom_tomb]. Both tiers use
    // the SAME prec_expr (cloned) and the SAME tombstone mapping; only Snapshot
    // differs (file tier synthesizes <0, false>; inline uses begin_snapshot /
    // loom_tombstone). Offset's tombstone is `loom_change_kind = '-D'` (delete is
    // uniform across all engines) for BOTH tiers — unchanged from today.
    let (file_prec, file_tomb) = match &precedence {
        Precedence::Snapshot => (lit(0_i64), lit(false)),
        Precedence::Offset { .. } => (prec_expr.clone(), cref("loom_change_kind").eq(lit("-D"))),
    };
    let (inline_prec, inline_tomb) = match &precedence {
        Precedence::Snapshot => (cref("begin_snapshot"), cref("loom_tombstone")),
        Precedence::Offset { .. } => (prec_expr.clone(), cref("loom_change_kind").eq(lit("-D"))),
    };

    // `_loom_off` is projected only for Offset precedence (the Versioned engine's
    // window tie-break orders by it). Computed once, cloned into each tier.
    let off: Option<Expr> =
        matches!(&precedence, Precedence::Offset { .. }).then(|| cref("loom_offset"));
    let file_df = match file {
        Some(f) => Some(
            ctx.read_table(Arc::new(f))
                .map_err(to_serving)?
                .select(tier_select(file_prec, file_tomb, off.clone()))
                .map_err(to_serving)?,
        ),
        None => None,
    };
    let inline_df = match inline {
        Some(i) => Some(
            ctx.read_table(Arc::new(i))
                .map_err(to_serving)?
                .select(tier_select(inline_prec, inline_tomb, off))
                .map_err(to_serving)?,
        ),
        None => None,
    };
    let unioned = match (file_df, inline_df) {
        (Some(f), Some(i)) => f.union(i).map_err(to_serving)?,
        (Some(f), None) => f,
        (None, Some(i)) => i,
        (None, None) => {
            return Err(EngineServingError::Engine(
                "build_merge_view: neither a file nor an inline tier".into(),
            ));
        }
    };

    // Per-identity window: rank 1 is the winner. Direction + keys come from the
    // engine: LastRow/Versioned order _loom_prec DESC (greatest precedence wins);
    // FirstRow orders ASC (smallest offset wins). Versioned adds a _loom_off
    // (loom_offset) DESC tie-break (last-write-within-version wins). Snapshot is
    // unchanged.
    let order_keys = match &precedence {
        Precedence::Snapshot => vec![cref("_loom_prec").sort(false, false)],
        Precedence::Offset { engine, .. } => match engine {
            control_plane_core::MergeEngine::LastRow
            | control_plane_core::MergeEngine::Versioned => {
                let mut keys = vec![cref("_loom_prec").sort(false, false)];
                if matches!(engine, control_plane_core::MergeEngine::Versioned) {
                    keys.push(cref("_loom_off").sort(false, false));
                }
                keys
            }
            control_plane_core::MergeEngine::FirstRow => {
                vec![cref("_loom_prec").sort(true, false)]
            }
        },
    };
    let ranked = row_number()
        .partition_by(vec![cref(identity)])
        .order_by(order_keys)
        .build()
        .map_err(to_serving)?
        .alias("_loom_rn");
    // Keep the winner per identity, hide tombstoned winners, then project back to
    // the mirror data schema — UNCHANGED (the version column is a user column
    // already in data_cols; the _loom_* helpers are dropped).
    let merged = unioned
        .window(vec![ranked])
        .map_err(to_serving)?
        .filter(cref("_loom_rn").eq(lit(1_u64)))
        .map_err(to_serving)?
        .filter(cref("_loom_tomb").eq(lit(false)))
        .map_err(to_serving)?
        .select(
            data_cols
                .iter()
                .map(|n| cref(n.as_str()))
                .collect::<Vec<_>>(),
        )
        .map_err(to_serving)?;
    Ok(merged.into_view())
}

/// Register `table`'s live data files (at its current snapshot) via the pruning-aware
/// `IcebergMirrorTableProvider` under the schema-qualified name `"schema"."table"`, so
/// the compiled read SQL resolves it. Files are registered by their absolute paths
/// (`file://` or `s3://`) as stored in the mirror (`iceberg_mirror.data_file.path`);
/// the provider's `scan` skips files a query's predicates provably cannot match.
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&ServingStore>,
    at: Option<SnapshotId>,
) -> Result<(), EngineServingError> {
    let Some(provider) = build_serving_provider(ctx, catalog, table, serving_store, at).await?
    else {
        return Ok(());
    };
    register_qualified(ctx, &table.schema, &table.name, provider)
}

/// Ensure `schema` exists in `ctx`'s default `datafusion` catalog (creating it if
/// absent), then register `provider` under the schema-qualified name
/// `"schema"."name"` so `"schema"."name"` references resolve. Shared by the
/// unguarded `register_iceberg_table` and the governed path
/// (`execute_governed_sql_stream`), which register the same shape of table under
/// either a raw or a `GovernedTableProvider`-wrapped provider.
pub(crate) fn register_qualified(
    ctx: &SessionContext,
    schema: &str,
    name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<(), EngineServingError> {
    let cat = ctx
        .catalog("datafusion")
        .ok_or_else(|| EngineServingError::Engine("no default datafusion catalog".into()))?;
    if cat.schema(schema).is_none() {
        cat.register_schema(schema, Arc::new(MemorySchemaProvider::new()))
            .map_err(to_serving)?;
    }
    ctx.register_table(TableReference::partial(schema, name), provider)
        .map_err(to_serving)?;
    Ok(())
}

/// Build the inline PG provider for `table` at `at`, or `None` when there is no
/// inline storage or no live inline rows (preserving the prior `inline_parquet`
/// `None` behavior). `schema` is the table's authoritative arrow schema (already
/// built by the caller); `cols` are the mirror column defs (for logical types).
///
/// When `identity` is `Some` the type serves an identity-dedup MERGE, so the
/// provider's schema is EXTENDED with four trailing columns, under their
/// physical names (every inline table carries all four regardless of stream
/// declaration — `iceberg_inline::inline_ddl`/`ensure_inline_schema`), so
/// `PgTableProvider`'s per-scan SELECT can resolve whichever pair
/// `build_merge_view`'s `Precedence` needs:
///   - `begin_snapshot` (i64 non-null) / `loom_tombstone` (bool non-null) — the
///     MVCC precedence a non-CDC identity table's merge uses;
///   - `loom_change_kind` (string non-null) / `loom_offset` (i64 nullable) — the
///     real per-identity framing a CDC table's merge uses instead.
///
/// When `identity` is `None` the schema stays data-only.
async fn build_inline_provider(
    catalog: &IcebergCatalog,
    table: &TableRef,
    schema: &SchemaRef,
    cols: &[control_plane_core::ColumnDef],
    at: control_plane_core::SnapshotId,
    identity: Option<&str>,
) -> Result<Option<PgTableProvider>, EngineServingError> {
    use control_plane_postgres::iceberg_inline::{has_live_inline_rows, inline_table_name};
    use control_plane_postgres::iceberg_mirror::live_table_id;

    let mut conn = catalog.pool.acquire().await.map_err(to_serving)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .map_err(to_serving)?
    else {
        return Ok(None);
    };
    if !has_live_inline_rows(&mut conn, table, at)
        .await
        .map_err(to_serving)?
    {
        return Ok(None);
    }
    drop(conn);

    // MVCC base predicate over the inline storage's snapshot columns. `at.0` is a
    // trusted integer; spliced via AssertSqlSafe in the provider (iceberg_inline
    // precedent).
    let base = format!(
        "begin_snapshot <= {0} and (end_snapshot is null or end_snapshot > {0}) \
         and (loom_change_kind is null or loom_change_kind <> '-U')",
        at.0
    );
    // Resolve every column's logical type ONCE at provider construction — an
    // unsupported type is rejected here, never mid-scan.
    let logical_types = cols
        .iter()
        .map(|c| {
            resolve_logical(&c.ty).ok_or_else(|| {
                EngineServingError::Engine(format!("unknown logical type `{}`", c.ty))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Merge mode: append BOTH precedence pairs (physical names) so the dedup can
    // rank rows and hide tombstoned identities under either `Precedence`. Data-only
    // otherwise. Unused columns for a given mode are never selected (DataFusion
    // projects only what the fold references), so this is a no-op cost for the
    // mode not in play.
    let (provider_schema, logical_types) = if identity.is_some() {
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new("begin_snapshot", DataType::Int64, false));
        fields.push(Field::new("loom_tombstone", DataType::Boolean, false));
        fields.push(Field::new("loom_change_kind", DataType::Utf8, false));
        fields.push(Field::new("loom_offset", DataType::Int64, true));
        let mut lts = logical_types;
        lts.push(control_plane_core::BaseType::Long);
        lts.push(control_plane_core::BaseType::Boolean);
        lts.push(control_plane_core::BaseType::String);
        lts.push(control_plane_core::BaseType::Long);
        (Arc::new(Schema::new(fields)) as SchemaRef, lts)
    } else {
        (schema.clone(), logical_types)
    };
    Ok(Some(PgTableProvider::new(
        catalog.pool.clone(),
        inline_table_name(tid),
        provider_schema,
        logical_types,
        Some(base),
    )))
}

/// Build the authoritative arrow schema for a table from the mirror's column
/// definitions (in column order). Each column's loom logical type resolves to an
/// arrow `DataType`; an unrecognized logical type is a hard error (the mirror should
/// never hold one). This schema — not the per-file Parquet footers — is what the
/// provider presents, so an additively-evolved table reads as its superset.
fn arrow_schema_from_mirror(
    cols: &[control_plane_core::ColumnDef],
) -> Result<SchemaRef, EngineServingError> {
    let fields = cols
        .iter()
        .map(|c| {
            let base = resolve_logical(&c.ty).ok_or_else(|| {
                EngineServingError::Engine(format!("unknown logical type `{}`", c.ty))
            })?;
            Ok(Field::new(&c.name, base.arrow_data_type(), c.nullable))
        })
        .collect::<Result<Vec<_>, EngineServingError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Map a mirror `StatValue` to a typed `ScalarValue` of the arrow `data_type`.
/// The variant is chosen by the column's arrow type (not the StatValue tag) so the
/// bound matches the schema the pruner compares against; a mismatch falls back to a
/// `Null` of the column type (unprunable on that column).
pub(crate) fn stat_to_scalar(v: &StatValue, data_type: &DataType) -> ScalarValue {
    match (data_type, v) {
        (DataType::Boolean, StatValue::Bool(b)) => ScalarValue::Boolean(Some(*b)),
        (DataType::Int32, StatValue::I32(i)) => ScalarValue::Int32(Some(*i)),
        (DataType::Int32, StatValue::I64(i)) => ScalarValue::Int32(Some(*i as i32)),
        (DataType::Int64, StatValue::I64(i)) => ScalarValue::Int64(Some(*i)),
        (DataType::Int64, StatValue::I32(i)) => ScalarValue::Int64(Some(*i as i64)),
        (DataType::Float64, StatValue::F64(f)) => ScalarValue::Float64(Some(*f)),
        (DataType::Utf8, StatValue::Str(s)) => ScalarValue::Utf8(Some(s.clone())),
        // Tag/type mismatch (or a type we don't prune on): unknown bound.
        _ => ScalarValue::try_from(data_type).unwrap_or(ScalarValue::Null),
    }
}

/// A `PruningStatistics` over a set of files (one container per file). Each column's
/// min/max array carries a row per file; a file with no stat for that column gets a
/// `null` bound (so the pruner cannot prune it on that column — it is kept).
struct FileSetStatistics<'a> {
    schema: SchemaRef,
    files: &'a [FileWithStats],
}

impl<'a> FileSetStatistics<'a> {
    /// Build the per-file min (or max) array for `column` as a typed arrow array,
    /// one row per file with `null` where a file lacks the stat. Returns `None` if
    /// the column is unknown to the schema (the pruner then skips it).
    fn bounds(&self, column: &Column, want_max: bool) -> Option<arrow::array::ArrayRef> {
        let field = self.schema.field_with_name(&column.name).ok()?;
        let dt = field.data_type();
        let scalars: Vec<ScalarValue> = self
            .files
            .iter()
            .map(|f| {
                let stat = f.column_stats.iter().find(|c| c.column_name == column.name);
                let bound = stat.and_then(|s| {
                    if want_max {
                        s.max.as_ref()
                    } else {
                        s.min.as_ref()
                    }
                });
                match bound {
                    Some(v) => stat_to_scalar(v, dt),
                    None => ScalarValue::try_from(dt).unwrap_or(ScalarValue::Null),
                }
            })
            .collect();
        ScalarValue::iter_to_array(scalars).ok()
    }
}

impl<'a> PruningStatistics for FileSetStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        self.bounds(column, false)
    }
    fn max_values(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        self.bounds(column, true)
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, _column: &Column) -> Option<arrow::array::ArrayRef> {
        None
    }
    fn row_counts(&self) -> Option<arrow::array::ArrayRef> {
        None
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        None
    }
}

/// Keep a file unless its stats prove it cannot match the conjunction of `filters`.
/// No filters, no usable stats, or an un-prunable predicate -> kept. Never fails: a
/// pruning limitation must never drop a file that might match (correctness over
/// efficiency).
pub fn prune_files<'a>(
    schema: &SchemaRef,
    filters: &[Expr],
    files: &'a [FileWithStats],
) -> Vec<&'a FileWithStats> {
    let keep_all = || files.iter().collect::<Vec<_>>();
    // Fold the filters into one conjunction; nothing to prune on -> keep all.
    let Some(predicate) = datafusion::logical_expr::utils::conjunction(filters.iter().cloned())
    else {
        return keep_all();
    };
    // Build the physical pruning predicate over the table schema. Any construction
    // failure (unsupported expr, planning error) -> keep all files, never fail.
    let Ok(df_schema) = DFSchema::try_from(schema.clone()) else {
        return keep_all();
    };
    let props = ExecutionProps::new();
    let Ok(phys) = create_physical_expr(&predicate, &df_schema, &props) else {
        return keep_all();
    };
    let Ok(pruner) = PruningPredicate::try_new(phys, schema.clone()) else {
        return keep_all();
    };
    let stats = FileSetStatistics {
        schema: schema.clone(),
        files,
    };
    // `prune` yields one bool per file: true = MAY match (keep), false = proven
    // non-matching (drop). On any pruning error, keep all.
    match pruner.prune(&stats) {
        Ok(mask) if mask.len() == files.len() => files
            .iter()
            .zip(mask)
            .filter_map(|(f, keep)| keep.then_some(f))
            .collect(),
        _ => keep_all(),
    }
}

#[async_trait]
impl TableProvider for IcebergMirrorTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: we use filters to prune whole files, but the survivors are not
        // row-filtered here, so DataFusion must still re-apply every predicate.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let kept = prune_files(&self.schema, filters, &self.files);
        let source = Arc::new(ParquetSource::new(self.schema.clone()));
        let store_url = match kept.first() {
            Some(f) => object_store_url_for(&f.path)?,
            None => ObjectStoreUrl::local_filesystem(),
        };
        let mut builder = FileScanConfigBuilder::new(store_url, source).with_limit(limit);
        for f in &kept {
            // Reuse the listing path's exact object-store path derivation so the
            // registered local-filesystem store resolves the absolute warehouse path.
            let url = ListingTableUrl::parse(&f.path)?;
            let pf = PartitionedFile::new(url.prefix().as_ref(), f.file_size_bytes as u64);
            builder = builder.with_file(pf);
        }
        let config = builder
            .with_projection_indices(projection.cloned())?
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

/// A pruning-aware `TableProvider` over an explicit set of Iceberg data files
/// plus their per-column stats. Unlike `ListingTable`, this skips opening files a
/// query's predicates provably cannot match: `scan` prunes the file set with a
/// `PruningPredicate` over the mirror stats, then builds a `DataSourceExec` over
/// only the survivors. The schema is fixed up front via `try_new_with_schema`,
/// which takes the mirror's authoritative schema so an evolved table's superset
/// is served and files missing a newer column are null-filled.
#[derive(Debug)]
pub struct IcebergMirrorTableProvider {
    schema: SchemaRef,
    files: Vec<FileWithStats>,
}

impl IcebergMirrorTableProvider {
    /// Build a provider whose authoritative schema is the mirror's (not inferred from
    /// Parquet footers), so an evolved table's superset schema is presented and files
    /// missing a newer column are null-filled by DataFusion's default schema adapter.
    pub fn try_new_with_schema(files: Vec<FileWithStats>, schema: SchemaRef) -> Self {
        Self { schema, files }
    }
}

/// Execute already-compiled, param-inlined read-only `sql` against all live Iceberg
/// tables and return the result batches. Delegates to [`execute_query_stream`] and
/// collects the resulting stream, so the two share one registration+planning path
/// and differ only in unary-vs-streaming consumption.
pub async fn execute_query(
    catalog: &IcebergCatalog,
    sql: &str,
    serving_store: Option<&ServingStore>,
) -> Result<Vec<RecordBatch>, EngineServingError> {
    let stream = execute_query_stream(catalog, sql, serving_store, None).await?;
    datafusion::physical_plan::common::collect(stream)
        .await
        .map_err(to_serving)
}

/// Streaming sibling of [`execute_query`]: register the same live Iceberg tables
/// into a fresh `SessionContext`, run the same compiled SQL, and return DataFusion's
/// `execute_stream()` result instead of collecting. The caller (the engine's Flight
/// `do_get`) encodes this stream directly, so neither the engine nor the client
/// holds the whole result — removing the unary path's ~4 MB message ceiling and its
/// double buffering. The returned stream is `'static` (DataFusion captures the plan
/// + task context), so the `SessionContext` may be dropped on return.
pub async fn execute_query_stream(
    catalog: &IcebergCatalog,
    sql: &str,
    serving_store: Option<&ServingStore>,
    at: Option<SnapshotId>,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table, serving_store, at).await?;
    }
    let df = ctx.sql(sql).await.map_err(EngineServingError::Plan)?;
    df.execute_stream().await.map_err(to_serving)
}

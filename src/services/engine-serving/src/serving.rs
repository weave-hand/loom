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
use control_plane_core::{TableRef, resolve_logical};
use control_plane_postgres::iceberg_catalog::{FileWithStats, IcebergCatalog};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{MemorySchemaProvider, Session, TableProvider};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
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
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError> {
    use control_plane_core::Catalog;

    // Local store for absolute file:// warehouse paths (back-compat default).
    // Idempotent across calls on one ctx.
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    // S3 store for s3:// warehouse paths, registered under s3://{bucket}.
    if let Some((bucket, store)) = serving_store {
        let url = ObjectStoreUrl::parse(format!("s3://{bucket}")).map_err(to_serving)?;
        ctx.register_object_store(url.as_ref(), store.clone());
    }

    let snap = catalog.current_snapshot(table).await.map_err(to_serving)?;
    // The MIRROR is authoritative for the served schema (not per-file Parquet
    // footers): an additively-evolved table presents its superset, and files written
    // before a newer column existed are null-filled by DataFusion's default schema
    // adapter (the column is nullable). File-backed data uses the pruning-aware
    // provider over the mirror's per-column stats; its `scan` skips files a query's
    // predicates provably cannot match.
    let table_schema = catalog.schema(table, snap.id).await.map_err(to_serving)?;
    let schema = arrow_schema_from_mirror(&table_schema.columns)?;
    let files_with_stats = catalog
        .files_with_stats(table, snap.id)
        .await
        .map_err(to_serving)?;
    let file_provider = if files_with_stats.is_empty() {
        None
    } else {
        Some(IcebergMirrorTableProvider::try_new_with_schema(
            files_with_stats,
            schema.clone(),
        ))
    };

    // Inline rows (mirror-only typed rows) are served DIRECTLY from Postgres via
    // a PG TableProvider that pushes filter/limit/projection into a per-query
    // SELECT — no Arrow->Parquet->Arrow round-trip. The snapshot is baked into a
    // base predicate so MVCC visibility matches `inline_live_batch`.
    let inline_provider =
        build_inline_provider(catalog, table, &schema, &table_schema.columns, snap.id).await?;

    // Combine: file-only, inline-only, or a UNION ALL view of both. The inline
    // provider carries the authoritative mirror schema, while the file provider's
    // schema is Parquet-footer-inferred, so a column's nullability may differ
    // (Parquet writers often mark columns nullable regardless of the logical
    // `required` flag); `DataFrame::union` widens nullability, so this is fine —
    // names + datatypes match because both derive from the same table schema.
    let provider: Arc<dyn datafusion::catalog::TableProvider> =
        match (file_provider, inline_provider) {
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
        };

    Ok(Some(provider))
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
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<(), EngineServingError> {
    let Some(provider) = build_serving_provider(ctx, catalog, table, serving_store).await? else {
        return Ok(());
    };

    // Ensure the schema exists in the default catalog, then register the table
    // schema-qualified so `"schema"."table"` references resolve.
    let cat = ctx
        .catalog("datafusion")
        .ok_or_else(|| EngineServingError::Engine("no default datafusion catalog".into()))?;
    if cat.schema(&table.schema).is_none() {
        cat.register_schema(&table.schema, Arc::new(MemorySchemaProvider::new()))
            .map_err(to_serving)?;
    }
    ctx.register_table(
        TableReference::partial(table.schema.clone(), table.name.clone()),
        provider,
    )
    .map_err(to_serving)?;
    Ok(())
}

/// Build the inline PG provider for `table` at `at`, or `None` when there is no
/// inline storage or no live inline rows (preserving the prior `inline_parquet`
/// `None` behavior). `schema` is the table's authoritative arrow schema (already
/// built by the caller); `cols` are the mirror column defs (for logical types).
async fn build_inline_provider(
    catalog: &IcebergCatalog,
    table: &TableRef,
    schema: &SchemaRef,
    cols: &[control_plane_core::ColumnDef],
    at: control_plane_core::SnapshotId,
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
        "begin_snapshot <= {0} and (end_snapshot is null or end_snapshot > {0})",
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
    Ok(Some(PgTableProvider::new(
        catalog.pool.clone(),
        inline_table_name(tid),
        schema.clone(),
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
/// only the survivors. The schema is fixed up front: `try_new` infers it from the
/// file set (same Parquet inference `listing_table` uses), while
/// `try_new_with_schema` takes the mirror's authoritative schema so an evolved
/// table's superset is served and files missing a newer column are null-filled.
#[derive(Debug)]
pub struct IcebergMirrorTableProvider {
    schema: SchemaRef,
    files: Vec<FileWithStats>,
}

impl IcebergMirrorTableProvider {
    /// Infer the arrow schema from `files` (the same `ParquetFormat` inference
    /// `listing_table` uses) and store it alongside the file set. The
    /// local-filesystem object store must already be registered on `ctx`.
    pub async fn try_new(
        ctx: &SessionContext,
        files: Vec<FileWithStats>,
    ) -> Result<Self, EngineServingError> {
        let urls: Vec<ListingTableUrl> = files
            .iter()
            .map(|f| ListingTableUrl::parse(&f.path))
            .collect::<Result<_, _>>()
            .map_err(to_serving)?;
        let format = ParquetFormat::default().with_force_view_types(false);
        let opts = ListingOptions::new(Arc::new(format));
        let cfg = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(opts)
            .infer_schema(&ctx.state())
            .await
            .map_err(to_serving)?;
        let schema = ListingTable::try_new(cfg).map_err(to_serving)?.schema();
        Ok(Self { schema, files })
    }

    /// Build a provider whose authoritative schema is the mirror's (not inferred from
    /// Parquet footers), so an evolved table's superset schema is presented and files
    /// missing a newer column are null-filled by DataFusion's default schema adapter.
    pub fn try_new_with_schema(files: Vec<FileWithStats>, schema: SchemaRef) -> Self {
        Self { schema, files }
    }
}

/// Execute already-compiled, param-inlined read-only `sql` against all live Iceberg
/// tables and return the result batches. (This is the body of the old
/// `DataFusionServingEngine::fetch_rows` minus the `Rows` flattening.)
pub async fn execute_query(
    catalog: &IcebergCatalog,
    sql: &str,
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<Vec<RecordBatch>, EngineServingError> {
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table, serving_store).await?;
    }
    let df = ctx.sql(sql).await.map_err(EngineServingError::Plan)?;
    df.collect().await.map_err(to_serving)
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
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table, serving_store).await?;
    }
    let df = ctx.sql(sql).await.map_err(EngineServingError::Plan)?;
    df.execute_stream().await.map_err(to_serving)
}

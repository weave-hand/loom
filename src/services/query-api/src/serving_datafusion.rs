//! loom-native, DataFusion-backed serving engine for file-backed Iceberg tables.
//! Reads the `iceberg_mirror` projection (via `IcebergCatalog`), registers each
//! live table's Parquet files (absolute `file://` paths) as a DataFusion table,
//! and runs the governed/compiled SQL through DataFusion — no DuckDB in the path.
//! See docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use control_plane_core::TableRef;
use control_plane_core::snapshot::StatValue;
use control_plane_postgres::iceberg_catalog::{FileWithStats, IcebergCatalog};
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
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
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use sqlx::PgPool;

use crate::serving::{
    ActionEngine, Rows, ServingEngine, ServingError, SqlValue, build_object_batch, inline_params,
};
use crate::sql::SqlDialect;

/// loom-native serving engine: serves governed reads for file-backed Iceberg
/// tables from the mirror via DataFusion. Holds only the mirror reader; the
/// `file://` object store and table registrations are built per query.
pub struct DataFusionServingEngine {
    catalog: IcebergCatalog,
}

impl DataFusionServingEngine {
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ServingEngine for DataFusionServingEngine {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        let ctx = SessionContext::new();
        // Register every live table so the compiled SQL's table refs resolve.
        // (Approach A: pre-register; the per-query mirror read is cheap. A schema
        // cache is a noted perf follow-up, not implemented here.)
        for table in self.catalog.live_tables().await.map_err(to_serving)? {
            register_iceberg_table(&ctx, &self.catalog, &table).await?;
        }
        // DataFusion has no positional bind slot here; inline params with the same
        // injection-safe renderer the Quack engine uses (`?` -> SQL literal).
        let inlined = inline_params(sql, params);
        let df = ctx.sql(&inlined).await.map_err(to_serving)?;
        let batches = df.collect().await.map_err(to_serving)?;
        Ok(batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        // DataFusion has no multi-file `LIMIT` corruption bug, so it keeps the bare
        // `LIMIT` (no order barrier). Rendering is otherwise DuckDB-identical.
        &crate::sql::DataFusionDialect
    }
}

/// Register `table`'s live data files (at its current snapshot) via the pruning-aware
/// `IcebergMirrorTableProvider` under the schema-qualified name `"schema"."table"`, so
/// the compiled read SQL resolves it. Files are registered by their ABSOLUTE `file://`
/// paths as stored in the mirror (`iceberg_mirror.data_file.path`); the provider's
/// `scan` skips files a query's predicates provably cannot match.
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
) -> Result<(), ServingError> {
    use control_plane_core::Catalog;

    // A non-prefixed local store for the absolute warehouse paths (a prefixed store
    // could not reach files outside data_path). Idempotent across calls on one ctx.
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );

    let snap = catalog.current_snapshot(table).await.map_err(to_serving)?;
    // File-backed data: the pruning-aware provider over the mirror's per-column
    // stats. A drop-in for the old `ListingTable` (same Parquet inference), but its
    // `scan` skips files a query's predicates provably cannot match.
    let files_with_stats = catalog
        .files_with_stats(table, snap.id)
        .await
        .map_err(to_serving)?;
    let file_provider = if files_with_stats.is_empty() {
        None
    } else {
        Some(IcebergMirrorTableProvider::try_new(ctx, files_with_stats).await?)
    };

    // Inline rows (mirror-only typed rows) are encoded to in-memory Parquet under a
    // memory:// store. A ListingTable can't span two object stores, so inline is a
    // SEPARATE provider, unioned with the file provider below.
    let inline_provider = if let Some(bytes) = catalog
        .inline_parquet(table, snap.id)
        .await
        .map_err(to_serving)?
    {
        let mem = Arc::new(InMemory::new());
        let key = format!(
            "inline/{}_{}_{}.parquet",
            table.schema, table.name, snap.id.0
        );
        mem.put(&ObjPath::from(key.clone()), bytes.into())
            .await
            .map_err(to_serving)?;
        ctx.register_object_store(
            ObjectStoreUrl::parse("memory://")
                .map_err(to_serving)?
                .as_ref(),
            mem,
        );
        let url = ListingTableUrl::parse(format!("memory:///{key}")).map_err(to_serving)?;
        Some(listing_table(ctx, vec![url]).await?)
    } else {
        None
    };

    // Combine: file-only, inline-only, or a UNION ALL view of both. The two
    // providers infer schema independently from their own Parquet, so a column's
    // nullability may differ (Parquet writers often mark columns nullable
    // regardless of the logical `required` flag); `DataFrame::union` widens
    // nullability, so this is fine — names + datatypes match because both derive
    // from the same table schema.
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
            (None, None) => return Ok(()), // a live table with no data; nothing to register
        };

    // Ensure the schema exists in the default catalog, then register the table
    // schema-qualified so `"schema"."table"` references resolve.
    let cat = ctx
        .catalog("datafusion")
        .ok_or_else(|| ServingError::Engine("no default datafusion catalog".into()))?;
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

/// Build a `ListingTable` over `urls` (all in one object store). Keeps string/binary
/// as canonical Utf8/Binary (not the `*View` variants) so `arrow_to_sqlvalue` maps
/// them — same choice as `datafusion_io::scan_table`.
async fn listing_table(
    ctx: &SessionContext,
    urls: Vec<ListingTableUrl>,
) -> Result<ListingTable, ServingError> {
    let format = ParquetFormat::default().with_force_view_types(false);
    let opts = ListingOptions::new(Arc::new(format));
    let cfg = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(opts)
        .infer_schema(&ctx.state())
        .await
        .map_err(to_serving)?;
    ListingTable::try_new(cfg).map_err(to_serving)
}

/// Any error (mirror/Postgres, DataFusion, object_store, URL) -> opaque serving error.
fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}

/// Encode a (one-row) arrow-58 `RecordBatch` to an Arrow IPC *stream* body — the
/// bytes `iceberg_landing::land` decodes in arrow-57 (the established cross-major IPC
/// boundary ingest already crosses). Any writer error maps to an opaque serving error.
pub fn encode_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ServingError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(to_serving)?;
        w.write(batch).map_err(to_serving)?;
        w.finish().map_err(to_serving)?;
    }
    Ok(buf)
}

/// A pruning-aware `TableProvider` over an explicit set of Iceberg data files
/// plus their per-column stats. Unlike `ListingTable`, this skips opening files a
/// query's predicates provably cannot match: `scan` prunes the file set with a
/// `PruningPredicate` over the mirror stats, then builds a `DataSourceExec` over
/// only the survivors. Schema is inferred from the file set up front (same Parquet
/// inference `listing_table` uses), so the served schema matches the listing path.
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
    ) -> Result<Self, ServingError> {
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
}

/// Map a mirror `StatValue` to a typed `ScalarValue` of the arrow `data_type`.
/// The variant is chosen by the column's arrow type (not the StatValue tag) so the
/// bound matches the schema the pruner compares against; a mismatch falls back to a
/// `Null` of the column type (unprunable on that column). `Task 4 reuses this.`
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
        let mut builder = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_limit(limit);
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

/// The `ActionEngine` for the Iceberg serving backend: a governed typed-insert is
/// built into the same one-row batch the DuckLake writer uses, encoded to Arrow IPC,
/// and forwarded to the atomic inline-write seam `iceberg_landing::land`. A single
/// action row inlines (mirror-only typed rows): one Postgres transaction committing
/// the row and its lineage together, drained to real Parquet later by the flush
/// vertical. Holds the same dependencies as ingest's `IcebergMaterializer`.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}

impl IcebergActionWriter {
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self {
            catalog,
            pool,
            inline_byte_limit,
            flush_byte_threshold,
        }
    }
}

#[async_trait]
impl ActionEngine for IcebergActionWriter {
    async fn write_object(
        &self,
        table: &TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        let ipc_body = encode_ipc_stream(&batch)?;
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            &specs,
            &ipc_body,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            event,
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
}

/// Which table-format backend the query-api binary serves reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingBackend {
    /// DuckLake via embedded DuckDB (default; today's behavior).
    DuckLake,
    /// File-backed Iceberg via the loom-native DataFusion engine.
    Iceberg,
}

/// Parse `LOOM_SERVING_BACKEND`. Unset -> DuckLake. Case-insensitive.
pub fn parse_serving_backend(v: Option<&str>) -> Result<ServingBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(ServingBackend::DuckLake),
        Some("iceberg") => Ok(ServingBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_SERVING_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}

/// Flatten DataFusion result batches into the engine-neutral `Rows`. Columns come
/// from the first batch's schema (DataFusion preserves projection order, satisfying
/// the handler's column-order contract); an empty result yields empty `Rows`.
pub fn batches_to_rows(batches: Vec<RecordBatch>) -> Rows {
    let Some(first) = batches.first() else {
        return Rows::default();
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(arrow_to_sqlvalue(batch.column(c), r));
            }
            rows.push(cells);
        }
    }
    Rows { columns, rows }
}

/// One Arrow cell -> `SqlValue`. Covers the scalar set loom serves; an unmapped
/// Arrow type falls back to a debug `Text` so a read never panics (mirrors the
/// DuckDB engine's `from_duck` fallback).
fn arrow_to_sqlvalue(array: &dyn Array, row: usize) -> SqlValue {
    if array.is_null(row) {
        return SqlValue::Null;
    }
    macro_rules! dc {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("arrow downcast")
        };
    }
    match array.data_type() {
        DataType::Utf8 => SqlValue::Text(dc!(StringArray).value(row).to_string()),
        DataType::LargeUtf8 => SqlValue::Text(dc!(LargeStringArray).value(row).to_string()),
        DataType::Boolean => SqlValue::Bool(dc!(BooleanArray).value(row)),
        DataType::Int8 => SqlValue::Int(dc!(Int8Array).value(row) as i64),
        DataType::Int16 => SqlValue::Int(dc!(Int16Array).value(row) as i64),
        DataType::Int32 => SqlValue::Int(dc!(Int32Array).value(row) as i64),
        DataType::Int64 => SqlValue::Int(dc!(Int64Array).value(row)),
        DataType::Float32 => SqlValue::Double(dc!(Float32Array).value(row) as f64),
        DataType::Float64 => SqlValue::Double(dc!(Float64Array).value(row)),
        DataType::Date32 => {
            let days = dc!(Date32Array).value(row);
            SqlValue::Date(time::macros::date!(1970 - 01 - 01) + time::Duration::days(days as i64))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = dc!(TimestampMicrosecondArray).value(row);
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            SqlValue::Timestamp(time::PrimitiveDateTime::new(odt.date(), odt.time()))
        }
        // Defensive: a type loom doesn't serve as a first-class scalar. Render the
        // single cell (not the whole array) so the fallback is bounded and
        // row-correct; mirrors the DuckDB engine's per-value `from_duck` fallback.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(fmt) => SqlValue::Text(fmt.value(row).to_string()),
            Err(_) => SqlValue::Null,
        },
    }
}

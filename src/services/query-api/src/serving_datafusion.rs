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
use control_plane_core::{PageReq, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::common::TableReference;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;

use crate::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue, inline_params};

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
    // dialect(): inherit the trait default (DuckDbDialect). The compiled SQL it
    // produces is valid DataFusion SQL, so no override is needed.
}

/// Register `table`'s live data files (at its current snapshot) as a DataFusion
/// `ListingTable` under the schema-qualified name `"schema"."table"`, so the
/// compiled read SQL resolves it. Files are registered by their ABSOLUTE `file://`
/// paths as stored in the mirror (`iceberg_mirror.data_file.path`).
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
    let files = catalog
        .files(table, snap.id, PageReq::unbounded())
        .await
        .map_err(to_serving)?;
    let file_urls: Vec<ListingTableUrl> = files
        .items
        .iter()
        .map(|f| ListingTableUrl::parse(&f.path))
        .collect::<Result<_, _>>()
        .map_err(to_serving)?;
    let file_provider = if file_urls.is_empty() {
        None
    } else {
        Some(listing_table(ctx, file_urls).await?)
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
                let df = ctx
                    .read_table(Arc::new(f))
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

/// The `ActionEngine` for the iceberg serving backend: governed action write-backs
/// are not supported, so writes are rejected. (loom now has inline writes via
/// `iceberg_inline::inline_append`, but those are a landing/ingest path, not the
/// single-row action-engine path this trait serves.) The action endpoint surfaces
/// this as an opaque error.
pub struct UnsupportedActionEngine;

#[async_trait]
impl ActionEngine for UnsupportedActionEngine {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> Result<(), ServingError> {
        Err(ServingError::Engine(
            "actions unsupported on the iceberg serving backend".into(),
        ))
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

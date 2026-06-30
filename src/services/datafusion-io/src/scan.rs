//! Read path: register a table's Parquet files (from the catalog file
//! list) as a named DataFusion table, so a transform's SQL can reference it.
//! The inverse of `write_dataset` — same loom object-store URL + relative layout.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use control_plane_core::{FileRef, TableRef};
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

use crate::write::LOOM_STORE_URL;

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

/// The object-store URL a data file's ABSOLUTE path resolves against.
/// `s3://bucket/...` => `s3://bucket`; everything else (absolute `file://` or local
/// filesystem paths) => the local filesystem store. Shared with the serving engine
/// (`engine-serving`), which registers data files by these same absolute mirror paths.
pub fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or("");
        ObjectStoreUrl::parse(format!("s3://{bucket}"))
    } else {
        Ok(ObjectStoreUrl::local_filesystem())
    }
}

/// Register `files` (a table's data files at some snapshot) as a DataFusion
/// table named `name`. Each `FileRef.path` is table-dir-relative; the full object key
/// is reconstructed as `<schema>/<table>/<path>` under the loom object store — matching
/// how `write_dataset` lays files out.
pub async fn scan_table(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    name: &str,
    table: &TableRef,
    files: &[FileRef],
) -> Result<(), ScanError> {
    // Each data file is either RELATIVE (landed tables: `<run_id>/part.parquet`,
    // resolved under the loom virtual store) or ABSOLUTE (transform/compaction output
    // promoted by `absolute_data_files`: `file://…`/`s3://bucket/…`). `FileRef` drops
    // the authoritative `path_is_relative` flag on read-back, so infer from the path
    // string: a `"://"` marks an absolute URI. Register each distinct object store once.
    // (`scan_table` is never called with an empty `files` slice — both callers guard it
    // — so registering inside the loop loses no store the old unconditional register did.)
    let mut registered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut paths: Vec<ListingTableUrl> = Vec::with_capacity(files.len());
    for f in files {
        if f.path.contains("://") {
            // Absolute mirror path: use verbatim; register its derived object store
            // (local filesystem for `file://`/local, the passed warehouse store for s3).
            let url = object_store_url_for(&f.path)?;
            if registered.insert(url.as_str().to_owned()) {
                let object_store: Arc<dyn ObjectStore> = if f.path.starts_with("s3://") {
                    store.clone()
                } else {
                    Arc::new(object_store::local::LocalFileSystem::new())
                };
                ctx.register_object_store(url.as_ref(), object_store);
            }
            paths.push(ListingTableUrl::parse(f.path.as_str())?);
        } else {
            // Relative landed path: reconstruct `{LOOM_STORE_URL}/{schema}/{table}/{rel}`
            // and register the passed store under the loom virtual URL.
            let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
            if registered.insert(url.as_str().to_owned()) {
                ctx.register_object_store(url.as_ref(), store.clone());
            }
            let key = format!("{LOOM_STORE_URL}/{}/{}/{}", table.schema, table.name, f.path);
            paths.push(ListingTableUrl::parse(key)?);
        }
    }

    // Keep string/binary columns as canonical Arrow types (Utf8/Binary) rather than
    // the `*View` variants ParquetFormat defaults to. The landing path's
    // `infer_columns` only maps the canonical types to logical types, so a scanned
    // column that flows into a transform output must stay canonical to commit.
    let format = ParquetFormat::default().with_force_view_types(false);
    let opts = ListingOptions::new(Arc::new(format));
    let cfg = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(opts)
        .infer_schema(&ctx.state())
        .await?;
    let provider = ListingTable::try_new(cfg)?;
    // Register under the EXACT `name` (e.g. an ontology type name like `Order`). The
    // `&str -> TableReference` conversion parses + lowercase-normalizes, which would
    // register `Order` as `order` and leave a case-quoted `FROM "Order"` unresolvable.
    // `TableReference::bare` preserves the name verbatim.
    ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
    Ok(())
}

/// Register an EMPTY DataFusion table named `name` with `schema` (zero rows) — the
/// empty-input analog of `scan_table`, for an input table that exists at a snapshot
/// but has no data files. Uses `TableReference::bare` to preserve the registration
/// name verbatim (same as `scan_table`), so a `FROM "Order"` resolves identically
/// whether the input is empty or scanned.
pub fn register_empty_table(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
) -> Result<(), ScanError> {
    // One partition holding zero batches — `MemTable::try_new` rejects an empty
    // partition list ("No partitions provided"), so the empty relation is a single
    // empty partition, not zero partitions.
    let provider = MemTable::try_new(schema, vec![vec![]])?;
    ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
    Ok(())
}

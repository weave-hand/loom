//! Read path: register a DuckLake table's Parquet files (from the catalog file
//! list) as a named DataFusion table, so a transform's SQL can reference it.
//! The inverse of `write_dataset` — same loom object-store URL + relative layout.

use std::sync::Arc;

use control_plane_core::{FileRef, TableRef};
use datafusion::common::TableReference;
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

/// Register `files` (a DuckLake table's data files at some snapshot) as a DataFusion
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
    let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
    ctx.register_object_store(url.as_ref(), store.clone());

    let paths: Vec<ListingTableUrl> = files
        .iter()
        .map(|f| {
            let key = format!(
                "{LOOM_STORE_URL}/{}/{}/{}",
                table.schema, table.name, f.path
            );
            ListingTableUrl::parse(key)
        })
        .collect::<Result<_, _>>()?;

    // Keep string/binary columns as canonical Arrow types (Utf8/Binary) rather than
    // the `*View` variants ParquetFormat defaults to. The landing path's
    // `infer_columns` only maps the canonical types to DuckLake types, so a scanned
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

//! Shared seed/serve plumbing for the transform e2e tests, all on the Iceberg
//! control plane + the loom-native serving engine — NO DuckDB anywhere.
//!
//! The four transform e2e suites (`transform_e2e`, `overwrite_e2e`, `compact_e2e`,
//! `typed_transform_e2e`) used to seed inputs and read output back through the
//! now-removed DuckLake/DuckDB serving path. This module replaces both halves:
//!
//! - **Seed** (`seed_table`): writes real Parquet via `datafusion_io::write_dataset`
//!   into the SAME object store the transform reads from, then registers it through
//!   the `IcebergControlPlane`'s `Tx` with a RELATIVE mirror path — exactly the shape
//!   the transform's `scan_table` resolves (`{store}/{schema}/{table}/{rel}`). This is
//!   the seed the canonical regression test (`iceberg_backend_e2e.rs`) uses; it cannot
//!   be `IcebergWriter::seed_arrays`, because that writer stores ABSOLUTE warehouse
//!   paths in its own tempdir, which the transform's relative-path `scan_table` cannot
//!   read. (Output readback below tolerates either, since serving resolves absolute.)
//! - **Serve / read back** (`execute_query` re-export usage + `count`/`col_csv`):
//!   reads the transform OUTPUT through `engine_serving::execute_query` over an
//!   `IcebergCatalog` — the same governed serving path query-api uses.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use control_plane_core::{
    ColumnSpec, ControlPlane, DataFile, DatasetRef, EventType, FileFormat, LineageEvent, RunId,
    TableRef,
};
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use datafusion_io::{WriteConfig, write_dataset};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use time::OffsetDateTime;
use uuid::Uuid;

pub fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Build the vendored Iceberg `SqlCatalog` over `dsn` + a `file://warehouse` root —
/// the catalog the `IcebergControlPlane` commits transform output through. Mirrors
/// `iceberg_backend_e2e::make_catalog`.
pub async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Lineage for a seed/transform whose only output is `out`.
pub fn lineage(out: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(out)],
        payload: serde_json::json!({}),
    }
}

/// `ColumnSpec`s from `(name, logical-type, nullable)` triples.
pub fn cols(specs: &[(&str, &str, bool)]) -> Vec<ColumnSpec> {
    specs
        .iter()
        .map(|(n, t, nul)| ColumnSpec {
            name: (*n).into(),
            ty: (*t).into(),
            nullable: *nul,
        })
        .collect()
}

/// Seed `table` with one batch of real Parquet, registered through the Iceberg
/// control plane under a RELATIVE mirror path (the shape the transform's
/// `scan_table` resolves against `store`). `file_prefix` is the per-append file
/// prefix (distinct prefixes => distinct data files, e.g. for compaction).
#[allow(clippy::too_many_arguments)]
pub async fn seed_table(
    cp: &IcebergControlPlane,
    store: &Arc<dyn object_store::ObjectStore>,
    table: &TableRef,
    columns: &[ColumnSpec],
    schema: Arc<Schema>,
    batch: RecordBatch,
    file_prefix: &str,
) {
    let dir = format!("{}/{}/{}", table.schema, table.name, file_prefix);
    let written = write_dataset(
        store.clone(),
        &dir,
        schema,
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .expect("write seed parquet");
    let files: Vec<DataFile> = written
        .into_iter()
        .map(|f| DataFile {
            // `write_dataset` already returns paths relative to the table dir
            // (`{file_prefix}/{filename}`), which is exactly what `scan_table`
            // resolves as `{store}/{schema}/{table}/{f.path}`.
            path: f.path,
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    tx.append_files(table, &files).await.unwrap();
    tx.emit(lineage(table)).await.unwrap();
    tx.commit().await.unwrap().expect("seed snapshot");
}

/// Like [`seed_table`], but stores an ABSOLUTE mirror path
/// (`{root_url}/{schema}/{table}/{rel}`) — the shape the serving engine resolves.
/// Use for files that are read back through `engine_serving` but never scanned by a
/// transform/compaction (which expect RELATIVE paths against `store`). The Parquet is
/// still written physically into `store`, so both readers find the bytes.
#[allow(clippy::too_many_arguments)]
pub async fn seed_table_absolute(
    cp: &IcebergControlPlane,
    store: &Arc<dyn object_store::ObjectStore>,
    root_url: &str,
    table: &TableRef,
    columns: &[ColumnSpec],
    schema: Arc<Schema>,
    batch: RecordBatch,
    file_prefix: &str,
) {
    let dir = format!("{}/{}/{}", table.schema, table.name, file_prefix);
    let written = write_dataset(
        store.clone(),
        &dir,
        schema,
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .expect("write seed parquet");
    let files: Vec<DataFile> = written
        .into_iter()
        .map(|f| DataFile {
            path: format!("{root_url}/{}/{}/{}", table.schema, table.name, f.path),
            path_is_relative: false,
            file_format: FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    tx.append_files(table, &files).await.unwrap();
    tx.emit(lineage(table)).await.unwrap();
    tx.commit().await.unwrap().expect("seed snapshot");
}

/// Read a single `count(*)`-shaped i64 scalar from the first row/column of a
/// one-batch serving result.
pub fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    let b = batches.first().expect("at least one batch");
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 scalar")
        .value(0)
}

/// Collect `(id, string-col)` rows from a `SELECT id, col ... ORDER BY id` serving
/// result and join the string column with `,` — the DataFusion-side replacement for
/// DuckDB's `string_agg(col, ',' ORDER BY id)`. Nulls render as empty (none expected
/// in the ported fixtures).
pub fn col_csv(batches: &[RecordBatch]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for b in batches {
        let col = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        for i in 0..col.len() {
            parts.push(if col.is_null(i) {
                String::new()
            } else {
                col.value(i).to_string()
            });
        }
    }
    parts.join(",")
}

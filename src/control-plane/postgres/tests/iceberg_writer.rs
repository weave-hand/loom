//! The writer produces real, valid Parquet and commits it as a fast_append.
//! Reads the bytes back with the parquet reader to prove they are real Parquet
//! (not just catalog metadata).
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use control_plane_postgres::iceberg_writer::append_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_real_parquet_and_commits() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Build the vendored catalog over the fixture DB + a file:// warehouse.
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(&db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");

    let ns = NamespaceIdent::new("wh".to_string());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("ns");
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(format!("file://{}/wh/t", warehouse.path().display()))
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.expect("create");

    let table = catalog
        .load_table(&TableIdent::new(ns.clone(), "t".to_string()))
        .await
        .expect("load");

    let batch = RecordBatch::try_new(
        Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
        ),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("batch");

    let data_files = append_batches(&catalog, &table, vec![batch])
        .await
        .expect("append");

    assert_eq!(data_files.len(), 1, "one rolling file for a small batch");
    let df = &data_files[0];
    assert_eq!(df.record_count, 3);
    assert!(df.file_size_bytes > 0);

    // Read the written file back with the parquet reader to prove valid Parquet.
    let path = df.path.strip_prefix("file://").unwrap_or(&df.path);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open parquet"))
        .expect("parquet reader")
        .build()
        .expect("build reader");
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 3, "parquet bytes hold the 3 written rows");
}

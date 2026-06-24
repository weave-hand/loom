//! Hermetic S3 round-trip: write an Iceberg table to MinIO and read it back, proving
//! the s3:// FileIO write path and the s3:// DataFusion serving read path.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use control_plane_postgres::fixture::{MinioFixture, PgFixture};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use control_plane_postgres::iceberg_writer::append_batches;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use service_runtime::{ObjectStoreConfig, build_serving_object_store, build_storage_factory};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_write_and_read_roundtrip() {
    // Boot Postgres and create a fresh migrated database.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pg_dsn = fx.pg_dsn(&db);

    // Boot MinIO and create the warehouse bucket.
    let minio = MinioFixture::start();
    let bucket = "warehouse";
    minio.create_bucket(bucket).await;

    // Build an ObjectStoreConfig for s3://warehouse/loom against the fixture endpoint.
    let warehouse_uri = format!("s3://{bucket}/loom");
    let os_cfg = ObjectStoreConfig::for_s3_test(
        warehouse_uri.clone(),
        bucket.to_string(),
        minio.endpoint(),
        minio.access_key().to_string(),
        minio.secret_key().to_string(),
    );

    // Build the Iceberg SQL catalog over S3.
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), pg_dsn.clone());
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone());
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(build_storage_factory(&os_cfg).unwrap())
        .load("loom", props)
        .await
        .expect("catalog");

    // Create namespace and table.
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
    let table_location = format!("{warehouse_uri}/wh/t");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(table_location)
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.expect("create");

    // Load the table and build a 3-row batch.
    let table_ident = TableIdent::new(ns.clone(), "t".to_string());
    let table = catalog.load_table(&table_ident).await.expect("load");
    let batch = RecordBatch::try_new(
        Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
        ),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("batch");

    // Append the batch — this writes real Parquet to MinIO and projects the mirror.
    let data_files = append_batches(&catalog, &table, vec![batch])
        .await
        .expect("append");
    assert_eq!(data_files.len(), 1, "one data file for a small batch");

    // --- Assertion (a): mirror data_file.path values start with "s3://" ---
    // This proves the data was written to MinIO, not local disk.
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(pg_dsn.as_str())
        .await
        .expect("connect pool");
    let paths: Vec<String> = sqlx::query_scalar(
        "select path from iceberg_mirror.data_file where end_snapshot is null",
    )
    .fetch_all(&pool)
    .await
    .expect("query data_file paths");

    assert!(!paths.is_empty(), "at least one data file in the mirror");
    for path in &paths {
        assert!(
            path.starts_with("s3://"),
            "data file path must start with s3://, got: {path}"
        );
    }

    // --- Assertion (b): read back through DataFusion against the S3 store ---
    // Register the S3 object store under s3://{bucket}, then scan the mirror's Parquet files.
    let (store_bucket, store) = build_serving_object_store(&os_cfg)
        .expect("build_serving_object_store ok")
        .expect("S3 backend returns Some");

    let ctx = SessionContext::new();
    ctx.register_object_store(
        ObjectStoreUrl::parse(format!("s3://{store_bucket}"))
            .expect("parse s3 url")
            .as_ref(),
        store,
    );

    // Use the paths from the mirror to scan via DataFusion.
    let row_count: usize = ctx
        .read_parquet(paths, ParquetReadOptions::default())
        .await
        .expect("read_parquet")
        .count()
        .await
        .expect("count");

    assert_eq!(row_count, 3, "read back exactly the 3 seeded rows via S3");
}

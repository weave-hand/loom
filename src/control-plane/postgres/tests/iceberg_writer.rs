//! The writer produces real, valid Parquet and commits it as a fast_append.
//! Reads the bytes back with the parquet reader to prove they are real Parquet
//! (not just catalog metadata).
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use control_plane_core::{DatasetId, EventType, Lineage, LineageEvent, PageReq, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use control_plane_postgres::iceberg_writer::{append_batches, append_batches_with_lineage};
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

    // Read the written file back with the parquet reader to prove valid Parquet, and
    // assert the actual cell values survived the writer chain (not just the row count).
    let path = df.path.strip_prefix("file://").unwrap_or(&df.path);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open parquet"))
        .expect("parquet reader")
        .build()
        .expect("build reader");
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for b in reader {
        let b = b.expect("batch");
        let id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let name = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for i in 0..b.num_rows() {
            ids.push(id.value(i));
            names.push(name.value(i).to_string());
        }
    }
    assert_eq!(ids, vec![1, 2, 3], "id values survive the writer chain");
    assert_eq!(
        names,
        vec!["a", "b", "c"],
        "name values survive the writer chain"
    );
}

/// `append_batches_with_lineage` emits exactly one lineage event, in the same tx
/// as the snapshot it describes (the `LineageEmittingCatalog` decorator routes
/// the commit through `do_update_table(.., Some(ev))`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_with_lineage_emits_one_event_atomically() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");

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
        .with_fields([Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
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
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("batch");

    let run = RunId(uuid::Uuid::new_v4());
    let out = TableRef {
        schema: "wh".to_string(),
        name: "t".to_string(),
    };
    let lineage = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    };

    append_batches_with_lineage(&catalog, &table, vec![batch], &lineage)
        .await
        .expect("append+lineage");

    let page = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(page.items.len(), 1, "exactly one lineage event for the run");
    assert_eq!(
        page.items[0].outputs[0].name, "wh.t",
        "lineage output is the schema-qualified landed table"
    );
}

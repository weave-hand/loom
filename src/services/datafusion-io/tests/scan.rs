use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{FileRef, TableRef};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, register_empty_table, scan_table, write_dataset};
use object_store::ObjectStore;
use object_store::memory::InMemory;

#[tokio::test]
async fn scan_registers_written_files_for_sql() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let written = write_dataset(
        store.clone(),
        "main/customer/run-1",
        schema.clone(),
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .unwrap();
    let files: Vec<FileRef> = written
        .iter()
        .map(|w| FileRef {
            path: w.path.clone(),
            record_count: w.record_count,
            file_size_bytes: w.file_size_bytes,
        })
        .collect();

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let ctx = SessionContext::new();
    scan_table(&ctx, store.clone(), "customer", &table, &files)
        .await
        .unwrap();

    let df = ctx
        .sql("SELECT id, name FROM customer ORDER BY id")
        .await
        .unwrap();
    let out = df.collect().await.unwrap();
    let n: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 2, "both rows are scannable via SQL");
}

#[test]
fn object_store_url_for_s3_uses_bucket_authority() {
    let url = datafusion_io::object_store_url_for("s3://my-bucket/schema/table/part-0.parquet")
        .expect("s3 url parses");
    assert_eq!(url.as_str(), "s3://my-bucket/");
}

#[test]
fn object_store_url_for_file_uses_local_filesystem() {
    let url = datafusion_io::object_store_url_for("file:///warehouse/schema/table/part-0.parquet")
        .expect("file url resolves");
    assert_eq!(
        url.as_str(),
        datafusion::execution::object_store::ObjectStoreUrl::local_filesystem().as_str()
    );
}

#[tokio::test]
async fn register_empty_table_runs_sql_over_zero_rows() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ctx = SessionContext::new();
    register_empty_table(&ctx, "input", schema).unwrap();

    // count(*) over an empty relation is one row of 0; SELECT * is empty.
    let n = ctx.sql("SELECT count(*) AS n FROM input").await.unwrap();
    let rows = n.collect().await.unwrap();
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "count(*) yields exactly one row");

    let star = ctx.sql("SELECT * FROM input").await.unwrap();
    let out = star.collect().await.unwrap();
    let data_rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(data_rows, 0, "the relation is empty");
}

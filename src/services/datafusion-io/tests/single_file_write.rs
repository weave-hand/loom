//! Regression guard for the multi-file split that corrupts DuckLake `LIMIT` reads.
//!
//! `write_dataset` size-targets its output: a tiny result must land as exactly ONE
//! Parquet file (`estimate_partitions` returns 1). Before the fix the file count was
//! NOT driven by that target — the parquet sink's demuxer split the single coalesced
//! input stream across `minimum_parallel_output_files` writers (default 4), opening one
//! file per incoming batch. A join emits a non-deterministic number of output batches
//! under multi-threaded execution, so a 3-row result intermittently landed as two files.
//! DuckLake/DuckDB then mis-read the multi-file table under a pushed-down `LIMIT`,
//! corrupting `id` values (e.g. 10 -> 266). The corruption is 1:1 with the multi-file
//! split, so asserting single-file output for small data is the root-cause guard.
//!
//! It mirrors the real `run_transform` path (scan inputs back from the object store,
//! then join + write) so the join emits multiple output batches — an in-memory
//! single-batch source would not exercise the demuxer's multi-file fan-out. Uses an
//! in-memory object store, so it stays fast and Postgres/DuckDB-free.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{FileRef, TableRef};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, scan_table, write_dataset};
use object_store::ObjectStore;
use object_store::memory::InMemory;

async fn land(
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Vec<FileRef> {
    let dir = format!("{}/{}/run-1", table.schema, table.name);
    let written = write_dataset(
        store.clone(),
        &dir,
        schema,
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .unwrap();
    written
        .iter()
        .map(|w| FileRef {
            path: w.path.clone(),
            record_count: w.record_count,
            file_size_bytes: w.file_size_bytes,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn small_join_result_lands_as_a_single_file() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let customers = TableRef {
        schema: "main".into(),
        name: "customers".into(),
    };
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_files = land(
        &store,
        &customers,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
    )
    .await;

    let orders = TableRef {
        schema: "main".into(),
        name: "orders".into(),
    };
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let ord_files = land(
        &store,
        &orders,
        ord_schema.clone(),
        RecordBatch::try_new(
            ord_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Float64Array::from(vec![5.5, 7.5, 2.5])),
            ],
        )
        .unwrap(),
    )
    .await;

    let ctx = SessionContext::new();
    scan_table(&ctx, store.clone(), "customers", &customers, &cust_files)
        .await
        .unwrap();
    scan_table(&ctx, store.clone(), "orders", &orders, &ord_files)
        .await
        .unwrap();

    let sql = "SELECT o.id AS id, c.region AS region, o.amount AS amount \
               FROM orders o JOIN customers c ON o.customer_id = c.id";
    let df = ctx.sql(sql).await.unwrap();
    let out_schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.unwrap();

    let written = write_dataset(
        store.clone(),
        "main/order_enriched/run-out",
        out_schema,
        &batches,
        &WriteConfig::default(),
    )
    .await
    .unwrap();

    // The corruption is 1:1 with the file count: tiny data must produce exactly one
    // file (estimate_partitions == 1), never the multi-file split that breaks the
    // DuckLake LIMIT read.
    let total_rows: i64 = written.iter().map(|w| w.record_count).sum();
    assert_eq!(
        written.len(),
        1,
        "small result must land as a single parquet file, got {} files: {:?}",
        written.len(),
        written
            .iter()
            .map(|w| (&w.path, w.record_count))
            .collect::<Vec<_>>(),
    );
    assert_eq!(total_rows, 3, "the single file holds all rows");
}

/// The fix pins file count to `estimate_partitions`; verify it does not over-coalesce —
/// a large result with a tiny `target_file_size_bytes` must still split into the intended
/// multiple files (with all rows preserved exactly once).
#[tokio::test(flavor = "multi_thread")]
async fn large_result_still_splits_into_the_targeted_file_count() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // The parquet sink's demuxer opens one file per incoming batch up to the targeted
    // file count, so a real multi-file split needs at least that many input batches (as a
    // large `df.collect()` produces). Build 8 batches against a 8-file target.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let per = 256_i64;
    let nbatches = 8_i64;
    let mut input_batches = Vec::new();
    for b in 0..nbatches {
        let base = b * per;
        input_batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from((base..base + per).collect::<Vec<_>>())),
                    Arc::new(Float64Array::from(
                        (base..base + per).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap(),
        );
    }
    let n = per * nbatches;

    // Tiny target size relative to the data forces estimate_partitions > 1.
    let cfg = WriteConfig {
        target_file_size_bytes: 1024,
        max_files: 8,
        compression_factor: 0.3,
    };
    let in_memory: u64 = input_batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let expected = datafusion_io::estimate_partitions(in_memory, &cfg);
    assert!(
        expected > 1,
        "test setup: expected a multi-file split, got {expected}"
    );

    let written = write_dataset(
        store.clone(),
        "main/big/run-out",
        schema,
        &input_batches,
        &cfg,
    )
    .await
    .unwrap();

    assert_eq!(
        written.len(),
        expected,
        "large result must split into exactly estimate_partitions files, got {}: {:?}",
        written.len(),
        written.iter().map(|w| w.record_count).collect::<Vec<_>>(),
    );
    let total: i64 = written.iter().map(|w| w.record_count).sum();
    assert_eq!(total, n, "every row written exactly once across the files");
}

//! Queue -> worker -> transform -> snapshot + lineage -> read-back, against real
//! Postgres + Iceberg. Seeds inputs as real Iceberg Parquet (mirror-registered),
//! runs a SQL join through the Iceberg control plane, and reads the output back
//! through the loom-native serving engine (`engine_serving::execute_query`). NO DuckDB.

mod transform_e2e_support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetRef, LineageEvent, NewJob, PageReq, Queue, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_worker::Worker;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;

use transform_e2e_support::{cols, lineage, make_catalog, scalar_i64, seed_table, tref};

#[tokio::test(flavor = "multi_thread")]
async fn transform_joins_two_inputs_into_a_new_snapshot() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg.clone(), catalog);

    let customers = tref("main", "customers");
    let cust_cols = cols(&[("id", "long", false), ("region", "string", true)]);
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    seed_table(
        &cp,
        &store,
        &customers,
        &cust_cols,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
        "seed-c",
    )
    .await;

    let orders = tref("main", "orders");
    let ord_cols = cols(&[("id", "long", false), ("customer_id", "long", false)]);
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));
    seed_table(
        &cp,
        &store,
        &orders,
        &ord_cols,
        ord_schema.clone(),
        RecordBatch::try_new(
            ord_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
            ],
        )
        .unwrap(),
        "seed-o",
    )
    .await;

    pg.enqueue(NewJob {
        kind: "transform".into(),
        payload: serde_json::json!({
            "inputs": [
                { "schema": "main", "name": "customers" },
                { "schema": "main", "name": "orders" }
            ],
            "output": { "schema": "main", "name": "orders_enriched" },
            "sql": "SELECT o.id AS id, c.region AS region \
                    FROM orders o JOIN customers c ON o.customer_id = c.id"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    // The queue is backend-neutral (dequeue through `pg`); the handler commits output
    // through the Iceberg control plane (`cp`), mirroring the transform binary.
    let cp_h: Arc<dyn ControlPlane> = Arc::new(IcebergControlPlane::new(
        pg.clone(),
        make_catalog(fx.pg_dsn(&db), &warehouse).await,
    ));
    let store_h = store.clone();
    let root_h = root_url.clone();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(pg.clone(), "transform-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                let root_url = root_h.clone();
                async move { transform_handler(cp.as_ref(), store, &root_url, job).await }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert!(
        pg.dequeue(&["transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "transform job completed"
    );

    // Read the output back through the serving engine.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count = engine_serving::execute_query(
        &serving,
        "SELECT count(*) FROM \"main\".\"orders_enriched\"",
        None,
    )
    .await
    .expect("serving count");
    assert_eq!(
        scalar_i64(&count),
        3,
        "serving reads the transform output (3 rows)"
    );

    let rows = engine_serving::execute_query(
        &serving,
        "SELECT \"id\", \"region\" FROM \"main\".\"orders_enriched\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving rows");
    assert_eq!(
        transform_e2e_support::col_csv(&rows),
        "CA,CA,NY",
        "join produced the right regions"
    );

    let out_ds = DatasetRef::from(&tref("main", "orders_enriched"));
    let ups = pg
        .lineage()
        .upstream(&out_ds, PageReq::unbounded())
        .await
        .unwrap();
    let up_names: std::collections::HashSet<String> =
        ups.items.iter().map(|d| d.name.clone()).collect();
    assert!(
        up_names.iter().any(|n| n.contains("customers"))
            && up_names.iter().any(|n| n.contains("orders")),
        "lineage upstream of orders_enriched includes both inputs, got {up_names:?}"
    );
}

/// Create an input table with a schema but NO data files (current_snapshot exists,
/// `files` is empty, `schema` resolves) — the empty-input fixture the edge-2 cases need.
async fn create_empty_table(cp: &IcebergControlPlane, table: &TableRef, columns: &[ColumnSpec]) {
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    // Register the table in the mirror with an EMPTY file list: the table/columns/schema
    // are projected at the snapshot (so `current_snapshot`/`schema` resolve), but no data
    // files exist — the "live input with zero files" fixture. A create-only commit would
    // allocate a snapshot without a mirror `table` row, so the input would read as NotFound.
    tx.append_files(table, &[]).await.unwrap();
    tx.commit().await.unwrap();
}

fn empty_input_lineage(input: &TableRef, output: &TableRef) -> LineageEvent {
    let mut ev = lineage(output);
    ev.inputs = vec![DatasetRef::from(input)];
    ev
}

/// Edge 2a: a live input with zero files registers as an empty relation, so
/// `SELECT count(*)` runs over it and commits a single row of `0` — NOT a Scan/Retry
/// error (which is what an empty file list passed to `scan_table` would produce).
#[tokio::test(flavor = "multi_thread")]
async fn empty_input_runs_transform_count_is_zero() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    let input = tref("main", "empty_in");
    create_empty_table(
        &cp,
        &input,
        &cols(&[("id", "long", false), ("region", "string", true)]),
    )
    .await;

    let out = tref("main", "empty_count");
    transform::run_transform(
        &cp,
        store.clone(),
        &format!("file://{warehouse}"),
        "run-empty-count",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &input,
                register_as: "empty_in",
            }],
            output: &out,
            sql: "SELECT count(*) AS n FROM empty_in",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: empty_input_lineage(&input, &out),
        },
    )
    .await
    .expect("empty input is an empty relation, not a scan error");

    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let n =
        engine_serving::execute_query(&serving, "SELECT \"n\" FROM \"main\".\"empty_count\"", None)
            .await
            .expect("serving read");
    assert_eq!(scalar_i64(&n), 0, "count(*) over the empty input is 0");
}

/// Edge 2b: `SELECT *` over a zero-file input commits an EMPTY output (zero rows)
/// rather than returning a Scan/Retry error. `write_dataset` of an empty result yields
/// zero files, so the output table is a row-less snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn empty_input_select_star_commits_empty_output() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    let input = tref("main", "empty_src");
    create_empty_table(
        &cp,
        &input,
        &cols(&[("id", "long", false), ("region", "string", true)]),
    )
    .await;

    let out = tref("main", "empty_passthrough");
    transform::run_transform(
        &cp,
        store.clone(),
        &format!("file://{warehouse}"),
        "run-empty-star",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &input,
                register_as: "empty_src",
            }],
            output: &out,
            sql: "SELECT * FROM empty_src",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: empty_input_lineage(&input, &out),
        },
    )
    .await
    .expect("SELECT * over the empty input commits an empty output, not a scan error");

    // The output is a row-less snapshot (zero data files). An empty table is not
    // registered by the serving engine (nothing to scan), so the row count is read
    // from the catalog's file list (sum of record counts == 0) rather than via SQL.
    let snap = cp.catalog().current_snapshot(&out).await.unwrap().id;
    let files = cp
        .catalog()
        .files(&out, snap, PageReq::unbounded())
        .await
        .unwrap();
    let rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(rows, 0, "the passthrough output is empty");
}

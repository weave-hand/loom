//! Queue -> worker -> transform -> snapshot + lineage -> read-back, against real
//! Postgres + DuckDB. Lands inputs via the ingest materializer, then runs a SQL join.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetRef, EventType, LineageEvent, NewJob, PageReq, Queue, RunId,
    TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_worker::Worker;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn transform_joins_two_inputs_into_a_new_snapshot() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let customers = tref("main", "customers");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    land(
        &cp,
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

    let orders = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));
    land(
        &cp,
        &store,
        &orders,
        ord_schema.clone(),
        RecordBatch::try_new(
            ord_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
            ],
        )
        .unwrap(),
    )
    .await;

    cp.enqueue(NewJob {
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

    let store_h = store.clone();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(cp.clone(), "transform-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                async move { transform_handler(cp.as_ref(), store, job).await }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert!(
        cp.dequeue(&["transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "transform job completed"
    );

    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.orders_enriched;")
        .await;
    assert_eq!(count, "3", "DuckDB reads the transform output");
    let regions = writer
        .query_scalar("SELECT string_agg(region, ',' ORDER BY id) FROM lake.main.orders_enriched;")
        .await;
    assert_eq!(regions, "CA,CA,NY", "join produced the right regions");

    let out_ds = DatasetRef::from(&tref("main", "orders_enriched"));
    let ups = cp
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
async fn create_empty_table(cp: &PgControlPlane, table: &TableRef, columns: &[ColumnSpec]) {
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    tx.commit().await.unwrap();
}

fn empty_input_lineage(input: &TableRef, output: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(input)],
        outputs: vec![DatasetRef::from(output)],
        payload: serde_json::json!({}),
    }
}

/// Edge 2a: a live input with zero files registers as an empty relation, so
/// `SELECT count(*)` runs over it and commits a single row of `0` — NOT a Scan/Retry
/// error (which is what an empty file list passed to `scan_table` would produce).
#[tokio::test(flavor = "multi_thread")]
async fn empty_input_runs_transform_count_is_zero() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let input = tref("main", "empty_in");
    create_empty_table(
        &cp,
        &input,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "long".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "region".into(),
                ty: "string".into(),
                nullable: true,
            },
        ],
    )
    .await;

    let out = tref("main", "empty_count");
    transform::run_transform(
        &cp,
        store.clone(),
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

    let n = writer
        .query_scalar("SELECT n FROM lake.main.empty_count;")
        .await;
    assert_eq!(n, "0", "count(*) over the empty input is 0");
}

/// Edge 2b: `SELECT *` over a zero-file input commits an EMPTY output (zero rows)
/// rather than returning a Scan/Retry error. `write_dataset` of an empty result yields
/// zero files, so the output table is a row-less snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn empty_input_select_star_commits_empty_output() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let input = tref("main", "empty_src");
    create_empty_table(
        &cp,
        &input,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "long".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "region".into(),
                ty: "string".into(),
                nullable: true,
            },
        ],
    )
    .await;

    let out = tref("main", "empty_passthrough");
    transform::run_transform(
        &cp,
        store.clone(),
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

    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.empty_passthrough;")
        .await;
    assert_eq!(count, "0", "the passthrough output is empty");
}

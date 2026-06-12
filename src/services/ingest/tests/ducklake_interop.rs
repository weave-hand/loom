//! Interop guardrail: the pinned DuckDB engine READS data that loom's materializer
//! wrote (Parquet + ducklake_* rows), and BUILDS ON IT (appends its own snapshot).
//! If loom's output diverges from what DuckDB expects, DuckDB rejects it here.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use uuid::Uuid;

#[tokio::test]
async fn duckdb_reads_loom_materialized_data_and_appends() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer.bootstrap().await;

    let t = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
        ],
    )
    .unwrap();

    let store = LocalFileSystem::new_with_prefix(writer.data_path()).unwrap();

    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "loom-ingest".into(),
            name: "main.customer".into(),
        }],
        payload: serde_json::json!({}),
    };

    let loom_snap: i64 = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[batch],
            file_name: "loom.parquet",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap()
    .0;

    // THE GUARDRAIL: DuckDB scans loom's materialized Parquet from loom's catalog.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.customer;")
        .await;
    assert_eq!(count, "2", "DuckDB must scan loom's materialized file");
    let emails = writer
        .query_scalar("SELECT string_agg(email, ',' ORDER BY id) FROM lake.main.customer;")
        .await;
    assert_eq!(emails, "a@x,b@x");

    // DuckDB appends its own row on top — proves counters/versioning are correct.
    writer
        .exec("INSERT INTO lake.main.customer VALUES (3, 'c@x');")
        .await;
    let count2 = writer
        .query_scalar("SELECT count(*) FROM lake.main.customer;")
        .await;
    assert_eq!(count2, "3");
    let max_snap = writer.max_snapshot_id().await;
    assert_eq!(
        max_snap,
        loom_snap + 1,
        "DuckDB's snapshot must sit atop loom's (loom_snap={loom_snap})"
    );
}

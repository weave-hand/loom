//! Overwrite output mode e2e: a transform with output_mode=overwrite replaces the output
//! table's contents (DuckDB read-back sees only the new result), a second overwrite
//! replaces again, and a read at the pre-overwrite snapshot still time-travels to the old
//! rows. Drives the real queue -> worker -> transform_handler path.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ControlPlane, DatasetRef, EventType, LineageEvent, NewJob, PageReq, Queue, RunId,
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

/// Run the worker once over the `transform` queue until the job drains.
async fn drain_transforms(cp: &PgControlPlane, store: &Arc<dyn ObjectStore>, root_url: &str) {
    let store_h = store.clone();
    let root_url = root_url.to_string();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(cp.clone(), "overwrite-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                let root_url = root_url.clone();
                async move { transform_handler(cp.as_ref(), store, &root_url, job).await }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}

/// Enqueue an overwrite transform that selects all rows of `src` into `out`.
async fn enqueue_overwrite(cp: &PgControlPlane, src: &str, out: &str) {
    cp.enqueue(NewJob {
        kind: "transform".into(),
        payload: serde_json::json!({
            "inputs": [{ "schema": "main", "name": src }],
            "output": { "schema": "main", "name": out },
            "sql": format!("SELECT id, label FROM {src}"),
            "output_mode": "overwrite"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_contents_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());
    let root_url = format!("file://{}", writer.data_path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    // Two source tables: src_a (2 rows), src_b (1 row, different labels).
    let src_a = tref("main", "src_a");
    land(
        &cp,
        &store,
        &src_a,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a1"), Some("a2")])),
            ],
        )
        .unwrap(),
    )
    .await;
    let src_b = tref("main", "src_b");
    land(
        &cp,
        &store,
        &src_b,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![9])),
                Arc::new(StringArray::from(vec![Some("b9")])),
            ],
        )
        .unwrap(),
    )
    .await;

    // First overwrite: out := src_a (2 rows). (Output table is new -> create + replace.)
    enqueue_overwrite(&cp, "src_a", "out").await;
    drain_transforms(&cp, &store, &root_url).await;
    let out = tref("main", "out");
    let snap_after_a = cp.current_snapshot(&out).await.unwrap().id;
    let count_a = writer
        .query_scalar("SELECT count(*) FROM lake.main.out;")
        .await;
    assert_eq!(count_a, "2", "first overwrite wrote src_a's rows");
    let labels_a = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.out;")
        .await;
    assert_eq!(labels_a, "a1,a2");

    // Second overwrite: out := src_b (1 row). Replaces, not appends.
    enqueue_overwrite(&cp, "src_b", "out").await;
    drain_transforms(&cp, &store, &root_url).await;
    let count_b = writer
        .query_scalar("SELECT count(*) FROM lake.main.out;")
        .await;
    assert_eq!(
        count_b, "1",
        "second overwrite REPLACED (not appended) -> 1 row"
    );
    let labels_b = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.out;")
        .await;
    assert_eq!(labels_b, "b9", "only src_b's row remains live");

    // Time travel: the snapshot after the first overwrite still has src_a's 2 rows. The
    // catalog lists exactly the files live at that snapshot (the b-file is excluded).
    let files_then = cp
        .files(&out, snap_after_a, PageReq::unbounded())
        .await
        .unwrap();
    let rows_then: i64 = files_then.items.iter().map(|f| f.record_count).sum();
    assert_eq!(
        rows_then, 2,
        "prior snapshot still sees src_a's 2 rows (time travel)"
    );
}

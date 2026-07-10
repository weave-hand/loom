//! Site 1: `iceberg_landing::write_steps` refuses when any staged target is a
//! declared stream table, and commits nothing (a co-staged batch target's rows
//! are absent). A pure-batch multi-target write is byte-identical to before.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, EventType, LineageEvent, RunId, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{StepLand, write_steps};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use loom_test_seed::local_sql_catalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}
fn cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).expect("batch")
}
// LineageEvent has no Default; mirror stream_overwrite_framing.rs's `lin()`.
fn lin(run: RunId) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_steps_refuses_stream_target_and_commits_nothing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let plain = tref("s", "plain");
    let streamt = tref("s", "stream_out");

    // Declare `stream_out` a log table BEFORE the write.
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &streamt.schema, &streamt.name, at)
        .await
        .expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // A two-target write: one batch target + the stream target → must refuse.
    let steps = vec![
        StepLand {
            table: plain.clone(),
            columns: cols(),
            batches: vec![batch()],
            overwrite: false,
        },
        StepLand {
            table: streamt.clone(),
            columns: cols(),
            batches: vec![batch()],
            overwrite: false,
        },
    ];
    let err = write_steps(
        &pool,
        &catalog,
        steps,
        lin(RunId(uuid::Uuid::new_v4())),
        &[],
    )
    .await
    .expect_err("stream target must be refused");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("stream-table target refused:"),
        "msg: {err}"
    );

    // Nothing committed: the co-staged batch target was never registered, so it has
    // no live mirror snapshot at all (the whole multi-target tx rolled back).
    let ice = IcebergCatalog::new(pool.clone());
    assert!(
        matches!(
            ice.current_snapshot(&plain).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "batch target must have no live snapshot after a refused multi-target commit"
    );
}

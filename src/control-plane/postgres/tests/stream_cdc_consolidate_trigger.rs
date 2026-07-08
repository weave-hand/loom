//! `write_inline_delta`'s consolidate trigger: a `kind='cdc'` table accrues
//! CDC delta-row counts across calls and enqueues one `stream_consolidate` job
//! on crossing the (per-call) `consolidate_threshold`, debounced exactly like
//! the byte-trigger flush (`inline_flush_trigger.rs`). loom_fixture_test
//! (Postgres). Constructors mirror `stream_cdc_dual_flush.rs`.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, RunId, STREAM_CONSOLIDATE_JOB_KIND, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn val_spec() -> ColumnSpec {
    ColumnSpec {
        name: "val".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn full_row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("full row batch")
}

fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn job_count(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*) from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("job count query")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_delta_threshold_enqueues_exactly_one_and_debounces() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "sales".to_string(),
        name: "orders_consolidate".to_string(),
    };
    let cols = vec![id_spec(), val_spec()];

    // Declare the table CDC (keyed on `id`) BEFORE any write, exactly as
    // `stream_cdc_dual_flush.rs` does.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // Seed id=1, val=100 (a plain +I append; not a delta, so it never touches
    // the consolidate trigger).
    inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 100),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");

    // A small threshold: one UPDATE emits a (-U, +U) pair == 2 delta rows,
    // crossing threshold=2 in a single call.
    let threshold = 2;

    let v0 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after seed");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 200),
        Some((&cols, &full_row_batch(1, 100))),
        lin(),
        v0,
        Some(threshold),
    )
    .await
    .expect("cdc update delta crossing the threshold");

    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        1,
        "exactly one stream_consolidate job enqueued on crossing the threshold"
    );
    let payload: serde_json::Value =
        sqlx::query_scalar("select payload from queue.jobs where kind = $1")
            .bind(STREAM_CONSOLIDATE_JOB_KIND)
            .fetch_one(&pool)
            .await
            .expect("payload");
    assert_eq!(payload["schema"], "sales");
    assert_eq!(payload["name"], "orders_consolidate");

    // A second delta run (another update, 2 more delta rows — still below the
    // NEXT multiple of the threshold reset point) must NOT enqueue a second
    // job: the trigger is armed-once until consolidation clears it.
    let v1 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after first update");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 300),
        Some((&cols, &full_row_batch(1, 200))),
        lin(),
        v1,
        Some(threshold),
    )
    .await
    .expect("second cdc update delta");

    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        1,
        "debounced: still exactly one stream_consolidate job after a second crossing"
    );
}

//! `write_inline_delta`'s consolidate trigger on a NON-CDC (COW) identity
//! table: a plain identity table (no `declare_cdc`) accrues one delta row per
//! `write_inline_delta` call and enqueues one `stream_consolidate` job on
//! crossing the (per-call) `consolidate_threshold`, debounced exactly like
//! the CDC path (`stream_cdc_consolidate_trigger.rs`). loom_fixture_test
//! (Postgres). Constructors mirror `inline_delta_cas.rs` /
//! `stream_cdc_consolidate_trigger.rs`.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, RunId, STREAM_CONSOLIDATE_JOB_KIND, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::clear_consolidate_trigger;

fn table() -> TableRef {
    TableRef {
        schema: "sales".to_string(),
        name: "orders_cow_consolidate".to_string(),
    }
}

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

/// Resolve the internal inline table id the same way the other inline tests do.
async fn tid_of(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Below-threshold writes enqueue nothing; the write that crosses the
/// threshold enqueues exactly one `stream_consolidate` job with the expected
/// payload; a further (already-armed) write enqueues no second job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn below_threshold_then_crossing_enqueues_once_and_debounces() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];

    // Seed id=1, val=100 as a plain identity (COW) table — NOT declared CDC.
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

    let threshold = 3;

    // Write 1: below threshold (1 accrued of 3).
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
        None,
        lin(),
        v0,
        Some(threshold),
        &[],
    )
    .await
    .expect("first cow delta");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        0,
        "below threshold after 1st delta: no job"
    );

    // Write 2: still below threshold (2 accrued of 3).
    let v1 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after 1st delta");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 300),
        None,
        lin(),
        v1,
        Some(threshold),
        &[],
    )
    .await
    .expect("second cow delta");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        0,
        "below threshold after 2nd delta: no job"
    );

    // Write 3: crosses the threshold (3 accrued of 3) -> exactly one job.
    let v2 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after 2nd delta");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 400),
        None,
        lin(),
        v2,
        Some(threshold),
        &[],
    )
    .await
    .expect("third cow delta crossing the threshold");
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
    assert_eq!(payload["name"], "orders_cow_consolidate");

    // Write 4: already armed -> no second job even though the counter keeps
    // accruing past the threshold.
    let v3 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after 3rd delta");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 500),
        None,
        lin(),
        v3,
        Some(threshold),
        &[],
    )
    .await
    .expect("fourth cow delta, already armed");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        1,
        "debounced: still exactly one stream_consolidate job after a fourth delta"
    );
}

/// `consolidate_threshold = None` disables triggering entirely, no matter how
/// many deltas accrue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_threshold_never_enqueues() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];

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

    let mut expected = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after seed");
    for val in [200, 300, 400, 500, 600] {
        write_inline_delta(
            &pool,
            &table,
            &cols,
            "id",
            false,
            &full_row_batch(1, val),
            None,
            lin(),
            expected,
            None,
            &[],
        )
        .await
        .expect("cow delta with triggering disabled");
        expected = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
            .await
            .expect("version after delta");
    }

    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        0,
        "None threshold never enqueues, regardless of accrued deltas"
    );
}

/// After `clear_consolidate_trigger` resets the counter and disarms, further
/// deltas accrue from zero and can enqueue a second job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rearm_after_clear_enqueues_again() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = table();
    let cols = vec![id_spec(), val_spec()];

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

    let threshold = 1;

    // One delta crosses threshold=1 immediately -> one job.
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
        None,
        lin(),
        v0,
        Some(threshold),
        &[],
    )
    .await
    .expect("cow delta crossing threshold=1");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        1,
        "first crossing enqueues one job"
    );

    // Simulate the engine's post-consolidation reset: clear the trigger row
    // (mirrors what the `consolidate_stream`/COW consolidation handler does
    // on completion).
    let tid = tid_of(&pool, &table).await;
    let mut tx = pool.begin().await.expect("begin clear tx");
    clear_consolidate_trigger(&mut tx, tid)
        .await
        .expect("clear_consolidate_trigger");
    tx.commit().await.expect("commit clear tx");

    // A further delta re-accrues from zero and crosses threshold=1 again ->
    // a second job is enqueued.
    let v1 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after first delta");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 300),
        None,
        lin(),
        v1,
        Some(threshold),
        &[],
    )
    .await
    .expect("cow delta after re-arm");
    assert_eq!(
        job_count(&pool, STREAM_CONSOLIDATE_JOB_KIND).await,
        2,
        "re-armed trigger enqueues a second job on the next crossing"
    );
}

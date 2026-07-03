//! Inline-flush triggering: inline_append accrues bytes and enqueues one
//! flush_table job on crossing the threshold, debounced. loom_fixture_test.
//! Constructors (RunId, ColumnSpec, LineageEvent, fixture) verified against
//! tests/iceberg_flush.rs.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::FLUSH_JOB_KIND;
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;

fn one_long_col() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "n".into(),
        ty: "long".into(),
        nullable: true,
    }]
}

fn batch(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn job_count(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*) from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sub_threshold_enqueues_nothing() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    // A huge threshold: one small batch can never cross it.
    inline_append(
        &pool,
        &table,
        &one_long_col(),
        &batch(&[1, 2, 3]),
        lineage(run, &table),
        Some(1 << 40),
    )
    .await
    .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_enqueues_exactly_one_with_payload() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    // A tiny threshold: the first batch crosses it.
    inline_append(
        &pool,
        &table,
        &one_long_col(),
        &batch(&[1, 2, 3]),
        lineage(run, &table),
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 1);
    let payload: serde_json::Value =
        sqlx::query_scalar("select payload from queue.jobs where kind = $1")
            .bind(FLUSH_JOB_KIND)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(payload["schema"], "wh");
    assert_eq!(payload["name"], "t");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_twice_is_debounced_to_one_job() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    for _ in 0..2 {
        let run = RunId(uuid::Uuid::new_v4());
        inline_append(
            &pool,
            &table,
            &one_long_col(),
            &batch(&[9]),
            lineage(run, &table),
            Some(1),
        )
        .await
        .unwrap();
    }
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 1, "debounced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn none_threshold_never_enqueues() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    inline_append(
        &pool,
        &table,
        &one_long_col(),
        &batch(&[1]),
        lineage(run, &table),
        None,
    )
    .await
    .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 0);
}

async fn trigger_row(pool: &sqlx::PgPool, schema: &str, name: &str) -> Option<(i64, bool)> {
    let tid: Option<i64> = sqlx::query_scalar(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace=$1 and table_name=$2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_optional(pool)
    .await
    .unwrap();
    let tid = tid?;
    sqlx::query_as::<_, (i64, bool)>(
        "select live_bytes, enqueued from iceberg_mirror.inline_trigger where table_id=$1",
    )
    .bind(tid)
    .fetch_optional(pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_resets_trigger_on_some() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // Cross the threshold (enqueued=true, live_bytes>0).
    inline_append(
        &pool,
        &table,
        &one_long_col(),
        &batch(&[1, 2, 3]),
        lineage(run, &table),
        Some(1),
    )
    .await
    .unwrap();
    assert!(
        trigger_row(&pool, "wh", "t").await.unwrap().1,
        "armed before flush"
    );

    // Flush: rows are live -> Some path.
    flush_table(&catalog, &pool, &table, run)
        .await
        .unwrap()
        .expect("flushed");
    assert_eq!(
        trigger_row(&pool, "wh", "t").await.unwrap(),
        (0, false),
        "reset after Some flush"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_flush_disarms_the_flag() {
    let fx = PgFixture::shared();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    inline_append(
        &pool,
        &table,
        &one_long_col(),
        &batch(&[1]),
        lineage(run, &table),
        Some(1),
    )
    .await
    .unwrap();
    // First flush drains the rows (Some).
    flush_table(&catalog, &pool, &table, run).await.unwrap();
    // Re-arm to simulate a stuck flag, then flush again -> None path must clear it.
    sqlx::query("update iceberg_mirror.inline_trigger set enqueued=true")
        .execute(&pool)
        .await
        .unwrap();
    let r = flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .unwrap();
    assert!(r.is_none(), "nothing live -> None");
    assert!(
        !trigger_row(&pool, "wh", "t").await.unwrap().1,
        "None path disarmed the flag"
    );
}

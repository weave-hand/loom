//! The changelog wakeup + resume substrate (road-stream-subscribe):
//!   * `changelog_positions_latest` reports the per-bucket high-water for a CDC
//!     table (matching `peek_offset`), and None for a non-CDC table;
//!   * a blocked `await_changelog` resolves PROMPTLY (well under its timeout)
//!     when a CDC inline write commits — proving the pg_notify fires inside the
//!     commit, not that the poll timer expired;
//!   * with no write, `await_changelog` returns Ok at its timeout (poll fallback).
//!
//! loom_fixture_test (Postgres).

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    BucketOffsets, ColumnSpec, EventType, LineageEvent, RunId, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::stream::{await_changelog, changelog_positions_latest};

fn table() -> TableRef {
    TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
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

/// A one-cell batch holding just the id column (`long`).
fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

/// A full one-row `{id, val}` batch (the full property set — used as before/after
/// images).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notify_fires_in_commit_and_latest_positions_track_peek() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = table();
    let cols = vec![id_spec(), val_spec()];

    // ensure_table + declare_cdc(2 buckets, "id") — the stream_cdc_emission shape.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // (a) Not-yet-written CDC table: positions exist and are all 0.
    let empty = changelog_positions_latest(&pool, &table)
        .await
        .expect("latest")
        .expect("declared cdc table has positions");
    assert_eq!(empty, std::collections::BTreeMap::from([(0, 0), (1, 0)]));

    // (b) A blocked waiter resolves promptly when a +I commits. The waiter's
    // timeout is 10s; the write lands ~immediately; anything under 5s proves the
    // NOTIFY path woke it (a missed notify would sleep the full 10s).
    let waiter = {
        let pool = pool.clone();
        let table = table.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            await_changelog(&pool, &table, Duration::from_secs(10))
                .await
                .expect("await_changelog");
            started.elapsed()
        })
    };
    // Give the listener time to subscribe before the write commits.
    tokio::time::sleep(Duration::from_millis(300)).await;
    iceberg_inline::inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 100),
        lin(),
        None,
        None,
    )
    .await
    .expect("+I append");
    let woke_after = waiter.await.expect("join");
    assert!(
        woke_after < Duration::from_secs(5),
        "notify (not the 10s poll fallback) woke the waiter: {woke_after:?}"
    );

    // (c) Positions advanced to peek: id=1's bucket now peeks 1, the other 0.
    let after = changelog_positions_latest(&pool, &table)
        .await
        .expect("latest")
        .expect("positions");
    let mut want = std::collections::BTreeMap::new();
    for b in 0..2 {
        want.insert(b, cp.peek_offset(tid, b).await.expect("peek"));
    }
    assert_eq!(
        after, want,
        "latest positions == BucketOffsets::peek per bucket"
    );
    assert_eq!(
        after.values().sum::<i64>(),
        1,
        "exactly one event allocated"
    );

    // (d) A mutation (write_inline_delta -U/+U) also notifies. Same waiter shape.
    let waiter = {
        let pool = pool.clone();
        let table = table.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            await_changelog(&pool, &table, Duration::from_secs(10))
                .await
                .expect("await_changelog");
            started.elapsed()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let v = iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version");
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 200),
        Some((&cols, &full_row_batch(1, 100))),
        lin(),
        v,
        None,
    )
    .await
    .expect("update");
    let woke_after = waiter.await.expect("join");
    assert!(
        woke_after < Duration::from_secs(5),
        "mutation notify: {woke_after:?}"
    );

    // (e) No write: the waiter returns Ok at its (short) poll-fallback timeout.
    let started = std::time::Instant::now();
    await_changelog(&pool, &table, Duration::from_millis(400))
        .await
        .expect("timeout is Ok, not an error");
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "waited out the timeout"
    );

    // (f) Non-CDC table reads None.
    let plain = TableRef {
        schema: "sales".into(),
        name: "plain".into(),
    };
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    ensure_table(&mut tx, &plain.schema, &plain.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    assert!(
        changelog_positions_latest(&pool, &plain)
            .await
            .expect("latest")
            .is_none(),
        "a non-stream table has no feed positions"
    );
}

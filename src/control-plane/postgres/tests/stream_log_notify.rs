//! road-stream-log-table-subscribe seam 3: a log-table inline append fires the
//! subscribe NOTIFY, so a blocked `await_changelog` wakes promptly (not only on
//! the poll-fallback timeout). loom_fixture_test.

use std::time::{Duration, Instant};

use control_plane_core::{DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::stream::await_changelog;
use loom_test_seed::{hot_limits, id_val_batch, id_val_columns, local_sql_catalog};

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "stream-log-notify-test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn await_wakes_on_a_log_append() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &warehouse).await;
    let table = TableRef {
        schema: "s".into(),
        name: "events".into(),
    };

    // Declare + seed one row so the mirror table exists (await resolves the tid).
    // `hot_limits()` = the INLINE append path — the only path that fires the
    // NOTIFY (the direct-Parquet bulk path does not; see the plan's scope note).
    let (schema, batches) = id_val_batch(&[1], &[10]);
    land(
        &pool,
        &catalog,
        &table,
        &id_val_columns(),
        schema,
        batches,
        hot_limits(),
        lineage(&table),
        Some(2),
    )
    .await
    .expect("seed one row");

    // Block on await with a generous timeout, then append after a short delay.
    // The NOTIFY must wake it well before the timeout.
    let appender = {
        let pool = pool.clone();
        let table = table.clone();
        // `SqlCatalog` isn't `Clone`; build a fresh one over the same dsn/warehouse
        // for the spawned task, matching the established pattern (e.g.
        // `iceberg_write_roundtrip.rs`'s concurrent-append test).
        let dsn = fx.pg_dsn(&db);
        let warehouse = warehouse.clone();
        tokio::spawn(async move {
            let catalog = local_sql_catalog(dsn, &warehouse).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (schema, batches) = id_val_batch(&[2], &[20]);
            land(
                &pool,
                &catalog,
                &table,
                &id_val_columns(),
                schema,
                batches,
                hot_limits(),
                lineage(&table),
                Some(2),
            )
            .await
            .expect("append triggers NOTIFY");
        })
    };

    let started = Instant::now();
    await_changelog(&pool, &table, Duration::from_secs(10))
        .await
        .expect("await returns");
    let waited = started.elapsed();
    appender.await.expect("appender joined");

    // Woken by the NOTIFY (~200ms), NOT the 10s poll-fallback timeout.
    assert!(
        waited < Duration::from_secs(2),
        "await woke on the log-append NOTIFY, not the timeout: waited {waited:?}"
    );
}

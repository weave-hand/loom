//! road-stream-log-table-subscribe seam 1: the positions probe stops refusing
//! log tables. `changelog_positions_latest` returns per-bucket next offsets for
//! a declared LOG table; `None` for an undeclared table. loom_fixture_test.

use control_plane_core::{DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::stream::{changelog_positions_latest, stream_meta_for};
use loom_test_seed::{hot_limits, id_val_batch, id_val_columns, local_sql_catalog};

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "stream-log-positions-test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positions_probe_answers_for_a_log_table() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "events".into(),
    };
    // 3 rows into a 2-bucket log stream (row_index % 2 → buckets 0,1,0).
    // `hot_limits()` = inline path (the offset-framed inline append), which is
    // what a log producer uses and what the subscribe feed's inline tier reads.
    let (schema, batches) = id_val_batch(&[1, 2, 3], &[10, 20, 30]);
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
    .expect("land log rows");

    let kind = stream_meta_for(&pool, &table)
        .await
        .expect("meta")
        .expect("declared")
        .kind;
    assert_eq!(kind, control_plane_core::StreamKind::Log);

    let positions = changelog_positions_latest(&pool, &table)
        .await
        .expect("probe")
        .expect("a declared log table is subscribable");
    // 2 buckets present; total next offsets == 3 rows landed.
    assert_eq!(positions.len(), 2, "one entry per bucket: {positions:?}");
    assert_eq!(
        positions.values().sum::<i64>(),
        3,
        "per-bucket next offsets sum to the 3 landed rows: {positions:?}"
    );

    // An undeclared table (no mirror row) is not subscribable.
    let missing = TableRef {
        schema: "s".into(),
        name: "nope".into(),
    };
    assert!(
        changelog_positions_latest(&pool, &missing)
            .await
            .expect("probe missing")
            .is_none(),
        "undeclared table probes to None"
    );
    assert!(
        stream_meta_for(&pool, &missing)
            .await
            .expect("meta missing")
            .is_none()
    );
}

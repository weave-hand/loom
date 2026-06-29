//! inline_append: a mirror-only typed inline write. loom_fixture_test (Postgres).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use sqlx::Row;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_append_writes_rows_snapshot_and_lineage() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    // Seed a real Parquet table first so the mirror table/columns exist.
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await;

    // Inline-append two rows; assert it advanced the snapshot and emitted lineage.
    let run = uuid::Uuid::new_v4();
    let snap = writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100"), (101, "row101")],
            run,
        )
        .await;
    assert!(snap > 0, "inline write returned a snapshot id");

    // The per-table inline table exists and holds the two rows at this snapshot.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='orders' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} where begin_snapshot = {snap}"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 2, "two inline rows at the new snapshot");

    // Lineage event was emitted for the run.
    let events: i64 = sqlx::query("select count(*) from lineage.event where run_id = $1")
        .bind(run)
        .fetch_one(&pool)
        .await
        .map(|r| r.get(0))
        .unwrap();
    assert_eq!(events, 1, "one lineage event for the inline write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_live_batch_reconstructs_live_rows() {
    use control_plane_postgres::iceberg_catalog::IcebergCatalog;

    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await;
    let snap = writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100"), (101, "row101")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let table = control_plane_core::TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let (_tid, row_ids, batch) = catalog
        .inline_live_batch(&table, control_plane_core::SnapshotId(snap))
        .await
        .expect("inline_live_batch")
        .expect("Some batch when inline rows exist");
    assert_eq!(batch.num_rows(), 2, "two live inline rows reconstructed");
    assert_eq!(row_ids.len(), 2, "two row ids returned");

    // A table that was never inline-written yields None at its seed snapshot.
    let other = control_plane_core::TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let none = catalog
        .inline_live_batch(&other, control_plane_core::SnapshotId(1))
        .await
        .expect("inline_live_batch at snapshot 1");
    assert!(none.is_none(), "no inline rows live at the seed snapshot");
}

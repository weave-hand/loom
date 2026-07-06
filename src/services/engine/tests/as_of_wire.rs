//! Engine-level as-of read plane wire test: boot a `FlightDataService` over a
//! UDS, land a table twice (S1 then S2), and assert `execute_as_of` pinned to
//! S1 sees only the first write while the plain `execute` sees the cumulative
//! (current-snapshot) result.

use loom_test_flight::spawn_flight_uds;

use arrow_array::{Array, Int64Array};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use engine_wire::flight::FlightSqlClient;

fn scalar_count(batches: &[arrow_array::RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1, "count(*) yields exactly one result batch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "count(*) yields exactly one row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) column is i64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_of_read_sees_only_first_write() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    // Land table twice: S1 (3 rows), then S2 (2 more rows, 5 cumulative).
    let snapshots = writer.seed("sales", "orders", &cols, &[3, 2]).await;
    let s1 = snapshots[0];

    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightSqlClient::connect(&eng.sock).await.expect("connect");

    let as_of_batches = client
        .execute_as_of(
            r#"SELECT count(*) AS n FROM "sales"."orders""#.to_string(),
            s1,
        )
        .await
        .expect("execute_as_of");
    assert_eq!(
        scalar_count(&as_of_batches),
        3,
        "as-of S1 must see only the first write"
    );

    let current_batches = client
        .execute(r#"SELECT count(*) AS n FROM "sales"."orders""#.to_string())
        .await
        .expect("execute");
    assert_eq!(
        scalar_count(&current_batches),
        5,
        "plain read must see the cumulative result"
    );
}

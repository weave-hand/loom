//! Cross-wire e2e: boots a real engine `FlightDataService` over a UDS, then connects
//! an `EngineServingClient` (now an internal **Flight SQL** client) and asserts:
//!   1. a small read unions file + inline rows in order;
//!   2. a result far larger than the old ~4 MB unary gRPC message cap streams back
//!      intact (the payoff of streaming);
//!   3. a malformed query surfaces as a `ServingError`.

use loom_test_flight::spawn_flight_uds;

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use query_api::engine_client::EngineServingClient;
use query_api::serving::{ServingEngine, ServingError, SqlValue};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_wire_unions_file_and_inline() {
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
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&eng.sock)
        .await
        .expect("connect");

    let rows = client
        .fetch_rows(r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#, &[])
        .await
        .expect("fetch_rows over flight-sql");
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 100]);

    // Malformed SQL → ServingError (engine maps it to internal; client surfaces Err).
    assert!(
        client.fetch_rows("SELECT FROM nope", &[]).await.is_err(),
        "malformed SQL must error"
    );
}

/// A result whose collected Arrow IPC would exceed the ~4 MB unary gRPC message cap
/// streams back intact over Flight SQL. 600_000 `i64` rows ≈ 4.8 MB for the id column
/// alone — the old unary `ExecuteQueryResponse{ipc}` would have exceeded tonic's
/// default 4 MB decode limit and failed; per-batch Flight messages do not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_result_streams_past_unary_cap() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("big", "rows", &cols, &[600_000]).await;

    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&eng.sock)
        .await
        .expect("connect");

    let rows = client
        .fetch_rows(r#"SELECT "id" FROM "big"."rows""#, &[])
        .await
        .expect("large result must stream back, not hit the 4 MB cap");
    assert_eq!(
        rows.rows.len(),
        600_000,
        "all rows must arrive over the stream"
    );
}

// ---------------------------------------------------------------------------
// WHITELISTED (road-engine-wire-dedup): over the wire, a planning-class SQL
// fault now reaches query-api as ServingError::Plan (HTTP 400), not an opaque
// Engine 500. (The code-agnostic is_err assertion above stays green.)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_sql_is_plan_class() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&eng.sock)
        .await
        .expect("connect");

    let err = client
        .fetch_rows("SELECT FROM nope", &[])
        .await
        .expect_err("malformed SQL must error");
    assert!(
        matches!(&err, ServingError::Plan(m) if m.starts_with("query planning failed: ")),
        "planning fault must carry the Plan class, got: {err:?}"
    );
}

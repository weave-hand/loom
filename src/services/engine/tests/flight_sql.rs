//! Engine-level Flight SQL wire test: boot a `FlightDataService` over a UDS, seed a
//! table (file rows + one inline row), issue a `CommandStatementQuery` via the
//! `FlightSqlClient`, and assert the streamed batches reassemble to the unioned
//! result. Also asserts a malformed query surfaces an error (mapped from the
//! engine's `do_get`).

use loom_test_flight::spawn_flight_uds;

use arrow_array::{Array, Int64Array};
use control_plane_core::ControlPlaneError;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use engine_wire::flight::FlightSqlClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_sql_streams_unioned_result() {
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
    let client = FlightSqlClient::connect(&eng.sock).await.expect("connect");

    let batches = client
        .execute(r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#.to_string())
        .await
        .expect("flight-sql execute");
    let mut ids = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    assert_eq!(
        ids,
        vec![0, 1, 2, 100],
        "file rows + inline row, streamed in id order"
    );

    // Malformed SQL must surface as an error (engine maps it from do_get).
    let err = client.execute("SELECT FROM nope".to_string()).await;
    assert!(err.is_err(), "malformed SQL must error");
}

// ---------------------------------------------------------------------------
// WHITELISTED (road-engine-wire-dedup): a statement that fails DataFusion
// PLANNING is the client's fault — invalid_argument on the wire, Validation off
// the client — no longer an opaque internal/Backend. (The pre-existing
// malformed-SQL assertion above is code-agnostic and stays green.)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_sql_is_validation_class() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightSqlClient::connect(&eng.sock).await.expect("connect");

    let err = client
        .execute("SELECT FROM nope".to_string())
        .await
        .expect_err("malformed SQL must error");
    assert!(
        matches!(&err, ControlPlaneError::Validation(m) if m.starts_with("query planning failed: ")),
        "planning fault must carry the Validation class, got: {err:?}"
    );
}

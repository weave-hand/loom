//! do_get(MvEnrichTicket) end-to-end over the engine's Flight UDS: keyed and
//! unkeyed enrich reads return the folded state; an unknown table maps to a
//! gRPC error status. loom_fixture_test.

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use engine_wire::flight::{FlightTableClient, MvEnrichTicket};
use loom_test_flight::spawn_flight_uds;

fn row_count(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(arrow_array::RecordBatch::num_rows).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_enrich_round_trips_over_flight() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let wh_str = wh.path().display().to_string();

    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    // Two rows: id=0, id=1.
    writer.seed("s", "customers", &cols, &[2]).await;

    let eng = spawn_flight_uds(fx, &db, &wh_str).await;
    let client = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect");

    let unkeyed = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: None,
            keys: vec![],
        })
        .await
        .expect("unkeyed fetch");
    assert_eq!(row_count(&unkeyed), 2, "full folded state");

    let keyed = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: Some("id".into()),
            keys: vec![serde_json::json!(1)],
        })
        .await
        .expect("keyed fetch");
    assert_eq!(row_count(&keyed), 1, "keyed lookup");

    let missing = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "nope".into(),
            key: None,
            keys: vec![],
        })
        .await;
    assert!(missing.is_err(), "unknown table is a wire error");
}

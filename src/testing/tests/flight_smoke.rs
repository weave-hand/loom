//! Smoke for `loom_test_flight`: `spawn_engine_uds` is READY on return —
//! a client connects immediately, no post-spawn sleep (the connect-retry
//! readiness contract that replaces the tree's fixed-delay readiness sleeps).

use control_plane_postgres::fixture::PgFixture;
use loom_test_flight::{EngineOpts, spawn_engine_uds, spawn_flight_uds};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_is_ready_on_return() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh.path().display().to_string(),
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    // No sleep: the readiness contract is that this connects first try.
    let client = engine_wire::client::GrpcQueueClient::connect(&eng.sock).await;
    assert!(
        client.is_ok(),
        "control reachable on return: {:?}",
        client.err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_only_spawn_accepts_connections() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let stream = tokio::net::UnixStream::connect(&eng.sock).await;
    assert!(stream.is_ok(), "flight socket accepts: {:?}", stream.err());
}

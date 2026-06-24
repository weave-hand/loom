//! Pure-socket smoke for the `spawn_http` wire harness: spawn a trivial axum
//! router on a real ephemeral port via `service_runtime::serve`, hit it with a
//! real `reqwest` client, and assert the round-trip. No Postgres/DuckDB — this
//! is a bare `rust_test`, not a fixture test.

use axum::Router;
use axum::routing::get;
use e2e_support::spawn_http;

#[tokio::test(flavor = "multi_thread")]
async fn spawn_http_serves_over_a_real_socket() {
    let router = Router::new().route("/ping", get(|| async { "ok" }));
    let (base, _guard) = spawn_http(router).await;

    let resp = reqwest::get(format!("{base}/ping")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

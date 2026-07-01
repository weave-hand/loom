//! `serve_with_shutdown` returns once its shutdown future resolves.
use std::time::Duration;

use axum::{Router, routing::get};

#[tokio::test]
async fn shutdown_future_stops_the_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let router = Router::new().route("/health", get(|| async { "ok" }));
    let handle = tokio::spawn(async move {
        service_runtime::serve_with_shutdown(listener, router, async move {
            drop(rx.await);
        })
        .await
    });

    // Server is accepting: a TCP connect to the bound port succeeds.
    tokio::net::TcpStream::connect(addr).await.unwrap();

    // Fire shutdown; the serve task must return Ok promptly.
    tx.send(()).unwrap();
    let joined = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("serve_with_shutdown did not return within 5s");
    assert!(joined.unwrap().is_ok());
}

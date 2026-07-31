//! `serve_with_shutdown` returns once its shutdown future resolves.
use std::time::Duration;

use axum::{Router, routing::get};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

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

#[tokio::test]
async fn an_in_flight_request_completes_across_a_shutdown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // A handler slow enough that shutdown is guaranteed to land mid-request.
    let router = Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            "done"
        }),
    );
    let serve = tokio::spawn(async move {
        service_runtime::serve_with_shutdown(listener, router, async move {
            drop(rx.await);
        })
        .await
    });

    let client = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // `Connection: close` makes the server close the socket after the response,
        // so `read_to_end` below terminates on the response rather than blocking on
        // an idle keep-alive connection.
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.unwrap();
        String::from_utf8_lossy(&body).into_owned()
    });

    // Signal shutdown while the request is still in flight.
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(()).unwrap();

    let body = tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .expect("the in-flight request never completed (or the socket stayed open)")
        .unwrap();
    assert!(
        body.contains("200 OK") && body.contains("done"),
        "the in-flight request was severed by shutdown: {body}"
    );

    let joined = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve_with_shutdown did not return after the drain");
    assert!(joined.unwrap().is_ok(), "a graceful drain returns Ok");
}

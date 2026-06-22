//! internal_error logs the detail server-side and returns an opaque 500 to the
//! client. chain_error and graph_error delegate to internal_error for their
//! catch-all arms.

use axum::http::StatusCode;
use http_body_util::BodyExt;
use query_api::handler::QueryError;
use query_api::http::{chain_error, graph_error, internal_error};
use query_api::serving::ServingError;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn internal_error_opaque_body_and_logs_detail() {
    let resp = internal_error("object read serving fault", "boom");
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert_eq!(body_str, "internal error");
    assert!(!body_str.contains("boom"), "detail must not leak to client");
    assert!(logs_contain("boom"));
}

#[tokio::test]
#[traced_test]
async fn chain_error_catch_all_logs_serving_error() {
    let e = QueryError::Serving(ServingError::Engine("chain-boom".into()));
    let resp = chain_error(e);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "internal error");
    assert!(logs_contain("chain-boom"));
}

#[tokio::test]
#[traced_test]
async fn graph_error_catch_all_logs_serving_error() {
    let e = QueryError::Serving(ServingError::Engine("graph-boom".into()));
    let resp = graph_error(e);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "internal error");
    assert!(logs_contain("graph-boom"));
}

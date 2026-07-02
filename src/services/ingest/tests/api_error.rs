//! `ApiError` variant → HTTP status mapping and `IngestError::into_api` routing.
//! Pure: `IntoResponse::into_response` is synchronous and `Response::status()`
//! needs no body collection, so no Postgres/tokio.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use http_body_util::BodyExt;
use ingest::IngestError;
use ingest::http::ApiError;
use tracing_test::traced_test;

fn status_of(e: ApiError) -> StatusCode {
    e.into_response().status()
}

#[test]
fn bad_request_is_400() {
    assert_eq!(
        status_of(ApiError::BadRequest("nope".into())),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn forbidden_is_403() {
    assert_eq!(status_of(ApiError::Forbidden), StatusCode::FORBIDDEN);
}

#[test]
fn violations_is_422() {
    assert_eq!(
        status_of(ApiError::Violations(vec![])),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[test]
fn internal_is_500() {
    assert_eq!(
        status_of(ApiError::internal("ctx", "boom")),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn into_api_maps_conformance_to_422() {
    assert_eq!(
        IngestError::DoesNotConform(vec![])
            .into_api("ctx")
            .into_response()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[test]
fn into_api_maps_no_snapshot_to_500() {
    assert_eq!(
        IngestError::NoSnapshot
            .into_api("ctx")
            .into_response()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

/// The crux of iss-ingest-model-500-unlogged: an `Internal` fault logs its detail
/// server-side (operator-visible) yet the client body stays the opaque
/// `"internal error"` — the detail never leaks. Mirrors query-api's
/// `serving_fault_logs_detail_and_returns_opaque_500`.
#[tokio::test]
#[traced_test]
async fn internal_logs_detail_and_returns_opaque_500() {
    let resp = ApiError::internal("ingest test context", "fault-detail-boom").into_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert_eq!(body_str, "internal error");
    assert!(
        !body_str.contains("fault-detail-boom"),
        "fault detail must not leak to the client"
    );
    assert!(
        logs_contain("fault-detail-boom"),
        "the fault detail is logged server-side for the operator"
    );
    assert!(logs_contain("ingest test context"), "the context is logged");
}

/// A 403 carries an empty body (no existence leak): the coarse ACL gate must not
/// reveal type existence via a distinguishable body.
#[tokio::test]
async fn forbidden_has_empty_body() {
    let resp = ApiError::Forbidden.into_response();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty(), "403 carries no body");
}

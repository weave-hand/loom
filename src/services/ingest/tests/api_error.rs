//! `ApiError` variant → HTTP status mapping and `IngestError::into_api` routing.
//! Pure: `IntoResponse::into_response` is synchronous and `Response::status()`
//! needs no body collection, so no Postgres/tokio.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use ingest::IngestError;
use ingest::http::ApiError;

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

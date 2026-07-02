//! Totality pin for the single QueryError -> HTTP response mapping. The match in
//! http.rs has no catch-all over QueryError's variants (a NEW variant fails
//! compilation there); this test pins the status each existing variant maps to —
//! the union of the four partial per-route copies it replaced.
use axum::http::StatusCode;
use control_plane_core::ControlPlaneError;
use query_api::filter::FilterError;
use query_api::handler::QueryError;
use query_api::http::query_error_response;
use query_api::serving::ServingError;
use query_api::sql::CompileError;

fn status(e: QueryError) -> StatusCode {
    query_error_response(e, "test context").status()
}

#[test]
fn not_found_variants() {
    assert_eq!(
        status(QueryError::UnknownType("T".into())),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(QueryError::UnknownLink("l".into())),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(QueryError::Serving(ServingError::NoIndex("i".into()))),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn bad_request_variants() {
    assert_eq!(
        status(QueryError::AmbiguousLink("l".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::BadFilter("c".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::BadFilterValue(FilterError::BadValue(
            "c".into(),
            "m".into()
        ))),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::BadChain("m".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::NoIdentity("t".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::NotCyclicPath("p".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::BadGraphPath("m".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::BadPagination("m".into())),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status(QueryError::Serving(ServingError::DimMismatch("d".into()))),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn forbidden_is_403() {
    assert_eq!(status(QueryError::Forbidden), StatusCode::FORBIDDEN);
}

#[test]
fn internal_variants_are_opaque_500() {
    assert_eq!(
        status(QueryError::ControlPlane(ControlPlaneError::NotFound(
            "x".into()
        ))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        status(QueryError::Serving(ServingError::Engine("e".into()))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        status(QueryError::Malformed(CompileError::MalformedFilter(
            "f".into()
        ))),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

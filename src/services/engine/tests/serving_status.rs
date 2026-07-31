//! Unit pins for `serving_status` — the one total `EngineServingError` -> gRPC
//! `Status` mapping shared by the engine's Flight data plane and the
//! `EngineControl` inline-delta write RPCs. NoIndex/DimMismatch/Conflict carry
//! the INNER message only (no enum prefix — the wire contract the clients'
//! inverse mappings were built against); Plan/Engine carry their full Display.

use datafusion::error::DataFusionError;
use engine::flight::serving_status;
use engine_serving::EngineServingError;

#[test]
fn no_index_is_not_found_with_inner_message() {
    let s = serving_status(EngineServingError::NoIndex("no index `by_flat`".into()));
    assert_eq!(s.code(), tonic::Code::NotFound);
    assert_eq!(s.message(), "no index `by_flat`");
}

#[test]
fn dim_mismatch_is_invalid_argument_with_inner_message() {
    let s = serving_status(EngineServingError::DimMismatch("query dim 2 != 4".into()));
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert_eq!(s.message(), "query dim 2 != 4");
}

#[test]
fn plan_is_invalid_argument_with_full_display() {
    let s = serving_status(EngineServingError::Plan(DataFusionError::Plan(
        "no table".into(),
    )));
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(
        s.message().starts_with("query planning failed: "),
        "got: {}",
        s.message()
    );
}

#[test]
fn engine_is_internal_with_full_display() {
    let s = serving_status(EngineServingError::Engine("boom".into()));
    assert_eq!(s.code(), tonic::Code::Internal);
    assert_eq!(s.message(), "engine serving: boom");
}

#[test]
fn conflict_is_aborted_with_inner_message() {
    let s = serving_status(EngineServingError::Conflict(
        "expected version 3, saw 4".into(),
    ));
    assert_eq!(s.code(), tonic::Code::Aborted);
    assert_eq!(s.message(), "expected version 3, saw 4");
}

#[test]
fn resource_exhausted_is_resource_exhausted_with_inner_message() {
    let s = serving_status(EngineServingError::ResourceExhausted(
        "statement exceeded its wall-clock budget (LOOM_SQL_TIMEOUT_SECS)".into(),
    ));
    assert_eq!(s.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        s.message(),
        "statement exceeded its wall-clock budget (LOOM_SQL_TIMEOUT_SECS)"
    );
}

//! `governed_sql_status` / `governed_sql_flight_error` — the governed-SQL plane's
//! inverse mapping. Distinct from `sql_status` (which has only Validation/Backend):
//! this plane carries a THIRD class, `ResourceExhausted`, and a mapper that silently
//! collapsed a 429 (resource-budget breach) into an opaque Backend 500 would be a
//! real regression, so every branch — including the message payload, not just the
//! variant — is pinned here.

use engine_wire::client::{GovernedSqlError, governed_sql_flight_error, governed_sql_status};
use tonic::Status;

// --- governed_sql_status: tonic::Status -> GovernedSqlError -----------------

#[test]
fn invalid_argument_maps_to_plan() {
    let e = governed_sql_status(Status::invalid_argument("query planning failed: no table"));
    assert!(matches!(e, GovernedSqlError::Plan(m) if m == "query planning failed: no table"));
}

#[test]
fn resource_exhausted_maps_to_resource_exhausted() {
    let e = governed_sql_status(Status::resource_exhausted("memory pool budget exceeded"));
    assert!(
        matches!(e, GovernedSqlError::ResourceExhausted(m) if m == "memory pool budget exceeded")
    );
}

#[test]
fn other_codes_map_to_backend() {
    let e = governed_sql_status(Status::internal("boom"));
    match e {
        GovernedSqlError::Backend(inner) => assert!(inner.to_string().contains("boom")),
        other => panic!("expected Backend, got {other:?}"),
    }
}

// --- governed_sql_flight_error: FlightError -> GovernedSqlError -------------

#[test]
fn tonic_flight_error_resource_exhausted_maps_to_resource_exhausted() {
    let status = Status::resource_exhausted("wall-clock budget exceeded");
    let e = governed_sql_flight_error(arrow_flight::error::FlightError::Tonic(Box::new(status)));
    assert!(
        matches!(e, GovernedSqlError::ResourceExhausted(m) if m == "wall-clock budget exceeded")
    );
}

#[test]
fn non_tonic_flight_error_maps_to_backend() {
    let e = governed_sql_flight_error(arrow_flight::error::FlightError::ProtocolError(
        "unexpected message".into(),
    ));
    match e {
        GovernedSqlError::Backend(inner) => {
            assert!(inner.to_string().contains("unexpected message"))
        }
        other => panic!("expected Backend, got {other:?}"),
    }
}

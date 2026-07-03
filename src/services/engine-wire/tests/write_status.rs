//! `write_status` — the inline-delta write-plane inverse mapping. Distinct from
//! `cp_status` (governance) in name only; asserts the same `Aborted` -> `Conflict`
//! class-preservation the query-api client (Task 7) relies on to retry a CAS loss
//! instead of treating it as an opaque backend fault.

use control_plane_core::ControlPlaneError;
use engine_wire::client::write_status;
use tonic::Status;

#[test]
fn aborted_maps_to_conflict() {
    let e = write_status(Status::aborted("expected version 3, saw 4"));
    assert!(matches!(e, ControlPlaneError::Conflict(m) if m == "expected version 3, saw 4"));
}

#[test]
fn other_codes_map_to_backend() {
    let e = write_status(Status::internal("boom"));
    assert!(matches!(e, ControlPlaneError::Backend(_)));
}

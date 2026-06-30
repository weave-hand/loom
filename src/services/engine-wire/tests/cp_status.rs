//! The client maps tonic Status codes back to ControlPlaneError variants so that
//! query-api's error handling (e.g. NotFound -> 404) behaves identically whether
//! governance is read direct or over the wire.

use control_plane_core::ControlPlaneError;
use engine_wire::client::cp_status;
use tonic::Status;

#[test]
fn not_found_maps_to_not_found() {
    let e = cp_status(Status::not_found("no such type: customer"));
    assert!(matches!(e, ControlPlaneError::NotFound(m) if m == "no such type: customer"));
}

#[test]
fn aborted_maps_to_conflict() {
    let e = cp_status(Status::aborted("conflict"));
    assert!(matches!(e, ControlPlaneError::Conflict(_)));
}

#[test]
fn other_codes_map_to_backend() {
    let e = cp_status(Status::internal("boom"));
    assert!(matches!(e, ControlPlaneError::Backend(_)));
}

//! `ControlPlaneError` is `Send + Sync` and its `Display` strings are the stable
//! contract adapters map onto. (Integration test — buck2 doesn't run inline
//! `#[cfg(test)]` modules.)

use control_plane_core::ControlPlaneError;

fn _assert_send_sync<T: Send + Sync>() {}

#[test]
fn variants_display_and_are_send_sync() {
    _assert_send_sync::<ControlPlaneError>();
    assert_eq!(
        ControlPlaneError::NotFound("job 7".into()).to_string(),
        "not found: job 7"
    );
    assert_eq!(ControlPlaneError::Unauthorized.to_string(), "unauthorized");
    assert_eq!(
        ControlPlaneError::Conflict("dup key".into()).to_string(),
        "conflict: dup key"
    );
    assert_eq!(
        ControlPlaneError::Validation("bad filter".into()).to_string(),
        "validation error: bad filter"
    );
}

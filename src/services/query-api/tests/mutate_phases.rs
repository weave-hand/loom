//! Pure unit tests for the mutate write-path phases extracted from
//! `action.rs::run_mutate`: `locate_unique_row` (identity match + corrupt-PK
//! guard) and `enforce_mutate_policy` (the three ordered policy legs).
//! No fixture; the e2e twins live in update_delete_e2e / _governance_e2e.

use control_plane_core::ControlPlaneError;
use query_api::action::{ActionError, locate_unique_row};
use query_api::serving::SqlValue;

fn row(id: i64, qty: i64) -> Vec<SqlValue> {
    vec![SqlValue::Int(id), SqlValue::Int(qty)]
}

#[test]
fn locate_finds_the_single_match() {
    let rows = vec![row(1, 10), row(2, 20)];
    let (idx, existing) = locate_unique_row(&rows, 0, &SqlValue::Int(2), "id").unwrap();
    assert_eq!(idx, 1);
    assert_eq!(existing, row(2, 20));
}

#[test]
fn locate_no_match_is_not_found() {
    let rows = vec![row(1, 10)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(9), "id").unwrap_err();
    assert!(matches!(err, ActionError::NotFound), "got: {err:?}");
}

#[test]
fn locate_duplicate_identity_is_a_backend_fault() {
    // >1 live row for a primary key is a corrupt invariant, not a client error.
    let rows = vec![row(1, 10), row(1, 20)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(1), "id").unwrap_err();
    assert!(
        matches!(&err, ActionError::ControlPlane(ControlPlaneError::Backend(_))),
        "got: {err:?}"
    );
    assert!(
        err.to_string().contains("matches more than one live row"),
        "got: {err}"
    );
}

#[test]
fn locate_matches_on_the_identity_column_only() {
    // qty=10 in the id slot of no row: values in other columns never match.
    let rows = vec![row(1, 10)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(10), "id").unwrap_err();
    assert!(matches!(err, ActionError::NotFound), "got: {err:?}");
}

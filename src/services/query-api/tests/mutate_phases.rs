//! Pure unit tests for the mutate write-path phases extracted from
//! `action.rs::run_mutate`: `locate_unique_row` (identity match + corrupt-PK
//! guard) and `enforce_mutate_policy` (the three ordered policy legs).
//! No fixture; the e2e twins live in update_delete_e2e / _governance_e2e.

use control_plane_core::{
    CompareOp, ControlPlaneError, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName,
};
use query_api::action::{ActionError, WriteDenialReason, enforce_mutate_policy, locate_unique_row};
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
        matches!(
            &err,
            ActionError::ControlPlane(ControlPlaneError::Backend(_))
        ),
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

// --- enforce_mutate_policy: the three ordered legs ------------------------

/// One policy carrying a `qty < 5` row-filter and the given deny columns.
fn qty_lt_5_policy(deny: Vec<String>) -> Policy {
    Policy {
        target: PolicyTarget::Type(TypeName("Widget".into())),
        row_filter: Some(RowFilter::Compare {
            property: "qty".into(),
            op: CompareOp::Lt,
            value: ScalarValue::Int(5),
        }),
        deny_columns: deny,
        mask_columns: vec![],
    }
}

fn cols() -> Vec<String> {
    vec!["id".to_string(), "qty".to_string()]
}

#[test]
fn no_policies_admit_update_and_delete() {
    let existing = row(1, 9);
    let new = row(1, 1);
    let set = vec![("qty".to_string(), SqlValue::Int(1))];
    enforce_mutate_policy(&[], &cols(), &existing, &set, Some(&new), "updateWidget").unwrap();
    enforce_mutate_policy(&[], &cols(), &existing, &[], None, "deleteWidget").unwrap();
}

#[test]
fn leg1_existing_row_filter_runs_first() {
    // Existing row fails the filter AND the SET column is denied: the
    // existing-row leg must win (RowFilter) — pins the enforcement order.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 9); // fails qty < 5
    let new = row(1, 1);
    let set = vec![("qty".to_string(), SqlValue::Int(1))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn leg2_deny_column_runs_before_leg3_resulting_row() {
    // Existing row passes; the SET column is denied AND the resulting row
    // would fail: deny-column must win (Column) — pins the order.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 1); // passes
    let new = row(1, 9); // would fail
    let set = vec![("qty".to_string(), SqlValue::Int(9))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "qty"
        ),
        "got: {err:?}"
    );
}

#[test]
fn leg3_resulting_row_filter_denies_update() {
    let p = qty_lt_5_policy(vec![]);
    let existing = row(1, 1);
    let new = row(1, 9);
    let set = vec![("qty".to_string(), SqlValue::Int(9))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn delete_checks_only_the_existing_row() {
    // DELETE (new_row = None): deny-column is irrelevant (nothing is set);
    // only the existing-row leg applies.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 1); // passes the filter
    enforce_mutate_policy(&[p], &cols(), &existing, &[], None, "deleteWidget").unwrap();
    let failing = row(1, 9);
    let err = enforce_mutate_policy(
        &[qty_lt_5_policy(vec![])],
        &cols(),
        &failing,
        &[],
        None,
        "deleteWidget",
    )
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn unknown_filter_truth_fails_closed() {
    // A row-filter over a column absent from the row evaluates UNKNOWN -> denied
    // (three-valued logic, mirroring "an UNKNOWN WHERE row is excluded").
    let p = Policy {
        target: PolicyTarget::Type(TypeName("Widget".into())),
        row_filter: Some(RowFilter::Compare {
            property: "missing".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    let existing = row(1, 1);
    let err =
        enforce_mutate_policy(&[p], &cols(), &existing, &[], None, "deleteWidget").unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

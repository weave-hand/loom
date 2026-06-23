//! Pure shaping test for the structured write-denial reason: WriteVerdict ->
//! WriteDenialReason -> caller-scoped 403 JSON body. No fixture.

use query_api::action::WriteDenialReason;
use query_api::write_filter::WriteVerdict;
use serde_json::json;

#[test]
fn allow_verdict_has_no_reason() {
    assert_eq!(WriteDenialReason::from_verdict(WriteVerdict::Allow), None);
}

#[test]
fn deny_column_maps_to_column_reason_and_body() {
    let reason = WriteDenialReason::from_verdict(WriteVerdict::DenyColumn("ssn".into()))
        .expect("a column denial has a reason");
    assert_eq!(reason, WriteDenialReason::Column("ssn".into()));
    assert_eq!(
        reason.to_body(),
        json!({ "error": "write_denied", "reason": "column", "column": "ssn" })
    );
}

#[test]
fn deny_row_maps_to_row_filter_reason_and_body() {
    let reason = WriteDenialReason::from_verdict(WriteVerdict::DenyRow)
        .expect("a row-filter denial has a reason");
    assert_eq!(reason, WriteDenialReason::RowFilter);
    let body = reason.to_body();
    assert_eq!(
        body,
        json!({ "error": "write_denied", "reason": "row_filter" })
    );
    // The row-filter body discloses no column (and never the predicate).
    assert!(
        body.get("column").is_none(),
        "row_filter body must not carry a column field"
    );
}

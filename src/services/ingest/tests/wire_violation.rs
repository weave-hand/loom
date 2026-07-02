//! The wire-violation DTO (`WireViolation`/`ViolationsBody`) must serialize
//! byte-identically to the hand-rolled `serde_json::json!` shape it replaced, so
//! the 422 body on the wire is unchanged. Pure: no router/Postgres. loom's
//! serde_json has no `preserve_order`, so `Value` objects sort their keys — the
//! reference below is therefore key-sorted, matching `WireViolation`'s
//! alphabetically-declared fields.

use ingest::openapi::{ViolationsBody, WireViolation};
use ingest::{Violation, ViolationReason};

/// The pre-refactor hand-rolled shape (a verbatim copy of the deleted
/// `http::violations_json`), used only as the byte-identity oracle.
fn reference_json(violations: &[Violation]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = violations
        .iter()
        .map(|v| match &v.reason {
            ViolationReason::MissingRequired => {
                serde_json::json!({ "column": v.column, "reason": "missing_required" })
            }
            ViolationReason::TypeMismatch { expected, found } => serde_json::json!({
                "column": v.column,
                "reason": "type_mismatch",
                "expected": expected,
                "found": found,
            }),
            ViolationReason::Unsupported => {
                serde_json::json!({ "column": v.column, "reason": "unsupported" })
            }
            ViolationReason::Constraint { rule } => {
                serde_json::json!({ "column": v.column, "reason": "constraint", "rule": rule })
            }
        })
        .collect();
    serde_json::json!({ "violations": items })
}

fn all_reasons() -> Vec<Violation> {
    vec![
        Violation {
            column: "a".into(),
            reason: ViolationReason::MissingRequired,
        },
        Violation {
            column: "b".into(),
            reason: ViolationReason::TypeMismatch {
                expected: "long".into(),
                found: "string".into(),
            },
        },
        Violation {
            column: "c".into(),
            reason: ViolationReason::Unsupported,
        },
        Violation {
            column: "d".into(),
            reason: ViolationReason::Constraint {
                rule: "pattern".into(),
            },
        },
    ]
}

#[test]
fn wire_body_is_byte_identical_to_the_hand_rolled_shape() {
    let violations = all_reasons();
    let body = ViolationsBody {
        violations: violations.iter().map(WireViolation::from).collect(),
    };
    let got = serde_json::to_string(&body).unwrap();
    let want = serde_json::to_string(&reference_json(&violations)).unwrap();
    assert_eq!(got, want);
}

#[test]
fn absent_reason_fields_are_omitted_not_null() {
    let body = ViolationsBody {
        violations: vec![WireViolation::from(&Violation {
            column: "a".into(),
            reason: ViolationReason::MissingRequired,
        })],
    };
    let s = serde_json::to_string(&body).unwrap();
    assert_eq!(
        s,
        r#"{"violations":[{"column":"a","reason":"missing_required"}]}"#
    );
    assert!(
        !s.contains("null"),
        "skip_serializing_if must drop absent fields"
    );
}

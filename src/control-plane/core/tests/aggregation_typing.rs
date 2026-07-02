//! The `Aggregation` typing unit: column/label/applicability/result-category — the
//! cohesive home the bind derived-property validator delegates to (and the single
//! landing site for the future coercion taxonomy).

use control_plane_core::{Aggregation, BaseType, ResultExpectation};

#[test]
fn column_is_none_only_for_count() {
    assert_eq!(Aggregation::Count.column(), None);
    assert_eq!(Aggregation::Sum("amount".into()).column(), Some("amount"));
    assert_eq!(Aggregation::Avg("amount".into()).column(), Some("amount"));
    assert_eq!(Aggregation::Min("t".into()).column(), Some("t"));
    assert_eq!(Aggregation::Max("t".into()).column(), Some("t"));
}

#[test]
fn labels_match_variants() {
    assert_eq!(Aggregation::Count.label(), "Count");
    assert_eq!(Aggregation::Sum(String::new()).label(), "Sum");
    assert_eq!(Aggregation::Avg(String::new()).label(), "Avg");
    assert_eq!(Aggregation::Min(String::new()).label(), "Min");
    assert_eq!(Aggregation::Max(String::new()).label(), "Max");
}

#[test]
fn column_applicable_matches_numeric_ordered_rules() {
    // Sum/Avg require numeric.
    assert!(Aggregation::Sum(String::new()).column_applicable(Some(BaseType::Double)));
    assert!(!Aggregation::Sum(String::new()).column_applicable(Some(BaseType::String)));
    assert!(!Aggregation::Avg(String::new()).column_applicable(Some(BaseType::Boolean)));
    assert!(!Aggregation::Sum(String::new()).column_applicable(None));
    // Min/Max require ordered (everything except Boolean).
    assert!(Aggregation::Min(String::new()).column_applicable(Some(BaseType::String)));
    assert!(Aggregation::Max(String::new()).column_applicable(Some(BaseType::Date)));
    assert!(!Aggregation::Min(String::new()).column_applicable(Some(BaseType::Boolean)));
    assert!(!Aggregation::Max(String::new()).column_applicable(None));
    // Count is always applicable (no column).
    assert!(Aggregation::Count.column_applicable(None));
}

#[test]
fn result_expectation_accepts_and_describes() {
    // Count -> integer or long.
    let c = Aggregation::Count.result_expectation(None);
    assert!(c.accepts(Some(BaseType::Integer)));
    assert!(c.accepts(Some(BaseType::Long)));
    assert!(!c.accepts(Some(BaseType::Double)));
    assert_eq!(c.description(), "integer or long");

    // Sum/Avg -> numeric.
    let s = Aggregation::Sum("a".into()).result_expectation(Some(BaseType::Double));
    assert!(s.accepts(Some(BaseType::Long)));
    assert!(!s.accepts(Some(BaseType::String)));
    assert!(!s.accepts(None));
    assert_eq!(s.description(), "numeric");

    // Min/Max -> exact column type.
    let m = Aggregation::Max("t".into()).result_expectation(Some(BaseType::Timestamp));
    assert!(m.accepts(Some(BaseType::Timestamp)));
    assert!(!m.accepts(Some(BaseType::Date)));
    assert!(!m.accepts(None));
    assert_eq!(m.description(), BaseType::Timestamp.canonical_name());

    // Min/Max with an unresolved column type describes the fallback.
    let unknown = Aggregation::Min("t".into()).result_expectation(None);
    assert!(!unknown.accepts(Some(BaseType::Long)));
    assert_eq!(unknown.description(), "the target column's type");
}

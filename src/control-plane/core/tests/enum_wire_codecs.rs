//! The enum↔string wire codecs (persisted tokens). Round-trip + fail-loud:
//! an unknown token is a Validation error, never a silent default — the
//! postgres adapter's old free fns coerced corrupt rows to One/Start/Insert.

use std::str::FromStr;

use control_plane_core::{Action, ActionKind, Cardinality, ControlPlaneError, Effect, EventType};

#[test]
fn cardinality_round_trips_and_fails_loud() {
    assert_eq!(Cardinality::One.as_str(), "one");
    assert_eq!(Cardinality::Many.as_str(), "many");
    assert_eq!(Cardinality::from_str("one").unwrap(), Cardinality::One);
    assert_eq!(Cardinality::from_str("many").unwrap(), Cardinality::Many);
    assert!(matches!(
        Cardinality::from_str("both"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn event_type_round_trips_and_fails_loud() {
    let all = [
        (EventType::Start, "start"),
        (EventType::Running, "running"),
        (EventType::Complete, "complete"),
        (EventType::Abort, "abort"),
        (EventType::Fail, "fail"),
    ];
    for (v, s) in all {
        assert_eq!(v.as_str(), s);
        assert_eq!(EventType::from_str(s).unwrap(), v);
    }
    assert!(matches!(
        EventType::from_str("finished"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn action_kind_round_trips_and_fails_loud() {
    let all = [
        (ActionKind::Insert, "insert"),
        (ActionKind::Update, "update"),
        (ActionKind::Delete, "delete"),
    ];
    for (v, s) in all {
        assert_eq!(v.as_str(), s);
        assert_eq!(ActionKind::from_str(s).unwrap(), v);
    }
    assert!(matches!(
        ActionKind::from_str("upsert"),
        Err(ControlPlaneError::Validation(_))
    ));
}

#[test]
fn action_and_effect_tokens() {
    assert_eq!(Action::Read.as_str(), "read");
    assert_eq!(Action::Write.as_str(), "write");
    assert_eq!(Effect::Allow.as_str(), "allow");
    assert_eq!(Effect::Deny.as_str(), "deny");
}

//! Unit tests for the MergeEngine enum and the ObjectType.version builder field.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use control_plane_core::{MergeEngine, ObjectType};
use std::str::FromStr;

#[test]
fn merge_engine_round_trips_through_wire_token() {
    for engine in [
        MergeEngine::LastRow,
        MergeEngine::FirstRow,
        MergeEngine::Versioned,
    ] {
        let parsed = MergeEngine::from_str(engine.as_str()).expect("known token parses");
        assert_eq!(parsed, engine, "token round-trips");
    }
}

#[test]
fn merge_engine_unknown_token_is_a_loud_error() {
    assert!(
        MergeEngine::from_str("nonsense").is_err(),
        "unknown token rejected"
    );
}

#[test]
fn version_builder_field_round_trips() {
    let ty = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop_req("seq", "Long")
        .identity("id")
        .version("seq")
        .done();
    assert_eq!(ty.identity.as_deref(), Some("id"));
    assert_eq!(ty.version.as_deref(), Some("seq"));
    // A type with no version property reads None.
    let no_version = ObjectType::build("Other", ("main", "other"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    assert!(no_version.version.is_none());
}

#[test]
fn version_orderable_types_are_integer_long_timestamp() {
    use control_plane_core::BaseType;
    for ty in [BaseType::Integer, BaseType::Long, BaseType::Timestamp] {
        assert!(
            ty.is_version_orderable(),
            "{ty:?} should be version-orderable"
        );
    }
    for ty in [
        BaseType::Double,
        BaseType::Boolean,
        BaseType::String,
        BaseType::Date,
        BaseType::Vector(4),
    ] {
        assert!(
            !ty.is_version_orderable(),
            "{ty:?} should NOT be version-orderable"
        );
    }
}

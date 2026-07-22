//! Property tests for the query-api caller-string parsers. The invariant: arbitrary
//! caller input through the path/filter parsers never panics — every entry point
//! returns (Vec/Ok/Err) gracefully — plus a few cheap structural invariants. These
//! are the crate's designated pure-logic boundary (no ontology/fixture needed). See
//! git history: 2026-07-02-pillar-idioms-audit-design.

use proptest::prelude::*;
use query_api::filter::{coerce_filter, coerce_predicate, split_member, split_or_members};
use query_api::handler::{Direction, Hop};
use query_api::path_parse::{parse_direction, parse_graph_mode, parse_path_hops};

/// A known logical-type name, sampled alongside garbage so both the coercion happy
/// path and the UnknownLogicalType Err path are exercised.
fn logical_ty() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Integer".to_string()),
        Just("Long".to_string()),
        Just("Double".to_string()),
        Just("Boolean".to_string()),
        Just("String".to_string()),
        Just("Date".to_string()),
        Just("Timestamp".to_string()),
        Just("emailaddress".to_string()),
        Just("url".to_string()),
        Just("phonenumber".to_string()),
        Just("vector(4)".to_string()),
        ".{0,10}", // arbitrary / unknown (String)
    ]
}

/// A raw filter value: sometimes an op-prefixed form, sometimes arbitrary junk.
fn raw_value() -> impl Strategy<Value = String> {
    prop_oneof![
        ".{0,16}",
        (
            prop_oneof![
                Just("gt"),
                Just("lt"),
                Just("ge"),
                Just("le"),
                Just("ne"),
                Just("eq"),
                Just("in"),
                Just("nin"),
                Just("between"),
                Just("contains"),
                Just("startswith"),
                Just("endswith"),
                Just("isnull"),
                Just("isnotnull"),
            ],
            ".{0,16}"
        )
            .prop_map(|(op, rest)| format!("{op}:{rest}")),
    ]
}

fn hop() -> impl Strategy<Value = Hop> {
    (
        ".{0,8}",
        prop_oneof![Just(Direction::Forward), Just(Direction::Inverse)],
    )
        .prop_map(|(link, direction)| Hop { link, direction })
}

proptest! {
    /// parse_path_hops is infallible; assert it never panics and drops empty links.
    #[test]
    fn parse_path_hops_drops_empties(s in ".*") {
        let hops = parse_path_hops(&s);
        prop_assert!(hops.iter().all(|h| !h.link.is_empty()));
    }

    #[test]
    fn parse_direction_never_panics(s in proptest::option::of(".*")) {
        let res = parse_direction(s.as_deref());
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn parse_graph_mode_never_panics(
        path in prop::collection::vec(hop(), 0..5),
        links in prop::collection::vec(".{0,8}", 0..5),
        tree in any::<bool>(),
    ) {
        let res = parse_graph_mode(path, links, tree);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn coerce_filter_never_panics(name in ".{0,8}", ty in logical_ty(), raw in ".{0,16}") {
        let res = coerce_filter(&name, &ty, &raw);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn coerce_predicate_never_panics(col in ".{0,8}", ty in logical_ty(), raw in raw_value()) {
        let res = coerce_predicate(&col, &ty, &raw);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn split_or_members_never_panics(s in ".*") {
        let res = split_or_members(&s);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn split_member_never_panics(s in ".*") {
        let res = split_member(&s);
        prop_assert!(res.is_ok() || res.is_err());
    }
}

//! SubscribeCursor opaque-codec unit tests. External rust_test — see CLAUDE.md.

use std::collections::BTreeMap;

use query_api::subscribe::{CursorSpec, SubscribeCursor, parse_cursor};

#[test]
fn cursor_round_trips_through_the_opaque_form() {
    let c = SubscribeCursor {
        t: "Widget".into(),
        b: BTreeMap::from([(0, 3), (1, 17)]),
    };
    let opaque = c.to_opaque();
    // URL-safe: no '+', '/', '=' — usable raw in a query param.
    assert!(
        opaque
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'),
        "opaque form is base64url without padding: {opaque}"
    );
    let back = SubscribeCursor::from_opaque(&opaque).expect("decodes");
    assert_eq!(back, c, "round-trips");
}

#[test]
fn malformed_cursors_are_client_errors() {
    assert!(
        SubscribeCursor::from_opaque("!!not-base64!!").is_err(),
        "bad alphabet"
    );
    // Valid base64url of bytes that are not the cursor JSON.
    assert!(
        SubscribeCursor::from_opaque("aGVsbG8").is_err(),
        "not cursor JSON"
    );
}

#[test]
fn sentinels_parse_and_opaque_dispatches_to_resume() {
    assert!(matches!(parse_cursor(None), Ok(CursorSpec::Earliest)));
    assert!(matches!(
        parse_cursor(Some("earliest")),
        Ok(CursorSpec::Earliest)
    ));
    assert!(matches!(
        parse_cursor(Some("latest")),
        Ok(CursorSpec::Latest)
    ));
    let c = SubscribeCursor {
        t: "W".into(),
        b: BTreeMap::from([(0, 1)]),
    };
    match parse_cursor(Some(&c.to_opaque())) {
        Ok(CursorSpec::Resume(got)) => assert_eq!(got, c),
        other => panic!("expected Resume, got {other:?}"),
    }
    assert!(
        parse_cursor(Some("???")).is_err(),
        "garbage is a 400, not a sentinel"
    );
}

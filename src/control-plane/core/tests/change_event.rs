//! Unit tests for the transport-agnostic changelog feed contract types.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use std::collections::BTreeMap;

use control_plane_core::{ChangeEvent, ChangeFeedPage};

#[test]
fn change_event_serializes_to_the_wire_shape() {
    let mut fields = serde_json::Map::new();
    fields.insert("id".into(), serde_json::json!(1));
    fields.insert("qty".into(), serde_json::json!(9));
    let ev = ChangeEvent {
        bucket: 1,
        offset: 42,
        change_kind: "-U".into(),
        fields,
    };
    let json = serde_json::to_value(&ev).expect("serialize");
    assert_eq!(
        json,
        serde_json::json!({
            "bucket": 1, "offset": 42, "change_kind": "-U",
            "fields": { "id": 1, "qty": 9 }
        }),
        "the NDJSON line body is exactly the struct's serde shape"
    );
    let back: ChangeEvent = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, ev, "round-trips");
}

#[test]
fn change_feed_page_round_trips_positions() {
    let page = ChangeFeedPage {
        events: vec![],
        next: BTreeMap::from([(0, 3), (1, 0)]),
    };
    let json = serde_json::to_value(&page).expect("serialize");
    let back: ChangeFeedPage = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, page, "per-bucket next positions survive serde");
}

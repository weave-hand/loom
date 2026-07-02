use loom_ui_core::{cell_to_string, columns_from_objects, parse_objects_page};
use serde_json::json;

#[test]
fn parses_envelope_with_next() {
    let body =
        json!({ "objects": [ {"id": 1, "name": "a"}, {"id": 2, "name": "b"} ], "next": "cur42" });
    let page = parse_objects_page(&body);
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.next.as_deref(), Some("cur42"));
    assert_eq!(page.rows[0].get("name").unwrap(), &json!("a"));
}

#[test]
fn parses_last_page_null_next() {
    let body = json!({ "objects": [ {"id": 1} ], "next": serde_json::Value::Null });
    assert_eq!(parse_objects_page(&body).next, None);
}

#[test]
fn parses_missing_fields_gracefully() {
    assert_eq!(parse_objects_page(&json!({})).rows.len(), 0);
    assert_eq!(parse_objects_page(&json!({})).next, None);
    // non-object members skipped, not panicking
    let p = parse_objects_page(&json!({ "objects": [ 7, {"id": 1} ] }));
    assert_eq!(p.rows.len(), 1);
}

#[test]
fn columns_are_key_union_in_first_seen_order() {
    let rows = vec![
        serde_json::from_value(json!({"id": 1, "name": "a"})).unwrap(),
        serde_json::from_value(json!({"id": 2, "status": "ok"})).unwrap(),
    ];
    assert_eq!(columns_from_objects(&rows), vec!["id", "name", "status"]);
}

#[test]
fn cells_render_by_kind() {
    assert_eq!(cell_to_string(&json!(null)), "");
    assert_eq!(cell_to_string(&json!("hi")), "hi");
    assert_eq!(cell_to_string(&json!(42)), "42");
    assert_eq!(cell_to_string(&json!(true)), "true");
    assert_eq!(cell_to_string(&json!([1, 2])), "[1,2]");
}

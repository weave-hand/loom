//! Pure unit tests for `parse_dataset_runs` (the Catalog History tab parser). No
//! DOM, no network — runs as a native `rust_test`.

use loom_ui_core::{DatasetRunRow, parse_dataset_runs};

#[test]
fn parses_runs_in_server_order() {
    let body = serde_json::json!({"runs":[
        {"run_id":"r-b","latest_event_time":"2026-07-01T00:00:10Z","latest_event_type":"complete","role":"input"},
        {"run_id":"r-a","latest_event_time":"2026-07-01T00:00:00Z","latest_event_type":"complete","role":"output"}
    ],"next_cursor":null});
    let rows = parse_dataset_runs(&body);
    assert_eq!(
        rows,
        vec![
            DatasetRunRow {
                run_id: "r-b".into(),
                time: "2026-07-01T00:00:10Z".into(),
                event_type: "complete".into(),
                role: "input".into(),
            },
            DatasetRunRow {
                run_id: "r-a".into(),
                time: "2026-07-01T00:00:00Z".into(),
                event_type: "complete".into(),
                role: "output".into(),
            },
        ]
    );
}

#[test]
fn missing_fields_default_to_empty_strings() {
    let body = serde_json::json!({"runs":[{"run_id":"r"}]});
    let rows = parse_dataset_runs(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].run_id, "r");
    assert_eq!(rows[0].time, "");
    assert_eq!(rows[0].event_type, "");
    assert_eq!(rows[0].role, "");
}

#[test]
fn malformed_body_is_empty() {
    assert!(parse_dataset_runs(&serde_json::json!({})).is_empty());
    assert!(parse_dataset_runs(&serde_json::json!("not an object")).is_empty());
    assert!(parse_dataset_runs(&serde_json::json!({"runs": "not an array"})).is_empty());
}

//! Pure unit tests for the lineage read DTOs + param/serialization helpers. No
//! router, no Postgres — runs on remote execution.

use control_plane_core::{Cursor, DatasetRef, EventType, LineageEvent, Page, PageReq, RunId};
use query_api::lineage_read::{
    dataset_closure_body, event_type_str, lineage_event_view, parse_lineage_page, run_events_body,
};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

#[test]
fn parse_page_forwards_after_and_limit() {
    let p = parse_lineage_page(Some("cur1".to_string()), Some("3".to_string())).unwrap();
    assert_eq!(
        p,
        PageReq {
            after: Some(Cursor("cur1".to_string())),
            limit: Some(3)
        }
    );
}

#[test]
fn parse_page_absent_is_unbounded_from_start() {
    let p = parse_lineage_page(None, None).unwrap();
    assert_eq!(
        p,
        PageReq {
            after: None,
            limit: None
        }
    );
}

#[test]
fn parse_page_rejects_non_numeric_limit() {
    let err = parse_lineage_page(None, Some("abc".to_string()));
    assert!(err.is_err(), "non-numeric limit is a caller error");
}

#[test]
fn event_type_strings_cover_all_variants() {
    assert_eq!(event_type_str(EventType::Start), "start");
    assert_eq!(event_type_str(EventType::Running), "running");
    assert_eq!(event_type_str(EventType::Complete), "complete");
    assert_eq!(event_type_str(EventType::Abort), "abort");
    assert_eq!(event_type_str(EventType::Fail), "fail");
}

#[test]
fn closure_body_serializes_datasets_and_cursor() {
    let page = Page {
        items: vec![ds("w", "a"), ds("w", "b")],
        next: Some(Cursor("nextcur".to_string())),
    };
    let json = serde_json::to_value(dataset_closure_body(page)).unwrap();
    assert_eq!(json["datasets"][0]["namespace"], "w");
    assert_eq!(json["datasets"][1]["name"], "b");
    assert_eq!(json["next_cursor"], "nextcur");
}

#[test]
fn closure_body_last_page_has_null_cursor() {
    let page = Page {
        items: vec![ds("w", "a")],
        next: None,
    };
    let json = serde_json::to_value(dataset_closure_body(page)).unwrap();
    assert!(json["next_cursor"].is_null());
}

#[test]
fn event_view_shapes_time_type_inputs_outputs_payload() {
    let run = RunId(uuid::Uuid::nil());
    let ev = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ds("w", "in")],
        outputs: vec![ds("w", "out")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let view = lineage_event_view(ev);
    let json = serde_json::to_value(&view).unwrap();
    assert_eq!(json["run_id"], "00000000-0000-0000-0000-000000000000");
    assert_eq!(json["event_type"], "complete");
    assert!(
        json["event_time"].as_str().unwrap().contains('T'),
        "rfc3339 time"
    );
    assert_eq!(json["inputs"][0]["name"], "in");
    assert_eq!(json["outputs"][0]["name"], "out");
    assert_eq!(json["payload"]["k"], 1);
}

#[test]
fn run_events_body_wraps_events_and_cursor() {
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::nil()),
        event_type: EventType::Running,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![],
        outputs: vec![ds("w", "o")],
        payload: serde_json::json!({}),
    };
    let page = Page {
        items: vec![ev],
        next: None,
    };
    let json = serde_json::to_value(run_events_body(page)).unwrap();
    assert_eq!(json["events"][0]["event_type"], "running");
    assert!(json["next_cursor"].is_null());
}

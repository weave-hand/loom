use loom_ui_core::{parse_dataset_detail, parse_datasets, parse_preview};

#[test]
fn parses_dataset_list_rows() {
    let body = serde_json::json!({ "datasets": [
        { "schema": "main", "name": "txns", "project": "main", "updated": "2026-07-01T00:00:00Z" }
    ] });
    let rows = parse_datasets(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "txns");
    assert_eq!(rows[0].project, "main");
    assert_eq!(rows[0].updated, "2026-07-01T00:00:00Z");
}

#[test]
fn parses_dataset_detail_columns() {
    let body = serde_json::json!({
        "snapshot_time": "2026-07-01T00:00:00Z",
        "columns": [ { "name": "id", "ty": "Long", "nullable": false } ]
    });
    let d = parse_dataset_detail(&body);
    assert_eq!(d.snapshot_time, "2026-07-01T00:00:00Z");
    assert_eq!(d.columns.len(), 1);
    assert_eq!(d.columns[0].name, "id");
    assert!(!d.columns[0].nullable);
}

#[test]
fn parses_preview() {
    let body = serde_json::json!({
        "columns": ["id", "note"], "rows": [["1", "a"], ["2", ""]], "sampled": true
    });
    let p = parse_preview(&body);
    assert_eq!(p.columns, vec!["id", "note"]);
    assert_eq!(p.rows, vec![vec!["1", "a"], vec!["2", ""]]);
    assert!(p.sampled);
}

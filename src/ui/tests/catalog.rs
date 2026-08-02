use loom_ui_core::{
    DatasetRow, dataset_index, dataset_route_id, parse_dataset_detail, parse_datasets,
    parse_preview, split_dataset_id,
};

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
fn parses_dataset_row_count() {
    let body = serde_json::json!({ "datasets": [
        { "schema": "main", "name": "txns", "project": "main",
          "updated": "2026-07-01T00:00:00Z", "rows": 1234 },
        { "schema": "main", "name": "empty", "project": "main",
          "updated": "", "rows": serde_json::Value::Null },
    ] });
    let rows = parse_datasets(&body);
    assert_eq!(rows[0].rows, Some(1234));
    assert_eq!(rows[1].rows, None, "null rows parse to None");
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

/// Build `DatasetRow`s from `(schema, name)` pairs via the real parser, so this
/// test never has to restate `DatasetRow`'s field list.
fn rows(pairs: &[(&str, &str)]) -> Vec<DatasetRow> {
    let datasets: Vec<serde_json::Value> = pairs
        .iter()
        .map(|(schema, name)| serde_json::json!({ "schema": schema, "name": name }))
        .collect();
    parse_datasets(&serde_json::json!({ "datasets": datasets }))
}

#[test]
fn route_id_is_schema_dot_name() {
    let rows = rows(&[("main", "txns")]);
    assert_eq!(dataset_route_id(&rows[0]), "main.txns");
}

#[test]
fn route_id_splits_back_at_the_first_dot() {
    assert_eq!(split_dataset_id("main.txns"), Some(("main", "txns")));
    assert_eq!(
        split_dataset_id("main.a.b"),
        Some(("main", "a.b")),
        "the split is at the FIRST dot, matching how the id is composed"
    );
    assert_eq!(split_dataset_id("nodot"), None);
}

#[test]
fn dataset_index_finds_the_row_by_id() {
    let rows = rows(&[("main", "txns"), ("analytics", "daily")]);
    assert_eq!(dataset_index(&rows, "analytics.daily"), Some(1));
    assert_eq!(dataset_index(&rows, "main.txns"), Some(0));
}

#[test]
fn dataset_index_is_none_when_the_list_does_not_hold_it() {
    let rows = rows(&[("main", "txns")]);
    assert_eq!(
        dataset_index(&rows, "other.thing"),
        None,
        "a deep link into a filtered list highlights nothing rather than the wrong row"
    );
    assert_eq!(dataset_index(&[], "main.txns"), None);
}

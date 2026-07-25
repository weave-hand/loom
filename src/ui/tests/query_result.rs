use loom_ui_core::{QueryResult, parse_query_result};

#[test]
fn parses_columns_rows_truncated() {
    let v = serde_json::json!({
        "columns": ["id", "name"],
        "rows": [["1", "a"], ["2", "b"]],
        "truncated": true
    });
    let r: QueryResult = parse_query_result(&v);
    assert_eq!(r.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1], vec!["2".to_string(), "b".to_string()]);
    assert!(r.truncated);
}

#[test]
fn missing_fields_default_empty_not_truncated() {
    let r = parse_query_result(&serde_json::json!({}));
    assert!(r.columns.is_empty());
    assert!(r.rows.is_empty());
    assert!(!r.truncated);
}

#[test]
fn non_string_cells_stringify() {
    let v = serde_json::json!({ "columns": ["n"], "rows": [[5]], "truncated": false });
    let r = parse_query_result(&v);
    assert_eq!(r.rows[0], vec!["5".to_string()]);
}

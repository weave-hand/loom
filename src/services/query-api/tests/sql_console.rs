//! Unit tests for the pure SQL-console shaping helpers: row-cap truncation and the
//! JSON result body. Governance/execution is exercised end-to-end in
//! `sql_console_e2e.rs` against a real engine.

use query_api::serving::{GovernedRows, Rows, SqlValue};
use query_api::sql_console::{result_body, truncate_rows};

fn rows(n: usize) -> Rows {
    Rows {
        columns: vec!["id".into(), "name".into()],
        rows: (0..n)
            .map(|i| {
                vec![
                    SqlValue::Int(i64::try_from(i).unwrap()),
                    SqlValue::Text(format!("n{i}")),
                ]
            })
            .collect(),
    }
}

#[test]
fn truncate_under_cap_keeps_all_not_truncated() {
    let gr = truncate_rows(rows(3), 10);
    assert_eq!(gr.rows.rows.len(), 3);
    assert!(!gr.truncated);
}

#[test]
fn truncate_over_cap_truncates_and_flags() {
    let gr = truncate_rows(rows(15), 10);
    assert_eq!(gr.rows.rows.len(), 10);
    assert!(gr.truncated);
    assert_eq!(gr.rows.columns, vec!["id".to_string(), "name".to_string()]);
}

#[test]
fn truncate_at_exact_cap_not_truncated() {
    let gr = truncate_rows(rows(10), 10);
    assert_eq!(gr.rows.rows.len(), 10);
    assert!(!gr.truncated);
}

#[test]
fn result_body_shapes_columns_rows_truncated_with_null_blank() {
    let gr = GovernedRows {
        rows: Rows {
            columns: vec!["a".into(), "b".into()],
            rows: vec![vec![SqlValue::Int(1), SqlValue::Null]],
        },
        truncated: true,
    };
    let v = result_body(&gr);
    assert_eq!(v["columns"], serde_json::json!(["a", "b"]));
    assert_eq!(v["rows"], serde_json::json!([["1", ""]]));
    assert_eq!(v["truncated"], serde_json::json!(true));
}

#[test]
fn result_body_empty_result_is_empty_arrays_not_truncated() {
    let gr = GovernedRows::default();
    let v = result_body(&gr);
    assert_eq!(v["columns"], serde_json::json!([]));
    assert_eq!(v["rows"], serde_json::json!([]));
    assert_eq!(v["truncated"], serde_json::json!(false));
}

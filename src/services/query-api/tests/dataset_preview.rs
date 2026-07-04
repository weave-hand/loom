//! Pure shaping of a served `Rows` into the preview wire body.
use query_api::dataset_preview::preview_body;
use query_api::serving::{Rows, SqlValue};

#[test]
fn preview_body_stringifies_cells_and_marks_sampled() {
    let rows = Rows {
        columns: vec!["id".into(), "amount".into(), "ok".into()],
        rows: vec![
            vec![
                SqlValue::Int(1),
                SqlValue::Double(12.5),
                SqlValue::Bool(true),
            ],
            vec![SqlValue::Int(2), SqlValue::Null, SqlValue::Bool(false)],
        ],
    };
    let body = preview_body(&rows);
    assert_eq!(body["columns"], serde_json::json!(["id", "amount", "ok"]));
    assert_eq!(body["sampled"], serde_json::json!(true));
    assert_eq!(
        body["rows"],
        serde_json::json!([["1", "12.5", "true"], ["2", "", "false"]])
    );
}

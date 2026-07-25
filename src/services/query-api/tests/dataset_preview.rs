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

use control_plane_core::{CompareOp, ObjectType, RowFilter, ScalarValue};
use query_api::dataset_preview::governed_preview_sql;
use query_api::governed::GovernedType;
use query_api::handler::QueryError;
use query_api::sql::DataFusionDialect;
use std::collections::HashSet;

fn event_type() -> ObjectType {
    ObjectType::build("Event", ("main", "events"))
        .prop_req("id", "Long")
        .prop("note", "String")
        .done()
}

fn governed(row_filters: Vec<RowFilter>, denied: &[&str], masked: &[&str]) -> GovernedType {
    GovernedType {
        otype: event_type(),
        row_filters,
        denied: denied
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<_>>(),
        masked: masked
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<_>>(),
    }
}

#[test]
fn governed_preview_projects_masks_and_filters() {
    let g = governed(
        vec![RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Lt,
            value: ScalarValue::Int(3),
        }],
        &[],
        &["note"],
    );
    let (sql, params) = governed_preview_sql(&DataFusionDialect, &g, 20).unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", '***' AS \"note\" FROM \"main\".\"events\" WHERE (\"id\" < ?) LIMIT 20"
    );
    assert_eq!(params, vec![SqlValue::Int(3)]);
}

#[test]
fn governed_preview_drops_denied_columns() {
    let g = governed(vec![], &["note"], &[]);
    let (sql, params) = governed_preview_sql(&DataFusionDialect, &g, 5).unwrap();
    assert_eq!(sql, "SELECT \"id\" FROM \"main\".\"events\" LIMIT 5");
    assert!(params.is_empty());
}

#[test]
fn governed_preview_all_columns_denied_is_forbidden() {
    let g = governed(vec![], &["id", "note"], &[]);
    let err = governed_preview_sql(&DataFusionDialect, &g, 5).unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

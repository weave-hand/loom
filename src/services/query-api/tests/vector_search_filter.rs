//! Pure-logic tests for the `/search` endpoint's request validation and the row-filter
//! post-filter SQL compile. No fixtures, no engine — just the deserialize/validate
//! contract and the `compile_select_with` shape the governed flow relies on.

use query_api::http::{K_MAX, VectorSearchRequest, validate_search_request};

#[test]
fn request_rejects_unknown_field() {
    let v = serde_json::json!({ "query": [0.1, 0.2], "k": 3, "bogus": 1 });
    assert!(serde_json::from_value::<VectorSearchRequest>(v).is_err());
}

#[test]
fn request_rejects_missing_field() {
    // `k` omitted -> deserialize fails (k is not `#[serde(default)]`).
    let v = serde_json::json!({ "query": [0.1, 0.2] });
    assert!(serde_json::from_value::<VectorSearchRequest>(v).is_err());
}

#[test]
fn request_rejects_k_out_of_range() {
    let too_big = VectorSearchRequest {
        query: vec![0.1],
        k: K_MAX + 1,
        nprobe: None,
        ef_search: None,
    };
    assert!(validate_search_request(&too_big).is_err());
    let zero = VectorSearchRequest {
        query: vec![0.1],
        k: 0,
        nprobe: None,
        ef_search: None,
    };
    assert!(validate_search_request(&zero).is_err());
}

#[test]
fn request_rejects_empty_query() {
    let empty = VectorSearchRequest {
        query: vec![],
        k: 3,
        nprobe: None,
        ef_search: None,
    };
    assert!(validate_search_request(&empty).is_err());
}

#[test]
fn request_accepts_valid() {
    let ok = VectorSearchRequest {
        query: vec![0.1, 0.2],
        k: 5,
        nprobe: Some(8),
        ef_search: None,
    };
    assert!(validate_search_request(&ok).is_ok());
}

// The post-filter id-set SELECT: the candidate ids (an `In` predicate on the identity)
// ANDed to the subject's row-filters, projecting only the identity column.
mod post_filter {
    use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};
    use query_api::filter::CallerPredicate;
    use query_api::serving::SqlValue;
    use query_api::sql::compile_select;

    #[test]
    fn ands_candidate_ids_to_row_filter_on_identity_projection() {
        let table = TableRef {
            schema: "main".into(),
            name: "docs".into(),
        };
        // The subject's row-filter (tenant scoping).
        let row_filter = RowFilter::Compare {
            property: "tenant".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("acme".into()),
        };
        // The engine's candidate hit ids, lowered to an `In` predicate on the identity.
        let pred = CallerPredicate {
            column: "id".into(),
            op: CompareOp::In,
            values: vec![SqlValue::Int(7), SqlValue::Int(11)],
        };
        let (sql, params) = compile_select(
            &table,
            &["id".into()],
            &[],
            std::slice::from_ref(&row_filter),
            std::slice::from_ref(&pred),
            &[],
            &[],
            2,
        )
        .unwrap();
        // Projects the identity column, scopes by the candidate id set, AND keeps the
        // row-filter column — both conjuncts present.
        assert!(
            sql.contains(r#""id""#),
            "projects the identity column: {sql}"
        );
        assert!(
            sql.contains("IN ("),
            "scopes by the candidate id set: {sql}"
        );
        assert!(sql.contains(r#""tenant""#), "keeps the row-filter: {sql}");
        // Params: the row-filter value precedes the candidate ids (WHERE order).
        assert_eq!(
            params,
            vec![
                SqlValue::Text("acme".into()),
                SqlValue::Int(7),
                SqlValue::Int(11),
            ]
        );
    }
}

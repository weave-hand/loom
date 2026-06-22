//! compile_chain_pairs projects the two end-position identity columns through the same
//! governed joins as compile_chain_with.

use control_plane_core::LinkBacking;
use query_api::sql::{ChainType, DuckDbDialect, compile_chain_pairs};

#[test]
fn pairs_project_source_and_target_identity() {
    let types = vec![
        ChainType {
            table: control_plane_core::TableRef {
                schema: "main".into(),
                name: "customer".into(),
            },
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: control_plane_core::TableRef {
                schema: "main".into(),
                name: "order".into(),
            },
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) =
        compile_chain_pairs(&DuckDbDialect, &types, &hops, "id", "order_id", 1000).unwrap();
    assert!(params.is_empty());
    // DISTINCT pair of source (t_0) and final-target (t_1) identity columns.
    assert!(sql.contains("SELECT DISTINCT"), "got: {sql}");
    assert!(
        sql.contains(r#"t_0."id""#),
        "source identity projected: {sql}"
    );
    assert!(
        sql.contains(r#"t_1."order_id""#),
        "target identity projected: {sql}"
    );
    assert!(sql.contains("JOIN"), "joins present: {sql}");
}

#[test]
fn chain_pairs_orders_by_both_identity_columns_before_limit() {
    // 1-hop FK chain: customer -> order (mirrors pairs_project_source_and_target_identity)
    let types = vec![
        ChainType {
            table: control_plane_core::TableRef {
                schema: "main".into(),
                name: "customer".into(),
            },
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: control_plane_core::TableRef {
                schema: "main".into(),
                name: "order".into(),
            },
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, _params) =
        compile_chain_pairs(&DuckDbDialect, &types, &hops, "cust_id", "ord_id", 1000).unwrap();
    assert!(
        sql.contains(r#"ORDER BY t_0."cust_id", t_1."ord_id" LIMIT 1000"#),
        "got: {sql}"
    );
}

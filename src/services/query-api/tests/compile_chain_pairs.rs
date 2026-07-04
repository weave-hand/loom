//! compile_chain_pairs projects the two end-position identity columns through the same
//! governed joins as compile_chain_with.

use control_plane_core::LinkBacking;
use query_api::sql::{ChainType, DataFusionDialect, compile_chain_pairs};

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
        compile_chain_pairs(&DataFusionDialect, &types, &hops, "id", "order_id", 1000).unwrap();
    assert!(params.is_empty());
    // DISTINCT pair of source (t_0) and final-target (t_1) identity columns.
    assert!(sql.contains("SELECT DISTINCT"), "got: {sql}");
    // DataFusion rejects DISTINCT + LIMIT without an ORDER BY, so the pair query must
    // carry an explicit ordering on both id columns (regression guard for the 500 fix).
    assert!(
        sql.contains(r#"ORDER BY t_0."id" ASC, t_1."order_id" ASC"#),
        "DISTINCT+LIMIT needs ORDER BY: {sql}"
    );
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

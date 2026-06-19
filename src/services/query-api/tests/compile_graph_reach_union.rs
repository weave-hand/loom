//! compile_graph_reach_union emits a depth-bounded WITH RECURSIVE reachability query whose
//! recursive term is a UNION of one self-hop arm per self-link, governed by the queried
//! type's row-filters at the seed `s`, each arm's landing node `nxt`, and the projection `p`.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, compile_graph_reach_union};

fn person() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "person".into(),
    }
}

#[test]
fn two_self_links_union_with_row_filter() {
    // Person knows Person (FK knows_id -> id) UNION Person colleagues Person (join table).
    let fk = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "colleagues".into(),
        },
        from_key: "id".into(),
        from_column: "a".into(),
        to_column: "b".into(),
        to_key: "id".into(),
    };
    let row_filters = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &[fk, jt],
        &[], // no seed predicates
        &row_filters,
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "got: {sql}"
    );
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    // seed UNION arm0 UNION arm1 => exactly two " UNION " tokens.
    assert_eq!(
        sql.matches(" UNION ").count(),
        2,
        "two recursive arms unioned: {sql}"
    );
    // FK arm joins cur.knows_id = nxt.id.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Join-table arm uses the per-arm alias j1 (arm index 1).
    assert!(
        sql.contains(r#"cur."id" = j1."a""#) && sql.contains(r#"j1."b" = nxt."id""#),
        "join-table arm with j1 alias: {sql}"
    );
    // Row-filter rendered at seed s, each arm's nxt (x2), and projection p.
    assert!(
        sql.contains(r#"s."active""#)
            && sql.contains(r#"nxt."active""#)
            && sql.contains(r#"p."active""#),
        "row-filter at s/nxt/p: {sql}"
    );
    // Param count: seed s (1) + arm0 nxt (1) + arm1 nxt (1) + projection p (1) = 4.
    assert_eq!(
        params.len(),
        4,
        "1 seed + 2 arm-nxt + 1 projection; got {params:?}"
    );
    assert_eq!(params, vec![SqlValue::Bool(true); 4]);
    // Reachable in >= 1 hop, projected distinct.
    assert!(sql.contains("depth >= 1"), "reachability bound: {sql}");
    assert!(
        sql.contains("SELECT DISTINCT") && sql.contains(r#"p."name""#),
        "projection: {sql}"
    );
}

#[test]
fn single_self_link_with_seed_predicate() {
    // One FK self-link (parent_id -> id) with an object-set seed In-predicate on the identity.
    let fk = LinkBacking::ForeignKey {
        from_column: "parent_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(7)],
    }];
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &[fk],
        &seed,
        &[], // no row-filters
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // seed UNION arm0 => exactly one " UNION " token.
    assert_eq!(sql.matches(" UNION ").count(), 1, "single arm: {sql}");
    assert!(
        sql.contains(r#"cur."parent_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Only the seed In value is bound (FK arm needs no join-table alias).
    assert_eq!(params.len(), 1, "seed id only; got {params:?}");
    assert_eq!(params[0], SqlValue::Int(7));
}

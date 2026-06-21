//! compile_graph_reach_union emits a depth-bounded WITH RECURSIVE reachability query. The
//! recursive term has a single CTE self-reference: all edge arms are collapsed into a non-recursive
//! (from_id, to_id) subquery joined with UNION ALL, avoiding DuckDB's "Circular reference to CTE"
//! error that arises from multiple arms each referencing the CTE name directly. Row-filters are
//! applied at the seed `s`, the single shared landing node `nxt`, and the projection `p`.

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
    assert!(
        sql.contains("0 AS depth"),
        "anchor aliases depth explicitly (portable to DataFusion): {sql}"
    );
    // One CTE-level UNION (anchor vs recursive step); one UNION ALL inside the edge subquery.
    // Note: " UNION " is a substring of " UNION ALL ", so count bare UNION as (UNION - UNION ALL).
    let union_all = sql.matches(" UNION ALL ").count();
    let union_any = sql.matches(" UNION ").count();
    assert_eq!(union_all, 1, "one UNION ALL inside edge subquery: {sql}");
    assert_eq!(
        union_any - union_all,
        1,
        "one bare CTE-level UNION (not UNION ALL): {sql}"
    );
    // FK arm (arm index 0) joins cur.knows_id = nxt.id inside the edge subquery.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Join-table arm (arm index 1) uses the per-arm alias j1 inside the edge subquery.
    assert!(
        sql.contains(r#"cur."id" = j1."a""#) && sql.contains(r#"j1."b" = nxt."id""#),
        "join-table arm with j1 alias: {sql}"
    );
    // Row-filter rendered at seed s, the single shared nxt, and projection p.
    assert!(
        sql.contains(r#"s."active""#)
            && sql.contains(r#"nxt."active""#)
            && sql.contains(r#"p."active""#),
        "row-filter at s/nxt/p: {sql}"
    );
    // Param count: seed s (1) + one shared nxt (1) + projection p (1) = 3.
    // (The old per-arm-nxt approach emitted N sets of row-filter params; the new single
    // shared-nxt approach emits just one set regardless of how many arms there are.)
    assert_eq!(
        params.len(),
        3,
        "1 seed + 1 shared-nxt + 1 projection; got {params:?}"
    );
    assert_eq!(params, vec![SqlValue::Bool(true); 3]);
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
    // seed UNION recursive-step => exactly one " UNION " token; single arm has no UNION ALL.
    assert_eq!(sql.matches(" UNION ").count(), 1, "single arm: {sql}");
    assert_eq!(
        sql.matches(" UNION ALL ").count(),
        0,
        "no UNION ALL for single arm: {sql}"
    );
    assert!(
        sql.contains(r#"cur."parent_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Only the seed In value is bound (no row-filters, no projection filters).
    assert_eq!(params.len(), 1, "seed id only; got {params:?}");
    assert_eq!(params[0], SqlValue::Int(7));
}

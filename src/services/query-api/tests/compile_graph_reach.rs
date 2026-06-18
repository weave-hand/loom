//! compile_graph_reach emits a depth-bounded WITH RECURSIVE reachability query over a
//! self-link, governed by the type's row-filters at seed/expansion/projection.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, compile_graph_reach};

fn person() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "person".into(),
    }
}

#[test]
fn fk_self_link_recursive_reach() {
    // Person.knows_id -> Person.id (FK self-link).
    let backing = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &backing,
        &[], // no seed predicates
        &[], // no row-filters
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    assert!(params.is_empty());
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "got: {sql}"
    );
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    assert!(sql.contains("UNION"), "recursive union: {sql}");
    // FK hop cur -> nxt.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "fk join: {sql}"
    );
    // reachable in >= 1 hop, projected from p.
    assert!(sql.contains("depth >= 1"), "reachability bound: {sql}");
    assert!(
        sql.contains("SELECT DISTINCT") && sql.contains(r#"p."name""#),
        "projection: {sql}"
    );
}

#[test]
fn join_table_self_link_and_row_filter_and_seed() {
    // Person knows Person via a knows(a, b) join table; an ACL row-filter on `active`;
    // a seed In-predicate on the identity (object-set seeds).
    let backing = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "knows".into(),
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
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(5)],
    }];
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &backing,
        &seed,
        &row_filters,
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // join-table hop: cur.id = j.a  AND  j.b = nxt.id
    assert!(
        sql.contains(r#"cur."id" = j."a""#) && sql.contains(r#"j."b" = nxt."id""#),
        "jt join: {sql}"
    );
    // the row-filter is rendered at all three positions (seed s, expansion nxt, projection p).
    assert!(
        sql.contains(r#"s."active""#)
            && sql.contains(r#"nxt."active""#)
            && sql.contains(r#"p."active""#),
        "row-filter at 3 positions: {sql}"
    );
    // seed In-predicate bound; params: seed In (1) + active at s, nxt, p (3) = 4 bound values.
    assert_eq!(
        params.len(),
        4,
        "1 seed id + 3 row-filter renderings; got {params:?}"
    );
    assert_eq!(params[0], SqlValue::Int(5));
}

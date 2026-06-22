//! compile_graph_reach emits a depth-bounded WITH RECURSIVE reachability query over a
//! path-cycle, governed by the type's row-filters at seed/expansion/projection.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, GraphStep, compile_graph_reach};

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
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
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
    assert!(
        sql.contains("0 AS depth"),
        "anchor aliases depth explicitly (portable to DataFusion, which ignores the CTE column list): {sql}"
    );
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
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
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

#[test]
fn two_step_path_cycle_with_intermediate_filter() {
    // Person --memberOf(FK Person.team_id -> Team.id)--> Team
    //        --hasMember(FK Team.id -> Person.team_id)--> Person   (a Person->Team->Person cycle)
    // Team has an ACL row-filter `active = true` (intermediate governance); Person (start) has
    // `region = 'US'` (seed + nxt + projection).
    let team = TableRef {
        schema: "main".into(),
        name: "team".into(),
    };
    let path = vec![
        GraphStep {
            backing: LinkBacking::ForeignKey {
                from_column: "team_id".into(),
                to_column: "id".into(),
            },
            next_table: team.clone(),
            next_filters: vec![RowFilter::Compare {
                property: "active".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
        },
        GraphStep {
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "team_id".into(),
            },
            next_table: person(),
            next_filters: vec![], // final step: nxt is the start type, governed by row_filters
        },
    ];
    let start_filters = vec![RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("US".into()),
    }];
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &path,
        &[],
        &start_filters,
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // Chain join cur -> g1(Team) -> nxt(Person).
    assert!(
        sql.contains(r#"cur."team_id" = g1."id""#),
        "step1 join: {sql}"
    );
    assert!(
        sql.contains(r#"g1."id" = nxt."team_id""#),
        "step2 join: {sql}"
    );
    // Intermediate Team filter at g1; start filter at s/nxt/p.
    assert!(
        sql.contains(r#"g1."active""#),
        "intermediate filter at g1: {sql}"
    );
    assert!(
        sql.contains(r#"s."region""#)
            && sql.contains(r#"nxt."region""#)
            && sql.contains(r#"p."region""#),
        "start filter at s/nxt/p: {sql}"
    );
    // Param order: seed start-filter (s) , g1 active, nxt region, p region = 4.
    assert_eq!(params.len(), 4, "got {params:?}");
    assert_eq!(params[0], SqlValue::Text("US".into())); // s.region
    assert_eq!(params[1], SqlValue::Bool(true)); // g1.active
    assert_eq!(params[2], SqlValue::Text("US".into())); // nxt.region (final node, start filter)
    assert_eq!(params[3], SqlValue::Text("US".into())); // p.region (projection)
}

/// A single Person -> Person FK self-link (knows_id -> id), reused by the ORDER BY barrier tests.
fn sample_self_link() -> (TableRef, Vec<GraphStep>) {
    let table = person();
    let path = vec![GraphStep {
        backing: LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
        next_table: person(),
        next_filters: vec![],
    }];
    (table, path)
}

#[test]
fn graph_reach_orders_by_identity_when_visible() {
    // Reuse the existing single-self-link fixture builder in this file for `table`,
    // `path`, etc. Project ["id","label"]; identity = "id" (visible).
    let (table, path) = sample_self_link(); // local construction as in existing tests
    let (sql, _params) = compile_graph_reach(
        &DuckDbDialect,
        &table,
        "id",
        &path,
        &[],
        &[],
        &["id".to_string(), "label".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    // Identity is visible -> order key is identity alone, qualified at the projection alias `p`.
    assert!(sql.contains(r#"ORDER BY p."id" LIMIT 1000"#), "got: {sql}");
}

#[test]
fn graph_reach_orders_by_projected_cols_when_identity_masked() {
    let (table, path) = sample_self_link();
    let (sql, _params) = compile_graph_reach(
        &DuckDbDialect,
        &table,
        "id",
        &path,
        &[],
        &[],
        &["id".to_string(), "label".to_string()],
        &["id".to_string()], // identity masked -> falls back to visible projected cols
        3,
        1000,
    )
    .unwrap();
    assert!(
        sql.contains(r#"ORDER BY p."label" LIMIT 1000"#),
        "got: {sql}"
    );
}

//! compile_graph_tree emits a depth-bounded WITH RECURSIVE reach(id, depth, pred) query,
//! a `settled` window that keeps one shortest-path parent per node (ROW_NUMBER … WHERE
//! rn = 1), includes depth = 0 roots, projects __depth/__parent/__id, drops the LIMIT,
//! and binds seed/filter params in the same order as compile_graph_reach.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DataFusionDialect, GraphStep, ReachSpec, compile_graph_tree};

fn person() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "person".into(),
    }
}

#[test]
fn fk_self_link_tree_shape() {
    // Person.knows_id -> Person.id (FK self-link).
    let backing = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let (sql, params) = compile_graph_tree(
        &DataFusionDialect,
        &ReachSpec {
            table: &person(),
            identity: "id",
            seed_predicates: &[], // no seed predicates
            row_filters: &[],     // no row-filters
            allowed_cols: &["id".to_string(), "name".to_string()],
            mask_cols: &[],
            depth: 3,
        },
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
    )
    .unwrap();
    assert!(params.is_empty());
    // predecessor carried through the CTE
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth, pred) AS"),
        "pred column in CTE: {sql}"
    );
    // anchor pred is a typed NULL, roots are parentless
    assert!(
        sql.contains("NULLIF(s.\"id\", s.\"id\") AS pred"),
        "typed-null anchor pred: {sql}"
    );
    // recursive term projects the expanded-from node as pred
    assert!(sql.contains("r.id AS pred"), "recursive pred = r.id: {sql}");
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    // settle: one parent per node, min depth then min pred
    assert!(
        sql.contains(
            "ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC NULLS FIRST)"
        ),
        "settle window: {sql}"
    );
    assert!(
        sql.contains("WHERE t.rn = 1"),
        "keep the settled parent: {sql}"
    );
    // roots (depth 0) are INCLUDED — no `depth >= 1` filter as in reachability
    assert!(
        !sql.contains("depth >= 1"),
        "tree includes depth-0 roots, unlike reachability: {sql}"
    );
    // output columns
    assert!(
        sql.contains("t.depth AS __depth")
            && sql.contains("t.pred AS __parent")
            && sql.contains("p.\"id\" AS __id"),
        "depth/parent/id output columns: {sql}"
    );
    // no LIMIT on the tree (depth cap bounds it; a LIMIT could orphan a child)
    assert!(
        !sql.to_uppercase().contains("LIMIT"),
        "no LIMIT on the tree: {sql}"
    );
    // stable node order
    assert!(
        sql.contains("ORDER BY t.depth ASC, p.\"id\" ASC"),
        "node ordering: {sql}"
    );
    // FK hop cur -> nxt (reused reach_joins)
    assert!(
        sql.contains("cur.\"knows_id\" = nxt.\"id\""),
        "fk join: {sql}"
    );
    assert!(sql.contains("p.\"name\""), "object projection: {sql}");
}

#[test]
fn join_table_tree_with_row_filter_and_seed_param_order() {
    // Person knows Person via knows(a, b); an ACL row-filter active=true; a seed In-predicate.
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
    let (sql, params) = compile_graph_tree(
        &DataFusionDialect,
        &ReachSpec {
            table: &person(),
            identity: "id",
            seed_predicates: &seed,
            row_filters: &row_filters,
            allowed_cols: &["id".to_string()],
            mask_cols: &[],
            depth: 2,
        },
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
    )
    .unwrap();
    // join-table hop
    assert!(
        sql.contains("cur.\"id\" = j.\"a\"") && sql.contains("j.\"b\" = nxt.\"id\""),
        "jt join: {sql}"
    );
    // row-filter rendered at seed(s), expansion(nxt), projection(p)
    assert!(
        sql.contains("s.\"active\"")
            && sql.contains("nxt.\"active\"")
            && sql.contains("p.\"active\""),
        "row-filter at 3 positions: {sql}"
    );
    // params: seed In (1) + active at s, nxt, p (3) = 4, SAME ORDER as compile_graph_reach
    assert_eq!(
        params.len(),
        4,
        "1 seed id + 3 row-filter renderings; got {params:?}"
    );
    assert_eq!(params[0], SqlValue::Int(5));
    assert_eq!(params[1], SqlValue::Bool(true)); // s.active
    assert_eq!(params[2], SqlValue::Bool(true)); // nxt.active
    assert_eq!(params[3], SqlValue::Bool(true)); // p.active
}

#[test]
fn two_step_path_cycle_tree() {
    // Person --memberOf(FK team_id->id)--> Team --hasMember(FK id->team_id)--> Person cycle.
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
            next_filters: vec![],
        },
    ];
    let (sql, _params) = compile_graph_tree(
        &DataFusionDialect,
        &ReachSpec {
            table: &person(),
            identity: "id",
            seed_predicates: &[],
            row_filters: &[],
            allowed_cols: &["id".to_string()],
            mask_cols: &[],
            depth: 2,
        },
        &path,
    )
    .unwrap();
    assert!(
        sql.contains("cur.\"team_id\" = g1.\"id\""),
        "step1 join: {sql}"
    );
    assert!(
        sql.contains("g1.\"id\" = nxt.\"team_id\""),
        "step2 join: {sql}"
    );
    assert!(
        sql.contains("g1.\"active\""),
        "intermediate filter at g1: {sql}"
    );
}

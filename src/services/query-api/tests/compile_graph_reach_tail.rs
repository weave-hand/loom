//! compile_graph_reach_tail emits a recursive-core + relational-tail reachability query: a
//! single-self-link `WITH RECURSIVE reach(id, depth)` CTE, then a forward INNER-JOIN tail off
//! the depth>=1 reachable set, deduping the final tail type per object (windowed `ROW_NUMBER`
//! on the raw identity when declared, else `SELECT DISTINCT`). The tail's source
//! position t_0 is the queried type, constrained to the reach set; the queried type's
//! row-filters live in the CTE (seed `s` + recursive `nxt`), not at t_0. Param order: seed
//! predicates, seed row-filters (s), recursive row-filters (nxt), then the tail's per-position
//! params.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{ChainType, DataFusionDialect, compile_graph_reach_tail};

fn tref(n: &str) -> TableRef {
    TableRef {
        schema: "main".into(),
        name: n.into(),
    }
}

#[test]
fn fk_core_single_tail_shape() {
    // Person --knows(FK knows_id->id)*--> Person, then --worksAt(FK worksat_id->id)--> Company.
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DataFusionDialect,
        &tref("person"),
        "id",
        &core,
        &[],
        &[],
        &tail_types,
        &tail_hops,
        &["id".to_string()],
        &[],
        None,
        3,
        1000,
    )
    .unwrap();
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "recursive core CTE: {sql}"
    );
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    assert!(
        sql.contains("0 AS depth"),
        "anchor aliases depth explicitly (portable to DataFusion): {sql}"
    );
    // Core self-link join inside the CTE.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "core fk join: {sql}"
    );
    // Tail FK join (chain_from_where joins the final target back down to t_0).
    assert!(
        sql.contains(r#"t_0."worksat_id" = t_1."id""#),
        "tail fk join: {sql}"
    );
    // Glue: t_0 (queried type) constrained to the depth>=1 reach set.
    assert!(
        sql.contains(r#"t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)"#),
        "reach-membership glue: {sql}"
    );
    // final_identity=None => back-compatible DISTINCT projection of final type at t_1.
    assert!(
        sql.contains("SELECT DISTINCT") && sql.contains(r#"t_1."id""#),
        "distinct projection of final type: {sql}"
    );
    assert!(sql.contains("LIMIT 1000"), "limit: {sql}");
    // No filters, no seed => no bound params.
    assert!(params.is_empty(), "no params expected; got {params:?}");
}

#[test]
fn param_order_seed_core_tail() {
    // Seed In-predicate + a core row-filter (Person.active=true) + a tail row-filter
    // (Company.verified=true). Param order must be: seed pred, seed rf@s, recursive rf@nxt,
    // then tail rf@t_1.
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let core_rf = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![RowFilter::Compare {
                property: "verified".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DataFusionDialect,
        &tref("person"),
        "id",
        &core,
        &seed,
        &core_rf,
        &tail_types,
        &tail_hops,
        &["id".to_string()],
        &[],
        None,
        2,
        1000,
    )
    .unwrap();
    // Core filter rendered at the seed `s` and the recursive landing `nxt`; tail filter at t_1.
    assert!(
        sql.contains(r#"s."active""#) && sql.contains(r#"nxt."active""#),
        "core filter at s and nxt: {sql}"
    );
    assert!(
        sql.contains(r#"t_1."verified""#),
        "tail filter at t_1: {sql}"
    );
    // Param order: seed In(1), seed active@s, recursive active@nxt, tail verified@t_1.
    assert_eq!(
        params,
        vec![
            SqlValue::Int(1),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ],
        "param order seed/core-s/core-nxt/tail: {params:?}"
    );
}

#[test]
fn join_table_core_and_multi_hop_tail() {
    // Join-table self-link core (colleagues), 2-hop tail worksAt(FK) then locatedIn(FK).
    let core = LinkBacking::JoinTable {
        table: tref("colleagues"),
        from_key: "id".into(),
        from_column: "a".into(),
        to_column: "b".into(),
        to_key: "id".into(),
    };
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("city"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![
        LinkBacking::ForeignKey {
            from_column: "worksat_id".into(),
            to_column: "id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "city_id".into(),
            to_column: "id".into(),
        },
    ];
    let (sql, _params) = compile_graph_reach_tail(
        &DataFusionDialect,
        &tref("person"),
        "id",
        &core,
        &[],
        &[],
        &tail_types,
        &tail_hops,
        &["id".to_string(), "cname".to_string()],
        &[],
        None,
        4,
        1000,
    )
    .unwrap();
    // Join-table core uses alias `j` inside the CTE.
    assert!(
        sql.contains(r#"cur."id" = j."a""#) && sql.contains(r#"j."b" = nxt."id""#),
        "join-table core arm: {sql}"
    );
    // Two tail hops: worksAt (t_0->t_1) and locatedIn (t_1->t_2). Final projection at t_2.
    assert!(
        sql.contains(r#"t_0."worksat_id" = t_1."id""#),
        "tail hop 1: {sql}"
    );
    assert!(
        sql.contains(r#"t_1."city_id" = t_2."id""#),
        "tail hop 2: {sql}"
    );
    assert!(
        sql.contains(r#"t_2."id""#) && sql.contains(r#"t_2."cname""#),
        "final type projected at t_2: {sql}"
    );
    // Glue still references t_0.
    assert!(
        sql.contains(r#"t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)"#),
        "membership glue on t_0: {sql}"
    );
}

#[test]
fn masked_final_identity_dedups_via_window_below_the_mask() {
    // Person --knows(FK)*--> Person, then --worksAt(FK)--> Company. Company has a declared
    // identity "cid" that the subject may NOT see (masked). The tail dedup must partition on the
    // RAW cid (below the mask) so two distinct companies sharing "cname" are not collapsed.
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DataFusionDialect,
        &tref("person"),
        "id",
        &core,
        &[],
        &[],
        &tail_types,
        &tail_hops,
        &["cname".to_string(), "cid".to_string()],
        &["cid".to_string()],
        Some("cid"),
        3,
        1000,
    )
    .unwrap();
    // No projection DISTINCT — the windowed row-number replaces it.
    assert!(!sql.contains("SELECT DISTINCT"), "no DISTINCT: {sql}");
    // Partition on the RAW final-target identity at the final tail alias t_1.
    assert!(
        sql.contains(r#"ROW_NUMBER() OVER (PARTITION BY t_1."cid") AS _loom_rn"#),
        "partitions on raw identity t_1.cid: {sql}"
    );
    // Inner projection: visible column verbatim, masked identity still rendered '***'.
    assert!(
        sql.contains(r#"t_1."cname""#) && sql.contains(r#"'***' AS "cid""#),
        "inner projection keeps mask: {sql}"
    );
    // Outer select references bare quoted output names only — the raw cid never leaves the
    // subquery (dedup key is not caller-visible).
    assert!(
        sql.contains(r#"SELECT "cname", "cid" FROM ("#),
        "outer projects bare output names: {sql}"
    );
    // Outer dedup filter + limit on the outer query.
    assert!(
        sql.contains(") _dedup WHERE _loom_rn = 1 LIMIT 1000"),
        "outer dedup + limit: {sql}"
    );
    // Recursive core CTE preserved; membership glue still on t_0.
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS")
            && sql.contains(r#"t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)"#),
        "core CTE + glue preserved: {sql}"
    );
    assert!(params.is_empty(), "no params expected; got {params:?}");
}

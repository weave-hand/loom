//! Recursive-CTE reachability (WITH RECURSIVE, emitted by compile_graph_reach /
//! compile_graph_reach_union) executed through DataFusionServingEngine over
//! Iceberg-mirror-backed tables — closing iss-recursive-cte-iceberg. The DuckDB
//! graph e2es (graph-reach-e2e / graph-union-e2e) prove the same reachable sets
//! against the DuckLake/DuckDB engine; this proves them against loom's own engine.

use control_plane_core::{CompareOp, LinkBacking, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::filter::CallerPredicate;
use query_api::serving::{Rows, ServingEngine, SqlValue};
use query_api::serving_datafusion::DataFusionServingEngine;
use query_api::sql::{DuckDbDialect, GraphStep, compile_graph_reach, compile_graph_reach_union};

/// Sorted `id` column values from a `Rows` whose projection is `("id", "name")`.
fn ids_of(rows: &Rows) -> Vec<i64> {
    let idx = rows
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("id column present");
    let mut out: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[idx] {
            SqlValue::Int(n) => *n,
            other => panic!("id column not Int: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

fn person() -> TableRef {
    TableRef {
        schema: "graph".into(),
        name: "person".into(),
    }
}

// person(id, name, knows_id): FK self-link cycle 1->2->3->1. Reachability from
// {1} at depth 3 must visit 2, 3 and (via the cycle) 1 again, deduped — the same
// "cycle terminates + dedups" property graph-reach-e2e asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fk_self_link_recursive_reach_over_datafusion() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
        ("knows_id".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "graph",
            "person",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["ann", "bob", "cal"]),
                SeedCol::Long(vec![2, 3, 1]), // 1->2, 2->3, 3->1 (cycle)
            ],
        )
        .await;

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool), None);

    let step = GraphStep {
        backing: LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
        next_table: person(),
        next_filters: vec![],
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &[step],
        &seed,
        &[],
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        100,
    )
    .expect("compile_graph_reach");
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "sanity: recursive CTE compiled: {sql}"
    );

    let rows = engine
        .fetch_rows(&sql, &params)
        .await
        .expect("fetch_rows over iceberg");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        ids_of(&rows),
        vec![1, 2, 3],
        "FK cycle reachable from 1 (deduped, terminates): {rows:?}"
    );

    drop(writer);
}

// person(id, name, knows_id) FK self-link + colleagues(a, b) join-table self-link.
// Edges — knows: 1->2, 2->3, 5->6 ; colleagues: 1->4, 6->5. From {1} at depth 3 the
// UNION of both links reaches {2, 3, 4} (knows alone gives {2,3}; colleagues adds the
// colleagues-only node 4) — the same "union adds colleagues-only node" property
// graph-union-e2e asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn union_self_links_recursive_reach_over_datafusion() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
        ("knows_id".to_string(), "long".to_string(), true),
    ];
    writer
        .seed_arrays(
            "graph",
            "person",
            &person_cols,
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5, 6]),
                SeedCol::Str(vec!["ann", "bob", "cal", "dee", "eve", "fin"]),
                // knows: 1->2, 2->3, 5->6 (3, 4, 6 have no FK out-edge)
                SeedCol::NullableLong(vec![Some(2), Some(3), None, None, Some(6), None]),
            ],
        )
        .await;

    let colleagues_cols = vec![
        ("a".to_string(), "long".to_string(), false),
        ("b".to_string(), "long".to_string(), false),
    ];
    writer
        .seed_arrays(
            "graph",
            "colleagues",
            &colleagues_cols,
            // colleagues: 1->4, 6->5
            &[SeedCol::Long(vec![1, 6]), SeedCol::Long(vec![4, 5])],
        )
        .await;

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool), None);

    let backings = vec![
        LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
        LinkBacking::JoinTable {
            table: TableRef {
                schema: "graph".into(),
                name: "colleagues".into(),
            },
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    ];
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &backings,
        &seed,
        &[],
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        100,
    )
    .expect("compile_graph_reach_union");
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "sanity: recursive union CTE compiled: {sql}"
    );

    let rows = engine
        .fetch_rows(&sql, &params)
        .await
        .expect("fetch_rows over iceberg");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        ids_of(&rows),
        vec![2, 3, 4],
        "union reaches more than either link alone (colleagues adds node 4): {rows:?}"
    );

    drop(writer);
}

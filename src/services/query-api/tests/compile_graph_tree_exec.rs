//! Execution smoke: run compile_graph_tree's SQL directly through the in-process
//! Iceberg/DataFusion serving engine (no handler/HTTP) to prove the window-over-recursive-CTE
//! with a NULLIF typed-null anchor executes on DataFusion 54. Seeds person(id,name) plus a
//! knows(a,b) self-link 1->2->3 and asserts the served columns/rows carry __depth/__parent/__id.

use control_plane_core::{LinkBacking, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, tref};
use query_api::filter::CallerPredicate;
use query_api::serving::{ServingEngine, SqlValue};
use query_api::sql::{DataFusionDialect, GraphStep, ReachSpec, compile_graph_tree};

#[tokio::test(flavor = "multi_thread")]
async fn tree_sql_executes_on_datafusion() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // person(id, name): nodes 1, 2, 3.
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["a", "b", "c"]),
            ],
        )
        .await;
    // knows(a, b): 1->2, 2->3.
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(vec![1, 2]), SeedCol::Long(vec![2, 3])],
        )
        .await;

    let person: TableRef = tref("main", "person");
    let knows: TableRef = tref("main", "knows");
    let steps = vec![GraphStep {
        backing: LinkBacking::JoinTable {
            table: knows,
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
        next_table: person.clone(),
        next_filters: vec![],
    }];
    // Seed the read at {1}.
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: control_plane_core::CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let (sql, params) = compile_graph_tree(
        &DataFusionDialect,
        &ReachSpec {
            table: &person,
            identity: "id",
            seed_predicates: &seed,
            row_filters: &[],
            allowed_cols: &["id".to_string(), "name".to_string()],
            mask_cols: &[],
            depth: 3,
        },
        &steps,
    )
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    // The point of the test: this must not error on the DataFusion planner.
    let served = eng.fetch_rows(&sql, &params, None).await.unwrap();

    assert_eq!(
        served.columns,
        vec![
            "id".to_string(),
            "name".to_string(),
            "__depth".to_string(),
            "__parent".to_string(),
            "__id".to_string()
        ],
        "tree projection columns"
    );
    // Tree over 1->2->3 from {1}: root 1 + nodes 2, 3 => 3 rows.
    assert_eq!(
        served.rows.len(),
        3,
        "root + two descendants: {:?}",
        served.rows
    );
    // The first row (ORDER BY depth, id) is the root 1 at depth 0 with NULL parent.
    let root = &served.rows[0];
    assert_eq!(root.first(), Some(&SqlValue::Int(1)), "root id: {root:?}");
    // __depth is index 2, __parent index 3 (NULL for root), __id index 4.
    assert_eq!(
        root.get(2),
        Some(&SqlValue::Int(0)),
        "root depth 0: {root:?}"
    );
    assert_eq!(
        root.get(3),
        Some(&SqlValue::Null),
        "root parent NULL: {root:?}"
    );
    assert_eq!(root.get(4), Some(&SqlValue::Int(1)), "root __id: {root:?}");
    // Keep the writer alive until here (its TempDir holds the Parquet).
    drop(writer);
}

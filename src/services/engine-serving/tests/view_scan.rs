//! A catalog view scans as `SELECT <projection> FROM base WHERE <predicate>`
//! through both the ungoverned and governed SQL serving paths; the base is
//! unaffected; an unknown name still fails with the Plan error class; and the
//! governed path treats a view as an ordinary governed relation (mask on a
//! projected column, closed-world skip when unlisted, a view grant that needs
//! no base grant, and no base-policy contamination of the view scan).
//!
//! Harness: `IcebergWriter` seeds a real Iceberg base (`main.customers`) whose
//! mirror projection makes it a live table `define_view` can attach to.
//! loom_fixture_test (Postgres).
//! Spec: docs/superpowers/specs/2026-07-11-catalog-views-design.md

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use control_plane_core::{
    Catalog, CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef, ViewDef,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::execute_query;
use engine_serving::governed::execute_governed_sql_stream;
use futures::TryStreamExt;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn base_cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), false),
    ]
}

/// Seed `main.customers` with `(id long, region string)` rows
/// `(1,'EU'),(2,'US'),(3,'EU')` — one real Parquet file projected into the mirror.
async fn seed_customers(writer: &IcebergWriter) {
    writer
        .seed_arrays(
            "main",
            "customers",
            &base_cols(),
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["EU", "US", "EU"]),
            ],
        )
        .await;
}

/// Collect the i64 values of column `col` across all batches.
fn ids_of(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 column");
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// The column names of a result's schema, in order.
fn col_names(batches: &[RecordBatch]) -> Vec<String> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Collect the utf8 values of column `col` across all batches.
fn strs_of(batches: &[RecordBatch], col: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column");
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn run_governed(
    catalog: &IcebergCatalog,
    sql: &str,
    cat: &GovernedCatalog,
) -> Vec<RecordBatch> {
    let stream = execute_governed_sql_stream(catalog, sql, cat, None)
        .await
        .expect("governed stream");
    stream.try_collect().await.expect("collect")
}

// ---- ungoverned view expansion ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_scan_applies_predicate_and_projection() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    // gov.customers_eu = main.customers WHERE region = 'EU', columns [id]
    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_eu"),
            base: tref("main", "customers"),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("EU".into()),
            }),
            columns: Some(vec!["id".into()]),
        })
        .await
        .expect("define_view");

    let batches = execute_query(
        &catalog,
        "SELECT * FROM \"gov\".\"customers_eu\" ORDER BY id",
        None,
    )
    .await
    .expect("scan view");
    assert_eq!(
        ids_of(&batches, 0),
        vec![1, 3],
        "predicate narrows to EU rows"
    );
    assert_eq!(
        col_names(&batches),
        vec!["id"],
        "projection narrows to the id column only"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn base_scan_is_unaffected_by_views() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_eu"),
            base: tref("main", "customers"),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("EU".into()),
            }),
            columns: Some(vec!["id".into()]),
        })
        .await
        .expect("define_view");

    let batches = execute_query(
        &catalog,
        "SELECT * FROM \"main\".\"customers\" ORDER BY id",
        None,
    )
    .await
    .expect("scan base");
    assert_eq!(
        ids_of(&batches, 0),
        vec![1, 2, 3],
        "base still shows all rows"
    );
    assert_eq!(
        col_names(&batches),
        vec!["id", "region"],
        "base keeps all columns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projectionless_predicateless_view_mirrors_base() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    catalog
        .define_view(ViewDef {
            view: tref("gov", "all_customers"),
            base: tref("main", "customers"),
            predicate: None,
            columns: None,
        })
        .await
        .expect("define_view");

    let batches = execute_query(
        &catalog,
        "SELECT * FROM \"gov\".\"all_customers\" ORDER BY id",
        None,
    )
    .await
    .expect("scan view");
    assert_eq!(
        ids_of(&batches, 0),
        vec![1, 2, 3],
        "all rows mirror the base"
    );
    assert_eq!(
        col_names(&batches),
        vec!["id", "region"],
        "all columns mirror the base"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_view_name_is_still_a_plan_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    let err = execute_query(&catalog, "SELECT * FROM \"gov\".\"nope\"", None)
        .await
        .expect_err("an unknown view name must stay not-found");
    assert!(
        matches!(err, engine_serving::EngineServingError::Plan(_)),
        "unknown view keeps the Plan (client-fault / not-found) class, got: {err}"
    );
}

// ---- governed view expansion (mandatory) ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_view_masks_a_projected_column() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    // View projects both columns so `region` is masked in the view's output.
    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_v"),
            base: tref("main", "customers"),
            predicate: None,
            columns: Some(vec!["id".into(), "region".into()]),
        })
        .await
        .expect("define_view");

    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: tref("gov", "customers_v"),
            row_filters: vec![],
            denied: vec![],
            masked: vec!["region".into()],
        }],
    };
    let batches = run_governed(
        &catalog,
        "SELECT \"region\" FROM \"gov\".\"customers_v\"",
        &cat,
    )
    .await;
    let regions = strs_of(&batches, 0);
    assert!(
        !regions.is_empty() && regions.iter().all(|r| r == "***"),
        "masked column on the view is redacted, got: {regions:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_view_without_entry_is_not_queryable() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_v"),
            base: tref("main", "customers"),
            predicate: None,
            columns: Some(vec!["id".into()]),
        })
        .await
        .expect("define_view");

    // No GovernedTable entry for the view => closed-world skip => unresolvable.
    let cat = GovernedCatalog::default();
    let result = execute_governed_sql_stream(
        &catalog,
        "SELECT \"id\" FROM \"gov\".\"customers_v\"",
        &cat,
        None,
    )
    .await;
    let Err(err) = result else {
        panic!("an unlisted view must not resolve");
    };
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("no table"),
        "closed-world view reads like table-not-found; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_view_grant_suffices_without_base_grant() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_eu"),
            base: tref("main", "customers"),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("EU".into()),
            }),
            columns: Some(vec!["id".into()]),
        })
        .await
        .expect("define_view");

    // Only the VIEW is listed; the base has NO entry. The view grant alone must
    // make the view readable (the base is built privately and ungoverned).
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: tref("gov", "customers_eu"),
            row_filters: vec![],
            denied: vec![],
            masked: vec![],
        }],
    };
    let batches = run_governed(
        &catalog,
        "SELECT \"id\" FROM \"gov\".\"customers_eu\" ORDER BY \"id\"",
        &cat,
    )
    .await;
    assert_eq!(
        ids_of(&batches, 0),
        vec![1, 3],
        "view grant alone makes the view readable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn base_policy_does_not_contaminate_the_view_scan() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(&db));
    seed_customers(&writer).await;
    let catalog = IcebergCatalog::new(pool);

    // View over the full base, no view-side filter/projection restriction.
    catalog
        .define_view(ViewDef {
            view: tref("gov", "customers_v"),
            base: tref("main", "customers"),
            predicate: None,
            columns: Some(vec!["id".into()]),
        })
        .await
        .expect("define_view");

    // The BASE carries a restrictive row filter (id >= 2) on its own entry; the
    // VIEW's entry is empty. The base filter must NOT reach the view scan.
    let cat = GovernedCatalog {
        tables: vec![
            GovernedTable {
                table: tref("main", "customers"),
                row_filters: vec![RowFilter::Compare {
                    property: "id".into(),
                    op: CompareOp::Ge,
                    value: ScalarValue::Int(2),
                }],
                denied: vec![],
                masked: vec![],
            },
            GovernedTable {
                table: tref("gov", "customers_v"),
                row_filters: vec![],
                denied: vec![],
                masked: vec![],
            },
        ],
    };
    let batches = run_governed(
        &catalog,
        "SELECT \"id\" FROM \"gov\".\"customers_v\" ORDER BY \"id\"",
        &cat,
    )
    .await;
    assert_eq!(
        ids_of(&batches, 0),
        vec![1, 2, 3],
        "the base's own policy must not filter the view scan"
    );
}

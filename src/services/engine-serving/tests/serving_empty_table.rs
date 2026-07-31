//! A live-but-empty table (no inline rows, no cold Parquet files) must register
//! and read as ZERO ROWS with the mirror's authoritative schema — never as
//! `table not found`. Also pins the boundary: an unknown table still
//! plan-errors, and an as-of read of a table not live at the pinned snapshot
//! still yields no provider. Harness: `IcebergControlPlane`
//! create+empty-append (the live-zero-file fixture from
//! `worker/tests/transform_e2e.rs`). loom_fixture_test (Postgres).
//! Spec: git history: 2026-07-09-serving-empty-table-not-found-design

use std::sync::Arc;

use control_plane_core::{
    ColumnSpec, ControlPlane, GovernedCatalog, GovernedTable, ObjectType, SnapshotId,
    TableControlPlane, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use engine_serving::governed::execute_governed_sql_stream;
use engine_serving::{EngineServingError, build_serving_provider, execute_query};
use futures::TryStreamExt;
use loom_test_seed::local_sql_catalog;

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// Create `table` live in the mirror with declared columns and an EMPTY file
/// list: `current_snapshot`/`schema` resolve, zero data files, no inline
/// storage — the exact `(None, None)` serving case. Mirrors
/// `transform_e2e::create_empty_table` (a create-only commit would allocate a
/// snapshot without a mirror `table` row, so the empty append is required).
async fn create_empty_table(icp: &IcebergControlPlane, table: &TableRef) {
    let mut tx = icp.begin_table().await.expect("begin");
    tx.create_table(table, &cols()).await.expect("create");
    tx.append_files(table, &[]).await.expect("append empty");
    tx.commit().await.expect("commit");
}

/// Total rows across the batches of a full scan of `provider`.
async fn count_rows(provider: Arc<dyn TableProvider>) -> usize {
    let ctx = SessionContext::new();
    let df = ctx.read_table(provider).expect("read_table");
    let batches = df.collect().await.expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_live_table_serves_zero_rows_with_mirror_schema() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty");
    create_empty_table(&icp, &table).await;

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    let provider = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .expect("build")
        .expect("a live-but-empty table must yield a provider, not None");

    // The provider presents the mirror's authoritative schema exactly.
    let schema = provider.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"], "mirror column order preserved");
    assert_eq!(*schema.field(0).data_type(), DataType::Int64);
    assert!(
        !schema.field(0).is_nullable(),
        "id is non-null in the mirror"
    );
    assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    assert!(
        schema.field(1).is_nullable(),
        "name is nullable in the mirror"
    );

    assert_eq!(
        count_rows(provider).await,
        0,
        "an empty table reads as zero rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_live_table_select_star_returns_empty_not_not_found() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // The exact path previews and worker input reads consume.
    let batches = execute_query(&catalog, "SELECT * FROM \"s\".\"empty\"", None)
        .await
        .expect("SELECT * over a live-but-empty table must succeed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "empty result, not an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_table_still_plan_errors() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    // A live table exists so the serving context is non-trivially populated…
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // …but an undeclared table must STILL be a planning fault (the boundary).
    let err = execute_query(&catalog, "SELECT * FROM \"s\".\"never_declared\"", None)
        .await
        .expect_err("an unknown table must stay not-found");
    assert!(
        matches!(err, EngineServingError::Plan(_)),
        "unknown tables keep the Plan (client-fault / not-found) class, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_identity_table_serves_zero_rows() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty_typed");
    create_empty_table(&icp, &table).await;
    // Bind an identity-bearing type to the table: the identity-bearing
    // `(None, None)` arm (serving.rs:218) must also yield the empty provider.
    let ty = ObjectType::build("EmptyTyped", ("s", "empty_typed"))
        .prop_req("id", "Long")
        .prop("name", "String")
        .identity("id")
        .done();
    icp.ontology().define_type(ty).await.expect("define type");

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    let provider = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .expect("build")
        .expect("an identity-bearing empty table must also yield a provider");
    // Data schema ONLY — no loom_* framing columns may leak.
    let schema = provider.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "name"],
        "no framing columns on the empty provider"
    );
    assert_eq!(count_rows(provider).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_of_before_table_existed_stays_none() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty");
    create_empty_table(&icp, &table).await;

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    // Snapshot 0 predates every mirror row (begin_snapshot >= 1): the table is
    // NOT live at it, so the as-of skip (serving.rs:109-115) must still yield
    // None — the one remaining meaning of None after this change.
    let provider = build_serving_provider(&ctx, &catalog, &table, None, Some(SnapshotId(0)))
        .await
        .expect("build");
    assert!(
        provider.is_none(),
        "not-live-at-snapshot keeps yielding no provider"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_read_of_empty_table_returns_zero_rows() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // The governed path is CLOSED-WORLD since PR #417: a live table with NO
    // GovernedTable entry is skipped (`continue`) before build_serving_provider,
    // so `tables: vec![]` would be deny-all — not what we want to test here. An
    // entry with an EMPTY policy is fully visible (governed.rs struct doc); the
    // governed loop must then register the empty table (wrapped in
    // GovernedTableProvider) and read it as zero rows. Mirror governed_sql.rs:48.
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: tref("s", "empty"),
            row_filters: vec![],
            denied: vec![],
            masked: vec![],
        }],
    };
    let stream = execute_governed_sql_stream(
        &catalog,
        "SELECT * FROM \"s\".\"empty\"",
        &cat,
        None,
        &engine_serving::sql_limits::GovernedSqlLimits::unbounded(),
    )
    .await
    .expect("governed SELECT * over a live-but-empty table must plan");
    let batches: Vec<_> = stream.try_collect().await.expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
}

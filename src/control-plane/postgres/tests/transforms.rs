use loom_test_seed::local_sql_catalog;

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;

#[tokio::test]
async fn postgres_passes_transforms_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::transforms_contract(&cp).await;
}

async fn iceberg_cp(fx: &PgFixture) -> (IcebergControlPlane, tempfile::TempDir) {
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (IcebergControlPlane::new(pg, catalog), wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_passes_transform_run_commit_success_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::transform_run_commit_success_contract(&cp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_passes_transform_data_trigger_contract() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;
    control_plane_testkit::transform_data_trigger_contract(&cp).await;
}

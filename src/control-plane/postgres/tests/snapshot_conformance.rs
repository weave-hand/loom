use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

#[tokio::test]
async fn snapshot_commit_contract() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    control_plane_testkit::snapshot_commit_contract(&cp).await;
}

#[tokio::test]
async fn snapshot_replace_contract() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    control_plane_testkit::snapshot_replace_contract(&cp).await;
}

#[tokio::test]
async fn snapshot_compact_contract() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    control_plane_testkit::snapshot_compact_contract(&cp).await;
}

use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_lineage_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_lineage_closure_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_closure_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_lineage_pagination_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::lineage_pagination_contract(&cp).await;
}

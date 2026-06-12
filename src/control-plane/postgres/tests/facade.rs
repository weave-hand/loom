use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_control_plane_facade_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::control_plane_facade_contract(&cp).await;
}

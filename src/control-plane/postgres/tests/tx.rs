use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_tx_isolation_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::tx_isolation_contract(&cp).await;
}

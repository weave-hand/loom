use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_tx_contract() {
    let fixture = PgFixture::start();
    let cp: PgControlPlane = fixture.fresh_control_plane().await;
    control_plane_testkit::tx_contract(&cp).await;
}

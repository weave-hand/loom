use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_existence_validation_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::existence_validation_contract(&cp).await;
}

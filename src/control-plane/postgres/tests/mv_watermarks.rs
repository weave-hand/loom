use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_mv_watermarks_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::mv_watermarks_contract(&cp).await;
}

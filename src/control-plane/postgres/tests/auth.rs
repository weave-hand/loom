use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_auth_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::auth_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_service_account_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::service_account_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_password_lifecycle_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::password_lifecycle_contract(&cp).await;
}

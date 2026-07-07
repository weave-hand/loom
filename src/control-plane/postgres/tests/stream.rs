use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_bucket_offsets_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::bucket_offsets_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_bucket_offsets_concurrency_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::bucket_offsets_concurrency_contract(cp).await;
}

#[tokio::test]
async fn postgres_passes_stream_tables_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::stream_tables_contract(&cp).await;
}

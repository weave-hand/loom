use std::time::Duration;

use control_plane_postgres::fixture::PgFixture;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn postgres_passes_queue_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::queue_contract(&cp, LOCK_TIMEOUT).await;
}

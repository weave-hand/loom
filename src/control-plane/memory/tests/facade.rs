use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn memory_passes_control_plane_facade_contract() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    control_plane_testkit::control_plane_facade_contract(&cp).await;
}

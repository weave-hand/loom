use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn memory_passes_queue_contract() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    control_plane_testkit::queue_contract(&cp, LOCK_TIMEOUT).await;
}

#[tokio::test]
async fn memory_passes_await_jobs_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::await_jobs_contract(cp).await;
}

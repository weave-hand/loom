use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_tx_contract() {
    control_plane_testkit::tx_contract(&MemoryControlPlane::new(Duration::from_millis(500))).await;
}

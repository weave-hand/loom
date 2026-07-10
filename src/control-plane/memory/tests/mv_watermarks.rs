use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_mv_watermarks_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    control_plane_testkit::mv_watermarks_contract(&cp).await;
}

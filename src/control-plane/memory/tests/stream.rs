use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_bucket_offsets_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    control_plane_testkit::bucket_offsets_contract(&cp).await;
}

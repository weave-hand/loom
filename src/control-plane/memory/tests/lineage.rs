#[tokio::test]
async fn memory_passes_lineage_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_lineage_closure_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_closure_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_lineage_pagination_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_pagination_contract(&cp).await;
}

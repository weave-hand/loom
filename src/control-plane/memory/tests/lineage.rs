#[tokio::test]
async fn memory_passes_lineage_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::lineage_contract(&cp).await;
}

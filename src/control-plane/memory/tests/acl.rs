#[tokio::test]
async fn memory_passes_acl_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::acl_contract(&cp).await;
}

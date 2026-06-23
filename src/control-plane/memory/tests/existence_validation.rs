#[tokio::test]
async fn memory_passes_existence_validation_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::existence_validation_contract(&cp).await;
}

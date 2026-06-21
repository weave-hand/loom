#[tokio::test]
async fn memory_passes_tx_isolation_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::tx_isolation_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_tx_atomic_rollback_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::tx_atomic_rollback_contract(&cp).await;
}

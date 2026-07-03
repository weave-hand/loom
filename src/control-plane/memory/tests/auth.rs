#[tokio::test]
async fn memory_passes_auth_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::auth_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_service_account_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::service_account_contract(&cp).await;
}

#[tokio::test]
async fn memory_passes_password_lifecycle_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::password_lifecycle_contract(&cp).await;
}

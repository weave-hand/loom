use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn snapshot_commit_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_commit_contract(&cp).await;
}

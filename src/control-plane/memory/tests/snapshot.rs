use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn snapshot_commit_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_commit_contract(&cp).await;
}

#[tokio::test]
async fn snapshot_replace_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_replace_contract(&cp).await;
}

#[tokio::test]
async fn snapshot_compact_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_compact_contract(&cp).await;
}

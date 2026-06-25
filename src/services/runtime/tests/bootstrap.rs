use std::time::Duration;

use control_plane_core::Auth;
use control_plane_memory::MemoryControlPlane;
use service_runtime::{bootstrap_admin, hash_password};

#[tokio::test]
async fn seeds_admin_into_empty_store() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    assert!(!cp.has_any_user().await.unwrap());
    bootstrap_admin(&cp, "root", "s3cret").await.unwrap();
    let cred = cp.find_password_credential("root").await.unwrap().unwrap();
    assert_eq!(cred.subject_id.0, "root");
}

#[tokio::test]
async fn is_noop_when_users_exist() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.create_user(&control_plane_core::NewUser {
        subject_id: control_plane_core::SubjectId("existing".into()),
        username: "existing".into(),
        password_phc: hash_password("x").unwrap(),
    })
    .await
    .unwrap();
    // Should NOT create "root" because the store is non-empty.
    bootstrap_admin(&cp, "root", "s3cret").await.unwrap();
    assert!(cp.find_password_credential("root").await.unwrap().is_none());
}

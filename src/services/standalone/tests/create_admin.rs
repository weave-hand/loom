//! Fixture coverage for `service_runtime::create_admin::run_create_admin`: seeds
//! the first admin (role `ADMIN_ROLE`) and seals the instance; a second call is
//! refused because the instance is already sealed. This is the sole coverage for
//! first-admin bootstrap now that no service binary auto-bootstraps from env —
//! see the CONTROLLER ADDENDUM in the task-3 brief.
use control_plane_core::{Acl, Auth};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;

/// A fresh `PgControlPlane` bound to its own migrated database — a direct pool,
/// no composite/HTTP path, mirroring how the `loom create-admin` CLI itself
/// connects.
async fn fixture_control_plane() -> PgControlPlane {
    PgFixture::shared().fresh_control_plane().await
}

#[tokio::test]
async fn create_admin_seeds_then_seals() {
    let cp = fixture_control_plane().await;

    // First run: creates the admin, assigns ADMIN_ROLE, seals.
    service_runtime::create_admin::run_create_admin(&cp, "jack", "hunter2")
        .await
        .expect("first create-admin succeeds");

    assert!(cp.has_any_user().await.unwrap());
    assert!(cp.is_bootstrap_sealed().await.unwrap());
    assert!(
        cp.has_role(
            &control_plane_core::SubjectId("jack".into()),
            &control_plane_core::RoleId(control_plane_core::ADMIN_ROLE.into()),
        )
        .await
        .unwrap(),
        "jack holds the admin role"
    );

    // Second run: refused because sealed.
    let err = service_runtime::create_admin::run_create_admin(&cp, "mallory", "x")
        .await
        .expect_err("second create-admin is refused");
    assert!(matches!(
        err,
        service_runtime::create_admin::CreateAdminError::AlreadySealed
    ));

    // Empty password refused (on a fresh CP this would be EmptyPassword; here still refused).
    assert!(
        service_runtime::create_admin::run_create_admin(&cp, "x", "")
            .await
            .is_err()
    );
}

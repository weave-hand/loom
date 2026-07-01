//! Admin-provisioning e2e against the real postgres adapter: the gate (admin vs
//! non-admin vs unauthenticated) and create → login → disable through the stack.
//! Uses a bare `PgControlPlane` (auth + acl only) — no engine/serving needed.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Acl, Auth, NewUser, RoleId, SubjectId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use http_body_util::BodyExt;
use service_runtime::{
    AdminState, AuthState, admin_routes, generate_session_token, hash_password, token_sha256,
};
use tower::ServiceExt;

const ADMIN: &str = "root";

fn app(cp: Arc<PgControlPlane>) -> axum::Router {
    let admin = AdminState {
        auth: cp.clone() as Arc<dyn Auth + Send + Sync>,
        acl: cp.clone() as Arc<dyn Acl + Send + Sync>,
        admin_username: ADMIN.into(),
    };
    let auth = AuthState {
        auth: cp as Arc<dyn Auth + Send + Sync>,
        session_ttl: Duration::from_secs(3600),
    };
    admin_routes(admin, auth)
}

/// Seed a user (subject id == username) and mint a live session token.
async fn seed_session(cp: &PgControlPlane, username: &str) -> String {
    cp.create_user(&NewUser {
        subject_id: SubjectId(username.into()),
        username: username.into(),
        password_phc: hash_password("pw").expect("hash"),
    })
    .await
    .expect("create_user");
    let token = generate_session_token();
    cp.create_session(
        &SubjectId(username.into()),
        &token_sha256(&token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .expect("create_session");
    token
}

async fn status(app: axum::Router, req: Request) -> StatusCode {
    app.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn gate_admin_ok_nonadmin_403_unauth_401() {
    let fx = PgFixture::start();
    let cp = Arc::new(fx.fresh_control_plane().await);

    // admin session (subject id == "root" matches the gate)
    let admin_token = seed_session(&cp, ADMIN).await;
    assert_eq!(
        status(
            app(cp.clone()),
            Request::builder()
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::OK
    );

    // a different authenticated subject → 403
    let alice_token = seed_session(&cp, "alice").await;
    assert_eq!(
        status(
            app(cp.clone()),
            Request::builder()
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {alice_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::FORBIDDEN
    );

    // unauthenticated → 401
    assert_eq!(
        status(
            app(cp),
            Request::builder()
                .uri("/admin/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn create_with_role_then_disable_blocks_login() {
    let fx = PgFixture::start();
    let cp = Arc::new(fx.fresh_control_plane().await);
    let admin_token = seed_session(&cp, ADMIN).await;

    // a pre-existing role the create call will assign
    cp.define_role(&RoleId("reader".into())).await.unwrap();

    let res = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"provisioned","password":"pw123","roles":["reader"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["assigned_roles"], serde_json::json!(["reader"]));

    // the provisioned user is active (found for login)
    assert!(
        cp.find_password_credential("provisioned")
            .await
            .unwrap()
            .is_some()
    );

    // disable → login lookup hides the user
    let disabled = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users/provisioned/disable")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    assert!(
        cp.find_password_credential("provisioned")
            .await
            .unwrap()
            .is_none()
    );
}

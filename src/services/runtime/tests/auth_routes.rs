use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, NewUser, Redacted, SubjectId};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use service_runtime::{AuthState, hash_password, login_routes, session_routes, token_sha256};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
        lockout: service_runtime::LockoutPolicy::default(),
    }
}

async fn seed_user(cp: &MemoryControlPlane, username: &str, password: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(username.into()),
        username: username.into(),
        password_phc: Redacted::new(hash_password(password).unwrap()),
    })
    .await
    .unwrap();
}

async fn post_login(app: Router, body: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn login_success_returns_token() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw123").await;
    let (status, body) = post_login(
        login_routes(state(cp.clone())),
        r#"{"username":"alice","password":"pw123"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap();
    // the minted token resolves to alice
    assert_eq!(
        cp.resolve_session(&token_sha256(token), time::OffsetDateTime::now_utc())
            .await
            .unwrap(),
        Some(SubjectId("alice".into()))
    );
}

#[tokio::test]
async fn login_bad_password_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw123").await;
    let (status, _) = post_login(
        login_routes(state(cp)),
        r#"{"username":"alice","password":"wrong"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_unknown_user_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let (status, _) = post_login(
        login_routes(state(cp)),
        r#"{"username":"ghost","password":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logout_revokes_session() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw").await;
    let token = "tok-logout";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    let res = session_routes(state(cp.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/logout")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        cp.resolve_session(&token_sha256(token), time::OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_none()
    );
}

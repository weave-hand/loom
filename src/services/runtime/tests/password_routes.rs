//! Self-service password change: wrong `current` → 403 unchanged; right `current`
//! → the new password logs in, the old fails, other sessions are revoked, and the
//! caller's current session survives.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, LockoutPolicy, NewUser, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{AuthState, hash_password, session_routes, token_sha256};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
        lockout: LockoutPolicy::default(),
    }
}

async fn seed_user_session(cp: &MemoryControlPlane, user: &str, pw: &str, token: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(user.into()),
        username: user.into(),
        password_phc: hash_password(pw).unwrap(),
    })
    .await
    .unwrap();
    cp.create_session(
        &SubjectId(user.into()),
        &token_sha256(token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
}

async fn change(app: Router, token: &str, body: &str) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/password")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn wrong_current_is_403_and_unchanged() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user_session(&cp, "al", "orig", "tok-cur").await;
    let status = change(
        session_routes(state(cp.clone())),
        "tok-cur",
        r#"{"current":"WRONG","new":"fresh"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // password unchanged: original still verifies
    let cred = cp.find_password_credential("al").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("orig", &cred.password_phc));
}

#[tokio::test]
async fn right_current_rotates_revokes_others_keeps_current() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user_session(&cp, "al", "orig", "tok-cur").await;
    // a second, "other" session for the same subject
    cp.create_session(
        &SubjectId("al".into()),
        &token_sha256("tok-other"),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    let status = change(
        session_routes(state(cp.clone())),
        "tok-cur",
        r#"{"current":"orig","new":"fresh"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let now = time::OffsetDateTime::now_utc();
    // current session preserved
    assert_eq!(
        cp.resolve_session(&token_sha256("tok-cur"), now)
            .await
            .unwrap(),
        Some(SubjectId("al".into()))
    );
    // other session revoked
    assert!(
        cp.resolve_session(&token_sha256("tok-other"), now)
            .await
            .unwrap()
            .is_none()
    );
    // new password verifies, old does not
    let cred = cp.find_password_credential("al").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("fresh", &cred.password_phc));
    assert!(!service_runtime::verify_password("orig", &cred.password_phc));
}

#[tokio::test]
async fn unauthenticated_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let status = app_unauth(session_routes(state(cp))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

async fn app_unauth(app: Router) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/password")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"current":"x","new":"y"}"#))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

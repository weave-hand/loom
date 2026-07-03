//! Login lockout: N failures lock the account, the correct password is rejected
//! while locked, a success before the threshold resets the counter, and the lock
//! auto-expires after the configured duration.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use control_plane_core::{Auth, LockoutPolicy, NewUser, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{AuthState, hash_password, login_routes};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>, lockout: LockoutPolicy) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
        lockout,
    }
}

async fn seed(cp: &MemoryControlPlane, user: &str, pw: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(user.into()),
        username: user.into(),
        password_phc: hash_password(pw).unwrap(),
    })
    .await
    .unwrap();
}

async fn login(app: Router, user: &str, pw: &str) -> axum::http::StatusCode {
    let body = format!(r#"{{"username":"{user}","password":"{pw}"}}"#);
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn lockout_after_threshold_rejects_even_correct_password() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 3,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::hours(1),
    };
    // 3 wrong attempts → locked
    for _ in 0..3 {
        assert_eq!(
            login(login_routes(state(cp.clone(), policy)), "al", "wrong").await,
            axum::http::StatusCode::UNAUTHORIZED
        );
    }
    // correct password now rejected while locked (same generic 401)
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn success_before_threshold_resets_counter() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 3,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::hours(1),
    };
    // 2 failures, then a success (resets), then 2 more failures → still not locked
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK
    );
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK,
        "counter was reset by the earlier success, so 2 more failures do not lock"
    );
}

#[tokio::test]
async fn lock_auto_expires_after_duration() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 2,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::milliseconds(200),
    };
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    // locked now
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::UNAUTHORIZED
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    // lock expired → correct password logs in
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK
    );
}

async fn login_full(app: Router, user: &str, pw: &str) -> (axum::http::StatusCode, String) {
    let body = format!(r#"{{"username":"{user}","password":"{pw}"}}"#);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = http_body_util::BodyExt::collect(res.into_body())
        .await
        .unwrap()
        .to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

/// Spec test 6 — no enumeration: a locked existing account and an unknown username
/// are indistinguishable (same status AND body) beyond the fact of rejection.
#[tokio::test]
async fn locked_and_unknown_are_indistinguishable() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 2,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::hours(1),
    };
    // lock "al"
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    // locked existing account (correct password) vs an unknown username
    let locked = login_full(login_routes(state(cp.clone(), policy)), "al", "right").await;
    let unknown = login_full(login_routes(state(cp.clone(), policy)), "ghost", "right").await;
    assert_eq!(locked.0, axum::http::StatusCode::UNAUTHORIZED);
    assert_eq!(
        locked, unknown,
        "a locked account is indistinguishable from an unknown username"
    );
}

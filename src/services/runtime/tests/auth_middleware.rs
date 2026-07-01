use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::routing::get;
use control_plane_core::{Auth, NewServiceAccount, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{AuthState, Subject, protect, token_sha256};
use time::OffsetDateTime;
use tower::ServiceExt;

async fn whoami(subject: Subject) -> String {
    subject.0.0
}

fn app(cp: Arc<MemoryControlPlane>) -> Router {
    protect(
        Router::new().route("/whoami", get(whoami)),
        AuthState {
            auth: cp,
            session_ttl: Duration::from_secs(3600),
        },
    )
}

async fn bearer(app: Router, token: Option<&str>) -> StatusCode {
    let mut req = Request::builder().uri("/whoami");
    if let Some(t) = token {
        req = req.header(AUTHORIZATION, format!("Bearer {t}"));
    }
    app.oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn valid_token_authenticates() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = "deadbeef";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
    assert_eq!(bearer(app(cp), Some(token)).await, StatusCode::OK);
}

#[tokio::test]
async fn missing_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    assert_eq!(bearer(app(cp), None).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    assert_eq!(
        bearer(app(cp), Some("nope")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn expired_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = "stale";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() - time::Duration::hours(1),
    )
    .await
    .unwrap();
    assert_eq!(bearer(app(cp), Some(token)).await, StatusCode::UNAUTHORIZED);
}

async fn seed_service_token(
    cp: &MemoryControlPlane,
    account: &str,
    name: &str,
    token: &str,
    expires: OffsetDateTime,
) {
    cp.create_service_account(&NewServiceAccount {
        subject_id: SubjectId(account.into()),
        name: name.into(),
    })
    .await
    .unwrap();
    cp.create_service_token(
        &SubjectId(account.into()),
        &token_sha256(token),
        "t",
        expires,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn valid_service_token_authenticates() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-token-xyz",
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await;
    assert_eq!(
        bearer(app(cp), Some("svc-token-xyz")).await,
        StatusCode::OK,
        "a live service token passes require_auth like a session"
    );
}

#[tokio::test]
async fn expired_service_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-stale",
        OffsetDateTime::now_utc() - time::Duration::hours(1),
    )
    .await;
    assert_eq!(
        bearer(app(cp), Some("svc-stale")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn revoked_service_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-revoked",
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await;
    cp.revoke_service_token(&token_sha256("svc-revoked"))
        .await
        .unwrap();
    assert_eq!(
        bearer(app(cp), Some("svc-revoked")).await,
        StatusCode::UNAUTHORIZED
    );
}

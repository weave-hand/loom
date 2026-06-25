use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::routing::get;
use control_plane_core::{Auth, SubjectId};
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

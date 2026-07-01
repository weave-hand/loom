use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, SubjectId};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use service_runtime::{AuthState, service_account_routes, token_sha256};
use time::OffsetDateTime;
use tower::ServiceExt;

const ADMIN: &str = "root";
const MAX_TTL: Duration = Duration::from_secs(90 * 24 * 3600);

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
    }
}

/// Seed a session token for `subject` and return the bearer value.
async fn session_for(cp: &MemoryControlPlane, subject: &str, token: &str) {
    cp.create_session(
        &SubjectId(subject.into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
}

fn app(cp: Arc<MemoryControlPlane>) -> Router {
    service_account_routes(state(cp), Some(SubjectId(ADMIN.into())), MAX_TTL)
}

async fn send(
    app: Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: &str,
) -> (StatusCode, String) {
    let mut req = Request::builder().method(method).uri(uri);
    if !body.is_empty() {
        req = req.header("content-type", "application/json");
    }
    if let Some(b) = bearer {
        req = req.header(AUTHORIZATION, format!("Bearer {b}"));
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn admin_creates_account_and_mints_token_once() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;

    // create account
    let (st, _) = send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts",
        Some("admin-tok"),
        r#"{"name":"etl"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // mint token → plaintext returned once
    let (st, body) = send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        r#"{"label":"primary","ttl_secs":3600}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap();
    // the minted token resolves to the account
    assert_eq!(
        cp.resolve_service_token(&token_sha256(token), OffsetDateTime::now_utc())
            .await
            .unwrap(),
        Some(SubjectId("etl".into()))
    );
    // list never re-derives the plaintext
    let (st, list) = send(
        app(cp),
        "GET",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        "",
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        !list.contains(token),
        "list must not leak the plaintext token"
    );
}

#[tokio::test]
async fn non_admin_is_403_on_every_management_route() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, "not-admin", "user-tok").await;
    for (method, uri, body) in [
        ("POST", "/auth/service-accounts", r#"{"name":"x"}"#),
        ("GET", "/auth/service-accounts", ""),
        (
            "POST",
            "/auth/service-accounts/x/tokens",
            r#"{"label":"l","ttl_secs":10}"#,
        ),
        ("GET", "/auth/service-accounts/x/tokens", ""),
        ("DELETE", "/auth/service-accounts/x/tokens/00", ""),
    ] {
        let (st, _) = send(app(cp.clone()), method, uri, Some("user-tok"), body).await;
        assert_eq!(
            st,
            StatusCode::FORBIDDEN,
            "{method} {uri} must be admin-only"
        );
    }
}

#[tokio::test]
async fn unauthenticated_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let (st, _) = send(app(cp), "GET", "/auth/service-accounts", None, "").await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn mint_over_max_ttl_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts",
        Some("admin-tok"),
        r#"{"name":"etl"}"#,
    )
    .await;
    let over = MAX_TTL.as_secs() + 1;
    let (st, _) = send(
        app(cp),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        &format!(r#"{{"label":"l","ttl_secs":{over}}}"#),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn revoke_makes_token_stop_resolving() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts",
        Some("admin-tok"),
        r#"{"name":"etl"}"#,
    )
    .await;
    let (_, body) = send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        r#"{"label":"primary","ttl_secs":3600}"#,
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap().to_string();
    let token_id = v["token_id"].as_str().unwrap().to_string();
    // live before revoke
    assert!(
        cp.resolve_service_token(&token_sha256(&token), OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_some()
    );
    // revoke via the hex id
    let (st, _) = send(
        app(cp.clone()),
        "DELETE",
        &format!("/auth/service-accounts/etl/tokens/{token_id}"),
        Some("admin-tok"),
        "",
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        cp.resolve_service_token(&token_sha256(&token), OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_none(),
        "a revoked token no longer resolves"
    );
}

#[tokio::test]
async fn management_closed_when_no_admin_configured() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    // admin_subject = None ⇒ nobody can manage, even the would-be admin.
    let app = service_account_routes(state(cp), None, MAX_TTL);
    let (st, _) = send(app, "GET", "/auth/service-accounts", Some("admin-tok"), "").await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

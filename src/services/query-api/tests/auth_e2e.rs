//! Authenticated-read e2e: the full composed query-api app (protected object routes +
//! public /auth/login) exercised against the auth matrix from the auth spec.
//!
//! Cases:
//!   1. valid session token → governed GET /objects/<type> → 200
//!   2. missing token → 401
//!   3. bad password via POST /auth/login → 401
//!   4. revoked session → 401
//!   5. authn passes but ACL-denied subject → 403

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, ControlPlane, NewUser, SubjectId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use http_body_util::BodyExt;
use serde_json::json;
use service_runtime::{
    AuthState, generate_session_token, hash_password, login_routes, protect, token_sha256,
};
use tower::ServiceExt;

use e2e_support::{StubAction, grant_read, session_token, setup_iceberg, subject_with_role};

/// Build the full query-api app: protected object routes + public /auth/login.
fn app(cp: Arc<PgControlPlane>, eng: Arc<dyn query_api::serving::ServingEngine>) -> axum::Router {
    let auth = AuthState {
        auth: cp.clone() as Arc<dyn Auth + Send + Sync>,
        session_ttl: Duration::from_secs(3600),
    };
    protect(
        query_api::http::router(query_api::http::AppState {
            cp: cp as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
            default_limit: 1000,
        }),
        auth.clone(),
    )
    .merge(login_routes(auth))
}

/// Case 1: valid session token → governed GET /objects/Customer → 200.
#[tokio::test]
async fn valid_token_read_200() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup_iceberg(&fx).await;
    let cp = Arc::new(cp);

    // Give "alice" a read grant on Customer and mint a session.
    let (_, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    let token = session_token(&cp, "alice").await;

    let res = app(cp, eng)
        .oneshot(
            Request::builder()
                .uri("/objects/Customer")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

/// Case 2: missing Authorization header → 401.
#[tokio::test]
async fn missing_token_401() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup_iceberg(&fx).await;
    let cp = Arc::new(cp);

    let (_, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;

    let res = app(cp, eng)
        .oneshot(
            Request::builder()
                .uri("/objects/Customer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Case 3: POST /auth/login with wrong password → 401.
#[tokio::test]
async fn bad_password_login_401() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup_iceberg(&fx).await;
    let cp = Arc::new(cp);

    // Seed the user with a known password so there IS a user to authenticate.
    let phc = hash_password("correct-password").expect("hash");
    cp.create_user(&NewUser {
        subject_id: SubjectId("bob".into()),
        username: "bob".into(),
        password_phc: phc,
    })
    .await
    .unwrap();

    let body = json!({
        "username": "bob",
        "password": "wrong-password"
    });
    let res = app(cp, eng)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Case 4: session revoked → 401.
#[tokio::test]
async fn revoked_session_401() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup_iceberg(&fx).await;
    let cp = Arc::new(cp);

    let (_, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Customer").await;

    // Mint a session then immediately revoke it.
    let token = generate_session_token();
    let hash = token_sha256(&token);
    let expires = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    // Ensure user exists.
    let phc = hash_password("e2e-password").expect("hash");
    let _create_carol = cp
        .create_user(&NewUser {
            subject_id: SubjectId("carol".into()),
            username: "carol".into(),
            password_phc: phc,
        })
        .await;
    cp.create_session(&SubjectId("carol".into()), &hash, expires)
        .await
        .unwrap();
    cp.revoke_session(&hash).await.unwrap();

    let res = app(cp, eng)
        .oneshot(
            Request::builder()
                .uri("/objects/Customer")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Case 5: verified subject but NO ACL grant → 403.
#[tokio::test]
async fn authn_ok_acl_denied_403() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup_iceberg(&fx).await;
    let cp = Arc::new(cp);

    // "dave" has a valid session (session_token also creates the ACL subject via
    // create_user) but has NOT been granted read on Customer.
    let token = session_token(&cp, "dave").await;

    let res = app(cp, eng)
        .oneshot(
            Request::builder()
                .uri("/objects/Customer")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Authn passes (not 401), but ACL check denies (403).
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Consume body to avoid warnings.
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let _ = bytes;
}

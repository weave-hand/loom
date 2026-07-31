//! Admin route behavior against the in-memory fake: the gate (admin/non-admin/
//! unauthenticated), create + login, bundled roles (assigned / idempotent retry /
//! unknown role), list (no verifier, disabled state), disable/enable.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{
    ADMIN_ROLE, Acl, Aggregation, Auth, Cardinality, DerivedPropertyDef, LinkDef, NewUser,
    ObjectType, Ontology, Redacted, RoleId, SubjectId, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use service_runtime::{AdminState, AuthState, admin_routes, hash_password, token_sha256};
use tower::ServiceExt;

const ADMIN: &str = "root";

fn states(cp: Arc<MemoryControlPlane>) -> (AdminState, AuthState) {
    (
        AdminState {
            auth: cp.clone(),
            cp: cp.clone(),
        },
        AuthState {
            auth: cp,
            session_ttl: Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
        },
    )
}

/// Seed a user and a live session token; return the token.
async fn seed_session(cp: &MemoryControlPlane, username: &str) -> String {
    cp.create_user(&NewUser {
        subject_id: SubjectId(username.into()),
        username: username.into(),
        password_phc: Redacted::new(hash_password("pw").unwrap()),
    })
    .await
    .unwrap();
    let token = format!("tok-{username}");
    cp.create_session(
        &SubjectId(username.into()),
        &token_sha256(&token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
    token
}

/// Seed a user, define + grant the reserved admin role, and mint a session token.
async fn seed_admin_session(cp: &MemoryControlPlane, username: &str) -> String {
    let token = seed_session(cp, username).await;
    let subject = SubjectId(username.into());
    cp.define_subject(&subject).await.unwrap();
    let role = RoleId(ADMIN_ROLE.to_string());
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subject, &role).await.unwrap();
    token
}

fn app(cp: Arc<MemoryControlPlane>) -> axum::Router {
    let (admin, auth) = states(cp);
    admin_routes(admin, auth)
}

async fn send(app: axum::Router, req: Request) -> (StatusCode, String) {
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn post_json(uri: &str, token: &str, body: &str) -> Request {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn delete_req(uri: &str, token: &str) -> Request {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn unauthenticated_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let (status, _) = send(
        app(cp),
        Request::builder()
            .uri("/admin/users")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn non_admin_is_403() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_session(&cp, "alice").await; // not the admin
    let (status, _) = send(
        app(cp),
        Request::builder()
            .uri("/admin/users")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_creates_user_who_can_login() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp.clone()),
        post_json(
            "/admin/users",
            &token,
            r#"{"username":"newbie","password":"hunter2","roles":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // the created user has a credential and is not disabled
    let cred = cp
        .find_password_credential("newbie")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cred.subject_id, SubjectId("newbie".into()));
}

#[tokio::test]
async fn bundled_roles_assigned_and_idempotent_retry() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_role(&RoleId("reader".into())).await.unwrap();

    let body = r#"{"username":"carol","password":"pw","roles":["reader"]}"#;
    let (status, resp) = send(app(cp.clone()), post_json("/admin/users", &token, body)).await;
    assert_eq!(status, StatusCode::CREATED);
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["created"], serde_json::json!(true));
    assert_eq!(v["assigned_roles"], serde_json::json!(["reader"]));

    // Retry identical POST → not a hard conflict (200, created:false), role still listed.
    let (status, resp) = send(app(cp), post_json("/admin/users", &token, body)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["created"], serde_json::json!(false));
    assert_eq!(v["assigned_roles"], serde_json::json!(["reader"]));
}

#[tokio::test]
async fn unknown_role_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, resp) = send(
        app(cp),
        post_json(
            "/admin/users",
            &token,
            r#"{"username":"dave","password":"pw","roles":["ghost"]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["created"], serde_json::json!(true)); // user was created before the bad role
    assert!(v["error"].as_str().unwrap().contains("ghost"));
}

#[tokio::test]
async fn list_users_shows_state_no_verifier() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, resp) = send(
        app(cp),
        Request::builder()
            .uri("/admin/users")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!resp.contains("password"), "no verifier in the listing");
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    let users = v["users"].as_array().unwrap();
    assert!(users.iter().any(|u| u["username"] == "root"));
}

#[tokio::test]
async fn disable_blocks_session_and_login_then_enable_restores() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    let victim_token = seed_session(&cp, "mallory").await;

    // disable mallory
    let (status, _) = send(
        app(cp.clone()),
        post_json("/admin/users/mallory/disable", &admin_token, ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // mallory's session no longer resolves; login lookup hides her.
    assert!(
        cp.resolve_session(
            &token_sha256(&victim_token),
            time::OffsetDateTime::now_utc()
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        cp.find_password_credential("mallory")
            .await
            .unwrap()
            .is_none()
    );

    // enable restores login lookup.
    let (status, _) = send(
        app(cp.clone()),
        post_json("/admin/users/mallory/enable", &admin_token, ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        cp.find_password_credential("mallory")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn disable_unknown_user_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(app(cp), post_json("/admin/users/ghost/disable", &token, "")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_reset_revokes_all_sessions_and_sets_new_password() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    // victim user with a live session
    let victim_token = seed_session(&cp, "victim").await;

    let (status, _) = send(
        app(cp.clone()),
        post_json(
            "/admin/users/victim/password",
            &admin_token,
            r#"{"new":"reset-pw"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // all the victim's prior sessions are revoked
    assert!(
        cp.resolve_session(
            &token_sha256(&victim_token),
            time::OffsetDateTime::now_utc()
        )
        .await
        .unwrap()
        .is_none()
    );
    // the new password verifies
    let cred = cp
        .find_password_credential("victim")
        .await
        .unwrap()
        .unwrap();
    assert!(service_runtime::verify_password(
        "reset-pw",
        cred.password_phc.expose()
    ));
}

#[tokio::test]
async fn admin_reset_unknown_user_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp),
        post_json(
            "/admin/users/ghost/password",
            &admin_token,
            r#"{"new":"x"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn non_admin_reset_is_403() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let alice = seed_session(&cp, "alice").await; // not admin
    seed_session(&cp, "victim").await;
    let (status, _) = send(
        app(cp),
        post_json("/admin/users/victim/password", &alice, r#"{"new":"x"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Seed `Customer`, `Order`, and the `customer` link (Order -> Customer). When
/// `with_derived` is set, `Order` also carries a derived property naming that link.
async fn seed_order_customer_link(cp: &MemoryControlPlane, with_derived: bool) {
    cp.define_type(
        ObjectType::build("Customer", ("wh", "customer"))
            .prop_req("id", "Long")
            .prop("name", "String")
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    let mut order = ObjectType::build("Order", ("wh", "order"))
        .prop_req("id", "Long")
        .prop_req("customer_id", "Long")
        .identity("id");
    if with_derived {
        order = order.derived(DerivedPropertyDef::new(
            "customerCount",
            "Long",
            "customer",
            Aggregation::Count,
        ));
    }
    cp.define_type(order.done()).await.unwrap();
    cp.define_link(LinkDef::fk(
        "customer",
        "Order",
        "Customer",
        Cardinality::One,
        "customer_id",
        "id",
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn delete_link_referenced_by_derived_property_is_409() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_order_customer_link(&cp, true).await;

    let (status, resp) = send(
        app(cp.clone()),
        delete_req("/admin/links/Order/customer", &token),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert!(
        v["error"].as_str().unwrap().contains("customerCount"),
        "409 body names the blocking derived property: {resp}"
    );

    // Untouched: the link is still there.
    assert!(
        cp.links(
            &TypeName("Order".into()),
            control_plane_core::PageReq::unbounded()
        )
        .await
        .unwrap()
        .items
        .iter()
        .any(|l| l.name == "customer")
    );
}

#[tokio::test]
async fn delete_unreferenced_link_is_200() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_order_customer_link(&cp, false).await;

    let (status, _) = send(
        app(cp.clone()),
        delete_req("/admin/links/Order/customer", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !cp.links(
            &TypeName("Order".into()),
            control_plane_core::PageReq::unbounded()
        )
        .await
        .unwrap()
        .items
        .iter()
        .any(|l| l.name == "customer")
    );
}

#[tokio::test]
async fn define_model_with_non_numeric_agg_column_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // Seed Customer (id: Long, name: String), Order, and the resolvable `customer`
    // link (Order -> Customer).
    seed_order_customer_link(&cp, false).await;

    // Redefine Order with a derived Sum over Customer's DECLARED `name` (String)
    // property. The link + target resolve and `name` is a declared property whose
    // known logical type is non-numeric, so define-time validation rejects it as 400.
    // (An UNDECLARED / catalog-only column would instead be skipped best-effort.)
    let body = r#"{
        "name": "Order",
        "table": {"schema": "wh", "name": "order"},
        "identity": "id",
        "properties": [{"name": "id", "ty": "Long", "required": true},
                       {"name": "customer_id", "ty": "Long", "required": true}],
        "derived": [{"name": "bogus", "ty": "Double", "link": "customer",
                     "agg": {"kind": "sum", "column": "name"}}]
    }"#;
    let (status, resp) = send(app(cp), post_json("/admin/models", &token, body)).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "Sum over a declared non-numeric property → 400: {resp}"
    );
}

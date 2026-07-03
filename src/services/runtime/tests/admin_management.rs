//! Management admin routes against the in-memory fake: link + action
//! define/delete (idempotent), role delete, grant list/revoke (reflecting
//! grant→revoke), user↔role assign/list/unassign, plus the 404/400 edges and
//! one non-admin 403 spot-check of the shared gate.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{ADMIN_ROLE, Acl, Auth, NewUser, ObjectType, Ontology, RoleId, SubjectId};
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
        password_phc: hash_password("pw").unwrap(),
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

/// Define the `Widget` + `Gadget` types the link/action/grant bodies reference.
async fn seed_types(cp: &MemoryControlPlane) {
    for (ty, table) in [("Widget", "widget"), ("Gadget", "gadget")] {
        cp.define_type(
            ObjectType::build(ty, ("main", table))
                .prop("id", "Int")
                .done(),
        )
        .await
        .unwrap();
    }
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

fn req_json(method: &str, uri: &str, token: &str, body: &str) -> Request {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn req_empty(method: &str, uri: &str, token: &str) -> Request {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

const LINK_BODY: &str = r#"{
    "name": "gadgets",
    "from": "Widget",
    "to": "Gadget",
    "cardinality": "Many",
    "backing": {"ForeignKey": {"from_column": "id", "to_column": "widget_id"}}
}"#;

#[tokio::test]
async fn define_link_then_delete_idempotently() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_types(&cp).await;

    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/links", &token, LINK_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        cp.links(
            &control_plane_core::TypeName("Widget".into()),
            control_plane_core::PageReq::unbounded()
        )
        .await
        .unwrap()
        .items
        .iter()
        .any(|l| l.name == "gadgets"),
        "defined link listed"
    );

    let (status, body) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/links/Widget/gadgets", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["deleted"],
        serde_json::json!({"from": "Widget", "name": "gadgets"})
    );
    assert!(
        !cp.links(
            &control_plane_core::TypeName("Widget".into()),
            control_plane_core::PageReq::unbounded()
        )
        .await
        .unwrap()
        .items
        .iter()
        .any(|l| l.name == "gadgets"),
        "deleted link no longer listed"
    );

    // Idempotent second delete → still 200.
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/links/Widget/gadgets", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn define_link_unknown_endpoint_type_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // No types defined: the link's endpoints are unknown → NotFound → 404.
    let (status, _) = send(app(cp), req_json("POST", "/admin/links", &token, LINK_BODY)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn define_action_then_delete_idempotently() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_types(&cp).await;

    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/actions",
            &token,
            r#"{"name": "makeWidget", "target": "Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        cp.get_action(&control_plane_core::ActionName("makeWidget".into()))
            .await
            .is_ok(),
        "defined action readable"
    );

    let (status, body) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/actions/makeWidget", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], serde_json::json!({"name": "makeWidget"}));
    assert!(
        cp.get_action(&control_plane_core::ActionName("makeWidget".into()))
            .await
            .is_err(),
        "deleted action gone"
    );

    // Idempotent second delete → still 200.
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/actions/makeWidget", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn define_action_unknown_target_type_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/actions",
            &token,
            r#"{"name": "makeGhost", "target": "Ghost"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_role_idempotently() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_role(&RoleId("temp".into())).await.unwrap();

    let (status, body) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/roles/temp", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], serde_json::json!({"role": "temp"}));
    assert!(
        !cp.list_roles()
            .await
            .unwrap()
            .contains(&RoleId("temp".into())),
        "deleted role gone"
    );

    // Idempotent second delete → still 200.
    let (status, _) = send(app(cp), req_empty("DELETE", "/admin/roles/temp", &token)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn grants_list_reflects_grant_then_revoke() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_types(&cp).await;
    cp.define_role(&RoleId("mgmt".into())).await.unwrap();

    // Grant read on Widget via the POST route, then list it.
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/mgmt/grants",
            &token,
            r#"{"action": "read", "type": "Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/roles/mgmt/grants", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let grants = v["grants"].as_array().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0]["action"], "read");
    assert_eq!(grants[0]["effect"], "allow");
    assert_eq!(grants[0]["target"], serde_json::json!({"Type": "Widget"}));

    // Revoke it; the list is empty again.
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "DELETE",
            "/admin/roles/mgmt/grants",
            &token,
            r#"{"action": "read", "type": "Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        app(cp),
        req_empty("GET", "/admin/roles/mgmt/grants", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["grants"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn grants_list_unknown_role_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp),
        req_empty("GET", "/admin/roles/no-such-role/grants", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn revoke_bad_action_string_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_role(&RoleId("mgmt".into())).await.unwrap();
    let (status, _) = send(
        app(cp),
        req_json(
            "DELETE",
            "/admin/roles/mgmt/grants",
            &token,
            r#"{"action": "bogus", "type": "Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn assign_list_unassign_user_role_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_subject(&SubjectId("carol".into())).await.unwrap();
    cp.define_role(&RoleId("reader".into())).await.unwrap();

    let (status, _) = send(
        app(cp.clone()),
        req_empty("PUT", "/admin/users/carol/roles/reader", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/users/carol/roles", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["roles"], serde_json::json!(["reader"]));

    let (status, _) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/users/carol/roles/reader", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/users/carol/roles", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["roles"].as_array().unwrap().is_empty());

    // Idempotent second unassign (known user) → still 200.
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/users/carol/roles/reader", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn assign_unknown_role_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_subject(&SubjectId("carol".into())).await.unwrap();
    let (status, _) = send(
        app(cp),
        req_empty("PUT", "/admin/users/carol/roles/ghost", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn user_roles_unknown_user_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp),
        req_empty("GET", "/admin/users/ghost/roles", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unassign_unknown_user_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    cp.define_role(&RoleId("reader".into())).await.unwrap();
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/users/ghost/roles/reader", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn non_admin_bearer_is_403_on_management_routes() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let alice = seed_session(&cp, "alice").await; // not the admin
    let (status, _) = send(
        app(cp),
        req_empty("GET", "/admin/users/alice/roles", &alice),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn shape_invalid_define_bodies_are_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_types(&cp).await;
    // Well-formed JSON, wrong shape: the handler's serde_json::from_value
    // branch — the decode 400, not axum's syntax 400.
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/links", &token, r#"{"nonsense": true}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("invalid LinkDef"),
        "names the decode failure: {body}"
    );
    let (status, body) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/actions",
            &token,
            r#"{"steps": "not-an-array"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("invalid ActionDef"),
        "names the decode failure: {body}"
    );
}

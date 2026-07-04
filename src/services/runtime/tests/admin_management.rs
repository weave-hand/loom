//! Management admin routes against the in-memory fake: link + action
//! define/delete (idempotent), role delete, grant list/revoke (reflecting
//! grant→revoke), user↔role assign/list/unassign, transform define/get/
//! list/delete + run-now/ad-hoc/history/get-by-id, plus the 404/400 edges and
//! non-admin 403 spot-checks of the shared gate.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{
    ADMIN_ROLE, Acl, Auth, ControlPlane, NewUser, ObjectType, Ontology, RoleId, SubjectId,
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
async fn grant_table_target_roundtrips() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, "root").await;
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // listed with the PolicyTarget serde shape
    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/roles/admin/grants", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let grants = v["grants"].as_array().unwrap();
    assert!(
        grants
            .iter()
            .any(|g| g["target"]["Table"]["schema"] == "main"
                && g["target"]["Table"]["name"] == "widget"),
        "table grant listed: {body}"
    );
    // revoke with the same body shape (idempotent)
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "DELETE",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = send(
        app(cp),
        req_empty("GET", "/admin/roles/admin/grants", &token),
    )
    .await;
    assert!(
        !body.contains("\"Table\""),
        "revoked table grant gone: {body}"
    );
}

#[tokio::test]
async fn grant_requires_exactly_one_target() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // neither
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("exactly one of type or table"), "{body}");
    // both
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read","type":"Widget","table":{"schema":"main","name":"widget"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn grant_type_target_still_works_and_unknown_type_still_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read","type":"Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/roles/admin/grants",
            &token,
            r#"{"action":"read","type":"NoSuchType"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
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

const TRANSFORM_BODY: &str = r#"{
    "name": "daily",
    "body": {"kind": "physical",
             "inputs": [{"schema": "main", "name": "src"}],
             "output": {"schema": "main", "name": "dst"},
             "sql": "select * from src"}
}"#;

#[tokio::test]
async fn define_get_delete_transform() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/transforms/daily", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["body"]["kind"], "physical");
    // list
    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/transforms", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["transforms"].as_array().unwrap().len(), 1);
    // delete twice — idempotent
    let (status, body) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/transforms/daily", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["deleted"], "daily");
    let (status, _) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/transforms/daily", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(app(cp), req_empty("GET", "/admin/transforms/daily", &token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn define_transform_rejects_bad_shapes() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // not a TransformDef
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, r#"{"nope": 1}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // a valid cron schedule is now accepted
    let scheduled = TRANSFORM_BODY.replace(
        r#""name": "daily""#,
        r#""name": "daily", "schedule": "* * * * *""#,
    );
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, &scheduled),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // an invalid cron expression is still a 400
    let bad_cron = TRANSFORM_BODY.replace(
        r#""name": "daily""#,
        r#""name": "daily", "schedule": "not a cron""#,
    );
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, &bad_cron),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // typed body referencing unknown types (deliberately unseeded — the point is
    // the 400, not the ontology lookup)
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/transforms",
            &token,
            r#"{
        "name": "t", "body": {"kind": "typed", "inputs": ["Nope"], "output": "AlsoNope", "sql": "select 1"}
    }"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn scheduled_transform_exposes_next_run_at() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let scheduled = TRANSFORM_BODY.replace(
        r#""name": "daily""#,
        r#""name": "daily", "schedule": "0 3 * * *""#,
    );
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, &scheduled),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/transforms/daily", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["schedule"], "0 3 * * *");
    let nra = v["next_run_at"]
        .as_str()
        .expect("next_run_at present when scheduled");
    assert!(nra.contains('T'), "RFC3339 timestamp: {nra}");
    // Unscheduled defs omit the field entirely.
    let (_, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    let _ = body;
    let (_, body) = send(app(cp), req_empty("GET", "/admin/transforms/daily", &token)).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v.get("next_run_at").is_none(),
        "field omitted when unscheduled"
    );
}

#[tokio::test]
async fn run_now_and_adhoc_submit_runs() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;

    let (status, body) = send(
        app(cp.clone()),
        req_empty("POST", "/admin/transforms/daily/run", &token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let rid = v["run_id"].as_str().unwrap().to_string();

    // run visible: by id, in the transform's history, newest first
    let (status, body) = send(
        app(cp.clone()),
        req_empty("GET", &format!("/admin/runs/{rid}"), &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["state"], "queued");
    assert_eq!(v["trigger"], "manual");
    assert_eq!(v["transform"], "daily");
    let (_, body) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/transforms/daily/runs", &token),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["runs"][0]["run_id"], rid.as_str());

    // the queue job exists and carries the run id
    let job = cp
        .queue()
        .dequeue(&["transform".to_string()], "t")
        .await
        .unwrap()
        .expect("job");
    assert_eq!(job.payload["run_id"], serde_json::json!(rid));

    // ad-hoc: body only, no definition
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/transforms/run",
            &token,
            r#"
        {"kind": "physical", "inputs": [{"schema": "main", "name": "a"}],
         "output": {"schema": "main", "name": "b"}, "sql": "select 1"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let (_, body) = send(
        app(cp.clone()),
        req_empty(
            "GET",
            &format!("/admin/runs/{}", v["run_id"].as_str().unwrap()),
            &token,
        ),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["trigger"], "ad-hoc");
    assert!(v["transform"].is_null());

    // 404s + 400s
    let (status, _) = send(
        app(cp.clone()),
        req_empty("POST", "/admin/transforms/nope/run", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/transforms/nope/runs", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/runs/not-a-uuid", &token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(
        app(cp),
        req_empty(
            "GET",
            &format!("/admin/runs/{}", uuid::Uuid::new_v4()),
            &token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn define_transform_rejects_reserved_name() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let reserved = TRANSFORM_BODY.replace(r#""name": "daily""#, r#""name": "run""#);
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/transforms", &token, &reserved),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn define_data_triggered_transform_is_accepted() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let body = serde_json::json!({
        "name": "dt",
        "body": {"kind": "physical",
                 "inputs": [{"schema": "main", "name": "src"}],
                 "output": {"schema": "main", "name": "dst"},
                 "sql": "select 1"},
        "on_input_commit": true
    });
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, &body.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(app(cp), req_empty("GET", "/admin/transforms/dt", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["on_input_commit"], serde_json::json!(true));
}

#[tokio::test]
async fn define_trigger_cycle_is_rejected_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    let dt_a = serde_json::json!({
        "name": "dt-a",
        "body": {"kind": "physical",
                 "inputs": [{"schema": "main", "name": "src"}],
                 "output": {"schema": "main", "name": "dst"},
                 "sql": "select 1"},
        "on_input_commit": true
    });
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, &dt_a.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let dt_b = serde_json::json!({
        "name": "dt-b",
        "body": {"kind": "physical",
                 "inputs": [{"schema": "main", "name": "dst"}],
                 "output": {"schema": "main", "name": "src"},
                 "sql": "select 1"},
        "on_input_commit": true
    });
    // NOTE: `define_transform_route`'s error arm is a bare
    // `status_for(&e).into_response()` — service_runtime admin routes don't
    // yet surface `ControlPlaneError`'s Display text in the body (tracked as
    // the deliberately-deferred `fut-service-runtime-error-idiom`, unrelated
    // to this slice). So only the status is asserted here; the underlying
    // `ControlPlaneError::Validation` message itself does say "data-trigger
    // cycle among transforms: ..." (see `validate_no_trigger_cycle`), just
    // not wired through to this HTTP response yet.
    let (status, _body) = send(
        app(cp),
        req_json("POST", "/admin/transforms", &token, &dt_b.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn non_admin_bearer_is_403_on_transforms_route() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let alice = seed_session(&cp, "alice").await; // not the admin
    let (status, _) = send(app(cp), req_empty("GET", "/admin/transforms", &alice)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn policy_set_list_clear_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    let body = r#"{
        "action": "read",
        "type": "Widget",
        "row_filter": {"Compare": {"property": "id", "op": "Eq", "value": {"Int": 1}}},
        "deny_columns": ["cost"],
        "mask_columns": ["id"]
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, listed) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/roles/admin/policies", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let pols = v["policies"].as_array().unwrap();
    assert_eq!(pols.len(), 1, "{listed}");
    assert_eq!(pols[0]["action"], "read");
    assert_eq!(pols[0]["target"]["Type"], "Widget");
    assert_eq!(pols[0]["row_filter"]["Compare"]["property"], "id");
    assert_eq!(pols[0]["deny_columns"][0], "cost");
    assert_eq!(pols[0]["mask_columns"][0], "id");
    // clear (idempotent), listing empties
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "DELETE",
            "/admin/roles/admin/policies",
            &token,
            r#"{"action":"read","type":"Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, listed) = send(
        app(cp),
        req_empty("GET", "/admin/roles/admin/policies", &token),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert!(v["policies"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn policy_validation_errors_are_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // row_filter that does not decode as a RowFilter
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/policies",
            &token,
            r#"{"action":"read","type":"Widget","row_filter":{"Bogus":1}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid RowFilter"), "{body}");
    // unknown type target -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/policies",
            &token,
            r#"{"action":"read","type":"NoSuchType"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // row_filter naming an unknown property -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","type":"Widget","row_filter":{"Compare":{"property":"nope","op":"Eq","value":{"Int":1}}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // exactly-one-of-target
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/roles/admin/policies",
            &token,
            r#"{"action":"read"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("exactly one of type or table"), "{body}");
    // unknown role -> 404
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/roles/no-such-role/policies",
            &token,
            r#"{"action":"read","type":"Widget"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn policy_table_target_accepted() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, "root").await;
    // Table targets skip type/property validation by design (deferred existence).
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"},"mask_columns":["name"]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, listed) = send(
        app(cp),
        req_empty("GET", "/admin/roles/admin/policies", &token),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(
        v["policies"][0]["target"]["Table"]["name"], "widget",
        "{listed}"
    );
}

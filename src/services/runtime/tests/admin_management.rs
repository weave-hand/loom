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
    ADMIN_ROLE, Acl, Action, Aggregation, Auth, ControlPlane, NewUser, ObjectType, Ontology,
    PolicyTarget, RoleId, SubjectId, TableRef, TypeName,
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

const TYPED_TRANSFORM_BODY: &str = r#"{
    "name": "typed_daily",
    "body": {"kind": "typed",
             "inputs": ["Src"],
             "output": "Dst",
             "sql": "select 1"}
}"#;

/// A subject in the reserved admin role can read main.dst iff a Table grant exists.
async fn admin_can_read_table(cp: &MemoryControlPlane, schema: &str, name: &str) -> bool {
    // ADMIN is seeded into ADMIN_ROLE by seed_admin_session.
    cp.acl()
        .check(
            &SubjectId(ADMIN.into()),
            Action::Read,
            &PolicyTarget::Table(TableRef {
                schema: schema.into(),
                name: name.into(),
            }),
        )
        .await
        .unwrap()
        == control_plane_core::Decision::Allow
}

#[tokio::test]
async fn physical_define_grants_admin_read_on_output() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    assert!(!admin_can_read_table(&cp, "main", "dst").await);

    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(admin_can_read_table(&cp, "main", "dst").await);

    // Re-define is idempotent: still 201, still granted.
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(admin_can_read_table(&cp, "main", "dst").await);
}

#[tokio::test]
async fn typed_define_grants_no_table() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // Seed the Src/Dst ontology types the typed body references (define validates them).
    for name in ["Src", "Dst"] {
        cp.ontology()
            .define_type(ObjectType {
                name: TypeName(name.into()),
                properties: vec![],
                derived: vec![],
                table: control_plane_core::TableRef {
                    schema: "onto".into(),
                    name: name.to_lowercase(),
                },
                identity: None,
                version: None,
            })
            .await
            .unwrap();
    }
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TYPED_TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // No Table grant was created for a typed output.
    assert!(!admin_can_read_table(&cp, "onto", "dst").await);
}

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

#[tokio::test]
async fn define_model_with_derived_and_constraints_roundtrips() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, "root").await;
    let body = r#"{
        "name": "Order",
        "table": {"schema": "main", "name": "orders"},
        "identity": "id",
        "properties": [
            {"name": "id", "ty": "Int", "required": true},
            {"name": "qty", "ty": "Integer", "constraints": {"range": {"min": 1, "max": 100}}},
            {"name": "status", "ty": "String",
             "constraints": {"length": {"min": 2, "max": 16}, "one_of": ["open", "closed"]}}
        ],
        "derived": [
            {"name": "line_count", "ty": "Int", "link": "lines", "agg": {"kind": "count"}},
            {"name": "total", "ty": "Int", "link": "lines", "agg": {"kind": "sum", "column": "amount"}}
        ]
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // stored intact on the ontology (assert through the control plane)
    let t = cp.get_type(&TypeName("Order".into())).await.unwrap();
    assert_eq!(t.derived.len(), 2);
    assert_eq!(t.derived[0].name, "line_count");
    assert!(matches!(t.derived[0].agg, Aggregation::Count));
    assert!(matches!(t.derived[1].agg, Aggregation::Sum(ref c) if c == "amount"));
    let qty = t.properties.iter().find(|p| p.name == "qty").unwrap();
    assert_eq!(qty.constraints.range.as_ref().unwrap().min, Some(1.0));
    assert_eq!(qty.constraints.range.as_ref().unwrap().max, Some(100.0));
    let status_p = t.properties.iter().find(|p| p.name == "status").unwrap();
    assert_eq!(status_p.constraints.one_of.as_ref().unwrap().len(), 2);
    assert_eq!(status_p.constraints.length.as_ref().unwrap().max, Some(16));
}

async fn seed_vector_type(cp: &MemoryControlPlane) {
    cp.define_type(
        ObjectType::build("Doc", ("main", "doc"))
            .prop("id", "Int")
            .prop("embedding", "vector(3)")
            .done(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn vector_index_define_and_list_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_vector_type(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // defaults: flat + cosine
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"embed_idx","property":"embedding"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // explicit hnsw + l2
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"embed_hnsw","property":"embedding","metric":"l2",
                "spec":{"kind":"hnsw","m":16,"ef_construction":200}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        app(cp),
        req_empty("GET", "/admin/models/Doc/vector-indexes", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let idx = v["indexes"].as_array().unwrap();
    assert_eq!(idx.len(), 2, "{body}");
    let flat = idx.iter().find(|i| i["name"] == "embed_idx").unwrap();
    assert_eq!(flat["metric"], "cosine");
    assert_eq!(flat["spec"]["kind"], "flat");
    let hnsw = idx.iter().find(|i| i["name"] == "embed_hnsw").unwrap();
    assert_eq!(hnsw["metric"], "l2");
    assert_eq!(hnsw["spec"]["kind"], "hnsw");
    assert_eq!(hnsw["spec"]["m"], 16);
    assert_eq!(hnsw["spec"]["ef_construction"], 200);
}

#[tokio::test]
async fn vector_index_errors() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_vector_type(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // unknown metric
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"i","property":"embedding","metric":"dotproduct"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("metric"), "{body}");
    // unknown kind
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"i","property":"embedding","spec":{"kind":"annoy"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // tuning field on the wrong kind
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"i","property":"embedding","spec":{"kind":"hnsw","nlist":10}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // non-vector property -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models/Doc/vector-indexes",
            &token,
            r#"{"name":"i","property":"id"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // unknown type -> adapter NotFound -> 404
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/models/Nope/vector-indexes",
            &token,
            r#"{"name":"i","property":"embedding"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn define_model_agg_and_constraint_errors_are_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, "root").await;
    // unknown agg kind
    let (status, body) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models",
            &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"median"}}]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("unknown agg kind"), "{body}");
    // count with a column
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"count","column":"x"}}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // sum without a column
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/models",
            &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"sum"}}]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // range constraint on a String property -> define-gate Validation -> 400
    let (status, _) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/models",
            &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"s","ty":"String","constraints":{"range":{"min":1}}}]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Seed a live table (schema, columns, one data-file snapshot) into the
/// catalog so `Catalog::current_snapshot` resolves it — satisfies the
/// `/admin/schedules` define-time catalog-existence check for `gc_table` /
/// `compact_table` payloads.
fn seed_table(cp: &MemoryControlPlane, schema: &str, name: &str) {
    cp.seed_catalog(
        &control_plane_core::TableRef {
            schema: schema.into(),
            name: name.into(),
        },
        &[("id".to_string(), "Int".to_string(), false)],
        &[3],
    );
}

#[tokio::test]
async fn schedule_crud_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_table(&cp, "main", "orders");

    let body = r#"{"name":"nightly-gc","kind":"gc_table",
        "payload":{"schema":"main","name":"orders"},"cron":"0 3 * * *"}"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/schedules", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, listed) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/schedules", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let schedules = v["schedules"].as_array().unwrap();
    assert_eq!(schedules.len(), 1, "{listed}");
    assert_eq!(schedules[0]["name"], "nightly-gc");
    assert_eq!(schedules[0]["kind"], "gc_table");
    assert_eq!(schedules[0]["cron"], "0 3 * * *");
    assert_eq!(
        schedules[0]["payload"],
        serde_json::json!({"schema": "main", "name": "orders"})
    );
    let nra = schedules[0]["next_run_at"]
        .as_str()
        .expect("next_run_at present");
    assert!(nra.contains('T'), "RFC3339 timestamp: {nra}");

    let (status, _) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/schedules/nightly-gc", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, listed) = send(
        app(cp.clone()),
        req_empty("GET", "/admin/schedules", &token),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert!(v["schedules"].as_array().unwrap().is_empty());

    // Deleting again is 404 (unlike the other admin delete routes, which are
    // idempotent — schedule delete's `NotFound` is deliberate per Queue::delete_job_schedule).
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/schedules/nightly-gc", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A warehouse-scoped `sweep_orphans` schedule is accepted with an empty payload
/// and NO seeded table — pinning that `schedule_table_check` only gates the
/// table-scoped kinds (`gc_table`/`compact_table`) and lets `sweep_orphans` fall
/// through untouched.
#[tokio::test]
async fn sweep_orphans_schedule_accepted_without_table() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // Deliberately no seed_table(..): a warehouse-scoped kind needs no table.

    let body = r#"{"name":"nightly-sweep","kind":"sweep_orphans",
        "payload":{},"cron":"0 4 * * *"}"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/schedules", &token, body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "sweep_orphans schedule accepted with no table"
    );

    let (status, listed) = send(app(cp), req_empty("GET", "/admin/schedules", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let schedules = v["schedules"].as_array().unwrap();
    assert_eq!(schedules.len(), 1, "{listed}");
    assert_eq!(schedules[0]["kind"], "sweep_orphans");
    assert_eq!(schedules[0]["payload"], serde_json::json!({}));
}

#[tokio::test]
async fn schedule_define_rejects_bad_shapes() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_table(&cp, "main", "orders");

    // invalid cron expression
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/schedules",
            &token,
            r#"{"name":"bad-cron","kind":"gc_table",
                "payload":{"schema":"main","name":"orders"},"cron":"not a cron"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // unschedulable kind
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/schedules",
            &token,
            r#"{"name":"bad-kind","kind":"transform","payload":{},"cron":"0 3 * * *"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // payload missing `name`
    let (status, _) = send(
        app(cp.clone()),
        req_json(
            "POST",
            "/admin/schedules",
            &token,
            r#"{"name":"bad-payload","kind":"gc_table",
                "payload":{"schema":"main"},"cron":"0 3 * * *"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // payload names a table absent from the mirror — the 400 body names it
    let (status, body) = send(
        app(cp),
        req_json(
            "POST",
            "/admin/schedules",
            &token,
            r#"{"name":"bad-table","kind":"gc_table",
                "payload":{"schema":"main","name":"ghost"},"cron":"0 3 * * *"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("ghost"),
        "400 body names the missing table: {body}"
    );
}

#[tokio::test]
async fn schedule_routes_gated() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    // no token -> 401
    let (status, _) = send(
        app(cp.clone()),
        Request::builder()
            .uri("/admin/schedules")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // non-admin bearer -> 403
    let alice = seed_session(&cp, "alice").await;
    let (status, _) = send(app(cp), req_empty("GET", "/admin/schedules", &alice)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Seed a live `main.widget` base table (id + region columns, one data-file
/// snapshot) for the view-definition tests.
fn seed_widget_table(cp: &MemoryControlPlane) {
    cp.seed_catalog(
        &TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        &[
            ("id".to_string(), "Int".to_string(), false),
            ("region".to_string(), "Text".to_string(), false),
        ],
        &[3],
    );
}

const VIEW_BODY: &str = r#"{
    "view": {"schema": "gov", "name": "widget_eu"},
    "base": {"schema": "main", "name": "widget"},
    "predicate": {"Compare": {"property": "region", "op": "Eq", "value": {"Text": "EU"}}},
    "columns": ["id", "region"]
}"#;

/// Admin `POST /admin/views` / `DELETE /admin/views/{schema}/{name}`: define,
/// conflict on redefine, 404 on unknown base, the shared mapper's 400 (NOT
/// 422 — `status_for` maps `Validation` to `BAD_REQUEST`) on a predicate
/// naming an unknown base column, then drop + idempotent-404 on re-drop.
#[tokio::test]
async fn admin_defines_and_drops_a_view() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    seed_widget_table(&cp);
    let view_ref = TableRef {
        schema: "gov".into(),
        name: "widget_eu".into(),
    };

    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/views", &token, VIEW_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(
        cp.catalog().get_view(&view_ref).await.unwrap().is_some(),
        "view defined and resolvable"
    );

    // Re-POST the same view -> Conflict (409).
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/views", &token, VIEW_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Missing base table -> NotFound (404).
    let missing_base_body = r#"{
        "view": {"schema": "gov", "name": "ghost_view"},
        "base": {"schema": "main", "name": "ghost"}
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/views", &token, missing_base_body),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Predicate names an unknown base column -> Validation, mapped by the
    // shared `status_for` to BAD_REQUEST (400), not 422.
    let bad_pred_body = r#"{
        "view": {"schema": "gov", "name": "bad_pred_view"},
        "base": {"schema": "main", "name": "widget"},
        "predicate": {"Compare": {"property": "nope", "op": "Eq", "value": {"Text": "x"}}}
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/views", &token, bad_pred_body),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Drop -> 200, then get_view resolves to None.
    let (status, _) = send(
        app(cp.clone()),
        req_empty("DELETE", "/admin/views/gov/widget_eu", &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(cp.catalog().get_view(&view_ref).await.unwrap().is_none());

    // Drop again -> NotFound (404).
    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/views/gov/widget_eu", &token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn non_admin_bearer_is_403_on_view_routes() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_widget_table(&cp);
    let alice = seed_session(&cp, "alice").await;

    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/views", &alice, VIEW_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = send(
        app(cp),
        req_empty("DELETE", "/admin/views/gov/widget_eu", &alice),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

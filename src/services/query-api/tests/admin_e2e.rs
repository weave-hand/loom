//! Admin-provisioning e2e against the real postgres adapter: the gate (admin vs
//! non-admin vs unauthenticated) and create → login → disable through the stack.
//! Uses a bare `PgControlPlane` (auth + acl only) — no engine/serving needed.
//!
//! `governance_routes_end_to_end` additionally proves the governance surface
//! (`/admin/models`, `/admin/roles`, `/admin/roles/:role/grants`) against a real
//! Iceberg-backed serving engine: the admin is NOT an ACL bypass — a reader can
//! read `Widget` objects only after being granted `Read` on the type.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{
    ADMIN_ROLE, Acl, Action, Auth, ControlPlane, Decision, NewUser, PolicyTarget, Redacted, RoleId,
    SubjectId, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, StubAction};
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use service_runtime::{
    AdminState, AuthState, admin_routes, generate_session_token, hash_password, protect,
    token_sha256,
};
use tower::ServiceExt;

const ADMIN: &str = "root";

fn app(cp: Arc<PgControlPlane>) -> axum::Router {
    let admin = AdminState {
        auth: cp.clone() as Arc<dyn Auth + Send + Sync>,
        cp: cp.clone() as Arc<dyn ControlPlane>,
    };
    let auth = AuthState {
        auth: cp as Arc<dyn Auth + Send + Sync>,
        session_ttl: Duration::from_secs(3600),
        lockout: service_runtime::LockoutPolicy::default(),
    };
    admin_routes(admin, auth)
}

/// The admin routes merged with the read-only object router (behind the same auth
/// gate), so a single app can exercise both governance writes and governed reads.
fn full_app(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
) -> axum::Router {
    let admin = AdminState {
        auth: cp.clone() as Arc<dyn Auth + Send + Sync>,
        cp: cp.clone() as Arc<dyn ControlPlane>,
    };
    let auth = AuthState {
        auth: cp.clone() as Arc<dyn Auth + Send + Sync>,
        session_ttl: Duration::from_secs(3600),
        lockout: service_runtime::LockoutPolicy::default(),
    };
    let qapi = protect(
        router(AppState {
            cp: cp as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
            default_limit: 1000,
            gc_retention: e2e_support::TEST_GC_RETENTION,
            naming: query_api::lineage_filter::local_naming(),
        }),
        auth.clone(),
    );
    qapi.merge(admin_routes(admin, auth))
}

/// Seed a user (subject id == username) and mint a live session token.
async fn seed_session(cp: &PgControlPlane, username: &str) -> String {
    cp.create_user(&NewUser {
        subject_id: SubjectId(username.into()),
        username: username.into(),
        password_phc: Redacted::new(hash_password("pw").expect("hash")),
    })
    .await
    .expect("create_user");
    let token = generate_session_token();
    cp.create_session(
        &SubjectId(username.into()),
        &token_sha256(&token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .expect("create_session");
    token
}

/// Seed a user, grant the reserved admin role, and mint a session token.
async fn seed_admin_session(cp: &PgControlPlane, username: &str) -> String {
    let token = seed_session(cp, username).await;
    let subject = SubjectId(username.into());
    cp.define_subject(&subject).await.expect("define_subject");
    let role = RoleId(ADMIN_ROLE.to_string());
    cp.define_role(&role).await.expect("define_role");
    cp.assign_role(&subject, &role).await.expect("assign_role");
    token
}

async fn status(app: axum::Router, req: Request) -> StatusCode {
    app.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn gate_admin_ok_nonadmin_403_unauth_401() {
    let fx = PgFixture::shared();
    let cp = Arc::new(fx.fresh_control_plane().await);

    // admin session (subject id == "root" matches the gate)
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    assert_eq!(
        status(
            app(cp.clone()),
            Request::builder()
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::OK
    );

    // a different authenticated subject → 403
    let alice_token = seed_session(&cp, "alice").await;
    assert_eq!(
        status(
            app(cp.clone()),
            Request::builder()
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {alice_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::FORBIDDEN
    );

    // unauthenticated → 401
    assert_eq!(
        status(
            app(cp),
            Request::builder()
                .uri("/admin/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn create_with_role_then_disable_blocks_login() {
    let fx = PgFixture::shared();
    let cp = Arc::new(fx.fresh_control_plane().await);
    let admin_token = seed_admin_session(&cp, ADMIN).await;

    // a pre-existing role the create call will assign
    cp.define_role(&RoleId("reader".into())).await.unwrap();

    let res = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"provisioned","password":"pw123","roles":["reader"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["assigned_roles"], serde_json::json!(["reader"]));

    // the provisioned user is active (found for login)
    assert!(
        cp.find_password_credential("provisioned")
            .await
            .unwrap()
            .is_some()
    );

    // disable → login lookup hides the user
    let disabled = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users/provisioned/disable")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    assert!(
        cp.find_password_credential("provisioned")
            .await
            .unwrap()
            .is_none()
    );
}

/// Drive `app` with a request carrying an optional JSON body + bearer token; return
/// (status, parsed JSON body — `Null` if the body is empty/non-JSON).
async fn send_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    body: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"));
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let req = builder
        .body(Body::from(body.unwrap_or("").to_string()))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, v)
}

/// The governance surface end to end: as the admin-role subject, define a `Widget`
/// model over a landed `main.widget` table, declare a `reader` role, grant it `Read`
/// on `Widget`, provision a reader user, and confirm the reader — and only after the
/// grant — can read `Widget` objects. The admin is NOT an ACL bypass: a non-admin
/// gets 403 on every new route (including `GET /admin/roles`), and a grant to an
/// unknown type is rejected with 400.
#[tokio::test]
async fn governance_routes_end_to_end() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // land main.widget(id long, name string): (1,'a'), (2,'b')
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "widget",
            &cols,
            &[SeedCol::Long(vec![1, 2]), SeedCol::Str(vec!["a", "b"])],
        )
        .await;

    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(catalog));

    let cp = Arc::new(cp);
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    let alice_token = seed_session(&cp, "alice").await; // authenticated, not admin

    // POST /admin/models — define Widget over the landed table.
    let define_body = r#"{
        "name": "Widget",
        "table": {"schema": "main", "name": "widget"},
        "identity": "id",
        "properties": [
            {"name": "id", "ty": "Long", "required": true},
            {"name": "name", "ty": "String", "required": false}
        ]
    }"#;
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/models",
        &admin_token,
        Some(define_body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // POST /admin/roles — declare "reader".
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles",
        &admin_token,
        Some(r#"{"role":"reader"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // GET /admin/roles — contains admin + reader.
    let (status, body) = send_json(
        full_app(cp.clone(), eng.clone()),
        "GET",
        "/admin/roles",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let roles: Vec<String> = body["roles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_str().unwrap().to_string())
        .collect();
    assert!(roles.contains(&ADMIN_ROLE.to_string()), "roles: {roles:?}");
    assert!(roles.contains(&"reader".to_string()), "roles: {roles:?}");

    // POST /admin/users — provision a reader-role user, BEFORE the grant exists.
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/users",
        &admin_token,
        Some(r#"{"username":"reader1","password":"pw123","roles":["reader"]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Mint a session for the freshly-provisioned reader (subject id == username).
    let reader_token = generate_session_token();
    cp.create_session(
        &SubjectId("reader1".into()),
        &token_sha256(&reader_token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    // Before the grant, the reader-role subject is denied — the admin role held by
    // `admin_token` above is NOT a bypass for other subjects, and holding the
    // (still ungranted) "reader" role isn't enough on its own.
    let (status, body) = send_json(
        full_app(cp.clone(), eng.clone()),
        "GET",
        "/objects/Widget",
        &reader_token,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "reader read before grant should be 403: {body:?}"
    );

    // POST /admin/roles/reader/grants — grant Read on Widget.
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles/reader/grants",
        &admin_token,
        Some(r#"{"action":"read","type":"Widget"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // The reader can now read Widget objects — governed by the grant just made.
    let (status, body) = send_json(
        full_app(cp.clone(), eng.clone()),
        "GET",
        "/objects/Widget",
        &reader_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reader read after grant: {body:?}");
    assert_eq!(
        body["objects"].as_array().map(Vec::len),
        Some(2),
        "expected 2 landed widget rows: {body:?}"
    );

    // A non-admin gets 403 on every new route, including the list.
    for (method, uri, body) in [
        ("POST", "/admin/models", Some(define_body)),
        ("POST", "/admin/roles", Some(r#"{"role":"other"}"#)),
        ("GET", "/admin/roles", None),
        (
            "POST",
            "/admin/roles/reader/grants",
            Some(r#"{"action":"read","type":"Widget"}"#),
        ),
    ] {
        let (status, _) = send_json(
            full_app(cp.clone(), eng.clone()),
            method,
            uri,
            &alice_token,
            body,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri} by non-admin");
    }

    // A grant to an unknown type is a 400, not a 500.
    let (status, _) = send_json(
        full_app(cp, eng),
        "POST",
        "/admin/roles/reader/grants",
        &admin_token,
        Some(r#"{"action":"read","type":"NoSuchType"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Fine-grained governance authored purely over HTTP: an admin sets a
/// row-filter + column-mask policy via POST /admin/roles/{role}/policies and
/// the governed read path enforces both. Also proves a table-target grant is
/// grantable over HTTP (#361's documented workaround).
#[tokio::test]
async fn policy_authoring_end_to_end() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // land main.gizmo(id long, region string): (1,'emea'), (2,'emea'), (3,'apac')
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), false),
    ];
    writer
        .seed_arrays(
            "main",
            "gizmo",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["emea", "emea", "apac"]),
            ],
        )
        .await;

    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(catalog));

    let cp = Arc::new(cp);
    let admin_token = seed_admin_session(&cp, ADMIN).await;

    // POST /admin/models — define Gizmo over the landed table.
    let define_body = r#"{
        "name": "Gizmo",
        "table": {"schema": "main", "name": "gizmo"},
        "identity": "id",
        "properties": [
            {"name": "id", "ty": "Long", "required": true},
            {"name": "region", "ty": "String", "required": true}
        ]
    }"#;
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/models",
        &admin_token,
        Some(define_body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // POST /admin/roles — declare "reader".
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles",
        &admin_token,
        Some(r#"{"role":"reader"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // POST /admin/users — provision a reader-role user.
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/users",
        &admin_token,
        Some(r#"{"username":"reader1","password":"pw123","roles":["reader"]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Mint a session for the freshly-provisioned reader (subject id == username).
    let reader_token = generate_session_token();
    cp.create_session(
        &SubjectId("reader1".into()),
        &token_sha256(&reader_token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    // POST /admin/roles/reader/grants — coarse Read on Gizmo; the fine-grained
    // policy below narrows it further.
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles/reader/grants",
        &admin_token,
        Some(r#"{"action":"read","type":"Gizmo"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // POST /admin/roles/reader/policies — row filter region == "emea", mask id.
    let policy_body = r#"{
        "action": "read",
        "type": "Gizmo",
        "row_filter": {"Compare": {"property": "region", "op": "Eq", "value": {"Text": "emea"}}},
        "mask_columns": ["id"]
    }"#;
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles/reader/policies",
        &admin_token,
        Some(policy_body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Reader reads Gizmo: only the two "emea" rows come back, and id is masked.
    let (status, body) = send_json(
        full_app(cp.clone(), eng.clone()),
        "GET",
        "/objects/Gizmo",
        &reader_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reader read: {body:?}");
    let objects = body["objects"].as_array().expect("objects array");
    assert_eq!(objects.len(), 2, "expected 2 emea rows: {body:?}");
    for obj in objects {
        assert_eq!(obj["region"], serde_json::json!("emea"), "obj: {obj:?}");
        assert_eq!(
            obj["id"],
            serde_json::json!("***"),
            "id should be masked: {obj:?}"
        );
    }

    // POST /admin/roles/reader/grants — a table-target grant is also grantable
    // over HTTP (#361's documented workaround for pre-authorizing landing tables).
    let (status, _) = send_json(
        full_app(cp.clone(), eng.clone()),
        "POST",
        "/admin/roles/reader/grants",
        &admin_token,
        Some(r#"{"action":"read","table":{"schema":"main","name":"gizmo"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Confirm through the control plane directly: the table-target grant
    // resolves to Allow for the reader subject.
    let decision = cp
        .acl()
        .check(
            &SubjectId("reader1".into()),
            Action::Read,
            &PolicyTarget::Table(TableRef {
                schema: "main".to_string(),
                name: "gizmo".to_string(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(decision, Decision::Allow);

    // GET /admin/roles/reader/grants — lists the table-target grant.
    let (status, body) = send_json(
        full_app(cp.clone(), eng.clone()),
        "GET",
        "/admin/roles/reader/grants",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let grants = body["grants"].as_array().expect("grants array");
    assert!(
        grants
            .iter()
            .any(|g| g["target"]
                == serde_json::json!({"Table": {"schema": "main", "name": "gizmo"}})),
        "expected table-target grant listed: {grants:?}"
    );

    // GET /admin/roles/reader/policies — read-your-writes: the policy from
    // above is listed back over the real adapter.
    let (status, body) = send_json(
        full_app(cp, eng),
        "GET",
        "/admin/roles/reader/policies",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let policies = body["policies"].as_array().expect("policies array");
    assert_eq!(policies.len(), 1, "expected 1 policy: {policies:?}");
    assert_eq!(policies[0]["action"], serde_json::json!("read"));
    assert_eq!(policies[0]["target"], serde_json::json!({"Type": "Gizmo"}));
    assert_eq!(policies[0]["mask_columns"], serde_json::json!(["id"]));
}

/// Admin password reset over real Postgres: the victim's sessions are all revoked
/// and the new password is what verifies afterward.
#[tokio::test]
async fn admin_reset_over_postgres() {
    let fx = PgFixture::shared();
    let cp = Arc::new(fx.fresh_control_plane().await);
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    let victim_token = seed_session(&cp, "victim").await;

    let res = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users/victim/password")
                .header(AUTHORIZATION, format!("Bearer {admin_token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new":"reset-pw"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // victim's sessions revoked; the new password is what verifies afterward.
    assert!(
        cp.resolve_session(
            &token_sha256(&victim_token),
            time::OffsetDateTime::now_utc()
        )
        .await
        .unwrap()
        .is_none()
    );
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

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
use control_plane_core::{ADMIN_ROLE, Acl, Auth, ControlPlane, NewUser, RoleId, SubjectId};
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
        password_phc: hash_password("pw").expect("hash"),
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

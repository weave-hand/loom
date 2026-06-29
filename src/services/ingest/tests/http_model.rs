//! Hermetic `POST /models/{type}` e2e: define an ontology type, POST a conforming
//! Arrow batch as a Write-granted subject through the *protected* router, and prove
//! the rows land into the type's table and read back through the engine serving
//! path. Plus the 422 (non-conforming), 403 (ACL deny), and 403 (unknown type — no
//! existence leak) paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::Router;
use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Auth, Catalog, ControlPlaneError, Effect, NewUser, ObjectType, Ontology,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use http_body_util::BodyExt;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use ingest::http::{AppState, router};
use ingest::landing::IcebergMaterializer;
use service_runtime::{AuthState, generate_session_token, protect, token_sha256};
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::ServiceExt;

/// A 2-row batch matching the `Thing` model: id: Int64 (required), name: Utf8.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

/// Encode a batch as an Arrow IPC stream.
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// Build an Iceberg-backed `AppState` over the fixture db + temp warehouse, returning
/// the concrete `PgControlPlane` (needed for auth/ACL/ontology setup) and the pool.
async fn app_state(
    fx: &PgFixture,
    db: &str,
) -> (Arc<PgControlPlane>, PgPool, tempfile::TempDir, AppState) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().unwrap();
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", wh.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        Duration::from_millis(300),
    ));
    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp: pg.clone(),
    };
    (pg, pool, wh, state)
}

/// Wrap the router with the auth gate, exactly as the binary does.
fn protected(state: AppState, pg: Arc<PgControlPlane>) -> Router {
    protect(
        router(state),
        AuthState {
            auth: pg,
            session_ttl: Duration::from_secs(3600),
        },
    )
}

/// Create the user (ensures the ACL subject exists) and mint a live bearer token —
/// no password flow.
async fn session_token(pg: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    match pg
        .create_user(&NewUser {
            subject_id: SubjectId(subject.into()),
            username: subject.into(),
            password_phc: phc,
        })
        .await
    {
        Ok(()) | Err(ControlPlaneError::Conflict(_)) => {}
        Err(e) => panic!("create_user({subject}): {e}"),
    }
    let token = generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    pg.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}

/// Grant Write + Read on `type_name` to `subject` via a fresh role.
async fn grant_write(pg: &PgControlPlane, subject: &str, type_name: &str) {
    let subj = SubjectId(subject.into());
    let role = RoleId(format!("{subject}-role"));
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    pg.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    pg.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Define a `Thing` model (id: long identity, name: string) over `table`.
async fn define_thing(pg: &PgControlPlane, type_name: &str, table: TableRef) {
    pg.define_type(ObjectType {
        name: TypeName(type_name.into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        derived: vec![],
        table,
        identity: Some("id".into()),
    })
    .await
    .unwrap();
}

/// Drive a `POST /models/{type}` request with a bearer token; return (status, body).
async fn post_model(
    app: Router,
    type_name: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/{type_name}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

fn thing_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "thing".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conforming_land_into_model_round_trips() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    grant_write(&pg, "alice", "Thing").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, json) = post_model(app, "Thing", &token, ipc_bytes(&sample_batch())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["type"], "Thing");
    json["snapshot_id"]
        .as_i64()
        .expect("snapshot_id is an integer");

    // Round-trip: the landed rows are readable through the engine serving path.
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"main\".\"thing\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving read");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "two landed rows are servable as the model");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    assert_eq!(ids.value(0), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_is_422_and_nothing_lands() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    grant_write(&pg, "alice", "Thing").await;
    let token = session_token(&pg, "alice").await;

    // Batch missing the required identity property "id" (only "name").
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();

    let app = protected(state, pg.clone());
    let (status, json) = post_model(app, "Thing", &token, ipc_bytes(&batch)).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["violations"][0]["column"], "id");
    assert_eq!(json["violations"][0]["reason"], "missing_required");

    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&thing_table())
            .await
            .is_err(),
        "a rejected land writes no catalog rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_deny_is_403_and_nothing_lands() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    // `session_token` creates the user (a real ACL subject), so mallory is a valid
    // authenticated principal — but with NO Write grant, so `Acl::check` denies → 403.
    let token = session_token(&pg, "mallory").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model(app, "Thing", &token, ipc_bytes(&sample_batch())).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&thing_table())
            .await
            .is_err(),
        "a denied write lands nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_type_with_grant_is_403_no_leak() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, _pool, _wh, state) = app_state(&fx, &db).await;
    // Grant Write on a type that is never defined.
    grant_write(&pg, "alice", "Ghost").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model(app, "Ghost", &token, ipc_bytes(&sample_batch())).await;

    // get_type NotFound after the Write grant resolves to 403, not a 404 — no leak.
    assert_eq!(status, StatusCode::FORBIDDEN);
}

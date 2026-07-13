//! Hermetic `POST /models/{type}` e2e: define an ontology type, POST a conforming
//! Arrow batch as a Write-granted subject through the *protected* router, and prove
//! the rows land into the type's table and read back through the engine serving
//! path. Plus the 422 (non-conforming), 403 (ACL deny), 403 (unknown type — no
//! existence leak), and the infer-and-create paths.

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
        pool: pool.clone(),
        compact_small_file_bytes: 1 << 20,
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
            lockout: service_runtime::LockoutPolicy::default(),
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table,
        identity: Some("id".into()),
        version: None,
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
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
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
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
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
async fn constraint_violation_is_422_and_nothing_lands() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    // A `CThing(id long identity, code string [^[A-Z]+$])`.
    let ctable = TableRef {
        schema: "main".into(),
        name: "cthing".into(),
    };
    pg.define_type(ObjectType {
        name: TypeName("CThing".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "code".into(),
                ty: "string".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints {
                    pattern: Some("^[A-Z]+$".into()),
                    ..control_plane_core::PropertyConstraints::default()
                },
            },
        ],
        derived: vec![],
        table: ctable.clone(),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();
    grant_write(&pg, "alice", "CThing").await;
    let token = session_token(&pg, "alice").await;

    // A schema-conforming batch whose `code` value "ab" violates the `^[A-Z]+$` pattern.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["ab"])),
        ],
    )
    .unwrap();

    let app = protected(state, pg.clone());
    let (status, json) = post_model(app, "CThing", &token, ipc_bytes(&batch)).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {json}");
    assert_eq!(json["violations"][0]["column"], "code");
    assert_eq!(json["violations"][0]["reason"], "constraint");
    assert_eq!(json["violations"][0]["rule"], "pattern");

    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&ctable)
            .await
            .is_err(),
        "a constraint-rejected land writes no catalog rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_deny_is_403_and_nothing_lands() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
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
async fn unknown_type_is_403_no_leak() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, _pool, _wh, state) = app_state(fx, &db).await;
    // alice is a legitimately Write-granted user — but on an *existing* type. (ACL
    // grants validate the target type exists, so you cannot grant on a non-existent
    // type; the no-leak guarantee therefore comes from the coarse gate returning
    // Deny for any type alice lacks a grant on, never from a 404.)
    define_thing(&pg, "Thing", thing_table()).await;
    grant_write(&pg, "alice", "Thing").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    // alice posts to a type that does not exist. She holds Write elsewhere, yet the
    // response is 403 — indistinguishable from "type exists but forbidden", never a
    // 404 that would reveal the type's (non-)existence.
    let (status, _json) = post_model(app, "Ghost", &token, ipc_bytes(&sample_batch())).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Seed a Write grant on a type that does NOT exist yet (the public `grant` API
/// validates type existence, which the infer-and-create flow must precede). Inserts the
/// `acl.role_grant` row directly, mirroring the adapter's `(kind,a,b)`/action/effect
/// encoding. Standing in for the deferred ontology-authoring capability.
async fn grant_write_absent_type(
    pg: &PgControlPlane,
    pool: &PgPool,
    subject: &str,
    type_name: &str,
) {
    let subj = SubjectId(subject.into());
    let role = RoleId(format!("{subject}-role"));
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    sqlx::query(
        "insert into acl.role_grant (role_id, action, target_kind, target_a, target_b, effect) \
         values ($1, 'write', 'type', $2, '', 'allow') \
         on conflict (role_id, action, target_kind, target_a, target_b) do nothing",
    )
    .bind(&role.0)
    .bind(type_name)
    .execute(pool)
    .await
    .expect("seed grant on absent type");
}

/// Like `post_model` but appends a raw query string (e.g. "identity=id").
async fn post_model_q(
    app: Router,
    type_name: &str,
    query: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    let uri = if query.is_empty() {
        format!("/models/{type_name}")
    } else {
        format!("/models/{type_name}?{query}")
    };
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
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

#[tokio::test(flavor = "multi_thread")]
async fn infer_and_create_lands_and_records_the_type() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    // Absent type "gadget"; alice is Write-granted on it via the direct seed.
    grant_write_absent_type(&pg, &pool, "alice", "gadget").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, json) = post_model_q(app, "gadget", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "authorized POST to an absent type infers + creates"
    );
    assert_eq!(json["type"], "gadget");
    json["snapshot_id"]
        .as_i64()
        .expect("snapshot_id is an integer");

    // The inferred ObjectType matches the batch schema (names, logical types, nullability).
    let ot = pg
        .get_type(&TypeName("gadget".into()))
        .await
        .expect("type created");
    let props: Vec<(&str, &str, bool)> = ot
        .properties
        .iter()
        .map(|p| (p.name.as_str(), p.ty.as_str(), p.required))
        .collect();
    assert_eq!(props, vec![("id", "long", true), ("name", "string", false)]);
    assert_eq!(ot.identity, None);
    assert_eq!(
        ot.table,
        TableRef {
            schema: "main".into(),
            name: "gadget".into()
        }
    );

    // The rows landed and are servable through the engine path.
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"main\".\"gadget\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving read");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_query_param_is_honored() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "keyed").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model_q(
        app,
        "keyed",
        "identity=id",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let ot = pg
        .get_type(&TypeName("keyed".into()))
        .await
        .expect("type created");
    assert_eq!(ot.identity, Some("id".into()), "declared identity recorded");
    let id = ot
        .properties
        .iter()
        .find(|p| p.name == "id")
        .expect("id prop");
    assert!(id.required, "identity property is forced required");

    // The identity value addresses a row (the column carries addressable values).
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"name\" FROM \"main\".\"keyed\" WHERE \"id\" = 1",
        None,
    )
    .await
    .expect("serving read by identity");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "identity value round-trips");
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_naming_absent_column_is_rejected_and_nothing_created() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "badid").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model_q(
        app,
        "badid",
        "identity=nope",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a ?identity naming an absent column is a deterministic 400"
    );
    assert!(
        matches!(
            pg.get_type(&TypeName("badid".into())).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "nothing is created on a bad identity"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn re_post_conforms_then_rejects_a_differing_batch() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "again").await;
    let token = session_token(&pg, "alice").await;

    // First POST infers + creates.
    let app = protected(state.clone(), pg.clone());
    let (s1, _) = post_model_q(app, "again", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(s1, StatusCode::OK);

    // Second POST that CONFORMS to the now-existing type -> 200 (hits the slice-1 path).
    let app = protected(state.clone(), pg.clone());
    let (s2, _) = post_model_q(app, "again", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "a conforming re-post lands via slice-1 conform"
    );

    // Second POST that DIFFERS (missing the required "id") -> 422, nothing new lands.
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let differing =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["z"]))]).unwrap();
    let app = protected(state, pg.clone());
    let (s3, json) = post_model_q(app, "again", "", &token, ipc_bytes(&differing)).await;
    assert_eq!(
        s3,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a differing batch is a conformance failure"
    );
    assert_eq!(json["violations"][0]["column"], "id");

    // The type is unchanged (still the inferred shape).
    let ot = pg
        .get_type(&TypeName("again".into()))
        .await
        .expect("type still there");
    assert_eq!(ot.properties.len(), 2);

    let _ = pool; // keep the fixture db alive for the duration of the test
}

#[tokio::test(flavor = "multi_thread")]
async fn unmappable_column_is_422_and_nothing_created() {
    use arrow::array::Date32Array;
    use arrow::datatypes::DataType;

    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "evt").await;
    let token = session_token(&pg, "alice").await;

    // "when" is Date32 — no loom logical mapping.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("when", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Date32Array::from(vec![0])),
        ],
    )
    .unwrap();

    let app = protected(state, pg.clone());
    let (status, json) = post_model_q(app, "evt", "", &token, ipc_bytes(&batch)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["violations"][0]["column"], "when");
    assert_eq!(json["violations"][0]["reason"], "unsupported");
    assert!(
        matches!(
            pg.get_type(&TypeName("evt".into())).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "an unmappable batch creates nothing"
    );
}

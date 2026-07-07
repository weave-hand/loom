//! `?mode=cdc&buckets=N` on `POST /models/{type}`: declares an identity-bearing
//! type's table as a PK/CDC stream table (a `kind='cdc'` `stream.stream_table` row
//! keyed on the type's identity), in the SAME transaction as the first write. A
//! type with no declared identity, or a `mode=cdc` request against a table already
//! landed as batch, is rejected with 400. Hermetic Postgres fixture; tower
//! oneshot, no socket. Mirrors `tests/stream_declare.rs`'s raw-SQL readback
//! harness and `tests/http_model.rs`'s auth/ACL harness.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::Router;
use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use control_plane_core::{Acl, Auth, NewUser, RoleId, SubjectId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
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

/// A 2-row batch: id: Int64 (required), name: Utf8 (nullable).
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
/// the concrete `PgControlPlane` (needed for auth/ACL setup) and the pool.
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
            lockout: service_runtime::LockoutPolicy::default(),
        },
    )
}

/// Create the user (ensures the ACL subject exists) and mint a live bearer token —
/// no password flow.
async fn session_token(pg: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    pg.create_user(&NewUser {
        subject_id: SubjectId(subject.into()),
        username: subject.into(),
        password_phc: phc,
    })
    .await
    .expect("create_user");
    let token = generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    pg.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}

/// Seed a Write grant on a type that does NOT exist yet (the public `grant` API
/// validates type existence, which the infer-and-create flow must precede). Mirrors
/// `tests/http_model.rs`'s `grant_write_absent_type`.
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

/// Drive a `POST /models/{type}?{query}` request with a bearer token; return the
/// response status (body is checked only where the test needs it).
async fn post_model_q(
    app: Router,
    type_name: &str,
    query: &str,
    token: &str,
    body: Vec<u8>,
) -> StatusCode {
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
    // Drain the body so the connection/task tears down cleanly even for the 400s
    // whose plain-text body this test doesn't otherwise inspect.
    let _ = res.into_body().collect().await.unwrap().to_bytes();
    status
}

/// The declared stream metadata for `schema.table` — `(bucket_count, kind,
/// bucket_key)` — or `None` if it has no `stream.stream_table` row (i.e. it is a
/// batch table, or does not exist). Mirrors `tests/stream_declare.rs`'s
/// `stream_bucket_count`, extended to also read `kind`/`bucket_key` so this test
/// can assert the CDC-specific fields `pg_declare_cdc` writes.
async fn stream_meta_row(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Option<(i32, String, Option<String>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select st.bucket_count, st.kind, st.bucket_key from stream.stream_table st \
         join iceberg_mirror.table t on t.table_id = st.table_id \
         where t.table_namespace = '{schema}' and t.table_name = '{table}' \
         and t.end_snapshot is null"
    )))
    .fetch_optional(pool)
    .await
    .expect("stream_table lookup")
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_with_identity_declares_a_cdc_table() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "widget").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let status = post_model_q(
        app,
        "widget",
        "identity=id&mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "mode=cdc on an identity-bearing type lands"
    );

    let meta = stream_meta_row(&pool, "main", "widget")
        .await
        .expect("a stream_table row was declared");
    assert_eq!(meta, (2, "cdc".to_string(), Some("id".to_string())));
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_without_identity_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "gizmo").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    // No `identity=` — the inferred type has no declared identity, so `mode=cdc`
    // must be rejected before any write.
    let status = post_model_q(
        app,
        "gizmo",
        "mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=cdc without a declared identity is rejected"
    );
    assert_eq!(
        stream_meta_row(&pool, "main", "gizmo").await,
        None,
        "the rejected declare leaves no stream_table row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_on_existing_batch_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "doohickey").await;
    let token = session_token(&pg, "alice").await;

    // First write: declares the identity but no stream intent -> lands as a plain
    // batch table (no stream_table row).
    let app = protected(state.clone(), pg.clone());
    let status = post_model_q(
        app,
        "doohickey",
        "identity=id",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stream_meta_row(&pool, "main", "doohickey").await, None);

    // Second write, now requesting mode=cdc against the already-landed batch table
    // -> rejected (cannot convert an existing batch table to a stream table).
    let app = protected(state, pg.clone());
    let status = post_model_q(
        app,
        "doohickey",
        "mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=cdc against an existing batch table is rejected"
    );
    assert_eq!(
        stream_meta_row(&pool, "main", "doohickey").await,
        None,
        "the rejected conversion attempt leaves the table as batch"
    );
}

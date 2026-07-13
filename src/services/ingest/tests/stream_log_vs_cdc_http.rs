//! Cross-surface repro for iss-stream-log-vs-cdc-declare: declare a CDC table
//! via POST /models/{type}?mode=cdc, then attempt a matching-count
//! POST /datasets/{schema}/{table}?mode=stream against the SAME physical table.
//! Must 400 with the kind-mismatch message; the registry row stays kind='cdc'.
//! Hermetic Postgres fixture; tower oneshot, no socket. Mirrors
//! `tests/model_cdc_declare.rs`'s auth + raw-SQL readback harness.

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

/// POST /datasets/{schema}/{table}?{query} with a bearer token; returns
/// (status, body-text) so the kind-mismatch message can be asserted.
async fn post_dataset_q(
    app: Router,
    schema: &str,
    table: &str,
    query: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, String) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/datasets/{schema}/{table}?{query}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_stream_matching_count_against_cdc_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "widget").await;
    let token = session_token(&pg, "alice").await;

    // Surface 1: declare main.widget as CDC (buckets=2) via the models path.
    let app = protected(state.clone(), pg.clone());
    let status = post_model_q(
        app,
        "widget",
        "identity=id&mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mode=cdc declare lands");
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string())))
    );

    // Surface 2: the SAME physical table via the datasets path, mode=stream
    // with the MATCHING bucket count — the silently-accepted case before the
    // fix. Must 400 with the kind-mismatch message.
    let app = protected(state, pg.clone());
    let (status, body) = post_dataset_q(
        app,
        "main",
        "widget",
        "mode=stream&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=stream against a cdc table is rejected (body: {body})"
    );
    assert!(
        body.contains("different stream kind"),
        "the kind-mismatch message is echoed to the client, got: {body}"
    );

    // The rejected write changed nothing: still kind='cdc', count 2, key id.
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string()))),
        "the rejected log declare leaves the cdc registry row untouched"
    );
}

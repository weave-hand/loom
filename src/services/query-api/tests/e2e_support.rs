#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "test/fixture harness code, not a production path"
)]
//! Shared test-support helpers for query-api end-to-end tests.
//!
//! Provides the common `tref` / `setup_iceberg` / `ids` functions used by
//! `multi_hop_traversal_e2e` and `inverse_hops_e2e`.  Direction-specific
//! helpers (`inv`, `hopf`, `srcf`, …) stay local to their respective test
//! files.
//!
//! Also provides the shared HTTP/ACL harness used by the `*_e2e` route tests:
//! `prop` (a `PropertyDef` constructor), the no-op `StubAction` write engine,
//! `subject_with_role` / `grant_read` (ACL setup), `get` (drive the axum
//! router via a oneshot request), and `ids_i64` (parse an `{objects:[…]}`
//! body's `id`s as sorted `i64`s).

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Auth, Cardinality, ControlPlane, ControlPlaneError, Effect, LinkBacking, LinkDef,
    NewUser, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef,
    TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::execute_query;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::render::objects_to_json;
use query_api::serving::{ActionEngine, ServingError, SqlValue, inline_params};
use query_api::serving_datafusion::batches_to_rows;
use query_api::sql::DataFusionDialect;
use service_runtime::{AuthState, protect, token_sha256};
use time::OffsetDateTime;
use tower::ServiceExt;

/// Convenience constructor for a `TableRef`.
pub fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

pub fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// No-op atomic write engine: the read-only graph route never touches it, but
/// `AppState` requires one.
pub struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(0))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

/// In-process `ServingEngine` backed by `engine_serving::execute_query` for SQL reads
/// and optionally `engine_serving::vector_search` for kNN queries. Used by tests that
/// need a `dyn ServingEngine` without a gRPC hop.
///
/// Construct with `new(catalog)` for SQL-only use, or `new_with_search(catalog, pool,
/// sql_catalog)` when vector-search capability is also needed.
pub struct InProcessServingEngine {
    catalog: IcebergCatalog,
    /// Present only when the engine was constructed with vector-search capability.
    search: Option<(
        sqlx::PgPool,
        control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    )>,
}

impl InProcessServingEngine {
    /// Construct a SQL-only engine (the default for existing tests).
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self {
            catalog,
            search: None,
        }
    }

    /// Construct an engine with full vector-search capability in addition to SQL reads.
    pub fn new_with_search(
        catalog: IcebergCatalog,
        pool: sqlx::PgPool,
        sql_catalog: control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    ) -> Self {
        Self {
            catalog,
            search: Some((pool, sql_catalog)),
        }
    }
}

#[async_trait]
impl query_api::serving::ServingEngine for InProcessServingEngine {
    async fn fetch_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<query_api::serving::Rows, ServingError> {
        let inlined = inline_params(sql, params);
        let batches = execute_query(&self.catalog, &inlined, None)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(batches_to_rows(batches))
    }

    async fn vector_search(
        &self,
        table: &control_plane_core::TableRef,
        index_name: &str,
        query: &[f32],
        k: usize,
        nprobe: Option<u32>,
        ef_search: Option<u32>,
    ) -> Result<query_api::serving::Rows, ServingError> {
        let (pool, sql_catalog) = self.search.as_ref().ok_or_else(|| {
            ServingError::Engine("vector search not configured for this engine".to_string())
        })?;
        let batch = engine_serving::vector_search(
            sql_catalog,
            pool,
            table,
            index_name,
            query,
            k,
            nprobe,
            ef_search,
        )
        .await
        .map_err(|e| match e {
            engine_serving::EngineServingError::NoIndex(m) => ServingError::NoIndex(m),
            engine_serving::EngineServingError::DimMismatch(m) => ServingError::DimMismatch(m),
            other => ServingError::Engine(other.to_string()),
        })?;
        Ok(batches_to_rows(vec![batch]))
    }

    fn dialect(&self) -> &'static dyn query_api::sql::SqlDialect {
        &DataFusionDialect
    }
}

pub async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

pub async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Ensure `subject` has an auth.user (the session FK target) and mint a live
/// session token for it. Lets the existing e2e tests authenticate without
/// driving the password flow.
pub async fn session_token(cp: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    match cp
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
    let token = service_runtime::generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    cp.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}

/// Drive the HTTP router (behind the auth gate) and return (status, parsed JSON body).
pub async fn get(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let token = session_token(&cp, subject).await;
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
            default_limit: 1000,
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
        },
    );
    let res = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
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

/// Collect the sorted set of `id`s from an {"objects":[...]} reachability body. `id` is a
/// `Long`, rendered as a numeric STRING (int64 exceeds JSON's safe-integer range), so parse.
pub fn ids_i64(body: &serde_json::Value) -> Vec<i64> {
    let mut out: Vec<i64> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().parse::<i64>().unwrap())
        .collect();
    out.sort_unstable();
    out
}

/// Seed the customer→orders→line_items chain, define the three
/// ontology types and two FK links, and serve via the in-process Iceberg/DataFusion engine.
///
/// Returns `(cp, Arc<dyn ServingEngine>, IcebergWriter)`. The caller **must** keep the
/// returned `IcebergWriter` alive — its `TempDir` holds the Parquet warehouse; dropping it
/// removes the files from under the serving engine.
pub async fn setup_iceberg(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    Arc<dyn query_api::serving::ServingEngine>,
    IcebergWriter,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);

    // customer(id, region): (1,'CA'), (2,'NY')
    let cust_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "customer",
            &cust_cols,
            &[SeedCol::Long(vec![1, 2]), SeedCol::Str(vec!["CA", "NY"])],
        )
        .await;

    // orders(id, customer_id, status): (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped')
    let ord_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("customer_id".to_string(), "long".to_string(), false),
        ("status".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &ord_cols,
            &[
                SeedCol::Long(vec![10, 11, 20]),
                SeedCol::Long(vec![1, 1, 2]),
                SeedCol::Str(vec!["shipped", "pending", "shipped"]),
            ],
        )
        .await;

    // line_items(id, order_id, sku): (100,10,'A'),(101,10,'B'),(102,11,'C'),(200,20,'D')
    let li_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("order_id".to_string(), "long".to_string(), false),
        ("sku".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "line_items",
            &li_cols,
            &[
                SeedCol::Long(vec![100, 101, 102, 200]),
                SeedCol::Long(vec![10, 10, 11, 20]),
                SeedCol::Str(vec!["A", "B", "C", "D"]),
            ],
        )
        .await;

    let cust = tref("main", "customer");
    let ord = tref("main", "orders");
    let li = tref("main", "line_items");

    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "region".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: cust.clone(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "customer_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("LineItem".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "order_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: li.clone(),
        identity: None,
    })
    .await
    .unwrap();

    cp.define_link(LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Order".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    })
    .await
    .unwrap();

    let sql_catalog = writer.sql_catalog().await;
    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new_with_search(catalog, pool.clone(), sql_catalog),
    );
    (cp, eng, writer)
}

/// Extract sorted `id` values from a served object set.
///
/// loom renders a `Long` property as a JSON *string* (not a number), so `id`
/// is read via `as_str()`.
pub fn ids(rows: &query_api::handler::ObjectRows) -> Vec<String> {
    let body = objects_to_json(rows);
    let mut out: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    out.sort_unstable();
    out
}

/// A spawned in-process HTTP server bound to an ephemeral `127.0.0.1` port.
/// Holds the serving task; dropping the guard aborts the server, so the caller
/// must keep it alive for the duration of the test.
pub struct ServeGuard(tokio::task::JoinHandle<()>);

impl Drop for ServeGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bind an ephemeral local port, spawn `service_runtime::serve` on it (the real
/// `TcpListener` bind that the binaries use), and wait until the listener
/// accepts connections before returning.
///
/// Returns the base URL (no trailing slash) and a guard that keeps the serving
/// task alive. A short probe-bind discovers a free port, which `serve` then
/// re-claims; the readiness poll below closes the (tiny) re-bind race.
pub async fn spawn_http(router: axum::Router) -> (String, ServeGuard) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe-bind ephemeral port");
    let addr = probe.local_addr().expect("probe local_addr");
    drop(probe);

    let handle = tokio::spawn(async move {
        let _ = service_runtime::serve(addr, router).await;
    });

    // Readiness: poll-connect until the server accepts (or give up clearly).
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return (format!("http://{addr}"), ServeGuard(handle));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("spawn_http: server never became ready at {addr}");
}

//! Shared test-support helpers for query-api end-to-end tests.
//!
//! Provides the common `tref` / `land` / `setup` / `ids` functions used by
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

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Auth, Cardinality, ControlPlane, ControlPlaneError, DatasetRef, Effect, EventType,
    LineageEvent, LinkBacking, LinkDef, NewUser, ObjectType, Ontology, PolicyTarget, PropertyDef,
    RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::execute_query;
use http_body_util::BodyExt;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::http::{AppState, router};
use query_api::render::objects_to_json;
use query_api::serving::{ActionEngine, ServingError, SqlValue, inline_params};
use query_api::serving_datafusion::batches_to_rows;
use query_api::sql::DataFusionDialect;
use service_runtime::{AuthState, protect, token_sha256};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

pub use query_api::serving::EmbeddedDuckDb;

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
}

/// In-process `ServingEngine` backed by `engine_serving::execute_query`. Used by
/// tests that need a `dyn ServingEngine` over an `IcebergCatalog` without a gRPC hop.
pub struct InProcessServingEngine {
    catalog: IcebergCatalog,
}

impl InProcessServingEngine {
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self { catalog }
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

/// Materialize a single `RecordBatch` into the DuckLake-backed control plane.
pub async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Seed three tables forming the FK chain `customer -> orders -> line_items`,
/// define the three ontology types and the two FK links (`orders`, `lineItems`),
/// and attach the serving engine.
///
/// The caller **must** keep the returned `DuckLakeWriter` alive for the duration
/// of the test — its `TempDir` holds the Parquet files that DuckDB reads;
/// dropping it removes them out from under the engine.
pub async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(id, customer_id, status): (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // line_items(id, order_id, sku): (100,10,'A'),(101,10,'B'),(102,11,'C'),(200,20,'D')
    let li = tref("main", "line_items");
    let li_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
    ]));
    let li_batch = RecordBatch::try_new(
        li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 200])),
            Arc::new(Int64Array::from(vec![10, 10, 11, 20])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &li, li_schema, li_batch).await;

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

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
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

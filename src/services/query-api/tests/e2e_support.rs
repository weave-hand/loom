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

use arrow_array::RecordBatch;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, Auth, Cardinality, ColumnSpec, ControlPlane,
    ControlPlaneError, DatasetId, Effect, EventType, IndexSpec, LineageEvent, LinkBacking, LinkDef,
    Metric, NewUser, ObjectType, Ontology, ParamDef, Policy, PolicyTarget, PropertyDef, RoleId,
    RowFilter, RunId, SubjectId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::vector_index::build_vector_index;
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
        constraints: control_plane_core::PropertyConstraints::default(),
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

/// A `ServingEngine` that has no data backend — every data read errors. The lineage
/// read routes never touch the serving engine, so tests of those routes wire this
/// in to satisfy `AppState` without standing up an Iceberg warehouse.
pub struct NoServing;

#[async_trait]
impl query_api::serving::ServingEngine for NoServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> Result<query_api::serving::Rows, ServingError> {
        Err(ServingError::Engine("no serving engine configured".into()))
    }

    async fn vector_search(
        &self,
        _table: &TableRef,
        _index_name: &str,
        _query: &[f32],
        _k: usize,
        _nprobe: Option<u32>,
        _ef_search: Option<u32>,
    ) -> Result<query_api::serving::Rows, ServingError> {
        Err(ServingError::Engine("no serving engine configured".into()))
    }

    fn dialect(&self) -> &'static dyn query_api::sql::SqlDialect {
        &DataFusionDialect
    }
}

/// Drive the HTTP router (behind the auth gate) with NO Authorization header and
/// return just the status — for asserting the 401 on unauthenticated requests.
pub async fn get_unauth(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
) -> StatusCode {
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
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    res.status()
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

/// Read an identity value that may render as a JSON number OR a numeric string (`Long`
/// identities render as numeric strings — see `ids_i64`). `None` for JSON `null`.
fn as_id(v: &serde_json::Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

/// Extract `(id, depth, parent)` triples from a `{ "roots": [...], "nodes": [...] }`
/// shortest-path-tree body, in node order. `parent` is `None` for a root (JSON `null`).
/// Ids parse via `as_id` (Long ids are numeric strings); `depth` is a JSON number.
pub fn tree_nodes(body: &serde_json::Value) -> Vec<(i64, i64, Option<i64>)> {
    body["nodes"]
        .as_array()
        .map(|ns| {
            ns.iter()
                .map(|n| {
                    let id = as_id(&n["id"]).expect("node id");
                    let depth = n["depth"].as_i64().expect("node depth i64");
                    let parent = as_id(&n["parent"]);
                    (id, depth, parent)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The tree's root identity values as i64s, in response order.
pub fn tree_roots(body: &serde_json::Value) -> Vec<i64> {
    body["roots"]
        .as_array()
        .map(|rs| rs.iter().filter_map(as_id).collect())
        .unwrap_or_default()
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "region".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "customer_id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "order_id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
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

/// Grant a role `Read` on `type_name` and attach a row-filter post-filter policy.
/// Mirrors `grant_read` (the coarse Allow) and then refines it with a `RowFilter`,
/// matching the pattern the graph/object-set row-filter e2e tests use.
pub async fn grant_read_filtered(
    cp: &PgControlPlane,
    role: &RoleId,
    type_name: &str,
    filter: RowFilter,
) {
    grant_read(cp, role, type_name).await;
    cp.set_policy(
        role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName(type_name.into())),
            row_filter: Some(filter),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
}

/// Coarse `Read` Allow plus a column policy with the given denied/masked columns and
/// no row filter — the shape that governs the identity column. Mirrors
/// `grant_read_filtered` but exercises column governance instead of a row filter.
pub async fn grant_read_columns(
    cp: &PgControlPlane,
    role: &RoleId,
    type_name: &str,
    deny_columns: Vec<String>,
    mask_columns: Vec<String>,
) {
    grant_read(cp, role, type_name).await;
    cp.set_policy(
        role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName(type_name.into())),
            row_filter: None,
            deny_columns,
            mask_columns,
        },
    )
    .await
    .unwrap();
}

/// The `Docs` vector-type columns: `id: long` + `embedding: vector(4)`.
fn vector_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// Build an Arrow IPC body with `id: long` + `embedding: list<float32>` (4 elements).
/// Copied from `engine-serving/tests/vector_search.rs::ipc_body` (the canonical recipe).
fn vector_ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = arrow_array::Int64Array::from(ids);
    let emb_array = lb.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(emb_array)],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn vector_lineage_evt(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "e2e" }),
    }
}

/// Seed a `Docs(id Long identity, embedding vector(4))` type, land 4 orthogonal cold
/// rows (forced to Parquet via `inline_byte_limit = 0`), declare a Flat/Cosine index
/// named `by_sim`, and build it — returning a router-ready serving engine whose
/// `vector_search` resolves `by_sim`.
///
/// Reuses `IcebergWriter::sql_catalog()` (same pg DSN + warehouse the rows land into)
/// for both the build path and the engine's search path. The returned `IcebergWriter`
/// **must** be kept alive: its `TempDir` holds the Parquet warehouse; dropping it
/// removes the files from under the serving engine.
pub async fn seed_vector_type(
    fx: &PgFixture,
    db: &str,
) -> (
    PgControlPlane,
    Arc<dyn query_api::serving::ServingEngine>,
    IcebergWriter,
) {
    let pool = fx.pool_for(db).await;
    let dsn = fx.pg_dsn(db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cp = PgControlPlane::new(pool.clone(), std::time::Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Register the object type with the ontology (identity = "id").
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type Docs");

    // Land 4 orthogonal cold rows in two batches (Parquet: inline_byte_limit = 0).
    let build_catalog = writer.sql_catalog().await;
    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &build_catalog,
        &table,
        &vector_columns(),
        &vector_ipc_body(rows_1_2),
        0,
        i64::MAX,
        vector_lineage_evt(run, &table),
    )
    .await
    .expect("land rows 1-2");
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    land(
        &pool,
        &build_catalog,
        &table,
        &vector_columns(),
        &vector_ipc_body(rows_3_4),
        0,
        i64::MAX,
        vector_lineage_evt(run, &table),
    )
    .await
    .expect("land rows 3-4");

    // Declare the named Flat/Cosine index and build it.
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_sim".into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index by_sim");
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(&build_catalog, &pool, &table, "by_sim", build_run)
        .await
        .expect("build_vector_index by_sim");

    // Wire the in-process serving engine: IcebergCatalog for SQL reads (row-filter
    // post-filter), a fresh SqlCatalog over the SAME warehouse for vector search.
    let search_catalog = writer.sql_catalog().await;
    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new_with_search(catalog, pool.clone(), search_catalog),
    );
    (cp, eng, writer)
}

/// Define `Widget(id Long required identity, name String, qty Long)` + the
/// `createWidget` (insert), `updateWidget` (id + qty), and `deleteWidget` (id) actions.
/// Promoted from `update_delete_tiers_e2e.rs` so wire-client tests can reuse it.
pub async fn define_widget(cp: &PgControlPlane) -> TypeName {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
                ParamDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    binds: None,
                },
                ParamDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: false,
                    binds: None,
                },
            ],
            kind: ActionKind::Insert,
            assignments: vec![],
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("updateWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
                ParamDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
            ],
            kind: ActionKind::Update,
            assignments: vec![],
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("deleteWidget".into()),
            target: widget.clone(),
            parameters: vec![ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            }],
            kind: ActionKind::Delete,
            assignments: vec![],
        })
        .await
        .unwrap();
    widget
}

/// Grant `Write` + `Read` on `widget` to a fresh `writer` subject (role `writers`).
/// Promoted from `update_delete_tiers_e2e.rs`.
pub async fn grant_writer(cp: &PgControlPlane, widget: &TypeName) -> SubjectId {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(widget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(widget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    subj
}

/// Keeps the spawned engine server and its socket dir alive for the test's lifetime.
pub struct EngineGuard {
    _sock_dir: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for EngineGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn an `EngineControlService` on a UDS and return its socket path + keep-alive guard.
/// The engine connects to the same Postgres + warehouse the test uses.
pub async fn spawn_engine(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (String, EngineGuard) {
    use control_plane_postgres::iceberg_sql_catalog::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };
    use engine::service::EngineControlService;
    use engine_serving::IcebergActionWriter;
    use engine_wire::pb::engine_control_server::EngineControlServer;
    use iceberg::CatalogBuilder;
    use iceberg::io::LocalFsStorageFactory;
    use std::time::Duration;
    use tonic::transport::Server;

    let mk_props = || {
        let mut p = std::collections::HashMap::new();
        p.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
        p.insert(
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            format!("file://{}", warehouse.display()),
        );
        p
    };
    let build = || async {
        SqlCatalogBuilder::default()
            .with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory))
            .load("loom", mk_props())
            .await
            .expect("build SqlCatalog")
    };

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let catalog = build().await;
    let writer = IcebergActionWriter::new(
        std::sync::Arc::new(build().await),
        pool.clone(),
        inline_byte_limit,
        flush_byte_threshold,
    );
    let svc = EngineControlService {
        cp,
        catalog,
        pool,
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };

    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock = sock_dir.path().join("engine.sock");
    let sock_str = sock.to_string_lossy().to_string();
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    (
        sock_str,
        EngineGuard {
            _sock_dir: sock_dir,
            handle,
        },
    )
}

/// Connect a raw governance/queue client to a spawned engine socket.
pub async fn connect_gov_client(sock: &str) -> engine_wire::client::GrpcQueueClient {
    engine_wire::client::GrpcQueueClient::connect(sock.to_string())
        .await
        .expect("connect GrpcQueueClient")
}

/// Spawn an `EngineControlService` on a UDS backed by `db` + `warehouse`, and return a
/// query-api `EngineActionClient` pointing at it (plus a keep-alive guard). The engine
/// writes to the same Postgres + warehouse the test reads from.
pub async fn spawn_engine_writer(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (
    query_api::engine_action_client::EngineActionClient,
    EngineGuard,
) {
    let (sock, guard) =
        spawn_engine(fx, db, warehouse, inline_byte_limit, flush_byte_threshold).await;
    let client = query_api::engine_action_client::EngineActionClient::connect(sock)
        .await
        .expect("connect EngineActionClient");
    (client, guard)
}

/// Drive the HTTP router (behind the auth gate) with a `POST` carrying a JSON body and
/// return (status, parsed JSON body). Mirrors `get` for the `/search` (and other write)
/// routes.
pub async fn post_search(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
    body: &serde_json::Value,
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
                .method("POST")
                .uri(uri)
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(body).unwrap()))
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

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

use loom_test_seed::{vec4_batches, vec4_columns};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ActionStep, Assignment, Auth, Cardinality,
    ControlPlane, ControlPlaneError, DatasetId, Effect, EventType, IndexSpec, LineageEvent,
    LinkBacking, LinkDef, Metric, NewUser, ObjectType, Ontology, ParamDef, Policy, PolicyTarget,
    PropertyDef, RoleId, RowFilter, RunId, SubjectId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::vector_index::build_vector_index;
use http_body_util::BodyExt;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
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
        _jobs: &[control_plane_core::NewJob],
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
        _jobs: &[control_plane_core::NewJob],
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
        at: Option<control_plane_core::SnapshotId>,
    ) -> Result<query_api::serving::Rows, ServingError> {
        let inlined = inline_params(sql, params);
        // Honor `at`: use the streaming entry point directly (it threads `at` through to
        // `register_iceberg_table`) and collect, rather than the unary `execute_query`
        // helper (which hardcodes `at: None`) — this in-process double must serve the
        // same as-of semantics as the real engine wire client (`EngineServingClient`).
        let map_err = |e| match e {
            // Full Display, not the inner DataFusionError: the wire path's message
            // is the engine's `query planning failed: {df}` (serving_status uses
            // e.to_string()), and the in-process twin must match it byte-for-byte.
            e @ engine_serving::EngineServingError::Plan(_) => ServingError::Plan(e.to_string()),
            other => ServingError::Engine(other.to_string()),
        };
        let stream = engine_serving::execute_query_stream(&self.catalog, &inlined, None, at)
            .await
            .map_err(map_err)?;
        let batches = datafusion::physical_plan::common::collect(stream)
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
            engine_serving::VectorQuery {
                table,
                index_name,
                query,
                k,
                nprobe,
                ef_search,
            },
        )
        .await
        .map_err(|e| match e {
            engine_serving::EngineServingError::NoIndex(m) => ServingError::NoIndex(m),
            engine_serving::EngineServingError::DimMismatch(m) => ServingError::DimMismatch(m),
            other => ServingError::Engine(other.to_string()),
        })?;
        Ok(batches_to_rows(vec![batch]))
    }

    async fn changelog_latest(
        &self,
        table: &control_plane_core::TableRef,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError> {
        control_plane_postgres::stream::changelog_positions_latest(&self.catalog.pool, table)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn changelog_feed(
        &self,
        table: &control_plane_core::TableRef,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: usize,
        policy: &query_api::serving::ChangeFeedPolicy,
    ) -> Result<control_plane_core::ChangeFeedPage, ServingError> {
        let tp = engine_serving::TablePolicy {
            row_filters: policy.row_filters.clone(),
            denied: policy.denied.iter().cloned().collect(),
            masked: policy.masked.iter().cloned().collect(),
        };
        engine_serving::feed::changelog_feed_scan(&self.catalog, table, None, positions, limit, &tp)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn await_changelog(
        &self,
        table: &control_plane_core::TableRef,
        timeout: std::time::Duration,
    ) -> Result<(), ServingError> {
        control_plane_postgres::stream::await_changelog(&self.catalog.pool, table, timeout)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
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
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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

/// Drive the router and read the chunked NDJSON body incrementally: parse lines
/// as frames arrive, stop after `max_lines` (dropping the body = client
/// disconnect) or when the stream ends (`?max_events=` bounded mode). Panics if
/// `timeout` elapses first. Non-200 responses collect the whole body into a
/// single element.
pub async fn get_ndjson(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
    subject: &str,
    max_lines: usize,
    timeout: std::time::Duration,
) -> (StatusCode, Vec<serde_json::Value>) {
    let token = session_token(&cp, subject).await;
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
            default_limit: 1000,
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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
    if status != StatusCode::OK {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
            String::from_utf8_lossy(&bytes).into_owned(),
        ));
        return (status, vec![v]);
    }
    let deadline = tokio::time::Instant::now() + timeout;
    let mut body = res.into_body();
    let mut buf: Vec<u8> = Vec::new();
    let mut lines = Vec::new();
    while lines.len() < max_lines {
        let frame = tokio::time::timeout_at(deadline, http_body_util::BodyExt::frame(&mut body))
            .await
            .expect("get_ndjson timed out waiting for a frame");
        let Some(frame) = frame else { break }; // stream ended (bounded mode)
        let frame = frame.expect("body frame");
        if let Some(data) = frame.data_ref() {
            buf.extend_from_slice(data);
            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let line = &line[..line.len() - 1];
                if !line.is_empty() {
                    lines.push(serde_json::from_slice(line).expect("NDJSON line parses"));
                }
            }
        }
    }
    (status, lines)
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
        _at: Option<control_plane_core::SnapshotId>,
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
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
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
        version: None,
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
        version: None,
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
        version: None,
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
    let body = objects_to_json(rows, None);
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
            version: None,
        })
        .await
        .expect("define_type Docs");

    // Land 4 orthogonal cold rows in two batches (Parquet: inline_byte_limit = 0).
    let build_catalog = writer.sql_catalog().await;
    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let (schema_rows_1_2, batches_rows_1_2) = vec4_batches(rows_1_2);
    land(
        &pool,
        &build_catalog,
        &table,
        &vec4_columns(),
        schema_rows_1_2,
        batches_rows_1_2,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        vector_lineage_evt(run, &table),
        None,
    )
    .await
    .expect("land rows 1-2");
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    let (schema_rows_3_4, batches_rows_3_4) = vec4_batches(rows_3_4);
    land(
        &pool,
        &build_catalog,
        &table,
        &vec4_columns(),
        schema_rows_3_4,
        batches_rows_3_4,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        vector_lineage_evt(run, &table),
        None,
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

/// A required param renamed away from the property it writes (`binds`). Shared by the
/// `createOrderWithLines` seed below and the multi-object/response-envelope e2e tests'
/// own single-step actions.
fn param_bound(name: &str, ty: &str, required: bool, binds: &str) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: Some(binds.into()),
    }
}

/// Define the `createOrderWithLines` multi-step action: step0 inserts the Order (bind
/// `order`); step1 & step2 each insert a LineItem whose `orderId` is `@order.id` (a
/// StepRef into the parent's just-resolved identity). Two line items ⇒ two LineItem
/// steps. Callers must have already defined `Order` (with an `id` property) and
/// `LineItem` (with `id` and `orderId` properties) — this only defines the action.
/// Byte-identical seed shared by `action_multi_object_e2e` and `action_response_http`.
pub async fn define_create_order_with_lines_action(cp: &PgControlPlane) {
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createOrderWithLines".into()),
            steps: vec![
                ActionStep {
                    target: TypeName("Order".into()),
                    kind: ActionKind::Insert,
                    parameters: vec![param_bound("oid", "Long", true, "id")],
                    assignments: vec![],
                    bind: Some("order".into()),
                },
                ActionStep {
                    target: TypeName("LineItem".into()),
                    kind: ActionKind::Insert,
                    parameters: vec![param_bound("li1", "Long", true, "id")],
                    assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                    bind: None,
                },
                ActionStep {
                    target: TypeName("LineItem".into()),
                    kind: ActionKind::Insert,
                    parameters: vec![param_bound("li2", "Long", true, "id")],
                    assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
        })
        .await
        .unwrap();
}

/// Define `Widget(id Long required identity, name String, qty Long)` + the
/// `createWidget` (insert), `updateWidget` (id + qty), and `deleteWidget` (id) actions.
/// Promoted from `update_delete_tiers_e2e.rs` so wire-client tests can reuse it.
pub async fn define_widget(cp: &PgControlPlane) -> TypeName {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Widget", ("main", "widget"))
                .prop_req("id", "Long")
                .prop("name", "String")
                .prop("qty", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createWidget", "Widget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("name", "String")
                .param("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("updateWidget", "Widget", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("deleteWidget", "Widget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
    widget
}

/// A `VWidget` type keyed on `id` with a `seq` (Long) **version** column — for the
/// Versioned merge-engine e2e. `createVWidget` (insert id+qty+seq), `bumpVWidget`
/// (update id+seq), and `deleteVWidget` (delete id) let a test emit multiple
/// versions of one identity and a delete. The `seq` column is the type's declared
/// `version` property (precedence for the Versioned engine).
pub async fn define_versioned_widget(cp: &PgControlPlane) -> TypeName {
    let ty = TypeName("VWidget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("VWidget", ("main", "vwidget"))
                .prop_req("id", "Long")
                .prop("qty", "Long")
                .prop_req("seq", "Long")
                .identity("id")
                .version("seq")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createVWidget", "VWidget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("qty", "Long")
                .param_req("seq", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("bumpVWidget", "VWidget", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("seq", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("deleteVWidget", "VWidget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
    ty
}

/// Grant `Write` + `Read` on `widget` to a fresh `writer` subject (role `writers`),
/// returning both the subject and the role so callers can `set_policy` on the role.
/// Promoted from `update_delete_governance_e2e.rs` (the one signature delta in the
/// update_delete family).
pub async fn grant_writer_role(cp: &PgControlPlane, widget: &TypeName) -> (SubjectId, RoleId) {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for action in [Action::Write, Action::Read] {
        cp.grant(
            &role,
            action,
            PolicyTarget::Type(widget.clone()),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (subj, role)
}

/// Grant `Write` + `Read` on `widget` to a fresh `writer` subject (role `writers`).
/// Promoted from `update_delete_tiers_e2e.rs`.
pub async fn grant_writer(cp: &PgControlPlane, widget: &TypeName) -> SubjectId {
    grant_writer_role(cp, widget).await.0
}

/// The single Widget object visible to `subj` for `id`, as JSON (or `None`).
/// Promoted from `update_delete_e2e.rs`/`update_delete_tiers_e2e.rs`.
pub async fn read_widget(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    subj: &SubjectId,
    id: i64,
) -> Option<serde_json::Value> {
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    objects_to_json(&rows, None)["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == serde_json::json!(id.to_string()))
        .cloned()
}

/// The sorted set of LIVE `iceberg_mirror.data_file` paths for the table `(schema,
/// name)` — the Parquet files a read scans. An O(change) inline-shadow UPDATE/DELETE
/// writes only inline rows and NEVER touches `data_file`, so this set is invariant
/// across such a mutation; a whole-table copy-on-write (the old path) would end-cap the
/// old file and add a new one, changing it. The oracle for "no Parquet rewrite".
///
/// Runtime sqlx (untyped) — the `iceberg_mirror` tables are not in query-api's `.sqlx`
/// cache, and this is test-support, not a production query path.
pub async fn data_file_paths(pool: &sqlx::PgPool, schema: &str, name: &str) -> Vec<String> {
    let paths: Vec<String> = sqlx::query_scalar(
        "select f.path from iceberg_mirror.data_file f \
         join iceberg_mirror.table t on t.table_id = f.table_id \
         where t.table_namespace = $1 and t.table_name = $2 \
           and t.end_snapshot is null and f.end_snapshot is null \
         order by f.path",
    )
    .bind(schema)
    .bind(name)
    .fetch_all(pool)
    .await
    .expect("query live data_file paths");
    paths
}

/// Count the LIVE inline rows for the table `(schema, name)`. Inline delta rows are
/// never end-capped, so "live" = `end_snapshot is null`. Returns `0` when the table has
/// no inline storage yet (a file-only object). Used to assert that a COW mutation added
/// exactly the expected number of inline shadow rows. Runtime sqlx (dynamic
/// `inline_<table_id>` name, spliced via `AssertSqlSafe`).
pub async fn count_live_inline_rows(pool: &sqlx::PgPool, schema: &str, name: &str) -> i64 {
    let tid: Option<i64> = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_optional(pool)
    .await
    .expect("resolve live table_id");
    let Some(tid) = tid else {
        return 0;
    };
    let exists: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(format!("iceberg_mirror.inline_{tid}"))
        .fetch_one(pool)
        .await
        .expect("probe inline relation");
    if exists.is_none() {
        return 0;
    }
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} where end_snapshot is null"
    )))
    .fetch_one(pool)
    .await
    .expect("count live inline rows");
    n
}

pub use loom_test_flight::EngineGuard;

/// Spawn an `EngineControlService` on a UDS and return its socket path +
/// keep-alive guard. Now a facade over `loom_test_flight::spawn_engine_uds`
/// (control-only), which adds connect-retry readiness.
pub async fn spawn_engine(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (String, EngineGuard) {
    let eng = loom_test_flight::spawn_engine_uds(
        fx,
        db,
        &warehouse.display().to_string(),
        loom_test_flight::EngineOpts {
            control: true,
            flight: false,
            inline_byte_limit,
            flush_byte_threshold,
        },
    )
    .await;
    (eng.sock.clone(), eng)
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
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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

/// Drive `POST /actions/{name}` through the router (behind the auth gate) and return
/// (status, headers, parsed JSON body). Sibling to `post_search`, but also returns
/// headers (needed to assert `X-Loom-Run-Id`) and takes the action engine explicitly:
/// `StubAction`'s default `write_steps` errors, so a multi-step action needs a REAL
/// write engine (e.g. `spawn_engine_writer`) to actually commit.
pub async fn post_action_raw(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    action_engine: Arc<dyn query_api::serving::ActionEngine>,
    uri: &str,
    body: &serde_json::Value,
    subject: &str,
) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let token = session_token(&cp, subject).await;
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine,
            default_limit: 1000,
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, headers, json)
}

/// Sibling to [`post_action_raw`] for error bodies that render as plain text rather
/// than JSON (e.g. `ActionError::Unsupported`/`BadParams`/`Misconfigured`, each a bare
/// `(StatusCode, String).into_response()` in `http.rs`). `post_action_raw`'s JSON
/// decode falls back to `Value::Null` on a non-JSON body, which loses the message; this
/// falls back to `Value::String(<raw text>)` instead (mirrors the fallback `get_ndjson`
/// already uses for its non-200 case), so callers can assert on the refusal text.
pub async fn post_action_text(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    action_engine: Arc<dyn query_api::serving::ActionEngine>,
    uri: &str,
    body: &serde_json::Value,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let token = session_token(&cp, subject).await;
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine,
            default_limit: 1000,
            naming: query_api::lineage_filter::local_naming(),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
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
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
            String::from_utf8_lossy(&bytes).into_owned(),
        ))
    };
    (status, json)
}

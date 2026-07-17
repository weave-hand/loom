//! Route test: GET /ontology/types/{name} serves the full type detail (properties with
//! required flags, identity, outbound `links`, inbound `links_to`) from the ontology,
//! and 404s an unknown type. No socket is bound (tower oneshot); a seeded in-memory
//! control plane + canned serving stubs exercise the route.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Cardinality, LinkDef, ObjectType, Ontology, PropertyDef, SubjectId, TableRef,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

struct StubAction;

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

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    let p = PropertyDef::new(name, ty);
    if required { p.required() } else { p }
}

/// An ontology with `Customer` and `Order` and one FK link `Order.customer -> Customer`.
async fn seeded_control_plane() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Customer", ("main", "customers"))
            .add_prop(prop("id", "Long", true))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("Order", ("main", "orders"))
            .add_prop(prop("id", "Long", true))
            .add_prop(prop("note", "String", false))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_link(LinkDef::fk(
        "customer",
        "Order",
        "Customer",
        Cardinality::One,
        "customer_id",
        "id",
    ))
    .await
    .unwrap();
    cp
}

fn app(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    })
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("analyst".into())));
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_serves_properties_identity_and_links() {
    let app = app(seeded_control_plane().await);

    let (status, json) = get(&app, "/ontology/types/Order").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["name"], "Order");
    assert_eq!(json["table"]["schema"], "main");
    assert_eq!(json["table"]["name"], "orders");
    assert_eq!(json["identity"], "id");
    assert_eq!(
        json["properties"],
        serde_json::json!([
            { "name": "id", "ty": "Long", "required": true },
            { "name": "note", "ty": "String", "required": false },
        ])
    );
    assert_eq!(
        json["links"],
        serde_json::json!([
            { "name": "customer", "from": "Order", "to": "Customer", "cardinality": "one" },
        ])
    );
    assert_eq!(json["links_to"], serde_json::json!([]));
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_lists_inbound_links() {
    let app = app(seeded_control_plane().await);

    let (status, json) = get(&app, "/ontology/types/Customer").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["name"], "Customer");
    assert_eq!(json["links"], serde_json::json!([]));
    assert_eq!(
        json["links_to"],
        serde_json::json!([
            { "name": "customer", "from": "Order", "to": "Customer", "cardinality": "one" },
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_type_is_404() {
    let app = app(seeded_control_plane().await);
    let (status, _) = get(&app, "/ontology/types/no-such-type").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

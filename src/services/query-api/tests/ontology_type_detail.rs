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
    Aggregation, Cardinality, DerivedPropertyDef, IndexSpec, LinkDef, Metric, ObjectType, Ontology,
    PropertyDef, SubjectId, TableRef, VectorIndexDef,
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

/// A single `Doc` type carrying a description on the type itself, on one property
/// (`id`, described) alongside one without (`body`, undescribed), and on its one
/// outbound self-link (`parent`, described). Separate from `seeded_control_plane` so
/// the latter's exact-array oracle (no `description` keys) stays undisturbed.
async fn seeded_described() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Doc", ("main", "docs"))
            .described("A document.")
            .add_prop(
                PropertyDef::new("id", "Long")
                    .required()
                    .described("The document id."),
            )
            .prop("body", "String")
            .add_prop(PropertyDef::new("embedding", "vector(4)"))
            .identity("id")
            .derived(
                DerivedPropertyDef::new("child_count", "Long", "parent", Aggregation::Count)
                    .described("Number of child documents."),
            )
            .derived(DerivedPropertyDef::new(
                "weight_sum",
                "Double",
                "parent",
                Aggregation::Sum("weight".into()),
            ))
            .done(),
    )
    .await
    .unwrap();
    cp.define_link(
        LinkDef::fk("parent", "Doc", "Doc", Cardinality::One, "parent_id", "id")
            .described("The parent document."),
    )
    .await
    .unwrap();
    cp.define_vector_index(
        VectorIndexDef::new("flat", "Doc", "embedding", Metric::Cosine, IndexSpec::Flat)
            .described("Exact cosine index over the embedding."),
    )
    .await
    .unwrap();
    cp.define_vector_index(VectorIndexDef::new(
        "hnsw",
        "Doc",
        "embedding",
        Metric::L2,
        IndexSpec::Hnsw {
            m: Some(16),
            ef_construction: Some(200),
        },
    ))
    .await
    .unwrap();
    cp
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
    assert_eq!(json["derived"], serde_json::json!([]));
    assert_eq!(json["vector_indexes"], serde_json::json!([]));
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

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_carries_descriptions() {
    let app = app(seeded_described().await);

    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["description"], "A document.");
    assert_eq!(json["properties"][0]["description"], "The document id.");
    assert_eq!(json["links"][0]["description"], "The parent document.");
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_omits_absent_descriptions() {
    let app = app(seeded_described().await);

    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["properties"][1]["name"], "body");
    assert!(json["properties"][1].get("description").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_serves_derived_and_vector_indexes() {
    let app = app(seeded_described().await);
    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["derived"],
        serde_json::json!([
            {
                "name": "child_count", "ty": "Long", "link": "parent",
                "agg": "Count", "description": "Number of child documents."
            },
            {
                "name": "weight_sum", "ty": "Double", "link": "parent",
                "agg": { "Sum": "weight" }
            },
        ])
    );
    // vector_indexes_for's order is unspecified (HashMap-backed in memory); the handler
    // sorts by name, so "flat" precedes "hnsw" deterministically.
    assert_eq!(
        json["vector_indexes"],
        serde_json::json!([
            {
                "name": "flat", "property": "embedding", "metric": "Cosine",
                "spec": "Flat", "description": "Exact cosine index over the embedding."
            },
            {
                "name": "hnsw", "property": "embedding", "metric": "L2",
                "spec": { "Hnsw": { "m": 16, "ef_construction": 200 } }
            },
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_omits_absent_derived_and_index_descriptions() {
    let app = app(seeded_described().await);
    let (_, json) = get(&app, "/ontology/types/Doc").await;
    // The undescribed derived property + index carry no `description` key.
    assert_eq!(json["derived"][1]["name"], "weight_sum");
    assert!(json["derived"][1].get("description").is_none());
    assert_eq!(json["vector_indexes"][1]["name"], "hnsw");
    assert!(json["vector_indexes"][1].get("description").is_none());
}

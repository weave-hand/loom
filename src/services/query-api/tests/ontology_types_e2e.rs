//! `GET /ontology/types` e2e: lists the defined object-type names. Auth-required
//! (via `Subject`) but not per-type ACL-gated — ontology metadata, matching
//! `/openapi.json`.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ObjectType, Ontology, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, get, prop, tref};

/// Define two object types (`customer`, `orders`), each backed by its own table. No
/// data needs to be seeded — the endpoint only reads ontology metadata.
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    cp.define_type(ObjectType {
        name: TypeName("customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("main", "customer"),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("orders".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("main", "orders"),
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng)
}

#[tokio::test(flavor = "multi_thread")]
async fn lists_ontology_type_names() {
    let fx = PgFixture::shared();
    let (cp, eng) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (status, body) = get(cp.clone(), eng.clone(), "/ontology/types", "alice").await;
    assert_eq!(status, StatusCode::OK);
    let mut names: Vec<String> = body["types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["customer".to_string(), "orders".to_string()]);
}

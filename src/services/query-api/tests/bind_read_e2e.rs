//! Connective-tissue e2e: a dataset landed by the ingest materializer, bound to an
//! ontology type by `bind`, is retrievable through the governed read path. Proves
//! loom's two layers (landing + model) meet.

use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, PolicyTarget, PropertyDef, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use ingest::bind;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn landed_then_bound_dataset_is_queryable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };

    // 1. LAND: seed a dataset (id long, email string, amount double).
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("email".to_string(), "string".to_string(), true),
        ("amount".to_string(), "double".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "customer",
            &cols,
            &[
                SeedCol::Long(vec![1, 2]),
                SeedCol::Str(vec!["a@x", "b@x"]),
                SeedCol::NullableDouble(vec![Some(1.5), Some(2.5)]),
            ],
        )
        .await;

    // 2. BIND: a Customer type over the landed table (validated against physical schema).
    // The IcebergCatalog implements `Catalog` against iceberg_mirror.*, so bind() finds
    // the table that IcebergWriter just committed.
    let iceberg_cat = IcebergCatalog::new(pool.clone());
    bind(
        &iceberg_cat,
        &cp,
        ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "email".into(),
                    ty: "EmailAddress".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "amount".into(),
                    ty: "Double".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            table: table.clone(),
            identity: None,
        },
    )
    .await
    .unwrap();

    // 3. GRANT a Read ACL.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Customer".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    // 4. READ through the governed front door.
    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        rows.columns,
        vec!["id".to_string(), "email".to_string(), "amount".to_string()]
    );
    assert_eq!(rows.rows.len(), 2, "both landed rows are retrievable");

    // The typed wire contract end-to-end: id (Long) renders as a STRING, amount
    // (Double) as a number, through the real materialize -> bind -> read path.
    let body = objects_to_json(&rows, None);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    assert_eq!(
        objs,
        vec![
            json!({ "id": "1", "email": "a@x", "amount": 1.5 }),
            json!({ "id": "2", "email": "b@x", "amount": 2.5 }),
        ],
        "Long id serializes as a string; Double amount as a number"
    );
}

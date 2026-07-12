//! Derived properties e2e: Customer.orderCount (COUNT) + totalSpend (SUM) over the
//! Customer->Order FK link, served through read_object. Both-ends governance: without
//! Read on Order the derived props are omitted; an Order row-filter narrows the aggregate.

use control_plane_core::{
    Acl, Action, Aggregation, Cardinality, CompareOp, ControlPlane, DerivedPropertyDef, Effect,
    LinkBacking, LinkDef, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef, RoleId,
    RowFilter, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use serde_json::json;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Seed Customer + Order with an FK link Customer-(orders)->Order and the two derived
/// properties orderCount (COUNT) + totalSpend (SUM(amount)). Customer 1 has orders
/// (10, 5.0, 'shipped') and (11, 7.0, 'pending'); customer 2 has none.
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let cust = tref("main", "customer");
    writer
        .seed_arrays(
            "main",
            "customer",
            &[("id".to_string(), "long".to_string(), false)],
            &[SeedCol::Long(vec![1, 2])],
        )
        .await;

    let ord = tref("main", "orders");
    writer
        .seed_arrays(
            "main",
            "orders",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("customer_id".to_string(), "long".to_string(), false),
                ("amount".to_string(), "double".to_string(), true),
                ("status".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![10, 11]),
                SeedCol::Long(vec![1, 1]),
                SeedCol::NullableDouble(vec![Some(5.0), Some(7.0)]),
                SeedCol::Str(vec!["shipped", "pending"]),
            ],
        )
        .await;

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
                name: "amount".into(),
                ty: "Double".into(),
                required: false,
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
        name: TypeName("Customer".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![
            DerivedPropertyDef {
                name: "orderCount".into(),
                ty: "Long".into(),
                link: "orders".into(),
                agg: Aggregation::Count,
            },
            DerivedPropertyDef {
                name: "totalSpend".into(),
                ty: "Double".into(),
                link: "orders".into(),
                agg: Aggregation::Sum("amount".into()),
            },
        ],
        table: cust.clone(),
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    // Return the writer: it owns the `file://` warehouse TempDir, which is removed on
    // drop. Keeping it alive for the test's lifetime keeps the seeded Parquet on disk.
    (cp, eng, writer)
}

async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

fn sorted_objects(rows: &query_api::handler::ObjectRows) -> Vec<serde_json::Value> {
    let body = objects_to_json(rows, None);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    objs
}

#[tokio::test(flavor = "multi_thread")]
async fn derived_aggregates_are_served_and_governed() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };

    // ---- subject A: Read on Customer AND Order -> sees derived ----
    let (a, a_role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &a_role, "Customer").await;
    grant_read(&cp, &a_role, "Order").await;
    let rows = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let objs = sorted_objects(&rows);
    // Customer 1: 2 orders, total 12.0 ; Customer 2: 0 orders, total 0.0.
    // Long renders as a numeric string; Double as a number (see render.rs / bind_read_e2e).
    assert_eq!(
        objs[0],
        json!({ "id": "1", "orderCount": "2", "totalSpend": 12.0 })
    );
    assert_eq!(
        objs[1],
        json!({ "id": "2", "orderCount": "0", "totalSpend": 0.0 })
    );

    // ---- subject B: Read on Customer ONLY -> derived OMITTED (both-ends governance) ----
    let (b, b_role) = subject_with_role(&cp, "bob").await;
    grant_read(&cp, &b_role, "Customer").await;
    let rows_b = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(b),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        rows_b.columns,
        vec!["id".to_string()],
        "no Read on Order -> both derived props omitted"
    );

    // ---- subject C: Read on both + Order row-filter status='shipped' -> narrowed ----
    let (c, c_role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &c_role, "Customer").await;
    grant_read(&cp, &c_role, "Order").await;
    cp.set_policy(
        &c_role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("shipped".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let rows_c = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(c),
        &deps,
    )
    .await
    .unwrap();
    let objs_c = sorted_objects(&rows_c);
    // Only the 'shipped' order (amount 5.0) counts for customer 1; customer 2 still empty.
    assert_eq!(
        objs_c[0],
        json!({ "id": "1", "orderCount": "1", "totalSpend": 5.0 })
    );
    assert_eq!(
        objs_c[1],
        json!({ "id": "2", "orderCount": "0", "totalSpend": 0.0 })
    );
}

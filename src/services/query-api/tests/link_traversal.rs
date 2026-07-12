//! Governed link traversal e2e: seed Customer + Order tables, define an FK link and a
//! many-to-many link, and exercise the both-ends governance matrix.

use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, ControlPlane, Effect, LinkBacking, LinkDef, ObjectType,
    Ontology, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::handler::{
    ChainFilter, LinkQuery, QueryDeps, QueryError, Subject, read_linked_objects,
};
use query_api::serving::SqlValue;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn srcf(col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position: 0,
        column: col.into(),
        raw: val.into(),
    }
}

/// Seed two types (Customer, Order) + an FK link Customer-(orders)->Order, and seed
/// rows. Returns (cp, serving engine, writer). The writer is returned so the caller
/// can seed additional tables (e.g. the join-table in many_to_many_dedups_shared_targets).
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
            &[
                ("id".to_string(), "long".to_string(), false),
                ("region".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["CA", "NY", "CA"]),
            ],
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
                ("secret".to_string(), "string".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![10, 11, 12, 13]),
                SeedCol::Long(vec![1, 1, 2, 3]),
                SeedCol::NullableDouble(vec![Some(50.0), Some(200.0), Some(70.0), Some(300.0)]),
                SeedCol::Str(vec!["x", "y", "z", "w"]),
            ],
        )
        .await;

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
                name: "amount".into(),
                ty: "Double".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "secret".into(),
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
    (cp, eng, writer)
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

async fn analyst(cp: &PgControlPlane) -> (SubjectId, RoleId) {
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

fn ids_of(rows: &query_api::handler::ObjectRows) -> Vec<i64> {
    let mut ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(i) => *i,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn fk_traversal_returns_linked_targets() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![srcf("region", "CA")],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows.columns, vec!["id", "customer_id", "amount", "secret"]);
    assert_eq!(ids_of(&rows), vec![10, 11, 13]);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_read_on_source_is_forbidden() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_read_on_target_is_forbidden() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test(flavor = "multi_thread")]
async fn source_row_filter_closes_the_leak() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Customer".into())),
            row_filter: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("CA".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        ids_of(&rows),
        vec![10, 11, 13],
        "order 12 (NY customer) excluded via source policy"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn target_row_filter_and_projection_apply() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "amount".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(100),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert!(
        !rows.columns.contains(&"secret".to_string()),
        "secret column denied"
    );
    assert_eq!(ids_of(&rows), vec![11, 13]);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_filter_on_denied_column_is_bad_filter() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Customer".into())),
            row_filter: None,
            deny_columns: vec!["region".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            filters: vec![srcf("region", "CA")],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "region"));
}

#[tokio::test(flavor = "multi_thread")]
async fn many_to_many_dedups_shared_targets() {
    let fx = PgFixture::shared();
    let (cp, eng, writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // Seed the mapping table into the same catalog the engine reads.
    let map = tref("main", "customer_order");
    writer
        .seed_arrays(
            "main",
            "customer_order",
            &[
                ("customer_id".to_string(), "long".to_string(), false),
                ("order_id".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(vec![1, 3]), SeedCol::Long(vec![11, 11])],
        )
        .await;

    cp.define_link(LinkDef {
        name: "shared".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: map.clone(),
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "shared".into(),
            filters: vec![srcf("region", "CA")],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids_of(&rows), vec![11], "shared target deduped to one row");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_link_is_reported() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "nope".into(),
            filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::UnknownLink(l) if l == "nope"));
}

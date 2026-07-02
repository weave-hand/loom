//! A governed object read (deny-column + row-filter) returns IDENTICAL ObjectRows
//! whether governance (ontology + ACL) is read direct from PgControlPlane or over the
//! engine wire via WireControlPlane. The serving engine (data) is the same in-process
//! Iceberg engine in both legs; only the governance transport differs.

use std::sync::Arc;

use control_plane_core::{
    Acl, Action, CompareOp, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, connect_gov_client, spawn_engine};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::serving::SqlValue;
use query_api::wire_control_plane::WireControlPlane;

#[tokio::test(flavor = "multi_thread")]
async fn governed_read_parity_over_wire() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");

    // 1. Seed orders(id, status, secret): (1,'open','s1'),(2,'closed','s2'),(3,'open','s3').
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("status".to_string(), "string".to_string(), true),
        ("secret".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["open", "closed", "open"]),
                SeedCol::Str(vec!["s1", "s2", "s3"]),
            ],
        )
        .await;

    // 2. Ontology: type Order -> main.orders.
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
                name: "status".into(),
                ty: "String".into(),
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
        table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        identity: None,
    })
    .await
    .unwrap();

    // 3. ACL: analyst can Read Order; policy denies `secret`, restricts rows to status='open'.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("open".into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // 4. One in-process serving engine over the seeded catalog (data path, both legs).
    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);

    // 5. Spawn the engine for governance-over-wire (it reads the SAME Postgres db);
    //    build WireControlPlane. The warehouse it gets is unused by governance reads.
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let cp = Arc::new(cp);
    let wire = WireControlPlane::new(
        connect_gov_client(&sock).await,
        cp.clone() as Arc<dyn ControlPlane>,
    );

    // 6. Same query + subject, governance direct vs over the wire.
    let q = ObjectQuery {
        type_name: "Order".into(),
        filters: vec![],
        ids: vec![],
    };
    let s = Subject(subj.clone());
    let direct = read_object(
        &q,
        &s,
        &QueryDeps {
            ontology: cp.ontology(),
            acl: cp.acl(),
            serving: &eng,
            default_limit: 1000,
        },
    )
    .await
    .expect("direct governed read");
    let over_wire = read_object(
        &q,
        &s,
        &QueryDeps {
            ontology: wire.ontology(),
            acl: wire.acl(),
            serving: &eng,
            default_limit: 1000,
        },
    )
    .await
    .expect("wire governed read");

    // Parity: identical projection, logical types, and rows.
    assert_eq!(direct.columns, over_wire.columns);
    assert_eq!(direct.logical_types, over_wire.logical_types);
    assert_eq!(direct.rows, over_wire.rows);

    // And the governance actually applied over the wire: `secret` dropped, only open rows.
    assert_eq!(
        over_wire.columns,
        vec!["id".to_string(), "status".to_string()]
    );
    assert_eq!(over_wire.rows.len(), 2, "row filter kept only status=open");
    let ids: Vec<&SqlValue> = over_wire.rows.iter().map(|r| &r[0]).collect();
    assert!(ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(3)));

    // --- mask_columns parity over the wire ---
    // A masker subject sees `secret` present but redacted to "***" on both paths.
    let masker = SubjectId("masker".into());
    let mrole = RoleId("mask_role".into());
    cp.define_subject(&masker).await.unwrap();
    cp.define_role(&mrole).await.unwrap();
    cp.assign_role(&masker, &mrole).await.unwrap();
    cp.grant(
        &mrole,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &mrole,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["secret".into()],
        },
    )
    .await
    .unwrap();

    let mq = ObjectQuery {
        type_name: "Order".into(),
        filters: vec![],
        ids: vec![],
    };
    let ms = Subject(masker.clone());
    let direct_masked = read_object(
        &mq,
        &ms,
        &QueryDeps {
            ontology: cp.ontology(),
            acl: cp.acl(),
            serving: &eng,
            default_limit: 1000,
        },
    )
    .await
    .expect("direct masked read");
    let wire_masked = read_object(
        &mq,
        &ms,
        &QueryDeps {
            ontology: wire.ontology(),
            acl: wire.acl(),
            serving: &eng,
            default_limit: 1000,
        },
    )
    .await
    .expect("wire masked read");

    // Parity: both legs must produce identical columns, types, and rows.
    assert_eq!(
        direct_masked.columns, wire_masked.columns,
        "mask_columns parity: columns"
    );
    assert_eq!(
        direct_masked.logical_types, wire_masked.logical_types,
        "mask_columns parity: logical_types"
    );
    assert_eq!(
        direct_masked.rows, wire_masked.rows,
        "mask_columns parity: rows"
    );

    // And the masking was actually applied over the wire: secret column present but redacted.
    let secret_idx = wire_masked
        .columns
        .iter()
        .position(|c| c == "secret")
        .expect("secret column present (masked, not dropped)");
    assert!(
        wire_masked
            .rows
            .iter()
            .all(|r| r[secret_idx] == SqlValue::Text("***".into())),
        "every secret value is the redaction marker over the wire"
    );

    drop(writer);
}

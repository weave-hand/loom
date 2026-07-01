//! Typed input filters e2e: filter a Double and a Boolean column through read_object.
//! A Text bind would match nothing; coercion to the column's logical type makes it work.
//! An uncoercible value is a 400 (BadFilterValue, carrying the parse fault).

use control_plane_core::{
    Acl, Action, Effect, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::handler::{ObjectQuery, QueryDeps, QueryError, Subject, read_object};
use query_api::render::objects_to_json;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Seed an Order table with NON-TEXT columns: id Long, amount Double, active Boolean.
/// Rows: (1, 10.5, true), (2, 20.0, false), (3, 10.5, true), (4, NULL, NULL), (5, 30.0, NULL).
async fn setup(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    InProcessServingEngine,
    SubjectId,
    IcebergWriter,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let ord = tref("main", "orders");
    writer
        .seed_arrays(
            "main",
            "orders",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("amount".to_string(), "double".to_string(), true),
                ("active".to_string(), "boolean".to_string(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5]),
                SeedCol::NullableDouble(vec![Some(10.5), Some(20.0), Some(10.5), None, Some(30.0)]),
                SeedCol::NullableBool(vec![Some(true), Some(false), Some(true), None, None]),
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
                name: "amount".into(),
                ty: "Double".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "active".into(),
                ty: "Boolean".into(),
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

    // Subject `a` (alice) with a role granted Read on Order.
    let subj = SubjectId("alice".into());
    let role = RoleId("alice-role".into());
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    // Return the writer: it owns the `file://` warehouse TempDir, removed on drop.
    // Keeping it alive keeps the seeded Parquet on disk for the test's reads.
    (cp, eng, subj, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_filters_match_and_reject() {
    let fx = PgFixture::start();
    let (cp, eng, a, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows);
        let mut v: Vec<String> = body["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };

    // Double filter: amount = 10.5 -> rows 1, 3.
    let r = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("amount".into(), "10.5".into())],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["1".to_string(), "3".to_string()]);

    // Boolean filter: active = true -> rows 1, 3 ; active = false -> row 2.
    let r_true = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("active".into(), "true".into())],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r_true), vec!["1".to_string(), "3".to_string()]);
    let r_false = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("active".into(), "false".into())],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r_false), vec!["2".to_string()]);

    // Uncoercible value -> BadFilterValue (400), carrying the source parse error.
    let err = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("amount".into(), "abc".into())],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::BadFilterValue(_)),
        "uncoercible filter value -> BadFilterValue, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn comparison_set_and_null_operators() {
    let fx = PgFixture::start();
    let (cp, eng, a, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows);
        let mut v: Vec<String> = body["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    let run = |filters: Vec<(String, String)>| {
        let deps = &deps;
        let a = a.clone();
        async move {
            read_object(
                &ObjectQuery {
                    type_name: "Order".into(),
                    eq_filters: filters,
                    ids: vec![],
                },
                &Subject(a),
                deps,
            )
            .await
        }
    };

    // gt on a Double column: amount > 15 -> rows 2 (20.0) and 5 (30.0).
    let r = run(vec![("amount".into(), "gt:15".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["2".to_string(), "5".to_string()]);

    // Range (two predicates on one column): 11 <= amount <= 25 -> only row 2.
    // Both bounds are load-bearing here: ge:11 alone -> {2,5}, le:25 alone -> {1,2,3},
    // so only their AND yields {2} — neither predicate produces the result on its own.
    let r = run(vec![
        ("amount".into(), "ge:11".into()),
        ("amount".into(), "le:25".into()),
    ])
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string()]);

    // Set membership on Long id: id in (1,3) -> rows 1, 3.
    let r = run(vec![("id".into(), "in:1,3".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["1".to_string(), "3".to_string()]);

    // Null checks: amount isnull -> row 4; isnotnull -> rows 1,2,3,5.
    let r = run(vec![("amount".into(), "isnull".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["4".to_string()]);
    let r = run(vec![("amount".into(), "isnotnull".into())])
        .await
        .unwrap();
    assert_eq!(
        ids(&r),
        vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "5".to_string()
        ]
    );

    // Bad arity (gt with no operand) -> BadFilterValue (400): coerce_predicate returns a
    // FilterError for the malformed predicate, which the handler now carries through.
    let err = run(vec![("amount".into(), "gt".into())]).await.unwrap_err();
    assert!(
        matches!(err, QueryError::BadFilterValue(_)),
        "bad arity -> BadFilterValue, got {err:?}"
    );
}

//! Typed input filters e2e: filter a Double and a Boolean column through read_object.
//! A Text bind would match nothing; coercion to the column's logical type makes it work.
//! An uncoercible value is a 400 (BadFilterValue, carrying the parse fault).
//! Also covers `between` (against the real engine, proving it matches `ge`+`le`) and the
//! text-pattern operators `contains`/`startswith`/`endswith` (case-insensitive anchors,
//! plus literal `%` escaping; `_` escaping is unit-covered in `filter_coerce.rs`) — the
//! first exercise of the rendered `ILIKE ... ESCAPE '\'` SQL against DataFusion.

use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId,
    SubjectId, TableRef, TypeName,
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
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
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
            filters: vec![("amount".into(), "10.5".into())],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
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
            filters: vec![("active".into(), "true".into())],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
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
            filters: vec![("active".into(), "false".into())],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
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
            filters: vec![("amount".into(), "abc".into())],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
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
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
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
                    filters,
                    ids: vec![],
                    or_raw: Vec::new(),
                    as_of: None,
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

/// Seed an Order table with a ranged Double and a text `name`: id Long, amount Double,
/// name String. Rows: (1, 5.0, "50 off"), (2, 11.0, "ACME"), (3, 20.0, "Tacme"),
/// (4, 25.0, "beacon"), (5, 30.0, "50% off").
async fn setup_ranges_and_text(
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
                ("name".to_string(), "string".to_string(), false),
            ],
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5]),
                SeedCol::NullableDouble(vec![
                    Some(5.0),
                    Some(11.0),
                    Some(20.0),
                    Some(25.0),
                    Some(30.0),
                ]),
                SeedCol::Str(vec!["50 off", "ACME", "Tacme", "beacon", "50% off"]),
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
                name: "name".into(),
                ty: "String".into(),
                required: true,
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
async fn between_matches_ge_and_le() {
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup_ranges_and_text(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
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
                    filters,
                    ids: vec![],
                    or_raw: Vec::new(),
                    as_of: None,
                },
                &Subject(a),
                deps,
            )
            .await
        }
    };

    // amount rows: 5.0, 11.0, 20.0, 25.0, 30.0 (ids 1..5). between:11,25 must return exactly
    // the same rows as ge:11 AND le:25 -> ids 2, 3, 4.
    let via_between = ids(&run(vec![("amount".into(), "between:11,25".into())])
        .await
        .unwrap());
    let via_ge_le = ids(&run(vec![
        ("amount".into(), "ge:11".into()),
        ("amount".into(), "le:25".into()),
    ])
    .await
    .unwrap());
    assert_eq!(via_between, via_ge_le);
    assert_eq!(
        via_between,
        vec!["2".to_string(), "3".to_string(), "4".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn contains_is_case_insensitive_and_anchors() {
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup_ranges_and_text(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
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
                    filters,
                    ids: vec![],
                    or_raw: Vec::new(),
                    as_of: None,
                },
                &Subject(a),
                deps,
            )
            .await
        }
    };

    // name rows: "50 off"(1), "ACME"(2), "Tacme"(3), "beacon"(4), "50% off"(5).
    // contains:ac matches case-insensitively wherever "ac" appears: ACME, Tacme, beacon.
    let r = run(vec![("name".into(), "contains:ac".into())])
        .await
        .unwrap();
    assert_eq!(
        ids(&r),
        vec!["2".to_string(), "3".to_string(), "4".to_string()]
    );

    // startswith:ac anchors at the start: only ACME.
    let r = run(vec![("name".into(), "startswith:ac".into())])
        .await
        .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string()]);

    // endswith:me anchors at the end: ACME and Tacme (both end "me"), not beacon.
    let r = run(vec![("name".into(), "endswith:me".into())])
        .await
        .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string(), "3".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn contains_literal_percent_matches_the_character() {
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup_ranges_and_text(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
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
                    filters,
                    ids: vec![],
                    or_raw: Vec::new(),
                    as_of: None,
                },
                &Subject(a),
                deps,
            )
            .await
        }
    };

    // "50% off"(5) vs "50 off"(1): a literal '%' in the operand must match only the
    // character, not act as a wildcard -- proves `escape_like` + `ESCAPE '\'` round-trip
    // through the real engine's SQL parser/executor.
    let r = run(vec![("name".into(), "contains:50%".into())])
        .await
        .unwrap();
    assert_eq!(ids(&r), vec!["5".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn or_groups_union_and_governance() {
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows, None);
        let mut v: Vec<String> = body["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };

    // Union across columns: amount > 25 OR active = false -> rows {5 (30.0)} ∪ {2 (false)}.
    let r = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: vec!["amount:gt:25,active:eq:false".into()],
            as_of: None,
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string(), "5".to_string()]);

    // OR-group intersected (ANDed) with a plain predicate: (amount = 10.5) AND
    // (active = true OR amount > 100). amount=10.5 -> {1,3}; both are active=true. -> {1,3}.
    let r2 = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![("amount".into(), "10.5".into())],
            ids: vec![],
            or_raw: vec!["active:eq:true,amount:gt:100".into()],
            as_of: None,
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r2), vec!["1".to_string(), "3".to_string()]);

    // Two OR-groups are ANDed: (amount>=20 OR active=false) AND (amount<=20 OR active=true).
    // Per-row (amount, active):
    //   row1 (10.5,T): g1 = 10.5>=20 F | active=false F  -> F  => out
    //   row2 (20.0,F): g1 = 20>=20 T                     -> T ; g2 = 20<=20 T -> T => in
    //   row3 (10.5,T): g1 = 10.5>=20 F | active=false F  -> F  => out
    //   row4 (NULL,NULL): all comparisons on NULL are F  -> F  => out
    //   row5 (30.0,NULL): g1 = 30>=20 T -> T ; g2 = 30<=20 F | active=true F -> F => out
    // Intersection -> only row 2.
    let r3 = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: vec![
                "amount:ge:20,active:eq:false".into(),
                "amount:le:20,active:eq:true".into(),
            ],
            as_of: None,
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r3), vec!["2".to_string()]);

    // A one-member OR-group is rejected (BadFilterValue / 400).
    let e_single = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: vec!["amount:gt:25".into()],
            as_of: None,
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(e_single, QueryError::BadFilterValue(_)),
        "single-member OR-group -> BadFilterValue, got {e_single:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn or_group_never_weakens_governance() {
    let fx = PgFixture::shared();
    let (cp, eng, a, _writer) = setup(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };

    // A member naming a column the subject cannot filter on must fail the whole request.
    // (Use a column absent from the type — same BadFilter path as a denied column.)
    let denied = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: vec!["nonexistent:eq:x,amount:gt:1".into()],
            as_of: None,
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::BadFilter(_)),
        "member on a non-permitted column -> BadFilter (no leak), got {denied:?}"
    );
}

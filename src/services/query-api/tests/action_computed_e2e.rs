//! Computed expression assignments (`Assignment::expr`) e2e matrix: drives the real action
//! write path (Postgres + Iceberg + engine) exactly like `action_mapping_e2e.rs` and reads
//! results back through the governed read path. Proves (1) arithmetic composition, (2) a
//! conditional referencing an earlier-resolved property plus string functions/concat, (3)
//! `now()` lands a timestamp within the request window, (4) a runtime evaluation fault (div by
//! zero) is a client error and writes nothing, (5) a computed value is deny-column gated
//! identically to a literal, (6) a computed value trips a declared property constraint
//! identically to a literal, and (7) an UPDATE recomputes a patched value.

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, Assignment, ControlPlane, Effect, ObjectType,
    Policy, PolicyTarget, PropertyConstraints, PropertyDef, RangeConstraint, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{EngineGuard, InProcessServingEngine};
use query_api::action::{ActionDeps, ActionError, WriteDenialReason, run_action};
use query_api::engine_action_client::EngineActionClient;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use serde_json::json;

/// A booted fixture with a fully-granted writer over the seven-column `main.gadget`
/// (`id` Long required + identity, `qty` Long, `unitPrice` Double, `total` Double, `tier`
/// String, `label` String, `createdAt` Timestamp).
struct GadgetWriter {
    cp: PgControlPlane,
    pool: sqlx::PgPool,
    gadget: TypeName,
    subj: SubjectId,
    role: RoleId,
    engine: EngineActionClient,
    _eg: EngineGuard,
    warehouse: tempfile::TempDir,
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn param(
    name: &str,
    ty: &str,
    required: bool,
    binds: Option<&str>,
) -> control_plane_core::ParamDef {
    control_plane_core::ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: binds.map(str::to_string),
    }
}

/// The `Gadget` property list, with `total`'s constraints parameterized so the
/// constraint-composition test (6) can declare a `range.max` on it while every other test
/// gets the unconstrained default.
fn gadget_properties(total_constraints: PropertyConstraints) -> Vec<PropertyDef> {
    vec![
        prop("id", "Long", true),
        prop("qty", "Long", false),
        prop("unitPrice", "Double", false),
        PropertyDef {
            name: "total".into(),
            ty: "Double".into(),
            required: false,
            constraints: total_constraints,
        },
        prop("tier", "String", false),
        prop("label", "String", false),
        prop("createdAt", "Timestamp", false),
    ]
}

/// Boot a fixture, define the `Gadget` type, grant Write+Read to a `writer` subject, and
/// attach the engine action client. Actions are defined per-test (they differ).
async fn setup_gadget_writer(fx: &PgFixture) -> GadgetWriter {
    setup_gadget_writer_with_total_constraints(fx, PropertyConstraints::default()).await
}

/// Like `setup_gadget_writer`, but with an explicit `total` constraint (used only by the
/// constraint-composition test).
async fn setup_gadget_writer_with_total_constraints(
    fx: &PgFixture,
    total_constraints: PropertyConstraints,
) -> GadgetWriter {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let gadget = TypeName("Gadget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: gadget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "gadget".into(),
            },
            properties: gadget_properties(total_constraints),
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .unwrap();

    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(gadget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(gadget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        role,
        engine,
        _eg,
        warehouse,
    }
}

/// Read every Gadget visible to `subj` through the governed read path, as `{"objects":[…]}`.
/// Before any row lands the mirror table does not exist; treat that as no objects.
async fn read_gadgets(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    subj: &SubjectId,
) -> serde_json::Value {
    let catalog = IcebergCatalog::new(pool.clone());
    let exists = catalog
        .live_tables()
        .await
        .unwrap()
        .iter()
        .any(|t| t.schema == "main" && t.name == "gadget");
    if !exists {
        return json!({ "objects": [] });
    }
    let eng = InProcessServingEngine::new(catalog);
    let deps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Gadget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap();
    objects_to_json(&rows, None)
}

#[tokio::test(flavor = "multi_thread")]
async fn arithmetic_expression_writes_product() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty * unitPrice")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    run_action(
        "create",
        json!({"id": "1", "qty": "4", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("arithmetic-computed insert runs");

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["total"], json!(10.0));
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_integer_expression_widens_into_double_column() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    // `qty + 1` is an all-integer expression (evaluates to `SqlValue::Int`), assigned into the
    // Double `total` column. The type-checker allows the widening; this pins that the write
    // path (`one_cell`) also accepts it instead of 500ing on a Double/Int mismatch.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
            ],
            vec![Assignment::expr("total", "qty + 1")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    run_action(
        "create",
        json!({"id": "1", "qty": "4"}).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("an all-integer computed value widens into a Double column");

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["total"], json!(5.0));
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_and_string_expressions() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![
                Assignment::expr("total", "qty * unitPrice"),
                Assignment::expr("tier", "if @total > 100.0 then \"gold\" else \"std\""),
                Assignment::expr("label", "upper(\"wid\") ++ \"-\" ++ lower(\"GET\")"),
            ],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    run_action(
        "create",
        json!({"id": "1", "qty": "4", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("conditional/string-computed insert runs");

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["total"], json!(10.0));
    assert_eq!(got["objects"][0]["tier"], json!("std")); // 10 <= 100
    assert_eq!(got["objects"][0]["label"], json!("WID-get"));
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn now_lands_a_timestamp_in_window() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![param("id", "Long", true, None)],
            vec![Assignment::expr("createdAt", "now()")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let before = time::OffsetDateTime::now_utc();
    run_action(
        "create",
        json!({"id": "1"}).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("now()-computed insert runs");
    let after = time::OffsetDateTime::now_utc();

    let got = read_gadgets(&cp, &pool, &subj).await;
    let s = got["objects"][0]["createdAt"]
        .as_str()
        .expect("createdAt is a string");
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    let parsed = time::PrimitiveDateTime::parse(s, &fmt)
        .expect("parses as an ISO timestamp")
        .assume_utc();
    assert!(
        parsed >= before.replace_nanosecond(0).unwrap() && parsed <= after,
        "createdAt {parsed:?} not within [{before:?}, {after:?}]"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_fault_returns_bad_params_and_writes_nothing() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    // Conformance passes (Long/Int -> Long, widens to Double); eval faults at div-by-zero.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty / 0")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let err = run_action(
        "create",
        json!({"id": "1", "qty": "4", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::BadParams(_)),
        "expected BadParams, got {err:?}"
    );
    assert_eq!(
        read_gadgets(&cp, &pool, &subj).await["objects"]
            .as_array()
            .map(Vec::len),
        Some(0),
        "a runtime evaluation fault must write nothing"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn computed_value_is_governed_identically() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        role,
        engine,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget.clone(),
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty * unitPrice")],
        ))
        .await
        .unwrap();

    // Write policy DENIES the `total` column, exactly like the literal-value case.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(gadget),
            row_filter: None,
            deny_columns: vec!["total".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let err = run_action(
        "create",
        json!({"id": "1", "qty": "4", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "total"),
        "computed-value column is deny-column gated: {err:?}"
    );
    assert_eq!(
        read_gadgets(&cp, &pool, &subj).await["objects"]
            .as_array()
            .map(Vec::len),
        Some(0),
        "denied write wrote nothing"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn computed_value_is_constraint_gated() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer_with_total_constraints(
        fx,
        PropertyConstraints {
            range: Some(RangeConstraint {
                min: None,
                max: Some(100.0),
            }),
            ..PropertyConstraints::default()
        },
    )
    .await;

    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty * unitPrice")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // qty=100 * unitPrice=2.5 -> total=250, exceeding the declared max of 100.
    let err = run_action(
        "create",
        json!({"id": "1", "qty": "100", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::ConstraintViolation(_)),
        "expected ConstraintViolation, got {err:?}"
    );
    assert_eq!(
        read_gadgets(&cp, &pool, &subj).await["objects"]
            .as_array()
            .map(Vec::len),
        Some(0),
        "constraint-violating computed write wrote nothing"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_action_computes_patched_value() {
    let fx = PgFixture::shared();
    let GadgetWriter {
        cp,
        pool,
        gadget,
        subj,
        engine,
        role: _,
        _eg,
        warehouse,
    } = setup_gadget_writer(fx).await;

    // Seed one gadget via a plain computed insert.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("create".into()),
            gadget.clone(),
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty * unitPrice")],
        ))
        .await
        .unwrap();
    // Update action: identity `id` bound by a required `key` param; `qty`/`unitPrice` are
    // re-supplied and `total` is recomputed from the PATCHED values.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("recompute".into()),
            gadget,
            ActionKind::Update,
            vec![
                param("key", "Long", true, Some("id")),
                param("qty", "Long", false, None),
                param("unitPrice", "Double", false, None),
            ],
            vec![Assignment::expr("total", "qty * unitPrice")],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    run_action(
        "create",
        json!({"id": "1", "qty": "4", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert runs");

    run_action(
        "recompute",
        json!({"key": "1", "qty": "8", "unitPrice": 2.5})
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update via computed assignment runs");

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(
        got["objects"].as_array().map(Vec::len),
        Some(1),
        "still one row"
    );
    assert_eq!(got["objects"][0]["total"], json!(20.0));
    drop(warehouse);
}

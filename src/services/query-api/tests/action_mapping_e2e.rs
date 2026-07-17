//! Action param->property mapping e2e (slice 1): a parameter can bind a differently-named
//! property (`binds`), and a property can be filled by a declared constant assignment. Drives
//! the in-process action path (`run_action`) exactly like `action_e2e.rs` — real Postgres +
//! Iceberg — and reads the result back through the governed read path. Proves (1) a renamed
//! param writes its bound property, (2) a constant fills a property (incl. a REQUIRED one), (3)
//! the resolved row is governed identically whether a column is filled by a constant or a
//! renamed param, and (4) an UPDATE targets/patches via the bound property.

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, Assignment, ControlPlane, Effect, ObjectType,
    Policy, PolicyTarget, PropertyDef, RoleId, SubjectId, TypeName,
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

/// A booted fixture with a fully-granted writer over a three-column `main.gadget`
/// (`id` Long required + identity, `name` String, `status` String).
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
    let p = PropertyDef::new(name, ty);
    if required { p.required() } else { p }
}

fn param(
    name: &str,
    ty: &str,
    required: bool,
    binds: Option<&str>,
) -> control_plane_core::ParamDef {
    let mut p = control_plane_core::ParamDef::new(name, ty);
    if required {
        p = p.required();
    }
    if let Some(b) = binds {
        p = p.binds(b);
    }
    p
}

/// Boot a fixture, define the `Gadget` type, grant Write+Read to a `writer` subject, and
/// attach the engine action client. Actions are defined per-test (they differ).
async fn setup_gadget_writer(fx: &PgFixture) -> GadgetWriter {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let gadget = TypeName("Gadget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Gadget", ("main", "gadget"))
                .add_prop(prop("id", "Long", true))
                .add_prop(prop("name", "String", false))
                .add_prop(prop("status", "String", false))
                .identity("id")
                .done(),
        )
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
        gc_retention: e2e_support::TEST_GC_RETENTION,
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
async fn rename_param_writes_bound_property() {
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

    // `displayName` binds the `name` property; `id` covers itself.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("createGadget".into()),
            gadget,
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("displayName", "String", false, Some("name")),
            ],
            vec![],
        ))
        .await
        .unwrap();

    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    let (outcome, _run, _kind) = run_action(
        "createGadget",
        json!({ "id": "7", "displayName": "Widget A" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("rename action runs");
    let created = match outcome {
        query_api::action::ActionOutcome::Single(rows) => rows,
        query_api::action::ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
    };

    // Returned object is keyed by PROPERTY: the bound `name`, never the param `displayName`.
    let created_json = objects_to_json(&created, None);
    assert_eq!(created_json["objects"][0]["name"], json!("Widget A"));
    assert!(
        created_json["objects"][0].get("displayName").is_none(),
        "no param-named column leaks into the object"
    );

    // Reads back with the value written through the bound property.
    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["id"], json!("7"));
    assert_eq!(got["objects"][0]["name"], json!("Widget A"));
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn constants_fill_properties_including_a_required_one() {
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

    // A REQUIRED property (`id`) covered ONLY by a constant; `status` also constant-filled;
    // only `displayName`->`name` comes from the body.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("makeGadget".into()),
            gadget,
            ActionKind::Insert,
            vec![param("displayName", "String", false, Some("name"))],
            vec![
                Assignment::constant("id", json!("7")),
                Assignment::constant("status", json!("active")),
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
        "makeGadget",
        json!({ "displayName": "Widget B" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("constant-fill action runs");

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(
        got["objects"][0]["id"],
        json!("7"),
        "required id constant-filled"
    );
    assert_eq!(got["objects"][0]["name"], json!("Widget B"));
    assert_eq!(
        got["objects"][0]["status"],
        json!("active"),
        "status constant-filled"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn resolved_row_is_governed_identically_for_constant_and_renamed_param() {
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

    // Two actions that both write `status`: one via a constant, one via a renamed param.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("constStatus".into()),
            gadget.clone(),
            ActionKind::Insert,
            vec![param("id", "Long", true, None)],
            vec![Assignment::constant("status", json!("active"))],
        ))
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("paramStatus".into()),
            gadget.clone(),
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("state", "String", false, Some("status")),
            ],
            vec![],
        ))
        .await
        .unwrap();

    // Write policy DENIES the `status` column.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(gadget),
            row_filter: None,
            deny_columns: vec!["status".into()],
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

    // Constant-filled `status` -> denied on the resolved column.
    let err = run_action(
        "constStatus",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "status"),
        "constant-filled status is deny-column gated: {err:?}"
    );

    // Renamed-param-filled `status` -> the SAME denial (identical governance).
    let err = run_action(
        "paramStatus",
        json!({ "id": "2", "state": "active" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "status"),
        "renamed-param-filled status is deny-column gated: {err:?}"
    );

    // Nothing landed (both denied before the write).
    assert_eq!(
        read_gadgets(&cp, &pool, &subj).await["objects"]
            .as_array()
            .map(Vec::len),
        Some(0),
        "denied writes wrote nothing"
    );
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_targets_and_patches_via_bound_property() {
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

    // Seed one Gadget via a plain insert action.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("createGadget".into()),
            gadget.clone(),
            ActionKind::Insert,
            vec![
                param("id", "Long", true, None),
                param("name", "String", false, None),
            ],
            vec![],
        ))
        .await
        .unwrap();
    // Update action: identity `id` bound by a renamed required param `key`; `displayName`->`name`.
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("renameGadget".into()),
            gadget,
            ActionKind::Update,
            vec![
                param("key", "Long", true, Some("id")),
                param("displayName", "String", false, Some("name")),
            ],
            vec![],
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
        "createGadget",
        json!({ "id": "7", "name": "Original" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert runs");

    // Patch via the bound identity + bound name property.
    let (outcome, _run, _kind) = run_action(
        "renameGadget",
        json!({ "key": "7", "displayName": "Renamed" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update via binds runs");
    let updated = match outcome {
        query_api::action::ActionOutcome::Single(rows) => rows,
        query_api::action::ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
    };
    assert_eq!(
        objects_to_json(&updated, None)["objects"][0]["name"],
        json!("Renamed")
    );

    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(
        got["objects"].as_array().map(Vec::len),
        Some(1),
        "still one row"
    );
    assert_eq!(got["objects"][0]["id"], json!("7"));
    assert_eq!(
        got["objects"][0]["name"],
        json!("Renamed"),
        "patched via bound property"
    );
    drop(warehouse);
}

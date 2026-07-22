//! Atomic multi-target write seam (`ActionEngine::write_steps`) e2e: one call stages
//! N per-target writes AND one lineage event in ONE transaction (one snapshot), so
//! every target and the lineage land or roll back together. Drives the real wire-backed
//! `EngineActionClient` (client -> proto -> engine server -> `action_writer::write_steps`
//! -> the `begin_table` composition), reads both targets back through the in-process
//! DataFusion serving engine, and asserts the single lineage event lists both outputs.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{
    ControlPlane, DatasetRef, EventType, LineageEvent, ObjectType, PageReq, RunId, TableRef,
    TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{ActionEngine, SqlValue, StepWrite, WriteMode};
use serde_json::json;

/// Define an object type `name`/`table` with `id Long required` + `label String`.
async fn define_pair_type(cp: &PgControlPlane, type_name: &str, table_name: &str) -> TypeName {
    let ty = TypeName(type_name.into());
    cp.ontology()
        .define_type(
            ObjectType::build(type_name, ("main", table_name))
                .prop_req("id", "Long")
                .prop("label", "String")
                .done(),
        )
        .await
        .unwrap();
    ty
}

async fn read_labels(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    type_name: &str,
) -> serde_json::Value {
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    // Read as the `reader` subject the test grants Read to on both types.
    let subj = control_plane_core::SubjectId("reader".into());
    let rows = read_object(
        &ObjectQuery {
            type_name: type_name.into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj),
        &qdeps,
    )
    .await
    .unwrap();
    objects_to_json(&rows, None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_steps_lands_two_targets_and_one_lineage_atomically() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let alpha = define_pair_type(&cp, "Alpha", "alpha").await;
    let beta = define_pair_type(&cp, "Beta", "beta").await;
    // Grant Read on both to the single `reader` subject used by read_object.
    define_reader(&cp, "reader").await;
    for ty in [&alpha, &beta] {
        grant_read(&cp, ty, "reader").await;
    }

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;

    let alpha_table = TableRef {
        schema: "main".into(),
        name: "alpha".into(),
    };
    let beta_table = TableRef {
        schema: "main".into(),
        name: "beta".into(),
    };

    let run_id = RunId(uuid::Uuid::new_v4());
    let event = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&alpha), DatasetRef::from(&beta)],
        payload: json!({}),
    };

    let writes = vec![
        StepWrite {
            table: alpha_table.clone(),
            columns: vec!["id".into(), "label".into()],
            rows: vec![vec![SqlValue::Int(1), SqlValue::Text("a1".into())]],
            logical_types: vec!["Long".into(), "String".into()],
            nullable: vec![false, true],
            mode: WriteMode::Append,
        },
        StepWrite {
            table: beta_table.clone(),
            columns: vec!["id".into(), "label".into()],
            rows: vec![
                vec![SqlValue::Int(10), SqlValue::Text("b10".into())],
                vec![SqlValue::Int(11), SqlValue::Text("b11".into())],
            ],
            logical_types: vec!["Long".into(), "String".into()],
            nullable: vec![false, true],
            mode: WriteMode::Append,
        },
    ];

    engine
        .write_steps(&writes, event, &[])
        .await
        .expect("write_steps commits both targets atomically");

    // (a) Both tables read back their rows through the Iceberg serving engine.
    let alpha_json = read_labels(&cp, &pool, "Alpha").await;
    assert_eq!(
        alpha_json["objects"],
        json!([{ "id": "1", "label": "a1" }]),
        "Alpha has its one appended row"
    );
    let beta_json = read_labels(&cp, &pool, "Beta").await;
    let mut beta_labels: Vec<String> = beta_json["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["label"].as_str().unwrap().to_string())
        .collect();
    beta_labels.sort();
    assert_eq!(
        beta_labels,
        vec!["b10", "b11"],
        "Beta has its two appended rows"
    );

    // (b) Exactly ONE lineage event for the run, listing BOTH targets as outputs.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        events.items.len(),
        1,
        "one lineage event for the whole write"
    );
    let mut outs = events.items[0].outputs.clone();
    outs.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let mut want = vec![DatasetRef::from(&alpha), DatasetRef::from(&beta)];
    want.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(outs, want, "the single event lists both step targets");

    drop(warehouse);
}

/// Define the subject `name` once (before any per-type grants).
async fn define_reader(cp: &PgControlPlane, name: &str) {
    use control_plane_core::{Acl, SubjectId};
    cp.define_subject(&SubjectId(name.into())).await.unwrap();
}

/// Grant Read on `ty` to the already-defined subject `name` (via a per-type role).
async fn grant_read(cp: &PgControlPlane, ty: &TypeName, name: &str) {
    use control_plane_core::{Acl, Action, Effect, PolicyTarget, RoleId, SubjectId};
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-readers-{}", ty.0));
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(ty.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
}

//! View-aware typed-write e2e: a type bound to a catalog VIEW writes through the
//! view's physical BASE, gated by the view predicate. The engine writer is purely
//! physical (enforces nothing), so ALL view→base resolution and predicate gating
//! happens in `query-api`'s `action.rs` BEFORE the RPC. This proves:
//!   1. an in-view insert lands in the BASE (never mints a mirror under the view name);
//!   2. an insert escaping the view predicate is Forbidden (base unchanged);
//!   3. a PATCH moving a row out of the view is denied; an in-view PATCH succeeds;
//!   4. delete-by-identity only reaches in-view rows (an out-of-view identity is 404).
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
//! Spec: docs/superpowers/specs/2026-07-11-catalog-views-design.md

use control_plane_core::{
    ActionDef, ActionKind, Catalog, CompareOp, ControlPlane, ObjectType, RowFilter, ScalarValue,
    TypeName, ViewDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, grant_writer, read_widget, tref};
use query_api::action::{ActionDeps, ActionError, ActionOutcome, run_action};
use query_api::serving::{ServingEngine, SqlValue};
use serde_json::json;

/// Seed `main.widget(id long, name string, qty long, region string)` with the given
/// rows, then define the view `gov.widget_eu = main.widget WHERE region = 'EU'`, bind a
/// `Widget` type over the VIEW, define its create/update/delete actions, and grant a
/// `writer` subject Write+Read. Returns the writer subject; the caller must keep the
/// returned `IcebergWriter` alive (its `TempDir` holds the base's Parquet warehouse).
#[expect(
    clippy::too_many_arguments,
    reason = "test seed helper threads the fixture handles plus the four seed-row columns"
)]
async fn setup_view_widget(
    fx: &PgFixture,
    cp: &PgControlPlane,
    db: &str,
    pool: &sqlx::PgPool,
    ids: &[i64],
    names: &[&str],
    qtys: &[i64],
    regions: &[&str],
) -> (control_plane_core::SubjectId, IcebergWriter) {
    let writer = IcebergWriter::new(pool.clone(), fx.pg_dsn(db));
    // All columns nullable so the file schema matches the engine writer's all-nullable
    // action batch (an action write passes SqlValue::Null for any unset property).
    let cols = vec![
        ("id".to_string(), "long".to_string(), true),
        ("name".to_string(), "string".to_string(), true),
        ("qty".to_string(), "long".to_string(), true),
        ("region".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "widget",
            &cols,
            &[
                SeedCol::Long(ids.to_vec()),
                SeedCol::Str(names.to_vec()),
                SeedCol::Long(qtys.to_vec()),
                SeedCol::Str(regions.to_vec()),
            ],
        )
        .await;

    // The view over the physical base: only 'EU' rows are in scope.
    IcebergCatalog::new(pool.clone())
        .define_view(ViewDef {
            view: tref("gov", "widget_eu"),
            base: tref("main", "widget"),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("EU".into()),
            }),
            columns: None,
        })
        .await
        .expect("define_view");

    // Bind a type DIRECTLY to the physical base, identity `id`. This gives `main.widget`
    // an ontology identity, so every scan of it (the raw base oracle below AND the engine's
    // view expansion) applies the identity-dedup merge — which is what makes a COW
    // tombstone/`+U` inline shadow actually supersede a base file row. Without it a
    // view-bound-only base would have no dedup key and the shadow would never merge.
    cp.ontology()
        .define_type(
            ObjectType::build("WidgetBase", ("main", "widget"))
                .prop_req("id", "Long")
                .prop("name", "String")
                .prop("qty", "Long")
                .prop_req("region", "String")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type base");

    // Bind the action type to the VIEW ref (not the base). Every type property is in the
    // base schema, so the projectionless view exposes them all.
    cp.ontology()
        .define_type(
            ObjectType::build("Widget", ("gov", "widget_eu"))
                .prop_req("id", "Long")
                .prop("name", "String")
                .prop("qty", "Long")
                .prop_req("region", "String")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type");

    cp.ontology()
        .define_action(
            ActionDef::build("createWidget", "Widget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("name", "String")
                .param("qty", "Long")
                .param_req("region", "String")
                .done(),
        )
        .await
        .expect("define createWidget");
    cp.ontology()
        .define_action(
            ActionDef::build("updateWidget", "Widget", ActionKind::Update)
                .param_req("id", "Long")
                .param("qty", "Long")
                .param("region", "String")
                .done(),
        )
        .await
        .expect("define updateWidget");
    cp.ontology()
        .define_action(
            ActionDef::build("deleteWidget", "Widget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .expect("define deleteWidget");

    let subj = grant_writer(cp, &TypeName("Widget".into())).await;
    (subj, writer)
}

/// The live `id`s in the physical BASE `main.widget` (a direct base scan — unaffected by
/// any view), sorted. Proves where a write physically lands.
async fn base_ids(serving: &InProcessServingEngine) -> Vec<i64> {
    let rows = serving
        .fetch_rows("SELECT \"id\" FROM \"main\".\"widget\"", &[], None)
        .await
        .expect("base scan");
    let mut ids: Vec<i64> = rows
        .rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(SqlValue::Int(i)) => Some(*i),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Is there a physical mirror table under the view's own name? A correct view-aware
/// write NEVER mints one (it targets the base); the unfixed path would.
async fn view_mirror_exists(pool: &sqlx::PgPool) -> bool {
    let mut conn = pool.acquire().await.expect("acquire");
    control_plane_postgres::iceberg_mirror::live_table_id(&mut conn, "gov", "widget_eu")
        .await
        .expect("live_table_id")
        .is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_inside_view_lands_in_base_and_reads_back() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Seed one out-of-view (US) row so the base table exists; the view starts empty.
    let (subj, _writer) =
        setup_view_widget(fx, &cp, &db, &pool, &[99], &["seed"], &[0], &["US"]).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let (outcome, _run, _kind) = run_action(
        "createWidget",
        json!({ "id": "1", "name": "gadget", "qty": "3", "region": "EU" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("in-view insert succeeds");
    match outcome {
        ActionOutcome::Single(_) => {}
        ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
    }

    // The row landed in the physical BASE (main.widget), alongside the seed row.
    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert_eq!(
        base_ids(&base_serving).await,
        vec![1, 99],
        "the EU insert landed in the physical base main.widget"
    );

    // NO physical mirror table was minted under the view name — the bug this test catches.
    assert!(
        !view_mirror_exists(&pool).await,
        "a view-aware write must not mint a mirror table under the view name"
    );

    // Readable back through the view-bound type; the out-of-view seed row is invisible.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_some(),
        "the in-view row is readable through the view-bound type"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 99).await.is_none(),
        "the out-of-view seed row is not visible through the view"
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_escaping_view_predicate_is_forbidden() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let (subj, _writer) =
        setup_view_widget(fx, &cp, &db, &pool, &[99], &["seed"], &[0], &["US"]).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let err = run_action(
        "createWidget",
        json!({ "id": "2", "name": "escapee", "qty": "1", "region": "US" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("a row outside the view predicate is refused");
    assert!(
        matches!(err, ActionError::Forbidden),
        "an escaping insert is Forbidden (403), got {err:?}"
    );

    // Nothing was written to the base.
    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert_eq!(
        base_ids(&base_serving).await,
        vec![99],
        "base row count is unchanged by the refused insert"
    );
    assert!(
        !view_mirror_exists(&pool).await,
        "the refused insert minted no mirror table"
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_moving_row_out_of_view_is_forbidden() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let (subj, _writer) =
        setup_view_widget(fx, &cp, &db, &pool, &[99], &["seed"], &[0], &["US"]).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // An in-view row to mutate.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "gadget", "qty": "3", "region": "EU" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("in-view insert");

    // PATCH region -> 'US' would move the row out of the writer's view — refused.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "region": "US" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("a PATCH moving the post-image out of the view is refused");
    assert!(
        matches!(err, ActionError::WriteDenied(_) | ActionError::Forbidden),
        "moving a row out of the view is denied (403), got {err:?}"
    );

    // The row still reads back in-view, unchanged.
    let before = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("still in view");
    assert_eq!(before["region"], json!("EU"));

    // A PATCH that keeps the row in-view (qty only) succeeds.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "5" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("an in-view PATCH succeeds");
    let after = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("still in view");
    assert_eq!(after["qty"], json!("5"), "the in-view PATCH applied");
    assert_eq!(after["region"], json!("EU"), "region unchanged");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_targets_only_in_view_rows() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Seed one EU (in-view) + one US (out-of-view) row directly in the base.
    let (subj, _writer) = setup_view_widget(
        fx,
        &cp,
        &db,
        &pool,
        &[10, 20],
        &["eu", "us"],
        &[1, 2],
        &["EU", "US"],
    )
    .await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // The out-of-view US row is invisible to identity targeting through the view -> 404.
    let err = run_action(
        "deleteWidget",
        json!({ "id": "20" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("an out-of-view identity is not found through the view");
    assert!(
        matches!(err, ActionError::NotFound),
        "deleting an out-of-view row is NotFound (404), got {err:?}"
    );

    // The in-view EU row deletes fine.
    run_action(
        "deleteWidget",
        json!({ "id": "10" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("the in-view row deletes");

    // Base now holds only the untouched US row; the view is empty.
    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert_eq!(
        base_ids(&base_serving).await,
        vec![20],
        "only the in-view EU row was removed; the US row is untouched"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 10).await.is_none(),
        "the deleted EU row is gone from the view"
    );

    drop(warehouse);
}

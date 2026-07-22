//! View-aware typed-write e2e: a type bound to a catalog VIEW writes through the
//! view's physical BASE, gated by the view predicate. The engine writer is purely
//! physical (enforces nothing), so ALL view→base resolution and predicate gating
//! happens in `query-api`'s `action.rs` BEFORE the RPC. This proves:
//!   1. an in-view insert lands in the BASE (never mints a mirror under the view name);
//!   2. an insert escaping the view predicate is Forbidden (base unchanged);
//!   3. a PATCH moving a row out of the view is denied; an in-view PATCH succeeds;
//!   4. delete-by-identity only reaches in-view rows (an out-of-view identity is 404);
//!   5. the MULTI-STEP mutate path (which reads the BASE, not the view, so it CAN locate
//!      an out-of-view identity) surfaces that as `NotFound` too — not `WriteDenied` — so
//!      it is not an existence oracle through the view; an in-view multi-step update
//!      still succeeds.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
//! Spec: docs/superpowers/specs/2026-07-13-catalog-views-design.md

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ActionStep, Catalog, CompareOp, ControlPlane,
    Effect, ObjectType, ParamDef, PolicyTarget, RoleId, RowFilter, ScalarValue, TypeName, ViewDef,
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
    // Column nullability matches the `Widget` type's declared shape (identity `id` +
    // required `region` are non-null; `name`/`qty` optional), so the seeded base schema
    // agrees with the honest per-column nullability an action write now declares (#359).
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), true),
        ("qty".to_string(), "long".to_string(), true),
        ("region".to_string(), "string".to_string(), false),
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

    // NO base-bound type: identity for `main.widget`'s merge-on-read must resolve
    // purely through the VIEW-bound `Widget` type below (`identity_for_table` matches
    // a type bound to a view whose base is the scanned ref). This is the I-1 regression
    // guard — with only a view-bound type present, a PATCH's inline delta must still
    // shadow the old base file row and a DELETE tombstone must still hide it.

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

/// The live `qty` for `id` in the physical BASE `main.widget` (a direct base scan,
/// unaffected by any view). `None` if no live row matches. Used to prove an out-of-view
/// multi-step mutate attempt left the base row untouched.
async fn base_widget_qty(serving: &InProcessServingEngine, id: i64) -> Option<i64> {
    let rows = serving
        .fetch_rows(
            &format!("SELECT \"qty\" FROM \"main\".\"widget\" WHERE \"id\" = {id}"),
            &[],
            None,
        )
        .await
        .expect("base scan");
    rows.rows.first().and_then(|r| match r.first() {
        Some(SqlValue::Int(i)) => Some(*i),
        _ => None,
    })
}

/// EVERY live `qty` for `id` in the physical BASE `main.widget` (a direct base scan).
/// Unlike [`base_widget_qty`] this returns the whole set, so a caller can assert the
/// merge-on-read collapsed a PATCH's inline delta and the old file row to ONE row: if
/// identity dedup were not resolving through the view, an id would surface twice (the
/// stale file value AND the delta).
async fn base_widget_qtys(serving: &InProcessServingEngine, id: i64) -> Vec<i64> {
    let rows = serving
        .fetch_rows(
            &format!("SELECT \"qty\" FROM \"main\".\"widget\" WHERE \"id\" = {id}"),
            &[],
            None,
        )
        .await
        .expect("base scan");
    rows.rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(SqlValue::Int(i)) => Some(*i),
            _ => None,
        })
        .collect()
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

/// Multi-step mutate must not become an existence oracle through a view. Unlike the
/// single-object path (which locates its target through the VIEW name, so an out-of-view
/// identity is simply invisible ⇒ 404), the multi-step path's Update/Delete step
/// (`govern_and_build_mutate`) reads the physical BASE (its whole-table `Overwrite` would
/// otherwise drop every out-of-view row) — so it CAN locate an out-of-view identity there.
/// Before this fix that surfaced as `WriteDenied` (403) via the view-predicate leg-1 check
/// in `mutate_governance`, which — unlike a 404 — confirms to the caller that a row exists.
/// This test drives a genuine multi-step action (an Insert on an unrelated `Note` type
/// alongside an Update on the view-bound `Widget` type) so the Update rides
/// `govern_and_build_mutate`, not the single-object inline-delta path, and asserts:
///   - an out-of-view identity ⇒ `NotFound` (404), not `WriteDenied` (403);
///   - the base row is left untouched (the whole action is all-or-nothing);
///   - an in-view identity through the same multi-step action still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_step_update_out_of_view_identity_is_not_found() {
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

    // An unrelated second type + action step, so the Update genuinely rides the
    // multi-step file-tier path (`govern_and_build_mutate`) rather than being
    // single-stepped down to `run_mutate`.
    cp.ontology()
        .define_type(
            ObjectType::build("Note", ("main", "note"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type Note");
    cp.grant(
        &RoleId("writers".into()),
        Action::Write,
        PolicyTarget::Type(TypeName("Note".into())),
        Effect::Allow,
    )
    .await
    .expect("grant Note write");

    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("noteAndUpdateWidget".into()),
            steps: vec![
                ActionStep {
                    target: TypeName("Note".into()),
                    kind: ActionKind::Insert,
                    parameters: vec![ParamDef {
                        name: "noteId".into(),
                        ty: "Long".into(),
                        required: true,
                        binds: Some("id".into()),
                        description: None,
                    }],
                    assignments: vec![],
                    bind: None,
                },
                ActionStep {
                    target: TypeName("Widget".into()),
                    kind: ActionKind::Update,
                    parameters: vec![
                        ParamDef {
                            name: "wId".into(),
                            ty: "Long".into(),
                            required: true,
                            binds: Some("id".into()),
                            description: None,
                        },
                        ParamDef {
                            name: "wQty".into(),
                            ty: "Long".into(),
                            required: false,
                            binds: Some("qty".into()),
                            description: None,
                        },
                    ],
                    assignments: vec![],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
            description: None,
        })
        .await
        .expect("define noteAndUpdateWidget");

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // The out-of-view US row (id=20): the multi-step Update step reads the BASE and CAN
    // locate it — must surface `NotFound`, never `WriteDenied` (no existence oracle).
    let err = run_action(
        "noteAndUpdateWidget",
        json!({ "noteId": "501", "wId": "20", "wQty": "99" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("an out-of-view identity in a multi-step update is not found");
    assert!(
        matches!(err, ActionError::NotFound),
        "multi-step update targeting an out-of-view identity is NotFound (404), \
         not WriteDenied (403) — no existence oracle through the view, got {err:?}"
    );

    // All-or-nothing: nothing was written (the base row is untouched by the refused step).
    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert_eq!(
        base_widget_qty(&base_serving, 20).await,
        Some(2),
        "the out-of-view row's qty is untouched by the refused multi-step update"
    );

    // The in-view EU row (id=10) through the SAME multi-step action still succeeds.
    run_action(
        "noteAndUpdateWidget",
        json!({ "noteId": "502", "wId": "10", "wQty": "55" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("an in-view multi-step update succeeds");
    let after = read_widget(&cp, &pool, &subj, 10)
        .await
        .expect("still in view");
    assert_eq!(
        after["qty"],
        json!("55"),
        "the in-view multi-step update applied"
    );

    drop(warehouse);
}

/// I-1 regression: with ONLY a view-bound type present (no base-bound crutch type),
/// merge-on-read identity must still resolve for the physical base — so a PATCH's
/// inline `+U` delta SHADOWS the old base file row (one row survives, the new value)
/// and a DELETE tombstone HIDES the row entirely. `setup_view_widget` deliberately
/// binds no type to `main.widget`; `identity_for_table` resolves `id` for the base via
/// the `dataset_view.view` whose base is `main.widget`. If that resolution regressed,
/// the PATCH would serve TWO base rows for the id and the DELETE would not hide it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_bound_patch_shadows_and_delete_hides_on_base() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Seed one in-view EU row directly in the base (id=7, qty=3).
    let (subj, _writer) =
        setup_view_widget(fx, &cp, &db, &pool, &[7], &["gadget"], &[3], &["EU"]).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // In-view PATCH qty 3 -> 5. The inline delta must shadow the qty=3 file row.
    run_action(
        "updateWidget",
        json!({ "id": "7", "qty": "5" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("in-view PATCH");

    // The raw physical base scan sees id=7 EXACTLY ONCE, with the patched value —
    // proof the merge deduped the old file row against the inline delta with no
    // base-bound type present (view-resolved identity).
    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert_eq!(
        base_widget_qtys(&base_serving, 7).await,
        vec![5],
        "PATCH's inline delta shadows the old base row (identity resolved through the view)"
    );
    assert_eq!(
        read_widget(&cp, &pool, &subj, 7).await.expect("in view")["qty"],
        json!("5"),
        "the patched value reads back through the view-bound type"
    );

    // DELETE id=7 — the tombstone must hide the row from the physical base entirely.
    run_action(
        "deleteWidget",
        json!({ "id": "7" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("in-view delete");

    let base_serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    assert!(
        base_ids(&base_serving).await.is_empty(),
        "DELETE tombstone hides the base row (identity resolved through the view)"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 7).await.is_none(),
        "the deleted row is gone from the view too"
    );

    drop(warehouse);
}

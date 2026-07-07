//! UPDATE/DELETE actions e2e: define a `Widget(id Long identity, name String, qty Long)`
//! type with an `updateWidget` (PATCH) and a `deleteWidget` action, grant Write+Read, and
//! exercise the whole-table copy-on-write mutate path through `run_action`:
//!   1. UPDATE merges only the named columns (PATCH) — unset columns are retained.
//!   2. DELETE removes the located row.
//!   3. A mutate on an absent identity returns `ActionError::NotFound`.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::ControlPlaneError;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, define_widget, grant_writer, read_widget};
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::render::objects_to_json;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_merges_named_columns() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, name:"a", qty:1}.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // UPDATE {id:1, qty:9} — name is NOT a param, so it must be retained (PATCH).
    let (outcome, _run, _kind) = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs");
    let affected = match outcome {
        query_api::action::ActionOutcome::Single(rows) => rows,
        query_api::action::ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
    };
    // The affected object returns the new version.
    let affected_json = objects_to_json(&affected, None);
    assert_eq!(affected_json["objects"][0]["qty"], json!("9"));
    assert_eq!(affected_json["objects"][0]["name"], json!("a"));

    // Read back: qty updated to 9, name retained as "a".
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present");
    assert_eq!(row["qty"], json!("9"), "qty updated");
    assert_eq!(row["name"], json!("a"), "name retained (PATCH)");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_row() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed two rows so the table stays non-empty after deleting one (an empty table is
    // unregistered in the serving engine, which would make the read-back error rather than
    // report zero rows — the truncate-to-empty case is covered by overwrite_table_e2e).
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2");
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_some(),
        "row 1 present before delete"
    );

    // DELETE {id:1}.
    run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete runs");

    // Read back: no row with id=1, but row 2 is untouched.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_none(),
        "row 1 removed after delete"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 2).await.is_some(),
        "row 2 retained after deleting row 1"
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_found() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed one unrelated row so the table exists and the read returns rows.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // DELETE an absent identity -> NotFound.
    let err = run_action(
        "deleteWidget",
        json!({ "id": "999" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::NotFound),
        "mutate on absent identity is NotFound: {err:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Corrupt-PK guard: a duplicated identity (two live rows with id=1 — the insert
// path append-writes without PK enforcement, so corrupt/landed data can carry
// duplicates) is a corrupt invariant. The mutate's targeted read must surface it
// as a Backend fault (the operator's 500) — NOT NotFound, and NOT a silent
// pick-one mutate.
//
// The duplicate must be in the FILE tier (`inline_byte_limit = 0`): the O(change)
// mutate reads through the identity-aware merge view, which deterministically
// resolves duplicate *inline* rows to the latest version — so only duplicate live
// *file* rows (which the merge cannot dedup — file rows synthesize a single
// precedence 0) remain a corrupt-PK invariant the guard must catch.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_identity_is_a_backend_fault() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // FILE tier: each insert lands as its own Parquet file, so two inserts of id=1
    // leave two live file rows the merge view cannot dedup.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed the SAME identity twice (two file rows).
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1");
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "b", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2 (duplicate id)");

    // UPDATE {id:1}: two live matches -> corrupt-PK Backend fault, not NotFound.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::ControlPlane(ControlPlaneError::Backend(_))
        ),
        "expected a Backend fault for a duplicated identity, got: {err:?}"
    );
    assert!(
        err.to_string().contains("matches more than one live row"),
        "fault message names the invariant, got: {err}"
    );

    drop(warehouse);
}

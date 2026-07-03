//! End-to-end tests for the O(change) inline-shadow copy-on-write mutate path
//! (`run_action` → `run_mutate`). Unlike the whole-table COW these replace, an
//! UPDATE/DELETE here writes ONE inline delta row that shadows (UPDATE) or tombstones
//! (DELETE) the object at read time, WITHOUT rewriting the table's Parquet files.
//!
//! Each test forces the FILE tier (`inline_byte_limit = 0`) so the seed row lands as
//! Parquet; the assertions then prove the mutation did not touch the `data_file` set
//! (no rewrite) and that the merged read reflects the inline shadow/tombstone.
//!
//! The write goes over the real wire engine (`spawn_engine_writer`, a UDS-hosted
//! `EngineControl`); the read goes through the in-process Iceberg/DataFusion engine's
//! identity-aware merge view — the same split as production.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, count_live_inline_rows, data_file_paths, define_widget, grant_writer,
    read_widget,
};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

// ---------------------------------------------------------------------------
// Test 1 — an UPDATE on a file-resident row writes an inline shadow that wins on
//          read, WITHOUT rewriting the Parquet file (the data_file set is invariant).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_shadows_file_row_without_rewrite() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // inline_byte_limit = 0 forces the FILE tier: createWidget lands as Parquet.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} → file-resident.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (file tier)");

    // Record the live Parquet file set BEFORE the update.
    let files_before = data_file_paths(&pool, "main", "widget").await;
    assert!(
        !files_before.is_empty(),
        "createWidget produced at least one Parquet file (file tier)"
    );

    // UPDATE {id:1, qty:9} → one inline delta shadows the file row.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs");

    // The merged read reflects the inline shadow (qty == 9).
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present after update");
    assert_eq!(row["qty"], json!("9"), "inline version shadows the file row");

    // No Parquet was rewritten: the live data_file set is byte-for-byte unchanged.
    let files_after = data_file_paths(&pool, "main", "widget").await;
    assert_eq!(
        files_before, files_after,
        "data_file set unchanged — no whole-table rewrite"
    );

    // Exactly one inline delta row was added (the shadow).
    assert_eq!(
        count_live_inline_rows(&pool, "main", "widget").await,
        1,
        "exactly one live inline row (the shadow) after the update"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 2 — a DELETE writes a tombstone inline row that hides the targeted file row
//          on read, leaving sibling rows and the Parquet file set untouched.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_via_tombstone_hides_row() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // FILE tier: both seed rows land as Parquet.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed file rows {id:1} and {id:2}.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1 (file tier)");
    run_action(
        "createWidget",
        json!({ "id": "2", "qty": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2 (file tier)");

    let files_before = data_file_paths(&pool, "main", "widget").await;
    assert!(
        !files_before.is_empty(),
        "seed inserts produced Parquet files (file tier)"
    );

    // deleteWidget {id:1} → a tombstone inline row hides the file row for id=1.
    run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete runs");

    // id=1 is hidden by the tombstone; id=2 is untouched.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_none(),
        "row 1 hidden by the tombstone"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 2).await.is_some(),
        "row 2 retained (a sibling file row)"
    );

    // No Parquet was rewritten: the live data_file set is unchanged.
    let files_after = data_file_paths(&pool, "main", "widget").await;
    assert_eq!(
        files_before, files_after,
        "data_file set unchanged — the delete added a tombstone, not a rewrite"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3 — the latest inline version wins: two UPDATEs, the greatest begin_snapshot
//          (the most recent) is the merged read result.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_version_wins() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // FILE tier: the seed lands as Parquet, so each UPDATE is a fresh inline delta.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1}.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // Two UPDATEs in sequence: qty 5 then qty 9.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "5" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("first update (qty=5)");
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("second update (qty=9)");

    // The merged read returns the latest version (max begin_snapshot): qty == 9.
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present after updates");
    assert_eq!(row["qty"], json!("9"), "latest inline version wins");

    drop(warehouse);
}

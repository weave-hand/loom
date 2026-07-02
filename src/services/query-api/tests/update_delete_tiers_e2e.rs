//! Storage-tier tests for UPDATE/DELETE actions: time travel (inline tier snapshots
//! are accessible at their original snapshot id after a COW mutation that moves data
//! to the file tier) and file-tier mutations (COW on a row that was written as a
//! Parquet file rather than an inline row).
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use arrow_array::Int64Array;
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, define_widget, grant_writer, read_widget};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

// ---------------------------------------------------------------------------
// Test 6 — time travel: inline rows at S1 are accessible after an UPDATE that
//           moves data to the file tier (overwrite_parquet_snapshot end-caps inline rows).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_time_travel() {
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

    let widget_table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };

    // Seed {id:1, qty:1} as an inline row.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // Capture snapshot S1 (the inline insert).
    let ice = IcebergCatalog::new(pool.clone());
    let s1 = ice.current_snapshot(&widget_table).await.unwrap();

    // UPDATE {id:1, qty:9} — COW moves all data to a Parquet file and end-caps inline rows.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs");

    // Live state: qty=9.
    let live = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present after update");
    assert_eq!(live["qty"], json!("9"), "live qty updated to 9");

    // Time travel: inline rows at S1 still contain the original row (qty=1).
    let inline_at_s1 = ice.inline_live_batch(&widget_table, s1.id).await.unwrap();
    assert!(
        inline_at_s1.is_some(),
        "inline rows at S1 must still exist for time travel"
    );
    let (_tid, _row_ids, batch) = inline_at_s1.unwrap();
    assert_eq!(batch.num_rows(), 1, "exactly one inline row at S1");
    // The `qty` column (index 2: id, name, qty) should read as 1 at S1.
    let qty_col = batch
        .column_by_name("qty")
        .expect("qty column present in inline batch");
    let qty_arr = qty_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("qty is Int64");
    assert_eq!(qty_arr.value(0), 1_i64, "time-travel qty at S1 is 1");

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 7 — time travel: inline rows at S1 include both rows after a DELETE.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_time_travel() {
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

    let widget_table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };

    // Seed two rows as inline rows.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1");
    run_action(
        "createWidget",
        json!({ "id": "2", "qty": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2");

    // Capture snapshot S1 (both rows present inline).
    let ice = IcebergCatalog::new(pool.clone());
    let s1 = ice.current_snapshot(&widget_table).await.unwrap();

    // DELETE {id:1} — COW moves the surviving row to a Parquet file and end-caps inline rows.
    run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete runs");

    // Live state: only id=2 remains.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_none(),
        "row 1 removed after delete"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 2).await.is_some(),
        "row 2 retained after delete"
    );

    // Time travel: both inline rows are accessible at S1.
    let inline_at_s1 = ice.inline_live_batch(&widget_table, s1.id).await.unwrap();
    assert!(
        inline_at_s1.is_some(),
        "inline rows at S1 must still exist for time travel"
    );
    let (_tid, _row_ids, batch) = inline_at_s1.unwrap();
    assert_eq!(
        batch.num_rows(),
        2,
        "both rows accessible at S1 (time travel)"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 8 — COW works on a row that was flushed to the file tier (not inline).
//           Forcing inline_byte_limit=0 makes every INSERT go to Parquet.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutate_flushed_file_row() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // inline_byte_limit=0: every row's byte size exceeds the limit, so INSERT goes to Parquet.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} — goes to Parquet (file tier) because inline_byte_limit=0.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (file tier)");

    // UPDATE {id:1, qty:9} — COW reads the Parquet file, patches the row, rewrites.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update on file-tier row");

    // Read back: qty=9 confirms COW worked on a file-tier row.
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present after update");
    assert_eq!(row["qty"], json!("9"), "qty updated to 9 from file tier");

    drop(warehouse);
}

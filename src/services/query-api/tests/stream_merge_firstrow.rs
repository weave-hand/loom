//! FirstRow merge-engine e2e: "first write wins". Declare `main.widget` CDC with
//! `merge_engine=first_row`, then insert id=1, update id=1, delete id=1, insert
//! id=2. Current-state reads show id=1 at its FIRST value (qty=1 — the update and
//! the delete are ignored for current-state because they are not the earliest
//! event) and id=2 present. A `consolidate_stream` then folds the base to those
//! same first-row winners; the changelog holds every event (engine-agnostic) and
//! is untouched by consolidate; `GET /objects/Widget` is identical before/after.
//!
//! Mirrors `stream_cdc_consolidate.rs`'s fixture/spawn/action/get shape, differing
//! only in the declared engine and the expected winners. loom_fixture_test
//! (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use axum::http::StatusCode;
use control_plane_core::{Catalog, MergeEngine, PageReq, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::has_shadow;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use e2e_support::{InProcessServingEngine, connect_gov_client, define_widget, get, grant_writer};
use loom_test_seed::local_sql_catalog;
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// `(loom_change_kind, id, qty)` for every row of `table`'s live data files.
/// Mirrors `stream_cdc_consolidate.rs`'s `table_rows` helper.
async fn table_rows(
    catalog: &SqlCatalog,
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Vec<(String, i64, i64)> {
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(table).await.expect("current snapshot");
    let files = ice.files(table, snap.id, PageReq::unbounded()).await.expect("files");
    let paths: Vec<String> = files.items.into_iter().map(|f| f.path).collect();
    let (_schema, batches) = read_files_as_batches(catalog, table, &paths)
        .await
        .expect("read batches");
    let mut out = Vec::new();
    for b in &batches {
        out.extend(decode_rows(b));
    }
    out
}

fn decode_rows(b: &RecordBatch) -> Vec<(String, i64, i64)> {
    let kind_idx = b.schema().index_of("loom_change_kind").expect("kind col");
    let id_idx = b.schema().index_of("id").expect("id col");
    let qty_idx = b.schema().index_of("qty").expect("qty col");
    let kinds = b.column(kind_idx).as_any().downcast_ref::<StringArray>().expect("kind str");
    let ids = b.column(id_idx).as_any().downcast_ref::<Int64Array>().expect("id Int64");
    let qtys = b.column(qty_idx).as_any().downcast_ref::<Int64Array>().expect("qty Int64");
    (0..b.num_rows())
        .map(|i| (kinds.value(i).to_string(), ids.value(i), qtys.value(i)))
        .collect()
}

/// id -> qty for every object in a `{"objects":[...]}` body (Long renders as a
/// JSON string for int64 precision).
fn objects_id_qty(body: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
    body["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .map(|o| {
            (
                o["id"].as_str().expect("id str").to_string(),
                o["qty"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn firstrow_first_write_wins_and_delete_is_ignored_for_current_state() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &warehouse.path().display().to_string()).await;

    // Declare `main.widget` CDC keyed on `id` with the FirstRow engine.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "widget", at0).await.expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::FirstRow).await.expect("declare_cdc first_row");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps { cp: &cp, action_engine: &engine, serving: &serving };
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(IcebergCatalog::new(pool.clone())));

    // insert id=1 (qty=1), update id=1 (qty=9), delete id=1, insert id=2 (qty=2).
    run_action("createWidget", json!({ "id": "1", "name": "a", "qty": "1" }).as_object().unwrap(), &subj, &deps).await.expect("insert id=1");
    run_action("updateWidget", json!({ "id": "1", "qty": "9" }).as_object().unwrap(), &subj, &deps).await.expect("update id=1");
    run_action("deleteWidget", json!({ "id": "1" }).as_object().unwrap(), &subj, &deps).await.expect("delete id=1");
    run_action("createWidget", json!({ "id": "2", "name": "b", "qty": "2" }).as_object().unwrap(), &subj, &deps).await.expect("insert id=2");

    let table = TableRef { schema: "main".to_string(), name: "widget".to_string() };
    let clog = TableRef { schema: "main".to_string(), name: "widget__changelog".to_string() };

    // GET /objects/Widget before flush: FirstRow ⇒ id=1 is qty=1 (first write wins;
    // the qty=9 update and the delete are NOT the earliest event, so ignored for
    // current-state); id=2 is present. This is the baseline consolidate must
    // preserve.
    let (status_before, body_before) = get(cp_arc.clone(), read_eng.clone(), "/objects/Widget", "writer").await;
    assert_eq!(status_before, StatusCode::OK, "GET before: {body_before:?}");
    let before = objects_id_qty(&body_before);
    assert_eq!(before.get("1").map(String::as_str), Some("1"), "first write wins (qty=1, not 9): {before:?}");
    assert!(before.contains_key("2"), "id=2 present: {before:?}");

    // Flush so the deltas land in the base as real Parquet.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string()).await.expect("flush_table").expect("flush snapshot");

    let clog_before = table_rows(&catalog, &pool, &clog).await;
    assert!(clog_before.len() >= 4, "changelog holds every event (engine-agnostic): {clog_before:?}");

    assert!(has_shadow(&mut pool.acquire().await.expect("conn"), tid).await.expect("has_shadow"));

    // Consolidate: fold the base by FirstRow (smallest loom_offset per identity).
    let new_snap = gov.consolidate_stream("main".to_string(), "widget".to_string()).await.expect("consolidate_stream");
    assert!(new_snap > 0, "consolidate produced a real snapshot id");

    // Base folds to the first-row winners: id=1's +I (qty=1), id=2's +I (qty=2).
    let mut base_after = table_rows(&catalog, &pool, &table).await;
    base_after.sort_by_key(|r| r.1);
    assert_eq!(
        base_after,
        vec![("+I".to_string(), 1, 1), ("+I".to_string(), 2, 2)],
        "base folds to FirstRow-per-identity winners: {base_after:?}"
    );

    assert!(!has_shadow(&mut pool.acquire().await.expect("conn"), tid).await.expect("has_shadow"));

    // Changelog untouched by consolidate.
    let clog_after = table_rows(&catalog, &pool, &clog).await;
    assert_eq!(clog_after, clog_before, "consolidate never touches the changelog");

    // GET /objects/Widget after consolidate is identical to before.
    let (status_after, body_after) = get(cp_arc.clone(), read_eng.clone(), "/objects/Widget", "writer").await;
    assert_eq!(status_after, StatusCode::OK, "GET after: {body_after:?}");
    assert_eq!(objects_id_qty(&body_after), before, "GET identical before/after consolidate");

    drop(warehouse);
}

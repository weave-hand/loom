//! Regression for the CDC current-state read in the window AFTER a flush but
//! BEFORE consolidation (the bug `build_serving_provider`'s `(Some(f), None)`
//! branch introduced: it returned the raw, un-deduped file provider on the
//! assumption "file rows are already identity-unique", which held pre-CDC but
//! not once a CDC base's dual-write flush lands the `+I/+U/-D` change subset —
//! multiple physical rows per identity, plus `-D` tombstones — straight into the
//! file tier).
//!
//! Drives INSERT id=1 / UPDATE id=1 (qty -> 9) / INSERT id=2 / DELETE id=2
//! through the governed action router, flushes (so the deltas land in the base
//! as real, un-consolidated Parquet), and asserts `GET /objects/Widget`:
//!   * returns EXACTLY one row for id=1, with the latest value (qty=9) — not
//!     duplicated, not the stale `qty=1` version;
//!   * does not resurrect id=2 (its winner was a `-D` delete);
//!   * never leaks a `loom_*` framing column.
//!
//! Mirrors `stream_cdc_consolidate.rs`'s engine-spawning setup, but reads
//! through the router in the mid-window instead of only inspecting raw Parquet
//! rows.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::StreamTables;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{InProcessServingEngine, connect_gov_client, define_widget, get, grant_writer};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// Every top-level key across every object in a `{"objects":[...]}` body — used to
/// prove no `loom_*` column ever leaks through the governed read.
fn all_object_keys(body: &serde_json::Value) -> Vec<String> {
    body["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .flat_map(|o| {
            o.as_object()
                .expect("object is a JSON object")
                .keys()
                .cloned()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_read_after_flush_before_consolidate_dedups_and_hides_deletes() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Declare `main.widget` CDC (keyed on `id`), exactly as `stream_cdc_e2e.rs`/
    // `stream_cdc_consolidate.rs` do.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "widget", at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new(IcebergCatalog::new(pool.clone())),
    );

    // insert id=1, update id=1 (qty -> 9), insert id=2, delete id=2.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("insert id=1");
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update id=1");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("insert id=2");
    run_action(
        "deleteWidget",
        json!({ "id": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete id=2");

    // Flush so the deltas land in the base as real Parquet — but do NOT
    // consolidate. This is the broken window: the base now legitimately carries
    // MULTIPLE physical rows for id=1 (+I, +U) and a -D tombstone for id=2, with
    // no live inline tail to shadow them.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table")
        .expect("flush produced a snapshot");

    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET /objects/Widget in the post-flush/pre-consolidate window: {body:?}"
    );
    let objs = body["objects"].as_array().expect("objects array");
    assert_eq!(
        objs.len(),
        1,
        "exactly one live Widget (id=1) in the broken window \
         — no duplicate per physical file row, no resurrected id=2: {body}"
    );
    assert_eq!(
        objs[0]["id"],
        json!("1"),
        "id=1 is the only survivor: {body}"
    );
    assert_eq!(
        objs[0]["qty"],
        json!("9"),
        "current state reflects the latest (+U) version, not the stale +I: {body}"
    );
    assert!(
        all_object_keys(&body)
            .iter()
            .all(|k| !k.starts_with("loom_")),
        "no loom_* framing column leaks through the governed read: {body}"
    );

    drop(warehouse);
}

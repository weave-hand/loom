//! Versioned merge-engine e2e. Declares `main.vwidget` CDC with
//! `merge_engine=versioned` (the `VWidget` type's `seq` Long column is its
//! version property) and proves three things:
//!  1. **Highest domain version wins regardless of arrival order.** Emit id=1
//!     with versions 7, then 3, then 5 (in that offset order). Versioned picks
//!     seq=7 (highest version) — NOT seq=5, which has the greatest offset (that
//!     is what LastRow would pick, so this distinguishes the engines).
//!  2. **Correctness across consolidate cycles.** After a `consolidate_stream`
//!     folds the base to seq=7, a late event with seq=4 still loses on the next
//!     read (the folded winner's version is preserved and dominates).
//!  3. **Delete-wins.** id=2 (+I seq=1, then -D carrying seq=1) is DROPPED: the
//!     -D wins the version tie by offset, and the identity does not resurrect.
//!
//! The changelog holds every event (engine-agnostic). loom_fixture_test
//! (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use axum::http::StatusCode;
use control_plane_core::{Catalog, MergeEngine, PageReq, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, define_versioned_widget, get, grant_writer,
};
use loom_test_seed::local_sql_catalog;
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// `(loom_change_kind, id, seq)` for every row of `table`'s live data files.
async fn table_rows(
    catalog: &SqlCatalog,
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Vec<(String, i64, i64)> {
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(table).await.expect("current snapshot");
    let files = ice
        .files(table, snap.id, PageReq::unbounded())
        .await
        .expect("files");
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
    let seq_idx = b.schema().index_of("seq").expect("seq col");
    let kinds = b
        .column(kind_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("kind str");
    let ids = b
        .column(id_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id Int64");
    let seqs = b
        .column(seq_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("seq Int64");
    (0..b.num_rows())
        .map(|i| (kinds.value(i).to_string(), ids.value(i), seqs.value(i)))
        .collect()
}

/// id -> seq for every object in a `{"objects":[...]}` body (Long renders as a
/// JSON string for int64 precision).
fn objects_id_seq(body: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
    body["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .map(|o| {
            (
                o["id"].as_str().expect("id str").to_string(),
                o["seq"].as_str().expect("seq str").to_string(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_highest_version_wins_and_late_low_version_loses_and_delete_drops() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &warehouse.path().display().to_string()).await;

    // Declare `main.vwidget` CDC keyed on `id` with the Versioned engine. The
    // VWidget type declares `seq` (Long) as its version property.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "vwidget", at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::Versioned)
        .await
        .expect("declare_cdc versioned");

    let vwidget = define_versioned_widget(&cp).await;
    let subj = grant_writer(&cp, &vwidget).await;

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

    let table = TableRef {
        schema: "main".to_string(),
        name: "vwidget".to_string(),
    };

    // (1) Emit id=1 with versions 7, then 3, then 5 (in arrival/offset order).
    // Highest version (7) arrives FIRST; LastRow would pick seq=5 (greatest
    // offset). Versioned must pick seq=7.
    run_action(
        "createVWidget",
        json!({ "id": "1", "qty": "1", "seq": "7" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("create id=1 seq=7");
    run_action(
        "bumpVWidget",
        json!({ "id": "1", "seq": "3" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("bump id=1 seq=3");
    run_action(
        "bumpVWidget",
        json!({ "id": "1", "seq": "5" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("bump id=1 seq=5");

    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/VWidget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET: {body:?}");
    assert_eq!(
        objects_id_seq(&body).get("1").map(String::as_str),
        Some("7"),
        "Versioned: highest version (7) wins, not greatest-offset (5): {:?}",
        objects_id_seq(&body),
    );

    // Flush + consolidate: the base folds to the seq=7 winner.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "vwidget".to_string())
        .await
        .expect("flush_table")
        .expect("flush snapshot");
    let new_snap = gov
        .consolidate_stream("main".to_string(), "vwidget".to_string())
        .await
        .expect("consolidate_stream");
    assert!(new_snap > 0, "consolidate produced a real snapshot id");
    let base = table_rows(&catalog, &pool, &table).await;
    assert!(
        base.iter().any(|(_k, id, seq)| *id == 1 && *seq == 7),
        "folded base holds the seq=7 winner for id=1: {base:?}",
    );
    assert!(
        base.iter().filter(|(_, id, _)| *id == 1).count() <= 1,
        "one row per identity after fold: {base:?}"
    );

    // (2) A late low-version event (seq=4) still loses after the base was folded
    // to seq=7 — the folded winner's version is preserved and dominates.
    run_action(
        "bumpVWidget",
        json!({ "id": "1", "seq": "4" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("late bump id=1 seq=4");
    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/VWidget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after late: {body:?}");
    assert_eq!(
        objects_id_seq(&body).get("1").map(String::as_str),
        Some("7"),
        "late low-version (4) loses after consolidate; still seq=7: {:?}",
        objects_id_seq(&body),
    );

    // (3) Delete-wins: id=2 (+I seq=1, then -D carrying seq=1) is dropped — the
    // -D wins the version tie by offset, identity does not resurrect.
    run_action(
        "createVWidget",
        json!({ "id": "2", "qty": "2", "seq": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("create id=2 seq=1");
    run_action(
        "deleteVWidget",
        json!({ "id": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete id=2");
    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/VWidget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after delete: {body:?}");
    assert!(
        !objects_id_seq(&body).contains_key("2"),
        "Versioned delete-wins: id=2 dropped (no resurrection): {:?}",
        objects_id_seq(&body),
    );

    drop(warehouse);
}

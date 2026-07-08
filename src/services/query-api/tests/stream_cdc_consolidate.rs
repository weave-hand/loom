//! Engine-side `consolidate_stream`: fold a CDC base by LastRow-per-identity and
//! clear its inline-shadow flag, over the real gRPC wire.
//!
//! Drives INSERT/UPDATE/INSERT/DELETE through the governed action router (the
//! real gRPC-wire engine writer), flushes so the deltas land in the base as real
//! Parquet, then calls the new `EngineControl::ConsolidateStream` RPC (via
//! `engine_wire::client::GrpcQueueClient::consolidate_stream`, mirroring
//! `flush_table`'s direct-RPC test pattern) and asserts:
//!   * the base holds exactly ONE row for id=1 (the latest, greatest
//!     `loom_offset`), id=2 absent (its winner was a `-D` delete);
//!   * the base's physical framing survives (`physical_columns` still carries
//!     `loom_offset`);
//!   * `has_shadow(base_tid)` flips from true to false;
//!   * the changelog table is UNCHANGED (same row count, every event incl. `-U`
//!     still present) — consolidate never touches it;
//!   * a `GET /objects/Widget` current-state read is IDENTICAL before and after.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use axum::http::StatusCode;
use control_plane_core::{Catalog, PageReq, StreamTables, TableRef};
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

/// `(loom_change_kind, id, qty)` for every row of `table`'s live data files, read
/// through `catalog` (the same warehouse the engine wrote to). Mirrors
/// `stream_cdc_dual_flush.rs`'s `rows` helper.
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
    let qty_idx = b.schema().index_of("qty").expect("qty col");
    let kinds = b
        .column(kind_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("loom_change_kind is a string column");
    let ids = b
        .column(id_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64");
    let qtys = b
        .column(qty_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("qty is Int64");
    (0..b.num_rows())
        .map(|i| (kinds.value(i).to_string(), ids.value(i), qtys.value(i)))
        .collect()
}

/// Every top-level key across every object in a `{"objects":[...]}` body — used to
/// prove the two GET reads (before/after consolidate) are identical.
fn objects_json(body: &serde_json::Value) -> serde_json::Value {
    body["objects"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consolidate_stream_folds_base_to_last_row_per_identity() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &warehouse.path().display().to_string()).await;

    // Declare `main.widget` CDC (keyed on `id`), exactly as `stream_cdc_e2e.rs` does.
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

    // insert id=1, update id=1, insert id=2, delete id=2.
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

    let table = TableRef {
        schema: "main".to_string(),
        name: "widget".to_string(),
    };
    let clog = TableRef {
        schema: "main".to_string(),
        name: "widget__changelog".to_string(),
    };

    // GET /objects/Widget BEFORE the flush/consolidate: everything is still
    // inline, where merge-on-read already resolves the correct current state
    // (id=1's latest qty=9, id=2 absent — proven by `stream_cdc_e2e.rs`). Flush
    // and consolidate are a physical-layout operation only; they must be
    // invisible to this governed read, so this is the baseline it's compared
    // against below (NOT the raw, un-folded post-flush/pre-consolidate base,
    // which legitimately carries multiple physical rows per identity until
    // consolidated).
    let (status_before, body_before) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status_before, StatusCode::OK, "GET before: {body_before:?}");

    // Flush so the deltas land in the base as real Parquet.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table")
        .expect("flush produced a snapshot");

    // Base (post-flush, pre-consolidate): +I/+U/-D subset, id=1 appears twice
    // (+I then +U), id=2 appears once as its -D tombstone.
    let base_before = table_rows(&catalog, &pool, &table).await;
    assert_eq!(
        base_before.len(),
        4,
        "base holds +I(id1)/+U(id1)/+I(id2)/-D(id2) before consolidate: {base_before:?}"
    );
    let clog_before = table_rows(&catalog, &pool, &clog).await;
    assert_eq!(
        clog_before.len(),
        5,
        "changelog holds the full 5-event sequence before consolidate: {clog_before:?}"
    );

    assert!(
        has_shadow(&mut pool.acquire().await.expect("conn"), tid)
            .await
            .expect("has_shadow"),
        "the update/delete deltas set has_shadow before consolidate"
    );

    // Consolidate: fold the base to LastRow-per-identity.
    let new_snap = gov
        .consolidate_stream("main".to_string(), "widget".to_string())
        .await
        .expect("consolidate_stream");
    assert!(new_snap > 0, "consolidate produced a real snapshot id");

    // Base (post-consolidate): exactly one row, id=1's latest (+U, qty=9); id=2
    // is gone (its winning row was the -D tombstone).
    let base_after = table_rows(&catalog, &pool, &table).await;
    assert_eq!(
        base_after,
        vec![("+U".to_string(), 1, 9)],
        "base folds to LastRow-per-identity, dropping the deleted id=2: {base_after:?}"
    );

    // Framing survives the fold: physical_columns still carries loom_offset.
    let ice = IcebergCatalog::new(pool.clone());
    let after_snap = ice
        .current_snapshot(&table)
        .await
        .expect("post-consolidate snapshot");
    let phys = ice
        .physical_columns(tid, after_snap.id)
        .await
        .expect("physical_columns");
    assert!(
        phys.iter().any(|c| c.name == "loom_offset"),
        "consolidated base keeps its framing columns: {phys:?}"
    );

    // has_shadow is cleared.
    assert!(
        !has_shadow(&mut pool.acquire().await.expect("conn"), tid)
            .await
            .expect("has_shadow"),
        "consolidate clears has_shadow"
    );

    // The changelog is UNTOUCHED: same row count, every event (incl. -U) intact.
    let clog_after = table_rows(&catalog, &pool, &clog).await;
    assert_eq!(
        clog_after, clog_before,
        "consolidate never touches the changelog table"
    );

    // GET /objects/Widget AFTER consolidate is identical to the pre-flush read:
    // flush + consolidate is transparent to the governed read.
    let (status_after, body_after) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status_after, StatusCode::OK, "GET after: {body_after:?}");
    assert_eq!(
        objects_json(&body_after),
        objects_json(&body_before),
        "GET /objects/Widget is identical before/after consolidation"
    );

    drop(warehouse);
}

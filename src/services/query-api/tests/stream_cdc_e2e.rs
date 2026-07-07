//! End-to-end capstone for stream engine slice 2 (PK/CDC tables): declare a CDC
//! type, then drive INSERT/UPDATE/DELETE through the governed action router
//! (`run_action`, the real gRPC-wire engine writer) and prove the full change
//! sequence lands correctly framed:
//!
//!   * INSERT id=1  -> a `+I` inline row at `loom_offset=0`;
//!   * UPDATE id=1  -> an adjacent `(-U before-image, +U after-image)` pair at
//!     the next two offsets, `-U` first;
//!   * DELETE id=1  -> a `-D` carrying the FULL prior image with
//!     `loom_tombstone=true`, at the next offset;
//!   * every one of the four events for id=1 shares ONE bucket, with gapless
//!     per-bucket offsets `0..4`;
//!   * a `GET /objects/Widget` read at each stage returns exactly the correct
//!     current state and NEVER exposes a `loom_*` column or a `-U` row.
//!
//! The CDC framing is read directly off `iceberg_mirror.inline_<tid>` (mirroring
//! `stream_cdc_emission.rs`); the read-side invisibility is proven through the
//! real HTTP router (`e2e_support::get`), not just the in-process merge helper.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::StreamTables;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{InProcessServingEngine, define_widget, get, grant_writer};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// `(loom_change_kind, loom_tombstone, qty, loom_bucket, loom_offset)` for every
/// live inline row of `tid`, ordered by offset. Mirrors
/// `stream_cdc_emission.rs`'s `rows_by_offset`, over the `Widget` table's `qty`
/// column instead of `val`.
async fn rows_by_offset(
    pool: &sqlx::PgPool,
    tid: i64,
) -> Vec<(String, bool, Option<i64>, i32, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "select loom_change_kind, loom_tombstone, \"qty\", loom_bucket, loom_offset \
         from iceberg_mirror.inline_{tid} where end_snapshot is null order by loom_offset"
    )))
    .fetch_all(pool)
    .await
    .expect("framing rows readback")
}

/// Every top-level key across every object in a `{"objects":[...]}` body — used to
/// assert NO `loom_*` column ever leaks through the governed read.
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
async fn cdc_insert_update_delete_lifecycle_via_governed_actions() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Declare the `main.widget` table CDC (keyed on `id`, 2 buckets) BEFORE any
    // write — mirrors `stream_cdc_emission.rs`/`stream_cdc_bucket.rs`: ensure_table
    // to get the mirror table id, then declare_cdc.
    let mut conn = pool.acquire().await.expect("acquire");
    let at0 = next_snapshot(&mut conn, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut conn, "main", "widget", at0)
        .await
        .expect("ensure_table");
    drop(conn);
    let bucket_count = 2;
    cp.declare_cdc(tid, bucket_count, "id")
        .await
        .expect("declare_cdc");

    // Define Widget(id Long identity, name String, qty Long) + createWidget/
    // updateWidget/deleteWidget actions, and grant a writer subject Write+Read.
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Inline tier (large inline_byte_limit): every write lands as an inline row,
    // which is where the CDC framing columns live.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // The HTTP router side of the harness: Arc'd cp/serving engine over the SAME
    // pool, driven through `GET /objects/Widget` via `e2e_support::get`.
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new(IcebergCatalog::new(pool.clone())),
    );

    // ---------------------------------------------------------------------
    // 1. INSERT id=1 -> a +I inline row at loom_offset=0.
    // ---------------------------------------------------------------------
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

    let rows = rows_by_offset(&pool, tid).await;
    assert_eq!(rows.len(), 1, "exactly one row after insert: {rows:?}");
    let (kind, tombstone, qty, bucket, offset) = &rows[0];
    assert_eq!(kind, "+I", "insert emits a +I row: {rows:?}");
    assert!(!tombstone, "+I is not a tombstone: {rows:?}");
    assert_eq!(*qty, Some(1), "+I carries qty=1: {rows:?}");
    assert_eq!(*offset, 0, "+I lands at offset 0: {rows:?}");
    assert!(
        (0..bucket_count).contains(bucket),
        "bucket {bucket} out of range 0..{bucket_count}: {rows:?}"
    );
    let id1_bucket = *bucket;

    // GET /objects/Widget: current state is qty=1, and no loom_* column ever leaks.
    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after insert: {body:?}");
    let objs = body["objects"].as_array().expect("objects array");
    assert_eq!(
        objs.len(),
        1,
        "exactly one live Widget after insert: {body}"
    );
    assert_eq!(objs[0]["id"], json!("1"));
    assert_eq!(objs[0]["qty"], json!("1"), "current state reflects +I");
    assert!(
        all_object_keys(&body)
            .iter()
            .all(|k| !k.starts_with("loom_")),
        "no loom_* column leaks through the governed read: {body}"
    );

    // ---------------------------------------------------------------------
    // 2. UPDATE id=1 (qty 1 -> 9) -> an adjacent (-U, +U) pair at the next two
    //    offsets in the SAME bucket, -U first.
    // ---------------------------------------------------------------------
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update id=1");

    let rows = rows_by_offset(&pool, tid).await;
    assert_eq!(rows.len(), 3, "+I, -U, +U after update: {rows:?}");
    let minus_u = &rows[1];
    let plus_u = &rows[2];
    assert_eq!(
        minus_u.0, "-U",
        "second row is the -U before-image: {rows:?}"
    );
    assert_eq!(plus_u.0, "+U", "third row is the +U after-image: {rows:?}");
    assert!(!minus_u.1, "-U is not a tombstone: {rows:?}");
    assert!(!plus_u.1, "+U is not a tombstone: {rows:?}");
    assert_eq!(
        minus_u.2,
        Some(1),
        "-U carries the before-image qty=1: {rows:?}"
    );
    assert_eq!(
        plus_u.2,
        Some(9),
        "+U carries the after-image qty=9: {rows:?}"
    );
    assert_eq!(minus_u.3, id1_bucket, "-U shares id=1's bucket: {rows:?}");
    assert_eq!(plus_u.3, id1_bucket, "+U shares id=1's bucket: {rows:?}");
    assert_eq!(minus_u.4, 1, "-U at offset 1: {rows:?}");
    assert_eq!(
        plus_u.4, 2,
        "+U at offset 2 (consecutive after -U): {rows:?}"
    );

    // GET /objects/Widget: current state reflects the +U after-image (qty=9), the
    // -U before-image never surfaces as its own object, and still no loom_* leak.
    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after update: {body:?}");
    let objs = body["objects"].as_array().expect("objects array");
    assert_eq!(
        objs.len(),
        1,
        "exactly one live Widget after update (no -U leak as a second row): {body}"
    );
    assert_eq!(
        objs[0]["qty"],
        json!("9"),
        "current state reflects +U, not -U"
    );
    assert_eq!(objs[0]["name"], json!("a"), "PATCH retained name");
    assert!(
        all_object_keys(&body)
            .iter()
            .all(|k| !k.starts_with("loom_")),
        "no loom_* column leaks through the governed read: {body}"
    );

    // ---------------------------------------------------------------------
    // 3. DELETE id=1 -> a -D carrying the FULL prior image (qty=9, NOT NULL)
    //    with loom_tombstone=true, at the next offset.
    // ---------------------------------------------------------------------
    run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete id=1");

    let rows = rows_by_offset(&pool, tid).await;
    assert_eq!(rows.len(), 4, "+I, -U, +U, -D after delete: {rows:?}");
    let minus_d = &rows[3];
    assert_eq!(minus_d.0, "-D", "fourth row is the -D tombstone: {rows:?}");
    assert!(
        minus_d.1,
        "-D is a tombstone (hides the base row): {rows:?}"
    );
    assert_eq!(
        minus_d.2,
        Some(9),
        "-D carries the FULL prior image qty=9, not an id-only NULL tombstone: {rows:?}"
    );
    assert_eq!(minus_d.3, id1_bucket, "-D shares id=1's bucket: {rows:?}");
    assert_eq!(minus_d.4, 3, "-D at offset 3 (next after +U): {rows:?}");

    // All four events for id=1 share ONE bucket with gapless offsets 0..4.
    assert!(
        rows.iter().all(|(.., b, _)| *b == id1_bucket),
        "all four events share id=1's bucket: {rows:?}"
    );
    let mut offs: Vec<i64> = rows.iter().map(|(.., o)| *o).collect();
    offs.sort_unstable();
    assert_eq!(
        offs,
        vec![0, 1, 2, 3],
        "gapless per-bucket offsets: {rows:?}"
    );

    // GET /objects/Widget: id=1 no longer exists, and still no loom_* leak (there
    // is nothing to leak, but the -D tombstone row must not surface either).
    let (status, body) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after delete: {body:?}");
    let objs = body["objects"].as_array().expect("objects array");
    assert!(
        objs.is_empty(),
        "no live Widget after delete (the -D tombstone hides it): {body}"
    );
    assert!(
        all_object_keys(&body)
            .iter()
            .all(|k| !k.starts_with("loom_")),
        "no loom_* column leaks through the governed read: {body}"
    );

    drop(warehouse);
}

//! Task 7 — end-to-end: governed mutate → consolidate → governed read.
//!
//! Exercises the whole slice-2 consolidation feature through its public seams,
//! over the real gRPC-wire engine writer (mirroring `cow_inline_shadow_e2e.rs`'s
//! setup): a governed UPDATE + DELETE accrue an inline shadow tier on a
//! Parquet-resident `Widget` base, `engine_serving::consolidate_table` is driven
//! DIRECTLY (as the engine RPC handler would — the worker↔RPC plumbing is
//! already covered by `stream-consolidate-job`), and four invariants are pinned:
//!
//!   1. the fold actually ran (a real new snapshot id, `has_shadow` cleared);
//!   2. **governed read identical** — `GET /objects/Widget` after consolidation
//!      equals the pre-consolidation read (updated qty served, the deleted id
//!      absent), and a restricted subject's masked column stays masked, on both
//!      sides of the fold (reusing the masking pattern from
//!      `cow_inline_shadow_gov_e2e.rs`'s governed-merge-view test);
//!   3. **flush lifecycle restored** — a fresh plain append lands inline, and a
//!      direct `flush_table` now drains it (`Some(snap)`), proving the
//!      `has_shadow` suppression lifted;
//!   4. **CAS after consolidation** — a `write_delta` built against the
//!      PRE-consolidation `expected_version` gets `ServingError::Conflict` (the
//!      fold end-capped that inline row, so the live version reset to 0), and
//!      the retry against `expected_version = 0` succeeds.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use axum::http::StatusCode;
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::has_shadow;
use control_plane_postgres::iceberg_mirror::live_table_id;
use e2e_support::{
    InProcessServingEngine, define_widget, get, grant_read_columns, grant_writer, subject_with_role,
};
use loom_test_seed::local_sql_catalog;
use query_api::action::{ActionDeps, run_action};
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use serde_json::json;
use std::sync::Arc;

/// The `main.widget` table `define_widget` binds `Widget` to.
fn widget_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "widget".into(),
    }
}

/// The `{"objects":[...]}` array, for a before/after equality comparison.
fn objects_json(body: &serde_json::Value) -> serde_json::Value {
    body["objects"].clone()
}

/// The single object with `id` in a `{"objects":[...]}` body, or `None`.
fn find_object(body: &serde_json::Value, id: i64) -> Option<serde_json::Value> {
    body["objects"]
        .as_array()?
        .iter()
        .find(|o| o["id"] == json!(id.to_string()))
        .cloned()
}

fn cas_lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&widget_table())],
        outputs: vec![DatasetRef::from(&widget_table())],
        payload: json!({ "source": "cow-consolidate-e2e" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consolidate_e2e_governed_mutate_read_flush_cas() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let sql_catalog =
        local_sql_catalog(fx.pg_dsn(&db), &warehouse.path().display().to_string()).await;

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // A restricted reader: coarse Read Allow + `name` masked — proves the
    // governed-merge-view masking survives consolidation too.
    let (_reader_subj, reader_role) = subject_with_role(&cp, "reader").await;
    grant_read_columns(
        &cp,
        &reader_role,
        "Widget",
        vec![],
        vec!["name".to_string()],
    )
    .await;

    // FILE tier: both seeds land as Parquet, so the later UPDATE/DELETE accrue a
    // real inline shadow tier on top of a physical base (the COW consolidate arm).
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
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

    // Seed {id:1, name:"alpha-secret", qty:1} and {id:2, name:"beta-secret", qty:2}.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "alpha-secret", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=1 (file tier)");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "beta-secret", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=2 (file tier)");

    // Governed UPDATE: id=1 qty 1 -> 9 (an inline shadow row).
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update id=1");
    // Governed DELETE: id=2 (an inline tombstone).
    run_action(
        "deleteWidget",
        json!({ "id": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete id=2");

    // The pre-consolidation CAS witness for id=1 — its live inline shadow's
    // version (a real snapshot id > 0). Captured now so the CAS assertion below
    // can prove it goes stale once consolidation retires that inline row.
    let stale_v = engine
        .current_inline_version(&widget_table(), "id", &SqlValue::Int(1), "Long")
        .await
        .expect("pre-consolidation inline version for id=1");
    assert!(
        stale_v > 0,
        "id=1 carries a live inline shadow before consolidation, got version {stale_v}"
    );

    let table = widget_table();
    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("tid");
    assert!(
        has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "the update/delete deltas set has_shadow before consolidate"
    );
    drop(conn);

    // Pre-consolidation governed reads: the writer's full view, and the
    // reader's masked view. This is the baseline consolidation must reproduce
    // exactly (flush/consolidate is a physical-layout operation only).
    let (status_before, body_before) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status_before, StatusCode::OK, "GET before: {body_before:?}");
    let obj1_before = find_object(&body_before, 1).expect("id=1 present before consolidate");
    assert_eq!(
        obj1_before["qty"],
        json!("9"),
        "updated qty served pre-consolidation"
    );
    assert!(
        find_object(&body_before, 2).is_none(),
        "deleted id=2 absent pre-consolidation"
    );

    let (status_masked_before, body_masked_before) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "reader",
    )
    .await;
    assert_eq!(
        status_masked_before,
        StatusCode::OK,
        "masked GET before: {body_masked_before:?}"
    );
    let masked1_before =
        find_object(&body_masked_before, 1).expect("id=1 present in the masked read before");
    assert_eq!(
        masked1_before["name"],
        json!("***"),
        "masked name redacted pre-consolidation"
    );
    assert_ne!(
        masked1_before["name"],
        json!("alpha-secret"),
        "the real name never leaks through the masked read pre-consolidation"
    );

    // -------------------------------------------------------------------
    // 1. Drive consolidate_table DIRECTLY (as the engine RPC handler would).
    // -------------------------------------------------------------------
    let new_snap = engine_serving::consolidate_table(&cp, &sql_catalog, &pool, &table)
        .await
        .expect("consolidate_table");
    assert!(
        new_snap > 0,
        "consolidation produced a real new snapshot, got {new_snap}"
    );
    let mut conn = pool.acquire().await.expect("acquire");
    assert!(
        !has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "consolidate clears has_shadow"
    );
    drop(conn);

    // -------------------------------------------------------------------
    // 2. Governed read identical (writer view, and the restricted masked view).
    // -------------------------------------------------------------------
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
        "GET /objects/Widget is byte-identical before/after consolidation"
    );

    let (status_masked_after, body_masked_after) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "reader",
    )
    .await;
    assert_eq!(
        status_masked_after,
        StatusCode::OK,
        "masked GET after: {body_masked_after:?}"
    );
    assert_eq!(
        objects_json(&body_masked_after),
        objects_json(&body_masked_before),
        "the masked read is byte-identical before/after consolidation"
    );
    let masked1_after =
        find_object(&body_masked_after, 1).expect("id=1 present in the masked read after");
    assert_eq!(
        masked1_after["name"],
        json!("***"),
        "masked name stays redacted post-consolidation"
    );
    assert_ne!(
        masked1_after["name"],
        json!("alpha-secret"),
        "the real name never leaks through the masked read post-consolidation"
    );

    // -------------------------------------------------------------------
    // 3. Flush lifecycle restored: land a fresh append (a SECOND engine writer
    //    over the SAME db+warehouse, with a large inline_byte_limit so the
    //    append lands inline, not as Parquet), then call flush_table directly —
    //    it must now drain it (`Some(snap)`), proving the has_shadow
    //    suppression consolidation lifted.
    // -------------------------------------------------------------------
    let (engine_inline, _eg_inline) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let deps_inline = ActionDeps {
        cp: &cp,
        action_engine: &engine_inline,
        serving: &serving,
    };
    run_action(
        "createWidget",
        json!({ "id": "3", "name": "gamma", "qty": "3" })
            .as_object()
            .unwrap(),
        &subj,
        &deps_inline,
    )
    .await
    .expect("fresh append id=3 (inline tier, normal ingest path)");

    let flushed = flush_table(&sql_catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush_table");
    assert!(
        flushed.is_some(),
        "flush_table drains the fresh inline append — the suppression lifted"
    );

    // -------------------------------------------------------------------
    // 4. CAS after consolidation: a write_delta built against the
    //    PRE-consolidation expected_version conflicts (the fold end-capped that
    //    inline row, resetting id=1's live version to 0); the retry against
    //    version 0 succeeds.
    // -------------------------------------------------------------------
    let cas_columns = vec!["id".to_string(), "name".to_string(), "qty".to_string()];
    let cas_logical = vec!["Long".to_string(), "String".to_string(), "Long".to_string()];
    let cas_values = vec![
        SqlValue::Int(1),
        SqlValue::Text("cas-retry".to_string()),
        SqlValue::Int(42),
    ];

    let conflict = engine
        .write_delta(
            &table,
            "id",
            false,
            &cas_columns,
            &cas_values,
            &cas_logical,
            None,
            cas_lineage(),
            stale_v,
            &[],
        )
        .await
        .expect_err("a write_delta against the pre-consolidation version must conflict");
    assert!(
        matches!(conflict, ServingError::Conflict(_)),
        "expected ServingError::Conflict, got: {conflict:?}"
    );

    engine
        .write_delta(
            &table,
            "id",
            false,
            &cas_columns,
            &cas_values,
            &cas_logical,
            None,
            cas_lineage(),
            0,
            &[],
        )
        .await
        .expect("the retry against expected_version=0 succeeds");

    // The CAS retry's write is served: qty=42.
    let (status_final, body_final) = get(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Widget",
        "writer",
    )
    .await;
    assert_eq!(status_final, StatusCode::OK, "final GET: {body_final:?}");
    let obj1_final = find_object(&body_final, 1).expect("id=1 present after the CAS retry");
    assert_eq!(
        obj1_final["qty"],
        json!("42"),
        "the CAS retry's write is served"
    );

    drop(warehouse);
}

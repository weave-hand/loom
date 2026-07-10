//! GET /objects/Widget/changes governance + freshness e2e (road-stream-subscribe,
//! Task 7): the feed enforces ACL on EVERY event, on EVERY tier, and stays fresh
//! on unflushed writes.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
//!
//!   (a) `row_filter_hides_every_event_of_a_filtered_identity` — a row-filtered
//!       subject sees ONLY the filtered identity's events, on both the inline
//!       and (post-flush) file tier.
//!   (b) `masked_column_is_starred_on_every_event` — a masked column reads
//!       `"***"` on every event, including the `-U` before-image.
//!   (c) `reconnect_regates` — a reconnect with a valid cursor re-runs the full
//!       governance gate; an ungranted subject is 403'd before any line.
//!   (d) `unflushed_write_is_fresh` — a blocked reader observes an unflushed
//!       inline write within the notify/poll window.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::StreamTables;
use control_plane_core::{CompareOp, RowFilter, ScalarValue, SubjectId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{
    EngineGuard, InProcessServingEngine, connect_gov_client, define_widget, get_ndjson,
    grant_read_columns, grant_read_filtered, grant_writer, subject_with_role,
};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// Owns everything a case needs kept alive: the spawned gRPC-wire engine
/// (`eg` — also used to drive `flush_table` for the file-tier-parity case), the
/// Parquet warehouse (`_warehouse`), the owned control-plane/action-client/
/// serving-engine trio used to run MORE actions after the initial seed (via
/// `write`), and the Arc'd `(cp, ServingEngine)` pair `get_ndjson` drives the
/// router with. Mirrors `stream_subscribe_e2e.rs`'s `Harness`.
struct Harness {
    cp: PgControlPlane,
    engine: query_api::engine_action_client::EngineActionClient,
    serving: InProcessServingEngine,
    subj: control_plane_core::SubjectId,
    cp_arc: Arc<PgControlPlane>,
    read_eng: Arc<dyn query_api::serving::ServingEngine>,
    eg: EngineGuard,
    #[expect(
        dead_code,
        reason = "kept alive: its TempDir holds the Parquet warehouse"
    )]
    warehouse: tempfile::TempDir,
}

impl Harness {
    /// Run a governed action as the harness's writer subject.
    async fn write(&self, action: &str, body: serde_json::Value) {
        let deps = ActionDeps {
            cp: &self.cp,
            action_engine: &self.engine,
            serving: &self.serving,
        };
        run_action(
            action,
            body.as_object().expect("object body"),
            &self.subj,
            &deps,
        )
        .await
        .unwrap_or_else(|e| panic!("{action}: {e:?}"));
    }
}

/// Declare `main.widget` CDC (2 buckets), define `Widget` + its actions, grant a
/// writer, spawn the gRPC-wire engine, and write 5 events for id=1 (3 events)
/// and id=2 (2 events) — INTERLEAVED (id=2's insert first, then id=1's
/// insert+update, then id=2's delete) so a governance bug that leaks rows can't
/// hide behind "id=1 just happened to sort first". Nothing is flushed here —
/// governance is tier-agnostic; the one case that needs file-tier parity
/// (`row_filter_hides_every_event_of_a_filtered_identity`) flushes explicitly
/// mid-test via `h.eg.sock`.
async fn setup(fx: &PgFixture) -> Harness {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

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

    {
        let deps = ActionDeps {
            cp: &cp,
            action_engine: &engine,
            serving: &serving,
        };

        // +I(2) — id=2 first, so an unfiltered read's natural head is NOT
        // trivially all-id=1.
        run_action(
            "createWidget",
            json!({ "id": "2", "name": "b", "qty": "2" })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("+I(2)");

        // +I(1), -U/+U(1: qty 1->9).
        run_action(
            "createWidget",
            json!({ "id": "1", "name": "a", "qty": "1" })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("+I(1)");
        run_action(
            "updateWidget",
            json!({ "id": "1", "qty": "9" })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("-U/+U(1)");

        // -D(2).
        run_action(
            "deleteWidget",
            json!({ "id": "2" }).as_object().expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("-D(2)");
    }

    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new(IcebergCatalog::new(pool.clone())),
    );

    Harness {
        cp,
        engine,
        serving,
        subj,
        cp_arc,
        read_eng,
        eg,
        warehouse,
    }
}

/// Every returned line's `fields.id` (as an i64 — the identity is an unmasked
/// `Long`, so the changelog feed's `cell_to_json` renders it as a JSON number).
fn ids(lines: &[serde_json::Value]) -> Vec<i64> {
    lines
        .iter()
        .map(|l| l["fields"]["id"].as_i64().expect("fields.id is an i64"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_hides_every_event_of_a_filtered_identity() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // Sanity: the interleave is genuinely discriminating — an UNGOVERNED read
    // (the "writer" subject, full Read grant, no policy) of the first 3 events
    // is NOT id=1-only. If this ever failed, the case below would no longer
    // prove anything (the filter could be silently absent and still pass by
    // accident of ordering).
    let (status, unfiltered) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=3",
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unfiltered:?}");
    assert!(
        ids(&unfiltered).contains(&2),
        "sanity: the unfiltered head includes an id=2 event, so the filtered \
         case below is a genuine discrimination, not incidental ordering: {unfiltered:?}"
    );

    let (restricted, role) = subject_with_role(&h.cp, "restricted").await;
    assert_eq!(restricted, SubjectId("restricted".into()));
    grant_read_filtered(
        &h.cp,
        &role,
        "Widget",
        RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
    )
    .await;

    // --- Inline tier: id=1 has exactly 3 events (+I, -U, +U). ---
    let (status, inline_lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=3",
        "restricted",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{inline_lines:?}");
    assert_eq!(
        inline_lines.len(),
        3,
        "exactly id=1's 3 events, no more: {inline_lines:?}"
    );
    assert!(
        ids(&inline_lines).iter().all(|id| *id == 1),
        "every event is id=1, NO id=2 event ever appears: {inline_lines:?}"
    );
    let mut kinds: Vec<&str> = inline_lines
        .iter()
        .map(|l| l["change_kind"].as_str().expect("change_kind"))
        .collect();
    kinds.sort_unstable();
    let mut want = vec!["+I", "-U", "+U"];
    want.sort_unstable();
    assert_eq!(kinds, want, "id=1's change-kind multiset: {inline_lines:?}");

    // --- File-tier parity: flush, then re-run the SAME governed query from
    // scratch. The filter is tier-agnostic (it wraps the union BEFORE the
    // ordered read), so the result must be byte-identical. ---
    let gov = connect_gov_client(&h.eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");

    let (status, file_lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=3",
        "restricted",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{file_lines:?}");
    assert_eq!(
        file_lines, inline_lines,
        "post-flush read returns the identical id=1-only events (file-tier parity)"
    );
    assert!(
        ids(&file_lines).iter().all(|id| *id == 1),
        "post-flush: still no id=2 event: {file_lines:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn masked_column_is_starred_on_every_event() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (masked_subj, role) = subject_with_role(&h.cp, "masked").await;
    assert_eq!(masked_subj, SubjectId("masked".into()));
    grant_read_columns(&h.cp, &role, "Widget", vec![], vec!["qty".into()]).await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=5",
        "masked",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(lines.len(), 5, "all 5 events, none dropped: {lines:?}");

    assert!(
        lines.iter().all(|l| l["fields"]["qty"] == json!("***")),
        "qty is '***' on EVERY event: {lines:?}"
    );
    // Every other column is untouched.
    assert!(
        lines.iter().all(|l| l["fields"]["id"].is_i64()),
        "id is NOT masked (only qty was granted a mask): {lines:?}"
    );

    let minus_u = lines
        .iter()
        .find(|l| l["change_kind"] == "-U")
        .expect("-U line present");
    assert_eq!(
        minus_u["fields"]["qty"],
        json!("***"),
        "the -U before-image is masked too: {minus_u}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_regates() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // The granted "writer" subject reads fine and gets a valid cursor.
    let (status, head) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=2",
        "writer",
        2,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{head:?}");
    assert_eq!(head.len(), 2);
    let cursor = head[1]["cursor"]
        .as_str()
        .expect("cursor string")
        .to_string();

    // A DIFFERENT subject with NO grant reconnects with the SAME valid cursor.
    let (ungranted, _role) = subject_with_role(&h.cp, "nogrant").await;
    assert_eq!(ungranted, SubjectId("nogrant".into()));

    let uri = format!("/objects/Widget/changes?cursor={cursor}&max_events=1");
    let (status, body) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        &uri,
        "nogrant",
        1,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the cursor carries no authority — every connect re-runs resolve_governed: {body:?}"
    );
    // The non-200 path never entered NDJSON line parsing: the single collected
    // body element is the error body, not a change event.
    assert_eq!(body.len(), 1);
    assert!(
        body[0].get("change_kind").is_none() && body[0].get("bucket").is_none(),
        "no event line was ever emitted before the 403: {body:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unflushed_write_is_fresh() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // Spawn a blocked reader at the tail (?cursor=latest, max_events=1) BEFORE
    // the new write lands.
    let cp_arc = h.cp_arc.clone();
    let read_eng = h.read_eng.clone();
    let reader = tokio::spawn(async move {
        get_ndjson(
            cp_arc,
            read_eng,
            "/objects/Widget/changes?cursor=latest&max_events=1",
            "writer",
            1,
            Duration::from_secs(10),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    // ONE inline write, NO flush — bounded by FEED_POLL_INTERVAL (1s) even if
    // the notify is missed (the notify-vs-poll distinction is pinned
    // deterministically at the postgres layer by Task 3(b)).
    h.write("updateWidget", json!({ "id": "1", "qty": "42" }))
        .await;

    let (status, lines) = reader.await.expect("reader task joined");
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(
        lines.len(),
        1,
        "the blocked reader observed the unflushed inline write: {lines:?}"
    );
    assert_eq!(lines[0]["change_kind"], json!("-U"));
    assert_eq!(
        lines[0]["fields"]["qty"],
        json!(9),
        "the -U before-image (qty=9, from setup's earlier update): {lines:?}"
    );
}

//! Subscribe over the PRODUCTION WIRE (road-stream-subscribe-wire): the same
//! `GET /objects/{type}/changes` surface every other subscribe e2e drives, but read
//! through `EngineServingClient` against a real engine socket instead of the in-process
//! engine. Before this item the route answered 501 on this path.
//!
//!   1. the feed streams (200, not 501), ordered, gapless, framing-free;
//!   2. a cursor resumes gaplessly and dup-free;
//!   3. governance holds over the wire — masked, denied, AND row-filtered (the
//!      caller-resolved WirePolicy crosses as JSON and is enforced engine-side);
//!   4. the long-poll WAKES on a write (not just times out);
//!   5. a non-CDC type is 400, not 501.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::{
    CompareOp, ControlPlane, ObjectType, RoleId, RowFilter, ScalarValue, SubjectId,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, get_ndjson,
    grant_read, grant_read_columns, grant_read_filtered, grant_writer_role, spawn_engine_full,
    subject_with_role,
};
use query_api::action::{ActionDeps, run_action};
use query_api::engine_action_client::EngineActionClient;
use query_api::engine_client::EngineServingClient;
use serde_json::json;

fn assert_no_loom_keys(lines: &[serde_json::Value]) {
    for l in lines {
        let fields = l["fields"].as_object().expect("fields object");
        assert!(
            !fields.keys().any(|k| k.starts_with("loom_")),
            "framing leaked into fields: {l}"
        );
    }
}

fn offsets(lines: &[serde_json::Value]) -> Vec<i64> {
    lines
        .iter()
        .map(|l| l["offset"].as_i64().expect("offset"))
        .collect()
}

fn ids(lines: &[serde_json::Value]) -> Vec<i64> {
    lines
        .iter()
        .map(|l| l["fields"]["id"].as_i64().expect("fields.id"))
        .collect()
}

struct Wire {
    cp: PgControlPlane,
    cp_arc: Arc<PgControlPlane>,
    read_eng: Arc<dyn query_api::serving::ServingEngine>,
    #[expect(
        dead_code,
        reason = "kept alongside subj for symmetry with grant_writer_role's return; \
                  governance cases mint their own roles instead of reusing this one"
    )]
    role: RoleId,
    sock: String,
    engine: EngineActionClient,
    serving: InProcessServingEngine,
    subj: SubjectId,
    _guard: loom_test_flight::EngineGuard,
    _warehouse: tempfile::TempDir,
}

impl Wire {
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

/// `main.widget` as a 1-bucket CDC table with 4 events across a flush boundary:
/// `+I(1)` @0, `-U/+U(1)` @1,2 — flush — `+I(2)` @3. ALSO defines `Gadget`, a type bound
/// to a plain (NON-CDC) mirror table, so the 400 case is genuine rather than a 404.
/// Reads go over the WIRE.
async fn setup(fx: &PgFixture) -> Wire {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    // `grant_writer_role` (not `grant_writer`) — the governance cases need the RoleId.
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    // A plain, NON-CDC mirror table + a type bound to it. `declare_cdc` is deliberately
    // NOT called, so `changelog_positions_latest` returns None => `present: false` =>
    // query-api's 400. Without this, `/objects/Gadget/changes` would 404 at type
    // resolution and never reach the probe at all.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    ensure_table(&mut tx, "main", "gadget", at0)
        .await
        .expect("ensure_table gadget");
    tx.commit().await.expect("commit");
    cp.ontology()
        .define_type(
            ObjectType::build("Gadget", ("main", "gadget"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .expect("define Gadget");
    grant_read(&cp, &role, "Gadget").await;

    let (sock, guard) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let engine = EngineActionClient::connect(sock.clone())
        .await
        .expect("connect EngineActionClient");
    // Writes still go through the action path (which needs a serving engine for
    // current-state reads); the FEED is what this file reads over the wire.
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));

    let w = Wire {
        cp_arc: Arc::new(cp.clone()),
        cp,
        // THE substitution: the read engine is the production wire client.
        read_eng: Arc::new(
            EngineServingClient::connect(sock.clone())
                .await
                .expect("connect EngineServingClient"),
        ),
        role,
        sock: sock.clone(),
        engine,
        serving,
        subj,
        _guard: guard,
        _warehouse: warehouse,
    };

    w.write(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" }),
    )
    .await;
    w.write("updateWidget", json!({ "id": "1", "qty": "9" }))
        .await;
    connect_gov_client(&w.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    w.write(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" }),
    )
    .await;

    w
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_streams_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "writer",
        5,
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "no more 501 on the wire: {lines:?}");
    assert_eq!(
        lines.len(),
        4,
        "4 events across the flush boundary: {lines:?}"
    );
    for l in &lines {
        for k in ["bucket", "offset", "change_kind", "fields", "cursor"] {
            assert!(l.get(k).is_some(), "line missing `{k}`: {l}");
        }
    }
    assert_no_loom_keys(&lines);
    assert_eq!(offsets(&lines), vec![0, 1, 2, 3], "ordered and gapless");

    let kinds: Vec<&str> = lines
        .iter()
        .map(|l| l["change_kind"].as_str().expect("change_kind"))
        .collect();
    assert_eq!(
        kinds,
        vec!["+I", "-U", "+U", "+I"],
        "the full change sequence, incl. the -U before-image, crossed the wire"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_resume_is_gapless_and_dup_free_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, first) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=2",
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(offsets(&first), vec![0, 1]);

    let cursor = first[1]["cursor"].as_str().expect("cursor").to_string();
    let (status, rest) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        &format!("/objects/Widget/changes?max_events=2&cursor={cursor}"),
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(offsets(&rest), vec![2, 3], "resume: no gap, no duplicate");
}

/// THE load-bearing governance test: the caller-resolved policy crosses the wire as JSON
/// and is enforced ENGINE-side. Covers all three channels — masked, denied, row-filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_holds_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // --- Masked + denied: `qty` masked, `name` denied. ---
    let (_m, mrole) = subject_with_role(&h.cp, "masked").await;
    grant_read_columns(
        &h.cp,
        &mrole,
        "Widget",
        vec!["name".into()],
        vec!["qty".into()],
    )
    .await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "masked",
        5,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(lines.len(), 4, "all events, none dropped: {lines:?}");
    assert!(
        lines.iter().all(|l| l["fields"]["qty"] == json!("***")),
        "masked column is '***' on EVERY event, over the wire: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["fields"].get("name").is_none()),
        "denied column is ABSENT from every event, over the wire: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["fields"]["id"].is_i64()),
        "un-restricted columns are untouched: {lines:?}"
    );

    // --- Row filter: `id = 1` hides every event of identity 2. This is the newest serde
    // path — RowFilter is a recursive core enum, JSON-round-tripped through policy_json.
    let (_r, rrole) = subject_with_role(&h.cp, "restricted").await;
    grant_read_filtered(
        &h.cp,
        &rrole,
        "Widget",
        RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
    )
    .await;

    // Sanity: the unfiltered feed DOES contain an id=2 event, so the filter below is a
    // genuine discrimination and not an accident of ordering.
    let (_s, all) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "writer",
        5,
        Duration::from_secs(10),
    )
    .await;
    assert!(ids(&all).contains(&2), "sanity: id=2 is in the raw feed");

    let (status, filtered) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=3",
        "restricted",
        4,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{filtered:?}");
    assert_eq!(filtered.len(), 3, "exactly id=1's 3 events: {filtered:?}");
    assert!(
        ids(&filtered).iter().all(|id| *id == 1),
        "the row filter crossed the wire: NO id=2 event ever appears: {filtered:?}"
    );
}

/// The long-poll WAKES on a write — not merely times out. Mirrors
/// `stream_subscribe_gov_e2e::unflushed_write_is_fresh`, but the wait crosses the wire
/// (`AwaitChangelog`, whose client deadline is the server timeout + 2s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_wakes_on_a_write_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // A reader blocked at the tail BEFORE the new write lands.
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
    // ONE inline write, no flush — bounded by FEED_POLL_INTERVAL (1s) even if the notify
    // is missed.
    h.write("updateWidget", json!({ "id": "1", "qty": "42" }))
        .await;

    let (status, lines) = reader.await.expect("reader task joined");
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(
        lines.len(),
        1,
        "the blocked reader woke on the write, over the wire: {lines:?}"
    );
    assert_eq!(lines[0]["change_kind"], json!("-U"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_cdc_type_is_400_not_501() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // `Gadget` is a defined, readable type bound to a plain (non-CDC) table, so the
    // request reaches the probe: the engine answers `present: false` and query-api maps
    // it to 400 — NOT the 501 the wire client used to return for EVERY type.
    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Gadget/changes",
        "writer",
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a defined-but-non-CDC type is exactly 400 (the 501 path is gone): {lines:?}"
    );
}

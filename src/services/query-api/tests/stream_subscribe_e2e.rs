//! GET /objects/Widget/changes e2e (road-stream-subscribe): the full governed
//! NDJSON feed through the real router.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
//!
//!   (a) `full_stream_across_a_flush_boundary` — the complete ordered 5-event
//!       stream (inline ∪ files, -U included) closes on its own in bounded mode.
//!   (b) `resume_is_gapless_and_dup_free` — a reconnect from a returned opaque
//!       cursor picks up exactly where the first read left off.
//!   (c) `latest_cursor_joins_the_tail` — `?cursor=latest` skips the backlog and
//!       only surfaces events committed after connect.
//!   (d) `two_consumers_are_independent` — two concurrent readers from different
//!       cursors each see exactly their own slice.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::StreamTables;
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{
    EngineGuard, InProcessServingEngine, connect_gov_client, define_widget, get_ndjson,
    grant_writer,
};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// `(bucket, offset, change_kind)` for one rendered NDJSON line.
fn key(line: &serde_json::Value) -> (i64, i64, String) {
    (
        line["bucket"].as_i64().expect("bucket"),
        line["offset"].as_i64().expect("offset"),
        line["change_kind"]
            .as_str()
            .expect("change_kind")
            .to_string(),
    )
}

fn keys(lines: &[serde_json::Value]) -> Vec<(i64, i64, String)> {
    lines.iter().map(key).collect()
}

/// Per-bucket offsets are gapless from 0, and the whole sequence is sorted by
/// `(bucket, offset)`. JSON-line adaptation of `stream_subscribe_scan.rs`'s
/// `assert_ordered_gapless`.
fn assert_ordered_gapless(lines: &[serde_json::Value]) {
    let k = keys(lines);
    let mut sorted = k.clone();
    sorted.sort();
    assert_eq!(k, sorted, "events ordered by (bucket, offset): {k:?}");
    let mut cursor: BTreeMap<i64, i64> = BTreeMap::new();
    for (bucket, offset, _) in &k {
        let at = cursor.get(bucket).copied().unwrap_or(0);
        assert_eq!(*offset, at, "gapless per-bucket offsets from 0: {k:?}");
        cursor.insert(*bucket, at + 1);
    }
}

/// No `loom_*` key leaks, at the top level of a line or inside `fields`.
fn assert_no_loom_keys(lines: &[serde_json::Value]) {
    for l in lines {
        assert!(
            l.as_object()
                .expect("line is a JSON object")
                .keys()
                .all(|k| !k.starts_with("loom_")),
            "no top-level loom_* key: {l}"
        );
        let fields = l["fields"].as_object().expect("fields object");
        assert!(
            fields.keys().all(|k| !k.starts_with("loom_")),
            "no loom_* key in fields: {fields:?}"
        );
    }
}

/// Owns everything the lifecycle e2e needs kept alive for the duration of a
/// test: the spawned gRPC-wire engine (`eg`), the Parquet warehouse
/// (`_warehouse`), the owned control-plane/action-client/serving-engine trio
/// used to run MORE actions after the initial seed (via `write`), and the
/// Arc'd `(cp, ServingEngine)` pair `get_ndjson` drives the router with.
struct Harness {
    cp: PgControlPlane,
    engine: query_api::engine_action_client::EngineActionClient,
    serving: InProcessServingEngine,
    subj: control_plane_core::SubjectId,
    cp_arc: Arc<PgControlPlane>,
    read_eng: Arc<dyn query_api::serving::ServingEngine>,
    #[expect(dead_code, reason = "kept alive for the engine's UDS server task")]
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
/// writer, spawn the gRPC-wire engine, and write the 5-event sequence every case
/// in this file assumes: `+I(1)`, `-U/+U(1: qty 1->9)`, a `flush_table` boundary
/// (events move to the changelog files, inline end-capped), then `+I(2)`,
/// `-D(2)`.
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

        // +I(1), -U/+U(1: qty 1->9) — all inline.
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

        // Flush: the 3 events above move to the changelog files tier.
        let gov = connect_gov_client(&eg.sock).await;
        gov.flush_table("main".to_string(), "widget".to_string())
            .await
            .expect("flush_table");

        // +I(2), -D(2) — post-flush, inline again: the scan spans the boundary.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_stream_across_a_flush_boundary() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // `max_lines=6` (one more than the known 5) so a natural end-of-stream (not
    // the client's own cutoff) is what stops the read — proves bounded mode
    // actually closes the body.
    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=5",
        "writer",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(
        lines.len(),
        5,
        "the stream ended on its own at exactly 5 events (bounded mode closes): {lines:?}"
    );

    for l in &lines {
        for k in ["bucket", "offset", "change_kind", "fields", "cursor"] {
            assert!(l.get(k).is_some(), "line missing `{k}`: {l}");
        }
    }
    assert_no_loom_keys(&lines);
    assert_ordered_gapless(&lines);

    let mut kinds: Vec<&str> = lines
        .iter()
        .map(|l| l["change_kind"].as_str().expect("change_kind"))
        .collect();
    kinds.sort_unstable();
    let mut want = vec!["+I", "-U", "+U", "+I", "-D"];
    want.sort_unstable();
    assert_eq!(kinds, want, "change-kind multiset");

    let minus_u = lines
        .iter()
        .find(|l| l["change_kind"] == "-U")
        .expect("-U line present");
    assert_eq!(
        minus_u["fields"]["qty"],
        json!(1),
        "-U carries the before-image (qty=1): {minus_u}"
    );

    let minus_d = lines
        .iter()
        .find(|l| l["change_kind"] == "-D")
        .expect("-D line present");
    assert_eq!(
        minus_d["fields"]["qty"],
        json!(2),
        "-D carries the FULL prior image (qty=2), not an id-only tombstone: {minus_d}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_is_gapless_and_dup_free() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, full) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=5",
        "writer",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{full:?}");
    assert_eq!(full.len(), 5);

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
    let resume_cursor = head[1]["cursor"]
        .as_str()
        .expect("cursor string")
        .to_string();

    let uri = format!("/objects/Widget/changes?cursor={resume_cursor}&max_events=3");
    let (status, tail) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        &uri,
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{tail:?}");
    assert_eq!(tail.len(), 3);

    let mut joined = keys(&head);
    joined.extend(keys(&tail));
    assert_eq!(
        joined,
        keys(&full),
        "resume is gapless and dup-free: head+tail == full read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_cursor_joins_the_tail() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // The 5 pre-existing events, for the "none of these reappear" check below.
    let (status, old) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=5",
        "writer",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old:?}");
    assert_eq!(old.len(), 5);
    let old_keys = keys(&old);

    let cp_arc = h.cp_arc.clone();
    let read_eng = h.read_eng.clone();
    let reader = tokio::spawn(async move {
        get_ndjson(
            cp_arc,
            read_eng,
            "/objects/Widget/changes?cursor=latest&max_events=2",
            "writer",
            2,
            Duration::from_secs(10),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    h.write("updateWidget", json!({ "id": "1", "qty": "20" }))
        .await;

    let (status, lines) = reader.await.expect("reader task joined");
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(lines.len(), 2, "exactly the 2 NEW events: {lines:?}");
    assert_eq!(lines[0]["change_kind"], json!("-U"));
    assert_eq!(lines[1]["change_kind"], json!("+U"));

    let (bucket0, offset0, _) = key(&lines[0]);
    let (bucket1, offset1, _) = key(&lines[1]);
    assert_eq!(bucket0, bucket1, "-U/+U share one bucket: {lines:?}");
    assert_eq!(offset1, offset0 + 1, "consecutive offsets: {lines:?}");

    let max_old_offset_in_bucket = old_keys
        .iter()
        .filter(|(b, ..)| *b == bucket0)
        .map(|(_, o, _)| *o)
        .max()
        .unwrap_or(-1);
    assert!(
        offset0 > max_old_offset_in_bucket,
        "the new events are strictly after every pre-subscribe offset in bucket {bucket0}: \
         new offset0={offset0}, max old offset={max_old_offset_in_bucket}"
    );
    assert!(
        !old_keys.contains(&key(&lines[0])) && !old_keys.contains(&key(&lines[1])),
        "none of the 5 old events reappear: new={:?} old={old_keys:?}",
        keys(&lines)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_consumers_are_independent() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, full) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=5",
        "writer",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{full:?}");
    assert_eq!(full.len(), 5);

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
    let mid_cursor = head[1]["cursor"]
        .as_str()
        .expect("cursor string")
        .to_string();
    let expected_tail = keys(&full[2..]);

    let uri_a = "/objects/Widget/changes?max_events=5".to_string();
    let uri_b = format!("/objects/Widget/changes?cursor={mid_cursor}&max_events=3");

    let (res_a, res_b) = tokio::join!(
        get_ndjson(
            h.cp_arc.clone(),
            h.read_eng.clone(),
            &uri_a,
            "writer",
            5,
            Duration::from_secs(10),
        ),
        get_ndjson(
            h.cp_arc.clone(),
            h.read_eng.clone(),
            &uri_b,
            "writer",
            3,
            Duration::from_secs(10),
        ),
    );

    let (status_a, lines_a) = res_a;
    let (status_b, lines_b) = res_b;
    assert_eq!(status_a, StatusCode::OK, "{lines_a:?}");
    assert_eq!(status_b, StatusCode::OK, "{lines_b:?}");
    assert_eq!(
        keys(&lines_a),
        keys(&full),
        "consumer A (earliest, max_events=5): the full read"
    );
    assert_eq!(
        keys(&lines_b),
        expected_tail,
        "consumer B (mid-cursor, max_events=3): exactly its own slice"
    );
}

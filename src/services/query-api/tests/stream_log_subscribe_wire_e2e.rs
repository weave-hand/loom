//! Log-table subscribe over the PRODUCTION WIRE (road-stream-log-table-subscribe): the
//! same `GET /objects/{type}/changes` NDJSON feed the in-process
//! `stream_log_subscribe_e2e` proves, but read through `EngineServingClient` against a
//! real engine socket instead of the in-process engine.
//!
//! The whole point of this test: the `ChangelogFeed` unary `EngineControl` RPC (shipped
//! in #426) is kind-agnostic — it forwards to `changelog_feed_scan`, which now dispatches
//! to the LOG arm engine-side — so a log table serves over the production wire with ZERO
//! client/handler/proto change. Mirrors `stream_subscribe_wire_e2e.rs`'s wire-engine spawn
//! and wire `ServingEngine` construction verbatim; only the seed (a LOG stream, not a CDC
//! table), the type (`define_log_type`, no mutation actions), and the grant (a `reader`
//! subject — a log type has no write actions, so there is no writer subject to reuse)
//! differ.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::{RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use e2e_support::{
    define_log_type, get_ndjson, grant_read, seed_log_stream, spawn_engine_full, subject_with_role,
};
use loom_test_seed::local_sql_catalog;
use query_api::engine_client::EngineServingClient;

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

struct Wire {
    cp_arc: Arc<control_plane_postgres::PgControlPlane>,
    read_eng: Arc<dyn query_api::serving::ServingEngine>,
    _guard: loom_test_flight::EngineGuard,
    _warehouse: tempfile::TempDir,
}

/// A 2-bucket LOG stream with 5 events across a flush boundary: 3 inline (`+I` @0,1,2
/// across buckets) — flush — 2 more (`+I` @ next offsets). Defines `Event` over it with
/// NO mutation actions (a log type is read-only), grants a `reader` subject `Read`, spawns
/// the engine over a UDS serving BOTH `EngineControl` and Flight (the production shape),
/// and wires the READ path to the wire client (`EngineServingClient`) — the substitution
/// that makes this a wire e2e rather than an in-process one.
async fn setup(fx: &PgFixture) -> Wire {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(dsn, &warehouse.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "events".into(),
    };

    // 3 inline + flush + 2 more = 5 events, all '+I'.
    seed_log_stream(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30], 2).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    seed_log_stream(&pool, &catalog, &table, &[4, 5], &[40, 50], 2).await;

    let _ty = define_log_type(&cp, "Event", &table).await;
    // Grant a `reader` subject Read on `Event` BEFORE the request, or the coarse gate
    // (`resolve_governed`) 403s before the subscribe probe ever runs. A log type has no
    // write actions, so there is no writer subject to reuse here (unlike the CDC wire
    // template, which reads as its `writer` subject) — a fresh reader role is the only
    // path to a granted subject.
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Event").await;

    let (sock, guard) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;

    Wire {
        cp_arc: Arc::new(cp),
        // THE substitution: the read engine is the production wire client.
        read_eng: Arc::new(
            EngineServingClient::connect(sock.clone())
                .await
                .expect("connect EngineServingClient"),
        ),
        _guard: guard,
        _warehouse: warehouse,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_feed_streams_over_the_wire_across_a_flush_boundary() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Event/changes?max_events=5",
        "reader",
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "no 501 for a log table on the wire: {lines:?}"
    );
    assert_eq!(
        lines.len(),
        5,
        "the log stream ended on its own at 5 events: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["change_kind"] == "+I"),
        "all '+I' over the wire: {lines:?}"
    );
    for l in &lines {
        assert!(
            l["fields"]
                .as_object()
                .expect("fields")
                .keys()
                .all(|k| !k.starts_with("loom_")),
            "no framing keys leaked over the wire: {l}"
        );
    }
    // Ordered gapless per bucket from 0.
    let mut cursor: BTreeMap<i64, i64> = BTreeMap::new();
    for (b, o, _) in keys(&lines) {
        let at = cursor.get(&b).copied().unwrap_or(0);
        assert_eq!(o, at, "gapless offsets over the wire");
        cursor.insert(b, at + 1);
    }

    // Resume: head(2) then tail(3) from the returned cursor == full read.
    let (_s, head) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Event/changes?max_events=2",
        "reader",
        2,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(head.len(), 2);
    let resume = head[1]["cursor"].as_str().expect("cursor").to_string();
    let uri = format!("/objects/Event/changes?cursor={resume}&max_events=3");
    let (_s, tail) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        &uri,
        "reader",
        3,
        Duration::from_secs(10),
    )
    .await;
    let mut joined = keys(&head);
    joined.extend(keys(&tail));
    assert_eq!(
        joined,
        keys(&lines),
        "resume gapless + dup-free over the wire"
    );
}

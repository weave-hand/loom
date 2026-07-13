//! GET /objects/Event/changes e2e (road-stream-log-table-subscribe): the full
//! governed NDJSON feed for an append-only LOG type through the real router —
//! every event a '+I', ordered gapless, resumable, across a flush boundary, and
//! a non-stream type still 400s. loom_fixture_test.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::TableRef;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use e2e_support::{
    InProcessServingEngine, define_log_type, get_ndjson, grant_read, seed_log_stream,
    subject_with_role,
};
use loom_test_seed::local_sql_catalog;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_log_stream_across_a_flush_boundary() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "s".into(),
        name: "events".into(),
    };

    // 3 inline + flush + 2 more = 5 events, all '+I'.
    seed_log_stream(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30], 2).await;
    flush_table(
        &catalog,
        &pool,
        &table,
        control_plane_core::RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("flush");
    seed_log_stream(&pool, &catalog, &table, &[4, 5], &[40, 50], 2).await;

    let _ty = define_log_type(&cp, "Event", &table).await;
    // Grant the `reader` subject Read on `Event` — WITHOUT this the coarse gate
    // (`resolve_governed`) denies with 403 before the subscribe probe runs.
    // `get_ndjson(..., "reader", ...)` mints a session token for subject "reader".
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Event").await;
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new(IcebergCatalog::new(pool.clone())),
    );

    let (status, lines) = get_ndjson(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Event/changes?max_events=5",
        "reader", // the granted role name used by get_ndjson (match the grant helper)
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(
        lines.len(),
        5,
        "the log stream ended on its own at 5 events: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["change_kind"] == "+I"),
        "all '+I': {lines:?}"
    );
    // No loom_* keys leak; user fields present.
    for l in &lines {
        assert!(
            l["fields"]
                .as_object()
                .expect("fields")
                .keys()
                .all(|k| !k.starts_with("loom_"))
        );
    }
    // Ordered gapless per bucket from 0.
    let mut cursor: BTreeMap<i64, i64> = BTreeMap::new();
    for (b, o, _) in keys(&lines) {
        let at = cursor.get(&b).copied().unwrap_or(0);
        assert_eq!(o, at, "gapless offsets");
        cursor.insert(b, at + 1);
    }

    // Resume: head(2) then tail(3) from the returned cursor == full read.
    let (_s, head) = get_ndjson(
        cp_arc.clone(),
        read_eng.clone(),
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
        cp_arc.clone(),
        read_eng.clone(),
        &uri,
        "reader",
        3,
        Duration::from_secs(10),
    )
    .await;
    let mut joined = keys(&head);
    joined.extend(keys(&tail));
    assert_eq!(joined, keys(&lines), "resume gapless + dup-free");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_stream_type_is_not_subscribable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    // A type over a table with NO stream declaration (here: no mirror row at all,
    // so `stream_meta_for`/the positions probe resolve to `None`). The subject is
    // GRANTED Read so the coarse gate passes and we exercise the probe's None →
    // 400 path (not the 403 deny path).
    let table = TableRef {
        schema: "s".into(),
        name: "plain".into(),
    };
    let _ty = define_log_type(&cp, "Plain", &table).await;
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Plain").await;
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> = Arc::new(
        InProcessServingEngine::new(IcebergCatalog::new(pool.clone())),
    );
    let (status, _lines) = get_ndjson(
        cp_arc,
        read_eng,
        "/objects/Plain/changes?max_events=1",
        "reader",
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-stream type is not subscribable"
    );
}

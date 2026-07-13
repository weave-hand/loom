//! The three changelog RPCs on `EngineControl` (road-stream-subscribe-wire, Part B),
//! driven over a real engine socket: `ChangelogLatest` probes subscribability,
//! `ChangelogFeed` serves one bounded governed page as JSON, `AwaitChangelog`
//! long-polls. These three are what lift the production feed off its 501.

use std::collections::BTreeMap;
use std::time::Duration;

use control_plane_core::ChangeFeedPage;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_wide_widget,
    define_widget, grant_writer, seed_widget_create_then_update, spawn_engine_full,
};
use engine_wire::client::WirePolicy;
use query_api::action::{ActionDeps, run_action};
use query_api::engine_action_client::EngineActionClient;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_rpcs_probe_serve_and_wait() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Control + Flight on ONE socket, exactly as production serves them.
    let (sock, _eg) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let engine = EngineActionClient::connect(sock.clone())
        .await
        .expect("connect EngineActionClient");
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 3 events: +I(1) @0, -U(1) @1, +U(1) @2.
    seed_widget_create_then_update(&subj, &deps).await;

    let client = connect_gov_client(&sock).await;

    // 1. ChangelogLatest — a declared CDC table reports its per-bucket high-water.
    let latest = client
        .changelog_latest("main".to_string(), "widget".to_string())
        .await
        .expect("changelog_latest")
        .expect("a declared CDC table is subscribable");
    assert_eq!(
        latest,
        BTreeMap::from([(0, 3)]),
        "high-water is one past the last event"
    );

    // A table that is not a declared CDC table is Ok(None) — the "not subscribable"
    // discriminator query-api maps to its 400. NOT an error, NOT Unsupported.
    let plain = client
        .changelog_latest("main".to_string(), "nope".to_string())
        .await
        .expect("a non-CDC table is Ok(None), not an error");
    assert!(plain.is_none());

    // 2. ChangelogFeed — one bounded, ordered page with advanced positions.
    let page: ChangeFeedPage = client
        .changelog_feed(
            "main".to_string(),
            "widget".to_string(),
            &BTreeMap::from([(0, 0)]),
            100,
            &WirePolicy::default(),
        )
        .await
        .expect("changelog_feed");
    let coords: Vec<(i32, i64)> = page.events.iter().map(|e| (e.bucket, e.offset)).collect();
    assert_eq!(coords, vec![(0, 0), (0, 1), (0, 2)], "ordered page");
    assert_eq!(page.next.get(&0).copied(), Some(3), "positions advanced");
    let kinds: Vec<&str> = page.events.iter().map(|e| e.change_kind.as_str()).collect();
    assert_eq!(
        kinds,
        vec!["+I", "-U", "+U"],
        "full change sequence incl. -U"
    );

    // The reserved framing columns never leak into the event fields.
    for e in &page.events {
        assert!(
            !e.fields.keys().any(|k| k.starts_with("loom_")),
            "framing column leaked into fields: {:?}",
            e.fields
        );
    }

    // Resume from `next`: no events left, positions unchanged. An empty page is a
    // legitimate answer, not an error — it is what drives the long-poll.
    let empty = client
        .changelog_feed(
            "main".to_string(),
            "widget".to_string(),
            &page.next,
            100,
            &WirePolicy::default(),
        )
        .await
        .expect("empty feed page");
    assert!(empty.events.is_empty());
    assert_eq!(
        empty.next, page.next,
        "an empty page returns the caller's positions unchanged"
    );

    // FIX 2 regression: the caught-up fast path (a cheap Postgres probe that skips
    // the object-store scan when every bucket is exhausted) must NEVER wrongly
    // suppress a real event — it has to re-probe on every call, not cache a stale
    // "nothing available" verdict. Land one more write from the caught-up
    // position, then confirm the very next `changelog_feed` call returns it.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "42" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("-U/+U(1) landed after the caught-up probe");
    let after_write = client
        .changelog_feed(
            "main".to_string(),
            "widget".to_string(),
            &empty.next,
            100,
            &WirePolicy::default(),
        )
        .await
        .expect("changelog_feed right after a fresh write");
    let after_coords: Vec<(i32, i64)> = after_write
        .events
        .iter()
        .map(|e| (e.bucket, e.offset))
        .collect();
    assert_eq!(
        after_coords,
        vec![(0, 3), (0, 4)],
        "the fast path must not suppress an event landed just after an empty probe: {:?}",
        after_write.events
    );

    // 3. AwaitChangelog — returns cleanly at its timeout when no write lands.
    client
        .await_changelog(
            "main".to_string(),
            "widget".to_string(),
            Duration::from_millis(300),
        )
        .await
        .expect("await_changelog returns Ok at timeout, never errors");
}

/// FIX 1 regression: a WIDE-ROW CDC type must not become a poison pill. The feed
/// page was bounded by EVENT COUNT alone (`FEED_BATCH_LIMIT` / `MAX_FEED_LIMIT`),
/// never by BYTES — a caught-up subscriber of a type with a large text column could
/// ask for a full page whose `page_json` the wire client then fails to decode, and
/// because the HTTP response is already committed as 200 the body just ends: the
/// consumer reads a clean EOF, thinks it caught up, reconnects at the SAME
/// position, and reproduces the identical oversized page — forever.
///
/// This seeds a type whose rows are individually modest (~900 KB) but whose
/// COMBINED page comfortably exceeds the engine's byte budget, and proves: (i) the
/// call still SUCCEEDS (the decode ceiling was raised to cover one legitimately
/// wide row), (ii) the engine TRUNCATES rather than erroring or hanging — strictly
/// fewer events than were written/requested, (iii) `next` is exactly consistent
/// with what was actually returned (not what was scanned), and (iv) resuming from
/// `next` returns precisely the remaining events — no gap, no duplicate. That last
/// assertion is what proves the truncation is safe rather than just "doesn't crash".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_feed_wide_row_page_is_byte_truncated_and_resumable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "wide_widget", 1).await;
    let wide = define_wide_widget(&cp).await;
    let subj = grant_writer(&cp, &wide).await;

    let (sock, _eg) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let engine = EngineActionClient::connect(sock.clone())
        .await
        .expect("connect EngineActionClient");
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 12 rows x a ~900 KB blob each (~10.8 MB combined) — comfortably over the
    // engine's byte budget (8 MiB) while each individual row stays well under the
    // engine-wire channel's raised decode ceiling (64 MiB).
    const ROWS: i64 = 12;
    let blob = "x".repeat(900_000);
    for id in 0..ROWS {
        run_action(
            "createWideWidget",
            json!({ "id": id.to_string(), "blob": blob.clone() })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .unwrap_or_else(|e| panic!("createWideWidget({id}): {e:?}"));
    }

    let client = connect_gov_client(&sock).await;

    // Ask for every written row in one page: without byte-bounding, this would be
    // ~10.8 MB of `page_json`.
    let page = client
        .changelog_feed(
            "main".to_string(),
            "wide_widget".to_string(),
            &BTreeMap::from([(0, 0)]),
            ROWS as u64,
            &WirePolicy::default(),
        )
        .await
        .expect("changelog_feed must decode successfully despite the wide rows");

    let n = page.events.len();
    assert!(
        n >= 1,
        "truncation must NEVER drop to zero events (that is the poison-pill stall)"
    );
    assert!(
        (n as i64) < ROWS,
        "the page must have been byte-truncated: got {n} of {ROWS} written rows in one page"
    );

    let coords: Vec<i64> = page.events.iter().map(|e| e.offset).collect();
    let want_coords: Vec<i64> = (0..n as i64).collect();
    assert_eq!(
        coords, want_coords,
        "the retained prefix is contiguous from offset 0"
    );
    assert_eq!(
        page.next.get(&0).copied(),
        Some(n as i64),
        "`next` is exactly one past the last event ACTUALLY returned, not the full scan"
    );

    // Resume from `next`: the remaining rows come back with no gap, no duplicate —
    // the assertion that proves the truncation is SAFE, not merely non-crashing.
    let rest = client
        .changelog_feed(
            "main".to_string(),
            "wide_widget".to_string(),
            &page.next,
            ROWS as u64,
            &WirePolicy::default(),
        )
        .await
        .expect("resumed changelog_feed");
    let rest_coords: Vec<i64> = rest.events.iter().map(|e| e.offset).collect();
    let want_rest: Vec<i64> = (n as i64..ROWS).collect();
    assert_eq!(
        rest_coords, want_rest,
        "resuming from `next` yields exactly the remaining events: no gap, no duplicate"
    );
}

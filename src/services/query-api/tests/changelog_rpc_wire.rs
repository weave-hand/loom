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
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, grant_writer,
    spawn_engine_full,
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

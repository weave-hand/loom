//! Torn-read regression (`iss-stream-feed-torn-read`, closed by
//! `road-stream-subscribe-wire` Part A).
//!
//! The bug: the two tiers were read at two DB states. Because the changelog tier is
//! read FIRST it can only skew OLDER, so the harmful interleave is a HOLE, not a
//! duplicate — a flush (invisible to the stale file tier) plus a follow-on write
//! (which advances the base snapshot past the flush's inline end-cap) leaves the
//! flushed events in NEITHER tier, and the `next` fold then skips them forever.
//!
//! Three cases: the pinned page is self-consistent across that interleave; the inline
//! tier is genuinely as-of (the property Part A rests on); and the real, unpinned
//! public scan is gapless under ACTUAL concurrency (flush + writes racing a reader).

use std::collections::BTreeMap;

use control_plane_core::{Catalog, ChangeFeedPage, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::changelog_table_ref;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, grant_writer,
    spawn_engine_writer,
};
use engine_serving::TablePolicy;
use engine_serving::feed::{FeedPins, changelog_feed_scan, changelog_feed_scan_at};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// Every (bucket, offset) in the page, in emission order.
fn coords(page: &ChangeFeedPage) -> Vec<(i32, i64)> {
    page.events.iter().map(|e| (e.bucket, e.offset)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_page_is_self_consistent_across_a_concurrent_flush_and_write() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // ONE bucket => offsets are a plain 0,1,2,… sequence in write order.
    let table = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 3 events, all inline: +I(1) @0, then -U(1) @1 and +U(1) @2.
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

    let cat = IcebergCatalog::new(pool.clone());

    // --- PIN: exactly what the fixed scan does first, atomically. Nothing has flushed,
    // so the changelog table does not exist yet => clog pin is None. ---
    let clog = changelog_table_ref(&table);
    let (base_pin, clog_pin) = cat
        .current_snapshots_pair(&table, &clog)
        .await
        .expect("pair read");
    let pins = FeedPins {
        base: base_pin.expect("base table is live"),
        clog: clog_pin,
    };

    // --- The harmful interleave, committed AFTER the pin: a flush (one tx: appends the
    // changelog files AND end-caps the same inline rows in ONE tx), then a follow-on
    // inline write. ---
    connect_gov_client(&eg.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("+I(2) after the flush");

    // --- The read, at the pins captured BEFORE the flush. ---
    let page = changelog_feed_scan_at(
        &cat,
        &table,
        None,
        &BTreeMap::from([(0, 0)]),
        100,
        &TablePolicy::default(),
        &pins,
    )
    .await
    .expect("pinned scan");

    // The pinned view is exactly the 3 pre-flush events — contiguous from 0, NO HOLE.
    // The flush's changelog files are invisible (newer than the clog pin) and its
    // end-cap is invisible (end_snapshot > the base pin), so every event lives in
    // exactly ONE tier: the XOR invariant holds at the pinned pair.
    assert_eq!(
        coords(&page),
        vec![(0, 0), (0, 1), (0, 2)],
        "pinned page must be the pre-flush event set, contiguous and dup-free"
    );
    assert_eq!(
        page.next.get(&0).copied(),
        Some(3),
        "`next` advances to exactly one past the last event actually emitted"
    );

    // Resuming from `next` on a FRESH pin picks up precisely what the pinned page did
    // not carry — nothing skipped (the bug), nothing repeated.
    let rest = changelog_feed_scan(&cat, &table, None, &page.next, 100, &TablePolicy::default())
        .await
        .expect("resumed scan");
    assert_eq!(
        coords(&rest),
        vec![(0, 3)],
        "resume across the flush boundary is gapless and dup-free"
    );
}

/// The property Part A rests on: the inline tier is genuinely AS-OF, so a flush that
/// end-caps rows does NOT retract them from a reader pinned before it. (Spec's "as-of
/// inline visibility" test, expressed against the read that already implements it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tier_is_as_of_across_a_flush_end_cap() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let table = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
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

    let cat = IcebergCatalog::new(pool.clone());
    let before = cat
        .current_snapshot(&table)
        .await
        .expect("pre-flush snapshot");

    // Pre-flush pin: the row is live inline.
    let live = cat
        .inline_live_batch_full(&table, before.id)
        .await
        .expect("inline read")
        .expect("one live inline row before the flush");
    assert_eq!(live.2.num_rows(), 1, "the +I row is inline pre-flush");

    connect_gov_client(&eg.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");

    // AT THE PRE-FLUSH PIN the row is STILL visible: the flush end-capped it at a NEWER
    // snapshot (`end_snapshot > before`), and `mvcc_live_pred` is as-of. This is what
    // makes the pinned union disjoint rather than holey.
    let still = cat
        .inline_live_batch_full(&table, before.id)
        .await
        .expect("as-of inline read")
        .expect("the end-capped row is STILL visible at the pre-flush pin");
    assert_eq!(
        still.2.num_rows(),
        1,
        "an end-cap must not retract rows from a reader pinned before it"
    );

    // At the CURRENT snapshot it is gone from inline (it lives in the files now) — the
    // XOR invariant, on a single consistent state.
    let now = cat
        .current_snapshot(&table)
        .await
        .expect("post-flush snapshot");
    let after = cat
        .inline_live_batch_full(&table, now.id)
        .await
        .expect("inline read");
    assert!(
        after.is_none(),
        "post-flush the row is in the file tier, NOT inline (inline XOR files)"
    );
}

/// The real, unpinned, PUBLIC scan under ACTUAL concurrency: a reader pages the feed
/// while writes and a flush land underneath it. The concatenated stream must be
/// gapless and duplicate-free. This is the closest a test can get to asserting the
/// atomicity itself (rather than its consequences).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_scan_is_gapless_under_concurrent_flush_and_writes() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let table: TableRef = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    let cat = IcebergCatalog::new(pool.clone());

    // Writer: 6 creates, flushing midway — the flush races the reader's paging.
    let mut expected = 0i64;
    for i in 0..6i64 {
        run_action(
            "createWidget",
            json!({ "id": i.to_string(), "name": "x", "qty": "1" })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("+I");
        expected += 1;
        if i == 2 {
            connect_gov_client(&eg.sock)
                .await
                .flush_table("main".to_string(), "widget".to_string())
                .await
                .expect("flush_table");
        }
    }

    // Page the feed 2 events at a time, resuming from `next` — the production loop.
    let mut positions: BTreeMap<i32, i64> = BTreeMap::from([(0, 0)]);
    let mut seen: Vec<(i32, i64)> = Vec::new();
    for _ in 0..10 {
        let page = changelog_feed_scan(&cat, &table, None, &positions, 2, &TablePolicy::default())
            .await
            .expect("scan");
        if page.events.is_empty() {
            break;
        }
        seen.extend(coords(&page));
        positions = page.next;
    }

    let want: Vec<(i32, i64)> = (0..expected).map(|o| (0, o)).collect();
    assert_eq!(
        seen, want,
        "every event exactly once, in order, across the flush boundary: {seen:?}"
    );
}

//! changelog_feed_scan primitive e2e (road-stream-subscribe): the governed,
//! (bucket, offset)-ordered disjoint union of changelog files ∪ live inline
//! tail, resumable by per-bucket positions.
//!   1. 5 events written via governed actions (+I, -U, +U for id=1; +I, -D for
//!      id=2), all inline: scan from earliest returns all 5, ordered, -U incl.
//!   2. flush_table (events move to the changelog files, inline end-capped):
//!      the SAME scan returns the SAME 5 events — inline XOR files, no dup.
//!   3. two more events after the flush (update id=1): a scan spans the
//!      boundary (files ∪ inline) and returns all 7.
//!   4. resume: scan(limit=3) then scan(from next) concatenate to exactly the
//!      full sequence — no gap, no overlap.
//!   5. governance: a masked column reads '***' on every event; fields never
//!      carry a loom_* key.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::collections::BTreeMap;

use control_plane_core::{ChangeEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, grant_writer,
};
use engine_serving::TablePolicy;
use engine_serving::feed::changelog_feed_scan;
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

fn keys(evs: &[ChangeEvent]) -> Vec<(i32, i64, String)> {
    evs.iter()
        .map(|e| (e.bucket, e.offset, e.change_kind.clone()))
        .collect()
}

/// Per-bucket offsets are gapless from the given start positions, and the whole
/// sequence is sorted by (bucket, offset).
fn assert_ordered_gapless(evs: &[ChangeEvent], start: &BTreeMap<i32, i64>) {
    let k = keys(evs);
    let mut sorted = k.clone();
    sorted.sort();
    assert_eq!(k, sorted, "events ordered by (bucket, offset): {k:?}");
    let mut cursor = start.clone();
    for e in evs {
        let at = cursor.get(&e.bucket).copied().unwrap_or(0);
        assert_eq!(e.offset, at, "gapless per-bucket offsets: {k:?}");
        cursor.insert(e.bucket, at + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_scan_unions_files_and_inline_ordered_and_resumable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "widget", 2).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 5 events: +I(1), -U(1), +U(1), +I(2), -D(2).
    for (action, body) in [
        (
            "createWidget",
            json!({ "id": "1", "name": "a", "qty": "1" }),
        ),
        ("updateWidget", json!({ "id": "1", "qty": "9" })),
        (
            "createWidget",
            json!({ "id": "2", "name": "b", "qty": "2" }),
        ),
        ("deleteWidget", json!({ "id": "2" })),
    ] {
        run_action(action, body.as_object().unwrap(), &subj, &deps)
            .await
            .unwrap_or_else(|e| panic!("{action}: {e:?}"));
    }

    let catalog = IcebergCatalog::new(pool.clone());
    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let earliest: BTreeMap<i32, i64> = BTreeMap::from([(0, 0), (1, 0)]);
    let open = TablePolicy::default();

    // 1. All-inline scan.
    let page1 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan inline");
    assert_eq!(
        page1.events.len(),
        5,
        "+I,-U,+U,+I,-D: {:?}",
        keys(&page1.events)
    );
    assert_ordered_gapless(&page1.events, &earliest);
    let kinds: Vec<&str> = page1
        .events
        .iter()
        .map(|e| e.change_kind.as_str())
        .collect();
    assert_eq!(
        kinds.iter().filter(|k| **k == "-U").count(),
        1,
        "-U emitted: {kinds:?}"
    );
    for e in &page1.events {
        assert!(
            e.fields.keys().all(|k| !k.starts_with("loom_")),
            "no framing key in fields: {:?}",
            e.fields
        );
    }
    // The -U carries the before-image (qty=1).
    let minus_u = page1
        .events
        .iter()
        .find(|e| e.change_kind == "-U")
        .expect("-U");
    assert_eq!(
        minus_u.fields.get("qty"),
        Some(&json!(1)),
        "-U before-image"
    );

    // 2. Flush: same events, now from the changelog files. Identical keys, no dup.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    let page2 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan files");
    assert_eq!(
        keys(&page2.events),
        keys(&page1.events),
        "inline XOR files: no dup, no gap"
    );

    // 3. Two more events post-flush: the scan spans the flush boundary.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "11" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("post-flush update");
    let page3 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan union");
    assert_eq!(
        page3.events.len(),
        7,
        "files ∪ inline: {:?}",
        keys(&page3.events)
    );
    assert_ordered_gapless(&page3.events, &earliest);

    // 4. Resume: limit 3, then from `next` — exact concatenation.
    let head = changelog_feed_scan(&catalog, &table, None, &earliest, 3, &open)
        .await
        .expect("scan head");
    assert_eq!(head.events.len(), 3);
    let tail = changelog_feed_scan(&catalog, &table, None, &head.next, 100, &open)
        .await
        .expect("scan tail");
    let mut joined = keys(&head.events);
    joined.extend(keys(&tail.events));
    assert_eq!(
        joined,
        keys(&page3.events),
        "resume is gapless and dup-free"
    );

    // 5. Masked column: qty reads back as the mask marker on every event.
    let masked = TablePolicy {
        row_filters: vec![],
        denied: std::collections::HashSet::new(),
        masked: std::collections::HashSet::from(["qty".to_string()]),
    };
    let page5 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &masked)
        .await
        .expect("scan masked");
    assert!(
        page5
            .events
            .iter()
            .all(|e| e.fields.get("qty") == Some(&json!("***"))),
        "masked column is '***' on every event"
    );

    drop(eg);
    drop(warehouse);
}

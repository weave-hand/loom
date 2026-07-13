//! road-stream-log-table-subscribe seam 2: `changelog_feed_scan`'s LOG arm —
//! the governed, (bucket, offset)-ordered disjoint union of the base table's
//! own files ∪ live inline tail, resumable by per-bucket positions, every event
//! a stored '+I'. Mirrors `stream_subscribe_scan.rs` (the CDC scan test).
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::collections::BTreeMap;

use control_plane_core::{ChangeEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use e2e_support::seed_log_stream;
use engine_serving::TablePolicy;
use engine_serving::feed::changelog_feed_scan;
use loom_test_seed::local_sql_catalog;

fn keys(evs: &[ChangeEvent]) -> Vec<(i32, i64, String)> {
    evs.iter()
        .map(|e| (e.bucket, e.offset, e.change_kind.clone()))
        .collect()
}

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
async fn log_feed_scan_unions_files_and_inline_ordered_and_resumable() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let ice = IcebergCatalog::new(pool.clone());
    let table = TableRef {
        schema: "s".into(),
        name: "events".into(),
    };
    let open = TablePolicy::default();

    // 3 rows into a 2-bucket log stream, all inline (buckets: 0,1,0).
    seed_log_stream(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30], 2).await;

    let earliest: BTreeMap<i32, i64> = BTreeMap::from([(0, 0), (1, 0)]);

    // 1. All-inline scan: 3 events, ordered, every kind '+I', no loom_* in fields.
    let page1 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan inline");
    assert_eq!(
        page1.events.len(),
        3,
        "3 inline events: {:?}",
        keys(&page1.events)
    );
    assert_ordered_gapless(&page1.events, &earliest);
    assert!(
        page1.events.iter().all(|e| e.change_kind == "+I"),
        "log events are all '+I': {:?}",
        keys(&page1.events)
    );
    for e in &page1.events {
        assert!(
            e.fields.keys().all(|k| !k.starts_with("loom_")),
            "no framing key in fields: {:?}",
            e.fields
        );
        assert!(
            e.fields.contains_key("id") && e.fields.contains_key("val"),
            "user cols present"
        );
    }

    // 2. Flush: same 3 events, now from the base files tier. Identical keys, no dup.
    flush_table(
        &catalog,
        &pool,
        &table,
        control_plane_core::RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("flush");
    let page2 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan files");
    assert_eq!(
        keys(&page2.events),
        keys(&page1.events),
        "files == inline, no dup/gap"
    );

    // 3. Two more rows post-flush: the scan spans the flush boundary (files ∪ inline).
    seed_log_stream(&pool, &catalog, &table, &[4, 5], &[40, 50], 2).await;
    let page3 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan union");
    assert_eq!(
        page3.events.len(),
        5,
        "files ∪ inline: {:?}",
        keys(&page3.events)
    );
    assert_ordered_gapless(&page3.events, &earliest);

    // 4. Resume: limit 3, then from `next` — exact concatenation.
    let head = changelog_feed_scan(&ice, &table, None, &earliest, 3, &open)
        .await
        .expect("head");
    assert_eq!(head.events.len(), 3);
    let tail = changelog_feed_scan(&ice, &table, None, &head.next, 100, &open)
        .await
        .expect("tail");
    let mut joined = keys(&head.events);
    joined.extend(keys(&tail.events));
    assert_eq!(
        joined,
        keys(&page3.events),
        "resume is gapless and dup-free"
    );

    // 5. Governance: masking `val` reads '***' on every event; framing survives.
    let masked = TablePolicy {
        row_filters: vec![],
        denied: std::collections::HashSet::new(),
        masked: std::collections::HashSet::from(["val".to_string()]),
    };
    let page5 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &masked)
        .await
        .expect("scan masked");
    assert_eq!(page5.events.len(), 5, "masking does not drop events");
    assert!(
        page5
            .events
            .iter()
            .all(|e| e.fields.get("val") == Some(&serde_json::json!("***"))),
        "masked column is '***' on every event"
    );
}

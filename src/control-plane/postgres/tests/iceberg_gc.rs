//! Fixture tests for physical GC of end-capped Iceberg-mirror rows (`iceberg_gc`).
//!
//! Covers the `SqlCatalog::delete_file` object-store seam and the `gc_table`
//! reclaim primitive: aged-out end-capped data files (+ their Parquet/stats) and
//! inline rows are physically reclaimed, in-window rows are protected, an
//! unaged table is a no-op, and GC serializes with a concurrent flush.

use control_plane_core::{Catalog, RunId, TableRef, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::{GcSummary, gc_table};
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use control_plane_postgres::vector_index::{VectorIndexRow, insert_vector_index};
use gc_test_support::{
    SEVEN_DAYS, advance, age_all_snapshots, age_snapshot, batch, columns, harness,
    inline_table_exists, inline_trigger_count, ipc_body, lineage, live_tid, local_path,
    mirror_row_counts, reclaimed_through, register_mv,
};
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;

/// The delete seam removes the object and is idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_file_removes_object_and_is_idempotent() {
    let fx = PgFixture::shared();
    let (_cp, _db, wh, catalog, _pool) = harness(fx).await;

    let obj = wh.path().join("victim.parquet");
    std::fs::write(&obj, b"bytes").expect("write object");
    let url = format!("file://{}", obj.display());
    assert!(obj.exists(), "object exists before delete");

    catalog.delete_file(&url).await.expect("delete");
    assert!(!obj.exists(), "object gone after delete");

    // Idempotent: deleting an absent object is not an error.
    catalog
        .delete_file(&url)
        .await
        .expect("second delete is a no-op");
}

/// Happy path + in-window protection for real data files (+ their Parquet/stats).
///
/// land s1 (file A, 10 rows) → overwrite s2 (file B, 4 rows; end-caps A@s2) →
/// overwrite s3 (file C, 2 rows; end-caps B@s3). Age s2 only ⇒ H = s2.
/// Reclaimable: A (end=s2 ≤ H). Retained: B (end=s3 > H), C (live).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_aged_data_files_and_keeps_in_window() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s2");
    let b_path = local_path(&ice.files_with_stats(&t, s2).await.expect("files@s2")[0].path);

    let s3 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s3");
    let c_path = local_path(&ice.files_with_stats(&t, s3).await.expect("files@s3")[0].path);

    assert!(
        a_path.exists() && b_path.exists() && c_path.exists(),
        "all 3 objects on disk before gc"
    );

    age_snapshot(&pool, s2.0).await; // H = s2 (s1, s3 stay recent)

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(
        summary,
        GcSummary {
            data_file_rows: 1,
            inline_rows: 0,
            objects_deleted: 1,
            held_by_mv_floor: 0
        }
    );

    // (a) A's row + object reclaimed; (b) B retained (end=s3 > H); C live.
    assert!(!a_path.exists(), "aged-out object A deleted");
    assert!(b_path.exists(), "in-window object B retained");
    assert!(c_path.exists(), "live object C retained");

    // (c) live read at current (s3) unchanged: file C, 2 rows.
    let cur = ice.current_snapshot(&t).await.expect("current");
    assert_eq!(cur.id, s3);
    let now = ice
        .files_with_stats(&t, s3)
        .await
        .expect("files@s3 post-gc");
    assert_eq!(now.len(), 1);
    assert_eq!(now[0].record_count, 2);

    // (b) structural: B still resolvable via the mirror at s2 (end=s3 > H).
    let at_s2 = ice
        .files_with_stats(&t, s2)
        .await
        .expect("files@s2 post-gc");
    assert_eq!(at_s2.len(), 1, "B retained in the mirror");
    assert_eq!(at_s2[0].record_count, 4);
}

/// Inline source: inline_append → flush end-caps the inline rows at the flush
/// snapshot Sf; aging Sf makes them reclaimable. The flushed real file (live)
/// survives and still reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_aged_inline_rows() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "inl".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    inline_append(
        &pool,
        &t,
        &columns(),
        &batch(3),
        lineage(run, "wh", "inl"),
        None,
        None,
    )
    .await
    .expect("inline_append");
    let sf = flush_table(&catalog, &pool, &t, run)
        .await
        .expect("flush")
        .expect("flushed something");

    age_snapshot(&pool, sf.0).await; // H = Sf; inline rows end-capped @ Sf are reclaimable

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert!(
        summary.inline_rows >= 1,
        "end-capped inline rows reclaimed (got {summary:?})"
    );

    // The flushed real file (live, end=NULL) is untouched: current read still has 3 rows.
    let cur = ice.current_snapshot(&t).await.expect("current");
    let files = ice.files_with_stats(&t, cur.id).await.expect("files");
    let live_rows: i64 = files.iter().map(|f| f.record_count).sum();
    assert_eq!(live_rows, 3, "live flushed data intact after gc");
}

/// No-op: nothing aged out ⇒ horizon undefined ⇒ reclaim nothing, succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_is_a_noop_when_nothing_aged_out() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "fresh".into(),
    };

    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "fresh"),
        None,
    )
    .await
    .expect("land");
    let s2 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![batch(4)], None, &[])
        .await
        .expect("ow");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    // No age injection: snapshots are fresh, so a 7d horizon reclaims nothing.
    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(summary, GcSummary::default(), "nothing reclaimed");

    assert!(a_path.exists(), "end-capped-but-in-window object retained");
    assert_eq!(ice.current_snapshot(&t).await.expect("current").id, s2);
}

/// Lock coexistence: gc_table and a concurrent flush_table on the same table take
/// the same advisory key and serialize — neither errors, final state is consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_serializes_with_concurrent_flush() {
    let fx = PgFixture::shared();
    let (_cp, db, wh, catalog_g, pool) = harness(fx).await;
    let catalog_f = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "race".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let ice = IcebergCatalog::new(pool.clone());

    // Seed: land A (s1) → overwrite B (s2; end-caps A) → age s2 so gc reclaims A,
    // then add live inline rows so flush has work to drain.
    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog_g,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "race"),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    let s2 =
        overwrite_parquet_snapshot(&pool, &catalog_g, &t, &columns(), vec![batch(4)], None, &[])
            .await
            .expect("ow");
    age_snapshot(&pool, s2.0).await;
    inline_append(
        &pool,
        &t,
        &columns(),
        &batch(2),
        lineage(run, "wh", "race"),
        None,
        None,
    )
    .await
    .expect("inline_append");

    // Each racer gets its OWN pool (pool_for caps at 5 connections), so the
    // advisory-lock waiter parked in pg_advisory_xact_lock never starves the active
    // task's work connections.
    let pool_gc = fx.pool_for(&db).await;
    let pool_flush = fx.pool_for(&db).await;
    let flush = tokio::spawn({
        let t_f = t.clone();
        async move { flush_table(&catalog_f, &pool_flush, &t_f, run).await }
    });
    let gc = tokio::spawn({
        let t_g = t.clone();
        async move { gc_table(&catalog_g, &pool_gc, &t_g, SEVEN_DAYS).await }
    });
    let gc_res = gc.await.expect("gc join");
    let flush_res = flush.await.expect("flush join");
    gc_res.expect("gc ok under contention");
    flush_res.expect("flush ok under contention");

    // Both did their work under contention, in either serialization order: gc
    // reclaimed the aged file A; flush drained the inline rows into the live set;
    // the result reads back intact (no half-applied corruption).
    assert!(
        !a_path.exists(),
        "gc reclaimed aged file A under contention"
    );
    let cur = ice.current_snapshot(&t).await.expect("current");
    let rows: i64 = ice
        .files_with_stats(&t, cur.id)
        .await
        .expect("files readable")
        .iter()
        .map(|f| f.record_count)
        .sum();
    assert_eq!(rows, 6, "B(4) + flushed inline(2) are the live set");
    assert!(
        ice.inline_live_batch(&t, cur.id)
            .await
            .expect("inline_live_batch")
            .is_none(),
        "flush retired the inline rows"
    );
}

/// Dropped table reclaimed: land a file + inline rows, drop, age the whole history,
/// gc → data-file Parquet deleted, inline_<tid> dropped, table/column/data_file rows
/// gone; the object store no longer holds the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_a_dropped_table() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "gone".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(run, "wh", "gone"),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    // Some(threshold) (not None) so bump_inline_trigger actually runs and creates the
    // inline_trigger row this test now asserts on; the huge threshold keeps it far from
    // tripping so no flush job is enqueued (same pattern as inline_flush_trigger.rs).
    inline_append(
        &pool,
        &t,
        &columns(),
        &batch(3),
        lineage(run, "wh", "gone"),
        Some(1 << 40),
        None,
    )
    .await
    .expect("inline_append");
    let tid = live_tid(&pool, "wh", "gone").await;
    assert!(
        inline_table_exists(&pool, tid).await,
        "inline table exists before drop"
    );
    assert_eq!(
        inline_trigger_count(&pool, tid).await,
        1,
        "inline_append created a trigger row"
    );

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "gone".into());
    catalog.drop_table(&ident).await.expect("drop");
    age_all_snapshots(&pool).await; // drop snapshot D <= H → full reclaim

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert!(
        summary.data_file_rows >= 1 && summary.objects_deleted >= 1,
        "dropped data file reclaimed (got {summary:?})"
    );
    assert!(!a_path.exists(), "dropped table's Parquet deleted");
    assert!(
        !inline_table_exists(&pool, tid).await,
        "inline_<tid> dropped"
    );
    assert_eq!(
        mirror_row_counts(&pool, tid).await,
        (0, 0, 0),
        "table/column/data_file mirror rows removed"
    );
    assert_eq!(
        inline_trigger_count(&pool, tid).await,
        0,
        "inline_trigger orphan reclaimed"
    );
}

/// Within-window drop preserved: dropping then gc-ing BEFORE the drop snapshot ages
/// out deletes nothing (a time-travel read as-of before the drop still resolves); a
/// later gc after aging completes the reclaim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_preserves_a_within_window_dropped_table() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "recent".into(),
    };

    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "recent"),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    let tid = live_tid(&pool, "wh", "recent").await;

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "recent".into());
    catalog.drop_table(&ident).await.expect("drop"); // drop snapshot s2 (recent)
    age_snapshot(&pool, s1.0).await; // H = s1; drop snapshot s2 > H

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(
        summary,
        GcSummary::default(),
        "within-window drop reclaims nothing"
    );
    assert!(a_path.exists(), "Parquet retained while drop is in-window");
    assert_eq!(
        mirror_row_counts(&pool, tid).await.0,
        1,
        "dropped incarnation's table row retained while in-window"
    );

    // A later gc, after the drop snapshot ages out, completes the reclaim.
    age_all_snapshots(&pool).await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc2");
    assert!(!a_path.exists(), "Parquet reclaimed once aged out");
    assert_eq!(
        mirror_row_counts(&pool, tid).await,
        (0, 0, 0),
        "metadata reclaimed once aged out"
    );
}

/// Drop/recreate isolation: create (s,t), drop it, recreate (s,t) with a new table_id,
/// land into the live one, age the whole history, gc → the DROPPED incarnation's bytes
/// are reclaimed while the LIVE incarnation's current files + metadata are untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_isolates_dropped_from_recreated_incarnation() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "reused".into(),
    };

    // Incarnation 1: land, capture its file + tid, drop.
    let (schema1, batches1) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema1,
        batches1,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "reused"),
        None,
    )
    .await
    .expect("land1");
    let p1 = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    let tid1 = live_tid(&pool, "wh", "reused").await;
    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "reused".into());
    catalog.drop_table(&ident).await.expect("drop1");

    // Incarnation 2 (live): re-land under the same name → new table_id.
    let (schema2, batches2) = ipc_body(5);
    let s2 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema2,
        batches2,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "reused"),
        None,
    )
    .await
    .expect("land2");
    let p2 = local_path(&ice.files_with_stats(&t, s2).await.expect("files@s2")[0].path);
    let tid2 = live_tid(&pool, "wh", "reused").await;
    assert_ne!(tid1, tid2, "recreate allocates a fresh table_id");

    age_all_snapshots(&pool).await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");

    // Dropped incarnation reclaimed; live incarnation untouched.
    assert!(!p1.exists(), "dropped incarnation's Parquet reclaimed");
    assert_eq!(
        mirror_row_counts(&pool, tid1).await,
        (0, 0, 0),
        "dropped metadata gone"
    );
    assert!(p2.exists(), "live incarnation's current Parquet retained");
    assert_eq!(
        mirror_row_counts(&pool, tid2).await.0,
        1,
        "live table row retained"
    );
    let cur = ice.current_snapshot(&t).await.expect("current");
    let rows: i64 = ice
        .files_with_stats(&t, cur.id)
        .await
        .expect("files")
        .iter()
        .map(|f| f.record_count)
        .sum();
    assert_eq!(rows, 5, "live incarnation reads back intact");
}

/// Idempotent: a second gc on a fully-reclaimed dropped name is a clean no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_on_fully_reclaimed_dropped_name_is_a_noop() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "twice".into(),
    };

    let (schema, batches) = ipc_body(10);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "twice"),
        None,
    )
    .await
    .expect("land");
    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "twice".into());
    catalog.drop_table(&ident).await.expect("drop");
    age_all_snapshots(&pool).await;

    let first = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc1");
    assert!(
        first.data_file_rows >= 1,
        "first gc reclaims the dropped table"
    );
    let second = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc2");
    assert_eq!(second, GcSummary::default(), "second gc is a clean no-op");
}

/// A dropped table that carried a vector index reclaims cleanly: the vector_index rows
/// (FK children of iceberg_mirror.table) and their Puffin sidecar objects are removed, and
/// gc_table returns Ok rather than FK-aborting on the surviving vector_index -> table FK.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_a_dropped_table_with_vector_index() {
    let fx = PgFixture::shared();
    let (_cp, _db, wh, catalog, pool) = harness(fx).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "indexed".into(),
    };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "indexed"),
        None,
    )
    .await
    .expect("land");
    let tid = live_tid(&pool, "wh", "indexed").await;

    // A real Puffin sidecar file inside the warehouse, referenced by the binding row.
    let puffin = wh.path().join("indexed.idx.puffin");
    std::fs::write(&puffin, b"puffin-bytes").expect("write puffin");
    let puffin_url = format!("file://{}", puffin.display());
    let mut conn = pool.acquire().await.expect("conn");
    insert_vector_index(
        &mut conn,
        &VectorIndexRow {
            table_id: tid,
            column: "id".into(),
            index_name: "idx".into(),
            covered_snapshot: s1.0,
            metric: "l2".into(),
            index_kind: "flat".into(),
            dim: 4,
            row_count: 4,
            puffin_path: puffin_url,
        },
    )
    .await
    .expect("insert vector_index");
    drop(conn);
    assert!(puffin.exists(), "puffin sidecar on disk before gc");

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "indexed".into());
    catalog.drop_table(&ident).await.expect("drop");
    age_all_snapshots(&pool).await;

    // The crux: before the fix this Err'd on the vector_index -> table FK and aborted.
    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc must not FK-abort");

    let vi: i64 =
        sqlx::query_scalar("select count(*) from iceberg_mirror.vector_index where table_id = $1")
            .bind(tid)
            .fetch_one(&pool)
            .await
            .expect("vi count");
    assert_eq!(vi, 0, "vector_index rows reclaimed");
    assert!(
        !puffin.exists(),
        "puffin sidecar object deleted (commit-then-delete)"
    );
    assert!(
        summary.objects_deleted >= 1,
        "at least the puffin sidecar was deleted (got {summary:?})"
    );
    assert_eq!(
        mirror_row_counts(&pool, tid).await,
        (0, 0, 0),
        "table/column/data_file rows removed"
    );
}

/// GC records the highest `end_snapshot` it destroyed, in the same commit as the
/// deletes. Mirrors `gc_reclaims_aged_data_files_and_keeps_in_window`'s arrangement:
/// A lands at s1 and is end-capped at s2 by the first overwrite; B lands at s2 and is
/// end-capped at s3. Ageing ONLY s2 makes H = s2, so exactly A (end = s2) is reclaimed
/// -> the watermark is s2. B (end = s3 > H) is untouched, so the watermark does not
/// jump to s3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_records_the_reclaim_watermark() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    let (schema, batches) = ipc_body(10);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("land");

    let tid = live_tid(&pool, "wh", "t").await;
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        0,
        "a freshly landed table has reclaimed nothing"
    );

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s2");
    let _s3 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s3");

    age_snapshot(&pool, s2.0).await; // H = s2

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(summary.data_file_rows, 1, "exactly A reclaimed");
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2.0,
        "watermark == the end_snapshot of the reclaimed file A, not the tip"
    );
}

/// The watermark never regresses — even when a LATER run reclaims an OLDER straggler
/// than an earlier run already reclaimed (the `greatest(...)` branch of
/// `bump_reclaimed_through`), and even when a run reclaims nothing at all (the
/// early-return no-op on an empty victim set). The read guard's soundness argument
/// leans on monotonicity holding in BOTH cases.
///
/// Constructing the "older straggler" case needs the per-bucket MV floor
/// (`crate::mv_floor`): within one bucket, offset order == chronological order ==
/// `end_snapshot` order, so a single bucket alone can never surface an older
/// straggler after a newer reclaim — the floor guard only ever releases higher
/// offsets over time. Two INDEPENDENT buckets decouple that: bucket 0 races ahead
/// (both its generations reclaimed run 1, pinning the watermark at the newer
/// generation's `end_snapshot`) while bucket 1 lags completely behind (both its
/// generations held); when bucket 1's floor then advances just enough to release
/// ONLY its first (oldest, lowest-`end_snapshot`) generation, run 2 reclaims a row
/// strictly older than what run 1 already recorded — the one shape a bare
/// `set reclaimed_through = $2` would get wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_watermark_is_monotone() {
    let fx = PgFixture::shared();
    let (cp, _db, _wh, catalog, pool) = harness(fx).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let out = TableRef {
        schema: "wh".into(),
        name: "mv_out".into(),
    };

    // Generation 1 (bucket 0 offset 0, bucket 1 offset 0): land 2 rows into a
    // declared 2-bucket stream table (round-robin row_index % 2), inline (small
    // enough to stay under the byte limit), then flush — end-caps both rows at
    // the flush's snapshot `s1` (the OLDER generation).
    let (schema, batches) = ipc_body(2);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 1 << 20,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        Some(2),
    )
    .await
    .expect("land gen 1");
    let tid = live_tid(&pool, "wh", "t").await;
    let s1 = flush_table(&catalog, &pool, &t, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush gen 1")
        .expect("gen 1 had live rows to flush")
        .0;

    // Generation 2 (bucket 0 offset 1, bucket 1 offset 1): land 2 more rows,
    // flush again — end-caps both at a NEWER snapshot `s2 > s1`.
    let (schema2, batches2) = ipc_body(2);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema2,
        batches2,
        InlineLimits {
            inline_byte_limit: 1 << 20,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        Some(2),
    )
    .await
    .expect("land gen 2");
    let s2 = flush_table(&catalog, &pool, &t, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush gen 2")
        .expect("gen 2 had live rows to flush")
        .0;
    assert!(s2 > s1, "generation 2 is strictly newer than generation 1");

    // Register an MV over `t` (never run — the watermark is driven by hand) and set
    // ITS floor so bucket 0 has consumed BOTH generations (offsets 0 and 1: floor 2)
    // while bucket 1 has consumed NEITHER (no watermark row -> floors at 0).
    register_mv(&cp, "mv_a", &t, &out).await;
    let mv = mv_key(&out);
    advance(&cp, &mv, tid, 0, 0, 2).await;

    age_all_snapshots(&pool).await;

    // Run 1: bucket 0's both rows (offsets 0 and 1, end_snapshot s1 and s2) are
    // strictly below its floor (2) and reclaimed; bucket 1's both rows (floor 0)
    // are held. The watermark becomes s2 — the NEWER generation, because bucket 0's
    // own second row supplied it, not because every older row was swept first.
    let first = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc run 1");
    assert_eq!(first.inline_rows, 2, "bucket 0's two generations reclaimed");
    assert_eq!(first.held_by_mv_floor, 2, "bucket 1's two generations held");
    let after_first = reclaimed_through(&pool, tid).await;
    assert_eq!(
        after_first, s2,
        "run 1 records the newer generation's end_snapshot"
    );

    // Advance bucket 1's floor just enough to release ONLY its oldest row (offset 0,
    // end_snapshot s1) — its offset-1 row (end_snapshot s2) stays held.
    advance(&cp, &mv, tid, 1, 0, 1).await;

    // Run 2 reclaims exactly that one row: end_snapshot s1, STRICTLY OLDER than the
    // watermark run 1 already recorded (s2). The crux assertion: the watermark must
    // not regress to s1.
    let second = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc run 2");
    assert_eq!(
        second.inline_rows, 1,
        "exactly bucket 1's oldest (offset 0) row is reclaimed this run"
    );
    assert_eq!(
        second.held_by_mv_floor, 1,
        "bucket 1's offset-1 row (end_snapshot s2) is still held"
    );
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2,
        "run 2 reclaimed an OLDER straggler (end_snapshot s1 < s2) — the watermark \
         must stay at s2, not regress to s1. A bare `set reclaimed_through = $2` \
         (instead of `greatest(...)`) would fail this assertion."
    );

    // Run 3: nothing left to reclaim (bucket 1's last row is still held) — the
    // early-return no-op path must also not disturb the watermark.
    let third = gc_table(&catalog, &pool, &t, SEVEN_DAYS)
        .await
        .expect("gc run 3");
    assert_eq!(
        third.inline_rows, 0,
        "nothing left below either bucket's floor"
    );
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2,
        "a no-op run must not regress (or advance) the watermark"
    );
}

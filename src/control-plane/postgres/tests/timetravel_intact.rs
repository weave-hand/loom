//! `Catalog::snapshot_intact` against real end-caps, real GC, and the real truncate path
//! (`iss-timetravel-quiet-table-overconservative`).
//!
//! The guard this backs replaced a proxy (`at < H` => 410) that was over-conservative in
//! one direction. The tests below pin BOTH directions, because the obvious alternative fix
//! — a "quiet table" exemption ("nothing was written since `at`, so the read is fine") — is
//! not merely imprecise, it is UNSOUND, and `truncate_is_not_quiet` is the test that says
//! so. A governed delete-all end-caps every row and writes NO new row of any kind, so a
//! query over the SURVIVING mirror rows reports "quiet since S" and the proxy would serve
//! zero rows where the data used to be: a loud 410 traded for silent data loss. Do not
//! reintroduce it.

use control_plane_core::{Catalog, ControlPlaneError, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use gc_test_support::{
    SEVEN_DAYS, age_all_snapshots, age_snapshot, batch, columns, harness, inline_table_exists,
    ipc_body, lineage, live_tid, reclaimed_through,
};
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};

/// Nothing was ever end-capped: an append-only table below the horizon is INTACT.
/// This is the acceptance case — the read the old proxy refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_only_below_horizon_is_intact() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // s1: land 10 rows.
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
    .expect("land s1");

    // s2: append 10 more rows via a SECOND `land` — nothing end-capped, s1's file stays live.
    let (schema2, batches2) = ipc_body(10);
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
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("land s2 (append)");

    // Age the whole history so H = s2 (the tip) — the horizon a real `ensure_within_retention`
    // would compute under a ZERO/short retention.
    age_all_snapshots(&pool).await;

    assert!(
        ice.snapshot_intact(&t, s1, s2)
            .await
            .expect("snapshot_intact"),
        "append-only: s1 is below the horizon but nothing visible at s1 is end-capped, \
         so it is intact — this is the read the old `at < H` proxy wrongly refused"
    );

    // The read really is complete: s1's file is still there.
    let files = ice.files_with_stats(&t, s1).await.expect("files@s1");
    assert_eq!(files.len(), 1, "s1's file is still live");
    assert_eq!(files[0].record_count, 10);
}

/// An end-capped-and-reclaimed read is NOT intact — clause 1 (the watermark).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_below_horizon_is_not_intact() {
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
    .expect("land s1");

    // Overwrite at s2 — end-caps s1's file at s2.
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

    age_all_snapshots(&pool).await; // H = s2
    let tid = live_tid(&pool, "wh", "t").await;

    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2.0,
        "gc reclaimed s1's end-capped file and recorded the watermark at s2"
    );

    assert!(
        !ice.snapshot_intact(&t, s1, s2)
            .await
            .expect("snapshot_intact"),
        "s1 is not intact: its file was end-capped at s2 <= horizon and has been reclaimed"
    );
}

/// Eligible but NOT yet reclaimed is ALSO not intact — clause 2. The verdict must not
/// depend on GC having run: the row can be destroyed mid-read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligible_but_ungced_is_not_intact() {
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
    .expect("land s1");

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

    age_all_snapshots(&pool).await; // H = s2
    let tid = live_tid(&pool, "wh", "t").await;

    // Deliberately DO NOT run gc_table.
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        0,
        "clause 1 passes: nothing has been reclaimed (gc never ran)"
    );

    assert!(
        !ice.snapshot_intact(&t, s1, s2)
            .await
            .expect("snapshot_intact"),
        "clause 2: s1's file is end-capped at s2 <= horizon and so is ELIGIBLE for reclaim \
         at any moment, even though gc hasn't actually run yet"
    );
}

/// THE TRAP. A governed delete-all (`overwrite_parquet_snapshot` with a zero-row batch ->
/// `overwrite_truncate`) end-caps every live row and writes NOTHING. The surviving mirror
/// state is a live `table` row, its `column` rows, and zero data files — so any
/// "was this table written since S?" proxy reports QUIET and would serve an empty result
/// set for a snapshot that had rows. It must be 410 (not intact), via the watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_is_not_quiet() {
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
    .expect("land s1");

    // A governed delete-all: an all-zero-row batch routes `overwrite_with_cap` to
    // `overwrite_truncate`, which end-caps every live file and writes no replacement.
    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(0)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("truncate ow s2");

    let files_at_s2 = ice.files_with_stats(&t, s2).await.expect("files@s2");
    assert!(
        files_at_s2.is_empty(),
        "truncate writes NO replacement file — the surviving mirror looks quiet"
    );

    age_all_snapshots(&pool).await; // H = s2
    let tid = live_tid(&pool, "wh", "t").await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");

    assert!(
        !ice.snapshot_intact(&t, s1, s2)
            .await
            .expect("snapshot_intact"),
        "THE TRAP: s1 must NOT be intact even though the table looks quiet since s1 — a \
         quiet-table proxy would wrongly serve zero rows here"
    );
    // ...and it is false for the RIGHT reason: clause 1, the watermark. After gc there is
    // no surviving end-capped row left for clause 2 to find at all.
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2.0,
        "false via clause 1 (the watermark), not clause 2 (nothing survives for it to see)"
    );
}

/// A dropped-but-unreclaimed incarnation is still time-travellable, and its verdict is
/// per-INCARNATION. Then, once it is FULLY reclaimed, the `table` row goes with it and the
/// read degrades to 404 (`NotFound`) — not 410, not 200. This is the one case where the
/// evidence self-destructs safely.
///
/// Note this is a genuine BEHAVIOR CHANGE, not just new coverage: a read inside a dropped
/// incarnation whose drop snapshot `D` is ABOVE the horizon used to 410 (it is below `H`)
/// and now correctly serves — its files are end-capped at `D > H`, so nothing may be
/// reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_incarnation_serves_then_404s_once_fully_reclaimed() {
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
    .expect("land s1");

    // Capture the tid BEFORE the drop (`live_tid` only finds the still-live incarnation).
    let tid = live_tid(&pool, "wh", "t").await;
    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "t".into());
    catalog.drop_table(&ident).await.expect("drop");
    let d: i64 =
        sqlx::query_scalar("select end_snapshot from iceberg_mirror.table where table_id = $1")
            .bind(tid)
            .fetch_one(&pool)
            .await
            .expect("dropped incarnation's end_snapshot (drop snapshot D)");
    let d_row = control_plane_core::SnapshotId(d);

    // Leg 1: age ONLY s1 -> H = s1, drop snapshot D is ABOVE the horizon: nothing may be
    // reclaimed (D > H), so s1 is still intact.
    age_snapshot(&pool, s1.0).await;
    assert!(
        ice.snapshot_intact(&t, s1, s1)
            .await
            .expect("snapshot_intact leg 1"),
        "dropped, but D > horizon: nothing is eligible, s1 is intact"
    );

    // Leg 2: age the whole history -> H = D. The drop end-capped s1's files at D <= H:
    // clause 2 fires.
    age_all_snapshots(&pool).await;
    assert!(
        !ice.snapshot_intact(&t, s1, d_row)
            .await
            .expect("snapshot_intact leg 2"),
        "dropped, D <= horizon: s1's files are end-capped at D, eligible for reclaim"
    );

    // Leg 3: FULLY reclaim the dropped incarnation (D <= H already holds from leg 2's
    // aging) -> the `table`/`column`/`data_file` rows are deleted outright, so
    // `resolve_table` can no longer find ANY incarnation live at s1.
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert!(
        matches!(
            ice.snapshot_intact(&t, s1, d_row).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "fully reclaimed: no incarnation resolves at s1 any more -- the evidence \
         self-destructs safely into a 404, not a false 200 or a false 410"
    );
}

/// The inline-tier eligibility clause in `snapshot_intact` is the method's ONLY
/// runtime (non-compile-time-checked) SQL — a typo in the spliced `inline_<tid>`
/// identifier would be a runtime error invisible until production. Exercise it in
/// BOTH directions by landing rows INLINE (a non-zero `inline_byte_limit`, so the
/// bytes stay under it and `land` routes to the inline tier, not Parquet) and then
/// flushing — the flush end-caps those inline rows at the flush snapshot `Sf`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tier_eligibility_is_exercised() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef {
        schema: "wh".into(),
        name: "inl".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // Land INLINE: a generous inline_byte_limit keeps the (tiny) batch under it, so
    // `land` routes to `inline_append_decl` rather than `land_parquet`.
    let (schema, batches) = ipc_body(3);
    let s0 = land(
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
        lineage(run, "wh", "inl"),
        None,
    )
    .await
    .expect("land inline s0");

    let tid = live_tid(&pool, "wh", "inl").await;
    assert!(
        inline_table_exists(&pool, tid).await,
        "the rows really did go to the inline tier, not Parquet -- this is what makes \
         the inline-eligibility clause's runtime SQL execute at all"
    );

    // Flush end-caps the inline rows at the flush's own snapshot Sf.
    let sf = flush_table(&catalog, &pool, &t, run)
        .await
        .expect("flush")
        .expect("flushed something");

    // Below Sf: the inline rows visible at s0 are end-capped at Sf <= horizon -> eligible.
    assert!(
        !ice.snapshot_intact(&t, s0, sf)
            .await
            .expect("snapshot_intact at horizon Sf"),
        "inline rows end-capped at Sf are eligible for reclaim when the horizon reaches Sf"
    );

    // Strictly below Sf: end-capped ABOVE the horizon, so nothing may be reclaimed.
    let below_sf = control_plane_core::SnapshotId(sf.0 - 1);
    assert!(
        ice.snapshot_intact(&t, s0, below_sf)
            .await
            .expect("snapshot_intact below Sf"),
        "inline rows end-capped at Sf > horizon: nothing eligible, s0 is intact"
    );
}

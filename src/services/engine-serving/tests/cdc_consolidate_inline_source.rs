//! `consolidate_table`'s CDC arm over a base that has ONLY ever been inline-appended.
//!
//! A CDC declare pre-creates only the CHANGELOG Iceberg table (`land_cdc` ->
//! `ensure_iceberg_table(changelog_table_ref(table))`); the BASE gets its
//! `iceberg_tables` row at its first Parquet write, i.e. at flush. But the
//! consolidate job is enqueued by the inline delta-row trigger
//! (`bump_consolidate_trigger`, from `write_inline_delta`), which needs no flush
//! at all — so `consolidate_locked` can be handed a base with zero Parquet files
//! and no catalog row. It read the file tier unconditionally
//! (`read_files_as_batches` -> `catalog.load_table`), which errors even for an
//! EMPTY path list: `iss-consolidate-inline-only-base`, the same defect
//! `iss-mv-delta-inline-source-unflushed` (#436) fixed in `mv_delta_locked`.
//!
//! loom_fixture_test (Postgres + a local `tempfile` warehouse).
//! Spec: docs/superpowers/specs/2026-07-14-consolidate-inline-only-base-design.md

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, MergeEngine, RunId, SnapshotId, StreamTables, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::{
    arm_consolidate_trigger, bump_consolidate_trigger, ensure_table, next_snapshot,
};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// The `(id long, val long)` logical (framing-free) schema every case uses.
fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn id_spec() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn arrow_cols() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

/// A one-row `(id, val)` batch.
fn row(id: i64, val: i64) -> RecordBatch {
    RecordBatch::try_new(
        arrow_cols(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("row batch")
}

/// A one-cell `(id)` batch — the CAS-witness probe for `current_inline_version`.
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

/// A ZERO-row `(id, val)` batch — `inline_append_decl` mints the snapshot and
/// projects the mirror columns BEFORE its per-row insert loop, so appending this
/// leaves a live, schema-bearing table with NO file tier and NO live inline row:
/// the one shape that reaches the neither-tier arm.
fn empty_batch() -> RecordBatch {
    RecordBatch::try_new(
        arrow_cols(),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ],
    )
    .expect("empty batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "cdc-consolidate-inline-source-test" }),
    }
}

/// Create `table` in the mirror and declare it CDC (2 buckets, keyed on `id`,
/// `LastRow`) BEFORE any write — the `stream_cdc_consolidate_trigger.rs` idiom.
/// Declaring CDC pre-creates ONLY the changelog Iceberg table; the base gets no
/// `iceberg_tables` row until it is flushed, which is the whole point here.
/// Returns the live mirror table id.
async fn declare_cdc_table(cp: &PgControlPlane, pool: &PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    tid
}

/// A CDC `+U` update of `id` to `val` through the inline delta path — the ONLY
/// production caller of `bump_consolidate_trigger`. `stream_buckets: None` on the
/// seeding append means "don't change the declaration" (the table is already CDC).
async fn update(pool: &PgPool, table: &TableRef, id: i64, from: i64, to: i64) {
    let witness = current_inline_version(pool, table, &id_spec(), "id", &id_batch(id))
        .await
        .expect("current_inline_version");
    write_inline_delta(
        pool,
        table,
        &cols(),
        "id",
        false,
        &row(id, to),
        Some((&cols(), &row(id, from))),
        lin(),
        witness,
        None,
        &[],
    )
    .await
    .expect("cdc update delta");
}

/// Flatten batches into sorted `(id, val)` pairs, by column NAME (the folded base
/// carries framing columns after the user columns, so positional access is wrong).
fn rows_sorted(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let vals = b
            .column_by_name("val")
            .expect("val column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val is Int64");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vals.value(i)));
        }
    }
    out.sort_unstable();
    out
}

/// A fresh fixture db + pool + a local-filesystem `SqlCatalog` over a temp warehouse.
async fn setup(fx: &'static PgFixture) -> (PgControlPlane, PgPool, SqlCatalog, tempfile::TempDir) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (cp, pool, catalog, wh)
}

/// THE DEFECT: a CDC base that has only ever been inline-appended — no Parquet
/// file, no `iceberg_tables` row — must consolidate. Pre-fix this dies in
/// `read_files_as_batches`'s unconditional `catalog.load_table` ("No such table:
/// cdc.orders"), even though the file list is empty. Post-fix the fold runs over
/// the inline tier alone and the CONSUMING overwrite creates the base's Iceberg
/// table on the spot (`append_parquet_snapshot` -> `ensure_iceberg_table`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_cdc_base_consolidates_without_a_flush() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "orders");

    declare_cdc_table(&cp, &pool, &table).await;
    // Seed +I id=1 val=100, then two CDC updates — inline only, no flush anywhere.
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");
    update(&pool, &table, 1, 100, 200).await;
    update(&pool, &table, 1, 200, 300).await;

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("an inline-only CDC base must consolidate without a flush");
    assert!(
        snap > 0,
        "the fold committed a real new snapshot, got {snap}"
    );

    // The fold WROTE the base: its Iceberg table now exists and holds one Parquet
    // file with the merged row (greatest loom_offset per identity wins).
    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    assert!(
        !paths.is_empty(),
        "the fold materialized the inline-only base as Parquet"
    );
    let (_schema, batches) = read_files_as_batches(&catalog, &table, &paths)
        .await
        .expect("the base's Iceberg table was created by the fold");
    assert_eq!(
        rows_sorted(&batches),
        vec![(1, 300)],
        "one row per identity, the greatest-offset image wins"
    );

    // The folded inline rows were consumed in the same commit.
    let live = ice
        .inline_live_batch_full(&table, SnapshotId(snap))
        .await
        .expect("inline_live_batch_full");
    assert!(
        live.is_none(),
        "every folded inline row is end-capped, got {live:?}"
    );
}

/// A CDC base with NEITHER tier — declared and snapshotted, but no Parquet file
/// and no live inline row — is a clean no-op. The regression that matters is the
/// SECOND half: the no-op must CLEAR the consolidate trigger. `arm_consolidate_trigger`
/// sets `enqueued = true` and only a successful consolidate clears it, and the
/// enqueue condition is `delta_count >= effective && !enqueued` — so a consolidate
/// that fails and abandons latches the trigger FOREVER and the table can never
/// enqueue another `stream_consolidate`, even after a flush would have made it work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neither_tier_is_a_noop_that_unlatches_the_trigger() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "empty");

    let tid = declare_cdc_table(&cp, &pool, &table).await;
    // Zero-row append: mints the snapshot and projects the mirror columns, but
    // leaves NO file tier and NO live inline row.
    inline_append(&pool, &table, &cols(), &empty_batch(), lin(), None, None)
        .await
        .expect("declare-only zero-row append");

    // Put the table in exactly the state a real enqueue leaves it in: deltas
    // accrued past the threshold, job armed.
    let mut conn = pool.acquire().await.expect("acquire");
    let armed = bump_consolidate_trigger(&mut conn, tid, 128, 128)
        .await
        .expect("bump");
    assert!(
        armed.delta_count >= armed.effective,
        "sanity: the trigger is over threshold"
    );
    arm_consolidate_trigger(&mut conn, tid).await.expect("arm");
    drop(conn);

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("a base with neither tier is a no-op, not an error");
    assert_eq!(snap, 0, "nothing to fold: no new snapshot");

    // The wedge is gone: the trigger is disarmed AND its counter reset, so the
    // next accrual can enqueue again.
    let mut conn = pool.acquire().await.expect("acquire");
    let after = bump_consolidate_trigger(&mut conn, tid, 1, 128)
        .await
        .expect("bump after the no-op");
    assert!(
        !after.enqueued,
        "the no-op disarmed the trigger — a later consolidate can be enqueued again"
    );
    assert_eq!(
        after.delta_count, 1,
        "the no-op reset the delta counter; only the fresh bump is counted"
    );
}

/// The preserved invariant: the CDC arm does NOT early-return when the inline tier
/// is empty. A post-flush base is files-only, and folding it (re-writing the
/// coalesced survivors) is legitimate work — Task 1's guard must not have turned
/// that into a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_only_base_still_folds() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "flushed");

    declare_cdc_table(&cp, &pool, &table).await;
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");
    update(&pool, &table, 1, 100, 200).await;

    // Flush moves the whole inline tier (the +I, the -U/+U pair) into Parquet, so
    // the base is now files-only with nothing live inline.
    control_plane_postgres::iceberg_flush::flush_table(
        &catalog,
        &pool,
        &table,
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("flush")
    .expect("the inline rows were flushed to Parquet");

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("a files-only CDC base still folds");
    assert!(
        snap > 0,
        "the files-only fold committed a snapshot, got {snap}"
    );

    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let (_schema, batches) = read_files_as_batches(&catalog, &table, &paths)
        .await
        .expect("read the folded base");
    assert_eq!(
        rows_sorted(&batches),
        vec![(1, 200)],
        "the flushed change subset folds to one row per identity"
    );
}

/// The spec's second preserved invariant: a fold whose every identity's winner is
/// a `-D` yields ZERO rows, and the overwrite short-circuits to `overwrite_truncate`
/// — which is mirror-only and therefore safe with no `iceberg_tables` row. This is
/// the inline-only shape most likely to still reach the Iceberg catalog, so pin it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_all_tombstoned_fold_truncates() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "all_deleted");

    declare_cdc_table(&cp, &pool, &table).await;
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");

    // DELETE id=1 — a `-D` tombstone, the greatest-offset image for that identity,
    // so the fold drops it and yields an EMPTY row set. No flush anywhere. A CDC
    // delete's before-image carries the FULL prior row (its own columns, not just
    // the id spec) — see `stream_cdc_emission.rs`'s `at_delete` for the same shape.
    let witness = current_inline_version(&pool, &table, &id_spec(), "id", &id_batch(1))
        .await
        .expect("current_inline_version");
    write_inline_delta(
        &pool,
        &table,
        &id_spec(),
        "id",
        true,
        &id_batch(1),
        Some((&cols(), &row(1, 100))),
        lin(),
        witness,
        None,
        &[],
    )
    .await
    .expect("cdc delete delta");

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("an all-tombstoned inline-only fold truncates, it does not error");
    assert!(snap > 0, "the truncate committed a snapshot, got {snap}");

    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    assert!(
        files.is_empty(),
        "every identity was tombstoned: the folded base holds no data file"
    );
    let live = ice
        .inline_live_batch_full(&table, SnapshotId(snap))
        .await
        .expect("inline_live_batch_full");
    assert!(
        live.is_none(),
        "the truncate end-capped the folded inline rows, got {live:?}"
    );
}
